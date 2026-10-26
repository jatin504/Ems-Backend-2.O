# repo.py
import json
import logging
from typing import Any, Dict, Optional, List
from model import get_async_connection
from model import StrategyConfig, EMS_Status, CycleConfig, SetpointWrite, ExecutionLog
from LogColor import LogColor
from model.models import EnergyStrategy, NotificationEvent

log = logging.getLogger(__name__)

DB_POOL = None

async def get_async_pool():
    if DB_POOL is None:
        DB_POOL = await get_async_connection()
    
    return DB_POOL


# STRATEGY helpers (create/find)
async def get_strategy_by_name(conn, name: str):
    return await conn.fetchrow("SELECT * FROM strategies WHERE name=$1", name)

async def create_strategy(conn, name: str, description: str = None, enabled: bool = True):
    return await conn.fetchrow(
        "INSERT INTO strategies (name, description, enabled) VALUES ($1,$2,$3) RETURNING *",
        name, description, enabled
    )

# CONFIG helpers
async def upsert_strategy_config(conn, strategy_id: int, key: str, schema_type: str, value) -> None:
    """
    value is native python: bool/number/text/dict
    """
    v_bool = None
    v_num = None
    v_text = None
    if schema_type == "boolean":
        v_bool = bool(value)
    elif schema_type == "number":
        v_num = float(value)
    elif schema_type == "text":
        v_text = str(value)
    elif schema_type == "json":
        v_text = json.dumps(value)
    else:
        raise ValueError("invalid schema_type")

    await conn.execute("""
        INSERT INTO strategy_configs (strategy_id, key, schema_type, value_bool, value_num, value_text)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (strategy_id, key)
        DO UPDATE SET schema_type = EXCLUDED.schema_type,
                      value_bool = EXCLUDED.value_bool,
                      value_num = EXCLUDED.value_num,
                      value_text = EXCLUDED.value_text,
                      updated_at = now()
    """, strategy_id, key, schema_type, v_bool, v_num, v_text)

async def get_strategy_config_map(conn, strategy_id: int) -> Dict[str, Any]:
    rows = await conn.fetch("SELECT key, schema_type, value_bool, value_num, value_text FROM strategy_configs WHERE strategy_id=$1", strategy_id)
    result = {}
    for r in rows:
        if r["schema_type"] == "boolean":
            result[r["key"]] = r["value_bool"]
        elif r["schema_type"] == "number":
            result[r["key"]] = r["value_num"]
        elif r["schema_type"] == "text":
            result[r["key"]] = r["value_text"]
        elif r["schema_type"] == "json":
            try:
                result[r["key"]] = json.loads(r["value_text"]) if r["value_text"] else None
            except Exception:
                result[r["key"]] = r["value_text"]
    return result

# Flags
async def set_ems_status(flag_key: str, flag_value: bool):
    async with await get_async_connection() as conn:
        await conn.execute("""
            INSERT INTO ems_status (flag_key, flag_value, updated_at)
            VALUES ($1,$2,now())
            ON CONFLICT (flag_key)
            DO UPDATE SET flag_value = EXCLUDED.flag_value, updated_at = now()
        """, flag_key, flag_value)

async def get_ems_status_map(conn, flag_key: str) -> Dict[str, bool]:
    rows = await conn.fetch("SELECT flag_key, flag_value FROM ems_status WHERE flag_key=$1", flag_key)
    return {r["flag_key"]: r["flag_value"] for r in rows}

# Cycle config
async def upsert_cycle_config(conn, cfg: CycleConfig):
    await conn.execute("""
        INSERT INTO cycle_configs (strategy_id, max_cycles_per_day, cycle_min_soc, cycle_max_soc, min_rest_minutes, updated_at)
        VALUES ($1,$2,$3,$4,$5, now())
        ON CONFLICT (strategy_id) DO UPDATE
        SET max_cycles_per_day = EXCLUDED.max_cycles_per_day,
            cycle_min_soc = EXCLUDED.cycle_min_soc,
            cycle_max_soc = EXCLUDED.cycle_max_soc,
            min_rest_minutes = EXCLUDED.min_rest_minutes,
            updated_at = now()
    """, cfg.strategy_id, cfg.max_cycles_per_day, cfg.cycle_min_soc, cfg.cycle_max_soc, cfg.min_rest_minutes)

async def get_cycle_config(conn, strategy_id: int) -> Optional[dict]:
    row = await conn.fetchrow("SELECT * FROM cycle_configs WHERE strategy_id=$1", strategy_id)
    return dict(row) if row else None

# Setpoint writes & execution logs
async def write_setpoint_record(conn, rec: SetpointWrite):
    try:

        await conn.execute("""
            INSERT INTO setpoint_writes (strategy_id, timestamp, register_mode, setpoint_value, unit, device_id, success, response_text)
            VALUES ($1, coalesce($2, now()), $3, $4, $5, $6, $7, $8)
        """, rec.strategy_id, rec.timestamp, rec.register_mode, rec.setpoint_value, rec.unit, rec.device_id, rec.success, rec.response_text)
    except Exception as e:
        log.exception(f"{LogColor.RED} Error While Writing Setpoints to DB: {e} {LogColor.RESET}")

async def create_execution_log(conn, log_obj: ExecutionLog) -> int:
    try:

        row = await conn.fetchrow("""
            INSERT INTO strategy_execution_logs (strategy_id, started_at, input_snapshot, decision_snapshot)
            VALUES ($1, coalesce($2, now()), $3::jsonb, $4::jsonb)
            RETURNING id
        """, log_obj.strategy_id, log_obj.started_at, json.dumps(log_obj.input_snapshot or {}), json.dumps(log_obj.decision_snapshot or {}))
        return row["id"]
    except Exception as e:
        log.exception(f"{LogColor.RED} Error While creating Strategy Exec Log: {e} {LogColor.RESET}")

async def finish_execution_log(conn, log_id: int, finished_at, decision_snapshot=None, error_text: Optional[str] = None):
    try:

        await conn.execute("""
            UPDATE strategy_execution_logs
            SET finished_at = coalesce($2, now()),
                decision_snapshot = coalesce($3::jsonb, decision_snapshot),
                error_text = $4
            WHERE id = $1
        """, log_id, finished_at, json.dumps(decision_snapshot or {}), error_text)
    except Exception as e:
        log.info(f"{LogColor.RED} Error While Finishing Exec log: {e} {LogColor.RESET}")

async def write_energy_strategy_record(conn, rec: EnergyStrategy):
    try:
        updated = await conn.fetchval("""
            UPDATE energy_strategy
            SET
                ts = COALESCE($1, NOW()),
                type = $3,
                tariff = $4,
                power = $5,
                duration = $6,
                min_soc = $7,
                max_soc = $8,
                priority = $9,
                remark = $10,
                schedule_from = $11
            WHERE start_ts = $2
              AND end_ts = $12
            RETURNING 1
        """,
            rec.ts,
            rec.start_ts,
            rec.type,
            rec.tariff,
            rec.power,
            rec.duration,
            rec.min_soc,
            rec.max_soc,
            rec.priority,
            rec.remark,
            rec.schedule_from,
            rec.end_ts
        )

        if updated is None:
            await conn.execute("""
                INSERT INTO energy_strategy (
                    ts,
                    start_ts,
                    end_ts,
                    type,
                    tariff,
                    power,
                    duration,
                    min_soc,
                    max_soc,
                    priority,
                    remark,
                    schedule_from
                )
                VALUES (
                    COALESCE($1, NOW()),
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8,
                    $9,
                    $10,
                    $11,
                    $12
                )
            """,
                rec.ts,
                rec.start_ts,
                rec.end_ts,
                rec.type,
                rec.tariff,
                rec.power,
                rec.duration,
                rec.min_soc,
                rec.max_soc,
                rec.priority,
                rec.remark,
                rec.schedule_from
            )

    except Exception as e:
        log.exception(
            f"{LogColor.RED} "
            f"Error While Writing Energy Strategy to DB: {e} "
            f"{LogColor.RESET}"
        )


async def write_notification_event(
    conn,
    notification: NotificationEvent
):
    try:
        await conn.execute(
            """
            INSERT INTO notification_event (
                event_time,
                source,
                event_type,
                severity,
                title,
                message,
                entity_type,
                entity_id,
                status,
                acknowledged,
                acknowledged_by,
                acknowledged_time,
                cleared_time,
                metadata,
                created_at
            )
            VALUES (
                coalesce($1, now()),
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                coalesce($9, 'active'),
                coalesce($10, false),
                $11,
                $12,
                $13,
                $14::jsonb,
                coalesce($15, now())
            )
            ON CONFLICT DO NOTHING
            """,
            notification.event_time,
            notification.source,
            notification.event_type,
            notification.severity,
            notification.title,
            notification.message,
            notification.entity_type,
            notification.entity_id,
            notification.status,
            notification.acknowledged,
            notification.acknowledged_by,
            notification.acknowledged_time,
            notification.cleared_time,
            json.dumps(notification.metadata or {}),
            notification.created_at,
        )

    except Exception as e:
        log.info(
            f"{LogColor.RED} "
            f"Error While Writing Notification Event: {e} "
            f"{LogColor.RESET}"
        )
