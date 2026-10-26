import asyncio
import json
from pathlib import Path
from typing import Any, Dict

from quart import Blueprint, jsonify, request

from controller_layer.strategies import get_today_cycles

# Blueprint with url prefix `/cycle-management`
cycle_management_bp = Blueprint(
    "cycle_management_bp", __name__, url_prefix="/cycle-management"
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CYCLE_CONFIG_PATH = (
    PROJECT_ROOT
    / "controller_layer"
    / "strategies"
    / "Cycle_Management"
    / "cycle_config.json"
)

# BESS params file path (source for min_soc & backup_soc)
BESS_PARAMS_PATH = (
    PROJECT_ROOT
    / "controller_layer"
    / "strategies"
    / "bess_params_setting.json"
)


def _default_cycle_config() -> Dict[str, Any]:
    """
    Fallback config used if cycle_config.json is missing or broken.
    Adjust defaults to whatever you want.
    """
    return {
        "cycle_used_today": 3,
        "max_cycles_per_day": 4,
        "cycle_counting_method_option": {
            "0": "throughput",
            "1": "full_cycle",
            "2": "dod_weighted",
        },
        "cycle_counting_method": "0",
        "soc_safety_management": {
            "min_safe_soc": 30,
            "normal_soc_after_safety_charge": 80,
            "standby_soc_hold_band": 50,
        },
        "lockout_behaviour": {
            "allow_safety_charging": True,
            "allow_reserve_charging": True,
            "block_economic_charging": False,
            "block_discharging": False,
        },
        "price_logic_overrides": {
            "disable_aggrassive_charging": False,
            "disable_peak_shaving": True,
            "disable_arbitrage": False,
            "disable_low_price_charging": True,
        },
        "notifications": {
            "notify_at_80_percent_usage": True,
            "notify_lockout_activated": True,
            "mobile_push_notification": False,
        },
    }


def _load_cycle_config_sync() -> Dict[str, Any]:
    """
    Synchronous helper: read JSON from disk.
    Called via asyncio.to_thread in async endpoints.
    """
    if not CYCLE_CONFIG_PATH.exists():
        # Agar file missing hai, toh yahi CENTRAL file create hogi default se.
        cfg = _default_cycle_config()
        CYCLE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CYCLE_CONFIG_PATH.write_text(json.dumps(cfg, indent=4))
        return cfg

    try:
        with CYCLE_CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # If JSON is corrupted, fall back to defaults
        cfg = _default_cycle_config()
        CYCLE_CONFIG_PATH.write_text(json.dumps(cfg, indent=4))
        return cfg


def _save_cycle_config_sync(cfg: Dict[str, Any]) -> None:
    """
    Synchronous helper: write JSON to disk.
    """
    CYCLE_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CYCLE_CONFIG_PATH.write_text(json.dumps(cfg, indent=4))


# NEW: simple helpers for bess_params_setting.json
def _load_bess_params_sync() -> Dict[str, Any]:
    if not BESS_PARAMS_PATH.exists():
        return {}
    try:
        with BESS_PARAMS_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_bess_params_sync(bess_cfg: Dict[str, Any]) -> None:
    BESS_PARAMS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BESS_PARAMS_PATH.write_text(json.dumps(bess_cfg, indent=4))


# routes


@cycle_management_bp.route("/cycle-config", methods=["GET"])
async def get_cycle_config():
    """
    Returns the current cycle configuration from cycle_config.json
    (controller_layer/strategies/Cycle_Management/cycle_config.json)

    Minimum Safe SOC & Standby SOC Hold Band ko
    bess_params_setting.json ke min_soc / backup_soc se sync karte hain.
    """
    cfg = await asyncio.to_thread(_load_cycle_config_sync)

    # compute today's cycles & sync into JSON field `cycle_used_today` ---
    try:
        # default table name from get_today_cycles; override if needed later
        cycles_today = await get_today_cycles()
        cfg["cycle_used_today"] = cycles_today

        # persist so that cycle_config.json always reflects current day usage
        await asyncio.to_thread(_save_cycle_config_sync, cfg)
    except Exception as e:
        # Failure here should not break the API; we just keep old value
        # (you can replace print with proper logger if you want)
        print(f"[cycle-config] Failed to compute today's cycles: {e}")

    # --- NEW: override only min_safe_soc & standby_soc_hold_band from BESS file ---
    bess = await asyncio.to_thread(_load_bess_params_sync)
    bess_params = bess.get("bess_params", {}) if isinstance(bess, dict) else {}

    if isinstance(bess_params, dict):
        soc_cfg = cfg.get("soc_safety_management", {})
        if not isinstance(soc_cfg, dict):
            soc_cfg = {}

        # min_soc (0–1) -> min_safe_soc (0–100)
        min_soc = bess_params.get("min_soc")
        if isinstance(min_soc, (int, float)):
            soc_cfg["min_safe_soc"] = int(round(min_soc * 100))

        # backup_soc (0–1) -> standby_soc_hold_band (0–100)
        backup_soc = bess_params.get("backup_soc")
        if isinstance(backup_soc, (int, float)):
            soc_cfg["standby_soc_hold_band"] = int(round(backup_soc * 100))

        cfg["soc_safety_management"] = soc_cfg

    return jsonify(cfg), 200


@cycle_management_bp.route("/cycle-config", methods=["POST"])
async def update_cycle_config():
    """
    Update parts of the cycle configuration.

    Body example:
    {
        "max_cycles_per_day": 5,
        "cycle_counting_method": "0",
        "lockout_behaviour": {
            "allow_safety_charging": true,
            .
        },
        "price_logic_overrides": {
            "disable_aggrassive_charging": false,
            .
        },
        "notifications": {
            "notify_at_80_percent_usage": true,
            .
        },
        "soc_safety_management": {
            "min_safe_soc": 30,
            "standby_soc_hold_band": 5
        }
    }
    """
    data = await request.get_json(silent=True) or {}

    cfg = await asyncio.to_thread(_load_cycle_config_sync)

    # NOTE: we *do not* allow manual overwrite of cycle_used_today here,
    # because it is computed from controller data via get_today_cycles().

    # ---- max cycles per day ----
    if "max_cycles_per_day" in data:
        try:
            max_cycles = int(data["max_cycles_per_day"])
        except (TypeError, ValueError):
            return jsonify({"error": "max_cycles_per_day must be an integer"}), 400

        if max_cycles < 0 or max_cycles > 20:
            return (
                jsonify(
                    {
                        "error": "max_cycles_per_day must be between 0 and 20",
                        "value": max_cycles,
                    }
                ),
                400,
            )

        # cycle_config.json me update
        cfg["max_cycles_per_day"] = max_cycles

        # 🔁 NEW: bess_params_setting.json me bhi same value sync karo
        bess = await asyncio.to_thread(_load_bess_params_sync)
        if not isinstance(bess, dict):
            bess = {}

        bess_params = bess.get("bess_params")
        if not isinstance(bess_params, dict):
            bess_params = {}

        bess_params["max_cycles_per_day"] = max_cycles
        bess["bess_params"] = bess_params
        await asyncio.to_thread(_save_bess_params_sync, bess)

    if "cycle_counting_method" in data:
        method = str(data["cycle_counting_method"])
        # allow only valid keys if options exist
        options = cfg.get("cycle_counting_method_option")
        if isinstance(options, dict):
            if method in options:
                cfg["cycle_counting_method"] = method
        else:
            # if no options in JSON, still store whatever was sent
            cfg["cycle_counting_method"] = method

    def merge_section(section_name: str):
        incoming = data.get(section_name)
        if isinstance(incoming, dict):
            existing = cfg.get(section_name, {})
            if isinstance(existing, dict):
                existing.update(incoming)
                cfg[section_name] = existing
            else:
                cfg[section_name] = incoming

    # these keys are nested dicts in cycle_config.json
    merge_section("lockout_behaviour")
    merge_section("price_logic_overrides")
    merge_section("notifications")
    merge_section("soc_safety_management")

    # sync ONLY min_safe_soc & standby_soc_hold_band into bess_params_setting.json ---
    soc_cfg = cfg.get("soc_safety_management", {})
    if isinstance(soc_cfg, dict):
        bess = await asyncio.to_thread(_load_bess_params_sync)
        if not isinstance(bess, dict):
            bess = {}

        bess_params = bess.get("bess_params")
        if not isinstance(bess_params, dict):
            bess_params = {}
        # min_safe_soc (0–100) -> min_soc (0–1)
        if "min_safe_soc" in soc_cfg:
            try:
                min_safe_soc = float(soc_cfg["min_safe_soc"])
                bess_params["min_soc"] = min_safe_soc / 100.0
            except (TypeError, ValueError):
                pass

        # standby_soc_hold_band (0–100) -> backup_soc (0–1)
        if "standby_soc_hold_band" in soc_cfg:
            try:
                standby_band = float(soc_cfg["standby_soc_hold_band"])
                bess_params["backup_soc"] = standby_band / 100.0
            except (TypeError, ValueError):
                pass

        bess["bess_params"] = bess_params
        await asyncio.to_thread(_save_bess_params_sync, bess)

    await asyncio.to_thread(_save_cycle_config_sync, cfg)
    return jsonify(cfg), 200
