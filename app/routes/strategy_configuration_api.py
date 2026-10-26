import asyncio
import json
import logging
from pathlib import Path


from quart import Blueprint, request, jsonify
from app.utils.rbac import require_permission
from app.utils.dashboard_logger import insert_dashboard_log
from model.db import get_connection
from runtime_paths import DataPaths


logger = logging.getLogger(__name__)

# Blueprint for strategy configuration API
strategy_configuration_bp = Blueprint("strategy_configuration", __name__)

def upsert_config(scope, entity_id, key, value, value_type):
    conn = get_connection()
    cur = conn.cursor()

    value_text = None
    value_num = None
    value_bool = None
    value_json = None

    if value_type == "number":
        value_num = value
    elif value_type == "boolean":
        value_bool = value
    elif value_type == "json":
        value_json = json.dumps(value)
    else:
        value_text = str(value)

    cur.execute("""
        INSERT INTO configs(scope, entity_id, key,
                            value_text, value_num, value_bool, value_json,
                            value_type)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT(scope, entity_id, key)
        DO UPDATE SET
            value_text = EXCLUDED.value_text,
            value_num = EXCLUDED.value_num,
            value_bool = EXCLUDED.value_bool,
            value_json = EXCLUDED.value_json,
            value_type = EXCLUDED.value_type,
            updated_at = now()
    """, (
        scope, entity_id, key,
        value_text, value_num, value_bool, value_json,
        value_type
    ))

    conn.commit()
    cur.close()
    conn.close()

# ----------------- PATHS TO CONFIG FILES -----------------

DATA_ROOT = DataPaths.root()

STRATEGY_DIR = DATA_ROOT / "controller_layer" / "strategies"

STRATEGY_CONFIG_DIR = STRATEGY_DIR / "strategy_configuration"

FLAT_TOU_FILE = STRATEGY_CONFIG_DIR / "FlatTou.json"
PEAK_FILE = STRATEGY_CONFIG_DIR / "PeakShaving.json"
SMART_TOU_FILE = STRATEGY_CONFIG_DIR / "SmartTou.json"
VIRTUAL_FILE = STRATEGY_CONFIG_DIR / "VirtualTariff.json"

BESS_FILE = STRATEGY_DIR / "bess_params_setting.json"

CYCLE_CONFIG_FILE = STRATEGY_DIR / "cycle_management" / "cycle_config.json"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON in %s, returning empty object", path)
        return {}


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def is_bess_changed(new_bess: dict):
    old = _read_json(BESS_FILE) or {}
    old_bp = old.get("bess_params", {})

    def f(v):
        try:
            return float(v)
        except:
            return None

    return (
        f(new_bess.get("maxCharge")) != old_bp.get("max_charge_kw")
        or f(new_bess.get("maxDischarge")) != old_bp.get("max_discharge_kw")
        or f(new_bess.get("minSOC")) != old_bp.get("min_soc")
        or f(new_bess.get("maxSOC")) != old_bp.get("max_soc")
        or int(float(new_bess.get("maxCyclesPerDay", 0))) != old_bp.get("max_cycles_per_day")
        or f(new_bess.get("backupSOC")) != old_bp.get("backup_soc")
    )


# ----------------- MAPPING HELPERS (POST) -----------------
def update_bess_params_file(bess: dict):
    """
    Map frontend BESS params -> bess_params_setting.json structure

    bess = {
        "maxCharge": "500",
        "maxDischarge": "500",
        "minSOC": "10",   # in %
        "maxSOC": "95"    # in %
        "maxCyclesPerDay": "4",  # optional
        "backupSOC": "5"         # optional, % (UI)
    }
    """
    data = _read_json(BESS_FILE) or {"bess_params": {}}
    bp = data.setdefault("bess_params", {})

    def to_float(val, default=None):
        try:
            return float(val)
        except (TypeError, ValueError):
            return default

    def to_int(val, default=None):
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default

    max_charge = to_float(bess.get("maxCharge"))
    max_discharge = to_float(bess.get("maxDischarge"))
    min_soc_percent = to_float(bess.get("minSOC"))
    max_soc_percent = to_float(bess.get("maxSOC"))
    max_cycles_per_day = to_int(bess.get("maxCyclesPerDay"))
    backup_soc_percent = to_float(bess.get("backupSOC"))

    if max_charge is not None:
        bp["max_charge_kw"] = max_charge
    if max_discharge is not None:
        bp["max_discharge_kw"] = max_discharge

    # UI sends SOC in %, file stores fractional (0.1 -> 10%)
    if min_soc_percent is not None:
        bp["min_soc"] = min_soc_percent
    if max_soc_percent is not None:
        bp["max_soc"] = max_soc_percent

    # NEW: max cycles per day (integer)
    if max_cycles_per_day is not None:
        bp["max_cycles_per_day"] = max_cycles_per_day

    # NEW: backup SOC stored as fractional in file
    if backup_soc_percent is not None:
        try:
            bp["backup_soc"] = backup_soc_percent
        except Exception:
            logger.warning("Invalid backupSOC value, skipping write: %s", backup_soc_percent)

    _write_json(BESS_FILE, data)
    logger.info("Updated BESS params at %s", BESS_FILE)

    # ---- SYNC max_cycles_per_day INTO cycle_config.json AS WELL ----
    if max_cycles_per_day is not None:
        try:
            cycle_cfg = _read_json(CYCLE_CONFIG_FILE) or {}
            if not isinstance(cycle_cfg, dict):
                cycle_cfg = {}
            cycle_cfg["max_cycles_per_day"] = max_cycles_per_day
            _write_json(CYCLE_CONFIG_FILE, cycle_cfg)
            logger.info(
                "Synced max_cycles_per_day=%s into %s",
                max_cycles_per_day,
                CYCLE_CONFIG_FILE,
            )
        except Exception as exc:
            logger.warning(
                "Failed to sync max_cycles_per_day into cycle_config.json: %s",
                exc,
            )

    # ---------- DB SYNC ----------
    upsert_config("bess", None, "max_charge_kw", max_charge, "number")
    upsert_config("bess", None, "max_discharge_kw", max_discharge, "number")
    upsert_config("bess", None, "min_soc", min_soc_percent, "number")
    upsert_config("bess", None, "max_soc", max_soc_percent, "number")
    upsert_config("bess", None, "max_cycles_per_day", max_cycles_per_day, "number")
    upsert_config("bess", None, "backup_soc", backup_soc_percent, "number")


def update_flat_tou_file(windows: list, rules: dict):
    """
    Map TOU windows + rules -> FlatTou.json

    windows = [ { name, start, end, price }, ... ]
    rules = {
      allowOffPeakCharge: bool,
      dischargeOnlyPeak: bool,
      allowChargeOnlySolar: bool,
      reserveSOC: "20"
    }
    """
    data = _read_json(FLAT_TOU_FILE) or {}
    setting = {
        "peak": [],
        "off_peak": [],
        "mid_peak": [],
    }

    def to_float(val, default=None):
        try:
            return float(val)
        except (TypeError, ValueError):
            return default

    for w in windows or []:
        name = (w.get("name") or "").lower()

        if "off" in name:
            key = "off_peak"
        elif "mid" in name:
            key = "mid_peak"
        else:
            key = "peak"

        setting[key].append(
            {
                "start_time": w.get("start", "00:00"),
                "end_time": w.get("end", "00:00"),
                "price": to_float(w.get("price"), 0.0),
            }
        )

    data["setting"] = setting

    opt = data.get("optimization_rule", {})
    opt["allow_charging_during_off_peak"] = bool(rules.get("allowOffPeakCharge"))
    opt["allow_charging_only_from_solar"] = bool(rules.get("allowChargeOnlySolar"))
    opt.setdefault("allow_grid_charging_if_price_below", 3)
    opt["discharging_during_peak"] = bool(rules.get("dischargeOnlyPeak"))

    reserve_soc = rules.get("reserveSOC")
    try:
        opt["reserve_soc"] = (
            float(reserve_soc) if reserve_soc is not None else opt.get("reserve_soc", 20)
        )
    except ValueError:
        pass

    data["optimization_rule"] = opt
    _write_json(FLAT_TOU_FILE, data)
    logger.info("Updated Flat TOU config at %s", FLAT_TOU_FILE)

    upsert_config("strategy", 2, "flat_tou_setting", setting, "json")
    upsert_config("strategy", 2, "flat_tou_rules", opt, "json")


def update_peak_shaving_file(peak_conf: dict, windows: list = None):
    """
    Map Peak Shaving section -> PeakShaving.json
    """
    if not peak_conf and not windows:
        return

    data = _read_json(PEAK_FILE) or {}

    def to_float(val, default=None):
        try:
            return float(val)
        except (TypeError, ValueError):
            return default

    # ---------------- BASIC FIELDS ----------------
    if peak_conf.get("threshold") is not None:
        data["peak_shaving_threshold_kw"] = to_float(
            peak_conf["threshold"], data.get("peak_shaving_threshold_kw", 0.0)
        )

    if peak_conf.get("minSOC") is not None:
        data["reserve_soc_percent"] = to_float(
            peak_conf["minSOC"], data.get("reserve_soc_percent", 20)
        )

    if peak_conf.get("maxDischarge") is not None:
        data["max_discharge_power_kw"] = to_float(
            peak_conf["maxDischarge"], data.get("max_discharge_power_kw", 0.0)
        )

    forecast_map = {
        "none": "0",
        "ai": "1",
        "manual": "2",
        "external": "3",
    }

    fs = peak_conf.get("forecastSource")
    if fs:
        data["forcast_source"] = forecast_map.get(fs, "0")

    # ---------------- ADVANCED SETTINGS ----------------
    adv = data.get("advanced_settings", {})

    if peak_conf.get("softLimit") is not None:
        adv["soft_limit_kw"] = to_float(
            peak_conf["softLimit"], adv.get("soft_limit_kw", 0.0)
        )

    if peak_conf.get("hardLimit") is not None:
        adv["hard_limit_kw"] = to_float(
            peak_conf["hardLimit"], adv.get("hard_limit_kw", 0.0)
        )

    if peak_conf.get("maintainSoft") is not None:
        adv["maintain_load_under_soft_limit_using_bess"] = bool(
            peak_conf["maintainSoft"]
        )

    if peak_conf.get("forceHard") is not None:
        adv["force_control_to_stay_under_hard_limit"] = bool(
            peak_conf["forceHard"]
        )

    data["advanced_settings"] = adv

    # ---------------- NEW: UPDATE TIME WINDOWS ----------------
    if windows is not None:
        new_setting = {
            "peak": [],
            "mid_peak": [],
            "off_peak": []
        }

        for w in windows:
            name = (w.get("name") or "").lower()

            if "off" in name:
                key = "off_peak"
            elif "mid" in name:
                key = "mid_peak"
            else:
                key = "peak"

            new_setting[key].append({
                "start_time": w.get("start", "00:00"),
                "end_time": w.get("end", "00:00"),
                "load": to_float(w.get("price"), 0.0)
            })

        data["peak"] = new_setting["peak"]
        data["mid_peak"] = new_setting["mid_peak"]
        data["off_peak"] = new_setting["off_peak"]

    _write_json(PEAK_FILE, data)
    logger.info("Updated Peak Shaving config at %s", PEAK_FILE)

    upsert_config("strategy", 3, "peak_config", data, "json")


def update_smart_tou_file(capped: dict):
    """
    Map Capped Dynamic view -> SmartTou.json

    capped = {
      priceSource: "dayahead" | "real" | "custom" | "xls",
      apiUrl: "...",
      refreshMin: "60",
      priceCap: "12",
      priceFloor: "3",
      arbThreshold: "8",
      maxCyclesPerDay: "4",
      rules: {
        cappedAggressivelyCharge: bool,
        cappedPreventImport: bool,
        cappedAllowExport: bool
      }
    }
    """
    if not capped:
        return

    data = _read_json(SMART_TOU_FILE) or {}

    def to_float(val, default=None):
        try:
            return float(val)
        except (TypeError, ValueError):
            return default

    price_source = data.get("price_source", {})
    options = price_source.get(
        "price_source_options",
        {
            "0": "day_ahead_market",
            "1": "intraday_market",
            "2": "api_url",
            "3": "xls_file",
        },
    )

    ps_map = {
        "dayahead": "0",
        "real": "1",
        "custom": "2",
        "xls": "3",
    }

    src = capped.get("priceSource")
    if src:
        price_source["price_source_options"] = options
        price_source["price_source_default"] = ps_map.get(
            src, price_source.get("price_source_default", "2")
        )
        if src == "custom" and capped.get("apiUrl"):
            price_source["api_url"] = capped.get("apiUrl")
        if capped.get("refreshMin") is not None:
            try:
                price_source["refresh_interval_minutes"] = int(capped["refreshMin"])
            except ValueError:
                pass

    data["price_source"] = price_source

    caps = data.get("caps_and_thresholds", {})

    # Core price caps / thresholds
    caps["price_cap_threshold"] = to_float(
        capped.get("priceCap"), caps.get("price_cap_threshold", 1.0)
    )
    caps["price_floor_threshold"] = to_float(
        capped.get("priceFloor"), caps.get("price_floor_threshold", 0.0)
    )
    caps["arbitrage_threshold"] = to_float(
        capped.get("arbThreshold"), caps.get("arbitrage_threshold", 8)
    )

    # 🔁 Ensure any legacy per-capped max_cycles_per_day is removed
    caps.pop("max_cycles_per_day", None)

    data["caps_and_thresholds"] = caps

    rules_in = capped.get("rules") or {}
    rules = data.get("optimization_rule", {})
    rules[
        "Aggressively_charge_when_price_below_price_floor_threshold"
    ] = bool(rules_in.get("cappedAggressivelyCharge"))
    rules["prevent_charging_when_price_above_price_cap_threshold"] = bool(
        rules_in.get("cappedPreventImport")
    )
    rules["allow_discharge_when_profitable"] = bool(
        rules_in.get("cappedAllowExport")
    )
    data["optimization_rule"] = rules

    _write_json(SMART_TOU_FILE, data)
    logger.info("Updated Smart TOU (capped dynamic) config at %s", SMART_TOU_FILE)

    upsert_config("strategy", 1, "smart_tou_config", data, "json")


def update_virtual_tariff_file(virtual: dict):
    """
    Map Virtual Tariff view -> VirtualTariff.json

    virtual = {
      customerTariffType: "0" | "1",          # "0" => FlatTou, "1" => SmartTou
      flatPrice: "8",
      aggregatorShare: "30",
      guaranteedSavings: "5000",
      settlementMethod: "0" | "1" | "2",      # matches settlement_method_option
      billingOutputs: { pdf, csv, api },
      virtualStrategy: "maxrev" | "balanced" | "solar"
    }
    """
    if not virtual:
        return

    data = _read_json(VIRTUAL_FILE) or {}

    def to_float(val, default=None):
        try:
            return float(val)
        except (TypeError, ValueError):
            return default

    ctype = virtual.get("customerTariffType")

    # 🔁 NEW: directly accept JSON codes "0" / "1" from UI
    if ctype in ("0", "1"):
        data["tou_type"] = ctype
        # FlatTou ke case me hi flat_tou_rate update karo
        if ctype == "0" and virtual.get("flatPrice") is not None:
            data["flat_tou_rate"] = to_float(
                virtual["flatPrice"], data.get("flat_tou_rate", 0)
            )
    else:
        # Backward compatibility: older UI "flat" / "time" / "dynamic"
        if ctype == "flat":
            data["tou_type"] = "0"
            if virtual.get("flatPrice") is not None:
                data["flat_tou_rate"] = to_float(
                    virtual["flatPrice"], data.get("flat_tou_rate", 0)
                )
        elif ctype is not None:
            data["tou_type"] = "1"

    if virtual.get("aggregatorShare") is not None:
        data["aggregator_profit_share_percent"] = to_float(
            virtual["aggregatorShare"],
            data.get("aggregator_profit_share_percent", 20),
        )

    if virtual.get("guaranteedSavings") is not None:
        data["guaranteed_savings_per_month"] = to_float(
            virtual["guaranteedSavings"],
            data.get("guaranteed_savings_per_month", 5000),
        )

    # 🔁 NEW: settlementMethod ab directly "0" | "1" | "2" ko support karega
    sm = virtual.get("settlementMethod")
    if sm in ("0", "1", "2"):
        data["settlement_method"] = sm
    elif sm:
        # Backward compatibility for old string values ("wholesale", "fixed")
        smap = {
            "wholesale": "0",
            "fixed": "1",
        }
        data["settlement_method"] = smap.get(
            sm, data.get("settlement_method", "0")
        )

    astrat_map = {
        "maxrev": "0",
        "balanced": "1",
        "solar": "2",
    }
    vs = virtual.get("virtualStrategy")
    if vs:
        data["arbitrage_strategy_option"] = data.get(
            "arbitrage_strategy_option",
            {
                "0": "maximise_revenue",
                "1": "balanced_cycles",
                "2": "solar_first_strategy",
            },
        )
        data["arbitrage_strategy"] = astrat_map.get(vs, "0")

    _write_json(VIRTUAL_FILE, data)
    logger.info("Updated Virtual Tariff config at %s", VIRTUAL_FILE)

    upsert_config("strategy", 1, "virtual_tariff", data, "json")


# ----------------- GET & POST ROUTES -----------------


@strategy_configuration_bp.get("/configuration")
async def get_strategy_configuration():
    """
    GET /strategy/configuration

    Returns current contents of all strategy JSON files so UI can pre-fill:
      - bess_params_setting.json
      - FlatTou.json
      - PeakShaving.json
      - SmartTou.json
      - VirtualTariff.json
    """
    bess = _read_json(BESS_FILE)
    flat_tou = _read_json(FLAT_TOU_FILE)
    peak = _read_json(PEAK_FILE)
    smart = _read_json(SMART_TOU_FILE)
    virtual = _read_json(VIRTUAL_FILE)

    return jsonify(
        {
            "bess_params": bess,
            "flat_tou": flat_tou,
            "peak_shaving": peak,
            "smart_tou": smart,
            "virtual_tariff": virtual,
        }
    )


@strategy_configuration_bp.post("/configuration")
async def save_strategy_configuration():

    print(">>>>>>>> ROUTE HIT <<<<<<<<")

    user, err = await require_permission("can_control_bess")
    if err:
        print("Permission denied")
        return err

    payload = await request.get_json()
    print("PAYLOAD =", payload)
    if not payload:
        return jsonify({"error": "Invalid or empty JSON payload"}), 400

    tariff_mode = payload.get("tariffMode")
    bess = payload.get("bess") or {}
    windows = payload.get("windows") or []
    rules = payload.get("rules") or {}
    peak_conf = payload.get("peakShaving") or {}
    capped_conf = payload.get("capped") or {}
    virtual_conf = payload.get("virtual") or {}

    try:

        #  UPDATE BESS PARAM ONLY IF CHANGED
        if bess and is_bess_changed(bess):
            await asyncio.to_thread(update_bess_params_file, bess)

            await insert_dashboard_log(
                action="APPLY",
                control_type="STRATEGY",
                module="BESS_PARAMETER",
                username=user["role"],
                ip=request.headers.get("X-Forwarded-For", request.remote_addr),
                status="SUCCESS",
                payload=bess
            )

        #  STRATEGY MODE UPDATE & LOG ONLY IF STRATEGY CHANGED
        strategy_changed = False

        if tariff_mode == "tou" and windows:
            strategy_changed = True
            await asyncio.to_thread(update_flat_tou_file, windows, rules)

        elif tariff_mode == "peak" and (peak_conf or windows):
            strategy_changed = True
            await asyncio.to_thread(update_peak_shaving_file, peak_conf, windows)

        elif tariff_mode == "capped" and capped_conf:
            strategy_changed = True
            await asyncio.to_thread(update_smart_tou_file, capped_conf)

        elif tariff_mode == "virtual" and virtual_conf:
            strategy_changed = True
            await asyncio.to_thread(update_virtual_tariff_file, virtual_conf)

        #  STRATEGY LOG ONLY IF STRATEGY CHANGED
        print("strategy_changed =", strategy_changed)
        if strategy_changed:
            print("Calling insert_dashboard_log...")
            await insert_dashboard_log(
                action="APPLY",
                control_type="STRATEGY",
                module=tariff_mode.upper(),
                username=user["role"],
                ip=request.headers.get("X-Forwarded-For", request.remote_addr),
                status="SUCCESS",
                payload=payload
            )

        return jsonify({"status": "ok", "tariffMode": tariff_mode})

    except Exception as e:

        await insert_dashboard_log(
            action="APPLY",
            control_type="STRATEGY",
            module=(tariff_mode or "UNKNOWN").upper(),
            username=user["role"],
            ip=request.headers.get("X-Forwarded-For", request.remote_addr),
            status="FAILED",
            payload=payload
        )

        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
