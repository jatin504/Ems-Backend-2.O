"""Interactive mapped-alarm test using runtime device/model/sampling config."""
import argparse
import asyncio
import copy
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# Set a single alias string OR a list of aliases here.
TAGS = "Battery_SOC"
# TAGS = ["Battery_SOC", "Your_Second_Alarm", "Your_Third_Alarm"]


def normalize_tags(value):
    tags = [value] if isinstance(value, str) else value
    if not isinstance(tags, list) or not tags or any(not isinstance(t, str) or not t.strip() for t in tags):
        raise ValueError("Tags must be a non-empty string or list of non-empty strings")
    return list(dict.fromkeys(t.strip() for t in tags))


async def main(args):
    from runtime_paths import DataPaths
    from device_library.resolver import DeviceLibraryResolver
    from processor.src.samplings.src.sampling_manager import SamplingManager

    devices = json.loads(DataPaths.resolve("config/device_config.json").read_text())["devices"]
    device = next((d for d in devices if d["device_id"] == args.device), None)
    if device is None:
        raise SystemExit("Device not found. Available: " + ", ".join(d["device_id"] for d in devices))
    mapping = DeviceLibraryResolver().get_mapping(device["deviceType"], device["make"], device["model"])
    tags = normalize_tags(args.tags if args.tags is not None else args.tag if args.tag is not None else TAGS)
    manager = SamplingManager()
    configs = manager._load_tag_configs(device["deviceType"])
    selected = []
    for tag in tags:
        matches = [m for m in mapping.values() if m.get("alias") == tag]
        if len(matches) != 1:
            raise SystemExit("Expected one model mapping for alias " + tag)
        meta = matches[0]
        if meta.get("type") != "alarm" or meta.get("alarmType", "normal") != "normal" or meta.get("isEvent", False):
            raise SystemExit(tag + ": must be type=alarm, alarmType=normal, isEvent=false in selected model")
        rule = configs.get(tag, {})
        if rule.get("is_debounce") is not True:
            raise SystemExit(tag + ": enable is_debounce in runtime " + device["deviceType"] + ".json")
        duration = float(rule.get("debounce_time", 4))
        if not 0 < duration < float("inf"):
            raise SystemExit(tag + ": invalid debounce_time")
        active = 0 if meta.get("isNormalReverse", False) else 1
        selected.append((tag, meta, duration, active, 1 - active))
    alarm = manager.sampling_strategies["alarm"]
    if not args.write_db:
        alarm._insert_alarm_to_db = lambda *a, **kw: print("DRY RUN: INSERT ACTIVE", a, kw, flush=True)
        alarm._clear_active_alarm = lambda *a, **kw: print("DRY RUN: UPDATE CLEARED", a, kw, flush=True)
    else:
        # Production callbacks log DB failures; additionally display actual persisted state.
        from model import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT current_database()")
                print("Database:", cur.fetchone()[0], flush=True)
    print("Device:", device["device_id"], "type:", device["deviceType"], "tags:", tags, flush=True)
    print("Mode:", "REAL DB" if args.write_db else "DRY RUN", "durations:", {item[0]: item[2] for item in selected}, flush=True)
    print("Synthetic readings go directly to SamplingManager. No Modbus reads/writes or latest-cache updates.", flush=True)
    print("Stop the normal telemetry processor during this test to avoid competing real readings/DB writes.", flush=True)

    async def hold(tag, meta, value, seconds):
        start = time.monotonic()
        last_second = -1
        while True:
            elapsed = time.monotonic() - start
            data = copy.deepcopy(meta)
            data.update(device_id=device["device_id"], device_type=device["deviceType"], value=value)
            await manager.process_mapped_data({tag: data})
            if int(elapsed) != last_second:
                last_second = int(elapsed)
                print(f"  tag={tag} value={value} elapsed={elapsed:.1f}s", flush=True)
            if elapsed >= seconds:
                break
            await asyncio.sleep(min(0.25, seconds - elapsed))

    def show_db(tag):
        if not args.write_db:
            return
        from model import get_connection
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status,time_stamp,clr_timestamp FROM alarms WHERE device_id=%s AND tag_name=%s ORDER BY time_stamp DESC LIMIT 5", (args.device, tag))
                print(tag, "actual DB rows (newest first):", cur.fetchall(), flush=True)

    print("Initial stable CLEAR baseline (may clear an existing ACTIVE row).", flush=True)
    for tag, meta, duration, active, clear in selected:
        await hold(tag, meta, clear, duration + 1)
    while True:
        print("\n1 = ACTIVE for debounce+1 seconds: expect ACTIVE")
        print("2 = CLEARED for debounce+1 seconds: expect CLEARED after case 1")
        print("3 = Short ACTIVE spike: run after case 2; expect no new ACTIVE")
        print("4 = Short CLEAR spike: run after case 1; expect row stays ACTIVE")
        print("0 = Exit")
        choice = (await asyncio.to_thread(input, "Choose: ")).strip()
        if choice == "0":
            break
        if choice not in ("1", "2", "3", "4"):
            print("Choose 0, 1, 2, 3 or 4.")
            continue
        # Run each tag separately: the current alarms schema conflicts on time_stamp.
        # Sequential cases avoid simultaneous inserts for distinct tags in one second.
        for tag, meta, duration, active, clear in selected:
            print("\nTesting:", tag, "case:", choice, flush=True)
            accepted = alarm.last_logged_values[args.device].get(tag)
            if choice == "1":
                await hold(tag, meta, active, duration + 1)
            elif choice == "2":
                await hold(tag, meta, clear, duration + 1)
            elif choice == "3":
                if accepted != clear:
                    print(tag + ": run case 2 first; skipping this tag.")
                    continue
                await hold(tag, meta, active, duration / 2)
                await hold(tag, meta, clear, duration + 1)
            elif choice == "4":
                if accepted != active:
                    print(tag + ": run case 1 first; skipping this tag.")
                    continue
                await hold(tag, meta, clear, duration / 2)
                await hold(tag, meta, active, duration + 1)
            show_db(tag)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--device", default="plant_1_pcs_controller_1")
    tag_options = parser.add_mutually_exclusive_group()
    tag_options.add_argument("--tag", help="One alias; overrides TAGS in this file")
    tag_options.add_argument("--tags", nargs="+", help="Multiple aliases separated by spaces; overrides TAGS")
    parser.add_argument("--write-db", action="store_true")
    asyncio.run(main(parser.parse_args()))
