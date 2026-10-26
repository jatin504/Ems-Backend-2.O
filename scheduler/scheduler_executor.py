import datetime
import asyncio
import logging

from controller_layer.strategies.schedule_strategy_manager import StrategyScheduleManager
from config.device_config_resolver import DeviceConfigResolver
from plant_architecture_resolver import PlantArchitectureResolver

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from model.db import get_connection

from LogColor import LogColor

logger = logging.getLogger(__name__)


async def bess_scheduler_loop():
    """
    Periodically calls the BESS strategy every 60 seconds.
    """

    logger.info(f'{LogColor.MAGENTA}Starting BESS Scheduler Loop{LogColor.RESET}')
    # date : 18-07-25 designed only for tou strategy

    manager = StrategyScheduleManager()
    await manager.run(device_id="plant_1_pcs_controller_1")


def _build_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def scrape_and_insert_tariff():
    """
    Scrape Maharashtra's CURRENT tariff from its state page and insert into:
        energy_strategy(ts, state, tariff)
    - Uses the state detail page: https://vidyutpravah.in/state-data/maharashtra
    - Parses 'CURRENT ... ₹/Unit' first; else falls back to first '/Unit' value
      when 'YESTERDAY' text is not present.
    - Per-minute timestamp; ON CONFLICT (ts, state) DO NOTHING
    """
    import time, re, datetime, decimal
    from decimal import Decimal, InvalidOperation

    # ---- Config ----
    STATE_NAME = "Maharashtra"
    STATE_CODE_FOR_TABLE = "MHA"   
    STATE_SLUG = "maharashtra"
    URL = f"https://vidyutpravah.in/state-data/{STATE_SLUG}"
    TIMEOUT_SEC = 25

    # Regexes (CURRENT priority, then generic '/Unit')
    CURRENT_RE = re.compile(r'CURRENT\s*[:\-]?\s*₹?\s*(\d+(?:\.\d{1,3})?)\s*₹?\s*/\s*Unit', re.IGNORECASE)
    RE_PRICE_PATTERNS = [
        re.compile(r"(\d+(?:\.\d{1,3})?)\s*₹\s*/\s*Unit"),
        re.compile(r"₹\s*(\d+(?:\.\d{1,3})?)\s*/\s*Unit"),
        re.compile(r"(\d+(?:\.\d{1,3})?)\s*/\s*Unit"),
    ]

    def _first_number(text: str) -> Decimal | None:
        for pat in RE_PRICE_PATTERNS:
            m = pat.search(text or "")
            if not m:
                continue
            try:
                return Decimal(m.group(1))
            except (InvalidOperation, ValueError):
                continue
        return None

    try:
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"[{datetime.datetime.now()}] Starting MHA Tariff Scraping (state page)…")

        # Prefer your helper if present; else build driver locally
        try:
            driver = _build_driver()
        except NameError:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)

        driver.get(URL)

        # Poll <body> text until CURRENT or safe fallback found (no JS regex => no \d warning)
        price_val: Decimal | None = None
        deadline = time.time() + TIMEOUT_SEC
        while time.time() < deadline and price_val is None:
            try:
                body_txt = driver.find_element(By.TAG_NAME, "body").text
            except Exception:
                body_txt = ""

            if body_txt:
                m = CURRENT_RE.search(body_txt)
                if m:
                    try:
                        price_val = Decimal(m.group(1))
                        break
                    except (InvalidOperation, ValueError):
                        price_val = None

                # Fallback: first '/Unit' number only when YESTERDAY is not present
                if price_val is None and "/Unit" in body_txt and not re.search(r"YESTERDAY", body_txt, re.IGNORECASE):
                    candidate = _first_number(body_txt)
                    if candidate is not None:
                        price_val = candidate
                        break

            time.sleep(0.2)

        try:
            driver.quit()
        except Exception:
            pass

        # Per-minute ts
        ts = datetime.datetime.now().replace(second=0, microsecond=0)

        # Insert into energy_strategy
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO energy_strategy (ts, state, tariff)
            VALUES (%s, %s, %s)
            ON CONFLICT (ts, state) DO NOTHING;
            """,
            (ts, STATE_CODE_FOR_TABLE, float(price_val) if isinstance(price_val, (Decimal, decimal.Decimal)) else None),
        )
        conn.commit()
        cur.close()
        conn.close()

        print(f"[SCRAPED & STORED] Time: {ts} | State: {STATE_NAME} ({STATE_CODE_FOR_TABLE}) | Tariff: {None if price_val is None else float(price_val)}")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    except Exception as e:
        try:
            driver.quit()
        except Exception:
            pass
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"[ERROR] scrape_and_insert_tariff failed: {e}")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")



def scrape_all_states_to_snapshot():
    """
    Scrape the 32 requested states from their individual pages:
      https://vidyutpravah.in/state-data/<slug>
    Parse CURRENT price first; fall back to first '/Unit' number if 'YESTERDAY' is absent.
    Insert into state_tariff_snapshot(ts, state_code, state_name, tariff, unit, source).
    ON CONFLICT (ts, state_code) DO NOTHING.
    """
    import os, re, time, decimal
    from decimal import Decimal, InvalidOperation


    BASE = "https://vidyutpravah.in/state-data/"
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "8"))
    BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", "18"))

  
    STATE_SLUGS = {
        "Andhra Pradesh": "andhra-pradesh","Arunachal Pradesh": "arunachal-pradesh", "Assam": "assam","Bihar": "bihar","Chandigarh": "chandigarh","Chhattisgarh": "chhattisgarh",
        "Delhi": "delhi","Goa": "goa","Gujarat": "gujarat","Haryana": "haryana","Himachal Pradesh": "himachal-pradesh","Jammu & Kashmir": "jammu-kashmir","Jharkhand": "jharkhand",
        "Karnataka": "karnataka","Kerala": "kerala","Madhya Pradesh": "madhya-pradesh","Maharashtra": "maharashtra","Manipur": "manipur",
        "Meghalaya": "meghalaya","Mizoram": "mizoram","Nagaland": "nagaland","Odisha": "odisha","Puducherry": "puducherry","Rajasthan": "rajasthan","Sikkim": "sikkim",
        "Tamil Nadu": "tamil-nadu","Telangana": "telangana","Tripura": "tripura","Uttar Pradesh": "uttar-pradesh","Uttarakhand": "uttarakhand","West Bengal": "west-bengal",
    }

    # Stable 2-letter codes for snapshot.state_code
    STATE_CODES = {
        "Andhra Pradesh":"AP","Arunachal Pradesh":"AR","Assam":"AS","Bihar":"BR","Chandigarh":"CH",
        "Chhattisgarh":"CG","Delhi":"DL","Goa":"GA","Gujarat":"GJ","Haryana":"HR","Himachal Pradesh":"HP",
        "Jammu & Kashmir":"JK","Jharkhand":"JH","Karnataka":"KA","Kerala":"KL",
        "Madhya Pradesh":"MP","Maharashtra":"MH","Manipur":"MN","Meghalaya":"ML","Mizoram":"MZ",
        "Nagaland":"NL","Odisha":"OD","Puducherry":"PY","Rajasthan":"RJ","Sikkim":"SK","Tamil Nadu":"TN",
        "Telangana":"TG","Tripura":"TR","Uttar Pradesh":"UP","Uttarakhand":"UT","West Bengal":"WB",
    }

    # CURRENT first; then generic '/Unit' value if page doesn't mention YESTERDAY
    CURRENT_RE = re.compile(r'CURRENT\s*[:\-]?\s*₹?\s*(\d+(?:\.\d{1,3})?)\s*₹?\s*/\s*Unit', re.IGNORECASE)
    RE_PRICE_PATTERNS = [
        re.compile(r"(\d+(?:\.\d{1,3})?)\s*₹\s*/\s*Unit"),
        re.compile(r"₹\s*(\d+(?:\.\d{1,3})?)\s*/\s*Unit"),
        re.compile(r"(\d+(?:\.\d{1,3})?)\s*/\s*Unit"),
    ]

    def _first_number(text: str) -> Decimal | None:
        for pat in RE_PRICE_PATTERNS:
            m = pat.search(text or "")
            if not m:
                continue
            try:
                return Decimal(m.group(1))
            except (InvalidOperation, ValueError):
                continue
        return None

    # --- FAST Selenium (single driver; open batches in parallel tabs) ---
    def _open_tabs(driver, urls):
        handles = []
        if urls:
            driver.get(urls[0])
            handles.append(driver.current_window_handle)
        for u in urls[1:]:
            driver.switch_to.new_window("tab")
            driver.get(u)
            handles.append(driver.current_window_handle)
        return handles

    def _close_extra_tabs(driver, handles):
        if not handles:
            return
        base = handles[0]
        for h in handles[1:]:
            try:
                driver.switch_to.window(h); driver.close()
            except Exception:
                pass
        try:
            driver.switch_to.window(base)
        except Exception:
            pass

    def _body_text(driver):
        try:
            return driver.find_element(By.TAG_NAME, "body").text
        except Exception:
            return ""

    def _poll_batch(driver, mapping):
        """
        mapping: [(state_name, slug), ...]
        returns dict: {state_name: Decimal|None}
        """
        results = {s: None for s, _ in mapping}
        urls = [BASE + slug for _, slug in mapping]
        handles = _open_tabs(driver, urls)
        handle2state = {handles[i]: mapping[i][0] for i in range(len(handles))}
        pending = set(handles)
        deadline = time.time() + BATCH_TIMEOUT

        while pending and time.time() < deadline:
            for h in list(pending):
                try:
                    driver.switch_to.window(h)
                except Exception:
                    pending.discard(h); continue

                txt = _body_text(driver)
                if not txt:
                    continue

                # 1) Strict CURRENT first
                m = CURRENT_RE.search(txt)
                if m:
                    try:
                        results[handle2state[h]] = Decimal(m.group(1))
                    except (InvalidOperation, ValueError):
                        results[handle2state[h]] = None
                    pending.discard(h)
                    continue

                # 2) Generic '/Unit' if YESTERDAY not present
                if "/Unit" in txt and not re.search(r"YESTERDAY", txt, re.IGNORECASE):
                    val = _first_number(txt)
                    if val is not None:
                        results[handle2state[h]] = val
                        pending.discard(h)
                        continue

            time.sleep(0.15)

        _close_extra_tabs(driver, handles)
        return results

    try:
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"[{datetime.datetime.now()}] Starting ALL-States Tariff Snapshot via state pages...")

        # driver: reuse your existing helper if you have one; else minimal fast options here
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.page_load_strategy = "eager"
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # scrape in batches
        items = list(STATE_SLUGS.items())
        results: dict[str, Decimal | None] = {}
        for i in range(0, len(items), BATCH_SIZE):
            chunk = items[i : i + BATCH_SIZE]
            batch_res = _poll_batch(driver, chunk)
            results.update(batch_res)

        # Done with browser
        try:
            driver.quit()
        except Exception:
            pass

        # Build rows for DB
        now = datetime.datetime.now()
        ts = now.replace(second=0, microsecond=0)  # per-minute
        UNIT = "₹/Unit"
        rows = []
        for state_name, value in results.items():
            code = STATE_CODES[state_name]
            price = float(value) if isinstance(value, (Decimal, decimal.Decimal)) else None
            rows.append((ts, code, state_name, price, UNIT, "vidyutpravah.in"))

        # Insert
        if rows:
            conn = get_connection()
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO state_tariff_snapshot (ts, state_code, state_name, tariff, unit, source)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ts, state_code) DO NOTHING;
                """,
                rows,
            )
            conn.commit()
            cur.close()
            conn.close()

        filled = sum(1 for r in rows if r[3] is not None)
        print(f"[SNAPSHOT STORED] Time: {ts} | States: {len(rows)} | With Tariff: {filled} | Missing (N/A or not on page yet): {len(rows)-filled}")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    except Exception as e:
        try:
            driver.quit()
        except Exception:
            pass
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        print(f"[ERROR] scrape_all_states_to_snapshot failed: {e}")
        print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
