import os
import json
import csv
import io
import datetime
import smtplib
import requests 
from email.message import EmailMessage
from zoneinfo import ZoneInfo

from apscheduler.triggers.interval import IntervalTrigger
from model.db import get_connection

# ENV 

def _as_bool(v: str, default: bool = True) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y"}

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
SMTP_FROM = os.getenv("SMTP_FROM")
SMTP_TLS = _as_bool(os.getenv("SMTP_TLS", "true"), True)

APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Asia/Kolkata"))
SUBJECT_PREFIX = os.getenv("REPORT_MAIL_SUBJECT_PREFIX", "EMS Report")

API_URL = "http://localhost:5179/api/device/data" 


# CSV Helpers

def _flatten(obj, prefix=""):
    rows = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            rows.extend(_flatten(v, key))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            rows.extend(_flatten(v, key))
    else:
        rows.append((prefix, "" if obj is None else obj))
    return rows


def _config_to_csv_bytes(config: dict, report_name: str):

    rows = _flatten(config or {})

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["key", "value"])

    for k, v in rows:
        if not isinstance(v, str):
            v = json.dumps(v)
        writer.writerow([k, v])

    data = buf.getvalue().encode("utf-8")
    buf.close()
    return data, f"{report_name}_config.csv"


def _data_to_csv_bytes(data: list, report_name: str):

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)

    if not data:
        writer.writerow(["No data available"])
    else:
        # Ensure 'timestamp' is always first column
        all_headers = list(data[0].keys())
        if "timestamp" in all_headers:
            all_headers.remove("timestamp")
            headers = ["timestamp"] + all_headers
        else:
            headers = all_headers
        
        writer.writerow(headers)
        for row in data:
            writer.writerow([row.get(h) for h in headers])

    data_bytes = buf.getvalue().encode("utf-8")
    buf.close()
    return data_bytes, f"{report_name}_data.csv"


# MAIL 

def _send_full_email(to_email: str, report_name: str, config: dict):

    print(f"Sending mail → {to_email}")

    try:
        cfg = config if isinstance(config, dict) else json.loads(config)
    except:
        cfg = {}

    # API CALL (CONTEXT SAFE)
    try:
        response = requests.post(API_URL, json=cfg, timeout=20)
        data = response.json() if response.ok else []
    except Exception as api_err:
        print(f"API ERROR → {api_err}")
        data = []

    msg = EmailMessage()
    msg["Subject"] = f"{SUBJECT_PREFIX}: {report_name}"
    msg["From"] = SMTP_FROM or SMTP_USER
    msg["To"] = to_email

    msg.set_content("Auto scheduled EMS report attached (Data).")

    # config_bytes, config_name = _config_to_csv_bytes(cfg, report_name)
    # msg.add_attachment(config_bytes, maintype="text", subtype="csv", filename=config_name)

    data_bytes, data_name = _data_to_csv_bytes(data or [], report_name)
    msg.add_attachment(data_bytes, maintype="text", subtype="csv", filename=data_name)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        if SMTP_TLS:
            smtp.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            smtp.login(SMTP_USER, SMTP_PASSWORD)
        smtp.send_message(msg)

    print(" MAIL SENT")


# CORE JOB

def dispatch_scheduled_reports():

    print("DISPATCHER RUNNING")

    now = datetime.datetime.now(APP_TZ)
    current_hour = now.hour
    current_minute = (now.minute // 5) * 5
    today = now.date()

    print(f"CURRENT TIME = {current_hour}:{current_minute:02d}")

    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT r.report_name, r.configuration, r.email
            FROM report_table r
            WHERE r.is_scheduled=TRUE
            AND r.email IS NOT NULL
            AND r.schedule_time=%s
            AND COALESCE(r.schedule_minute,0)=%s
            AND NOT EXISTS (
                SELECT 1 FROM report_delivery_log l
                WHERE l.report_name = r.report_name
                AND l.run_date = %s
                AND l.status = 'sent'
            )
            """,
            (current_hour, current_minute, today),
        )

        rows = cur.fetchall()
        print(f"ROWS FOUND = {len(rows)}")

        sent_names = set()

        for report_name, configuration, email in rows:

            if report_name in sent_names:
                print(f"Duplicate report_name skip → {report_name}")
                continue

            sent_names.add(report_name)

            status, error = "sent", None

            try:
                _send_full_email(email, report_name, configuration)
            except Exception as e:
                status, error = "failed", str(e)
                print(f"MAIL ERROR → {e}")

            try:
                cur.execute(
                    """
                    INSERT INTO report_delivery_log(report_name,run_date,status,error)
                    VALUES(%s,%s,%s,%s)
                    """,
                    (report_name, today, status, error),
                )
            except Exception as db_err:
                print(f" LOG INSERT SKIPPED → {db_err}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"DB ERROR → {e}")

    finally:
        cur.close()
        conn.close()


# REGISTER 

def schedule_report_dispatcher(scheduler):

    print("REGISTERING REPORT DISPATCHER JOB")

    scheduler.add_job(
        dispatch_scheduled_reports,
        trigger=IntervalTrigger(seconds=10),
        id="report_dispatcher_realtime",
        replace_existing=True,
        max_instances=1,
    )