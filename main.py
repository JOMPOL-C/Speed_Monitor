import argparse
import json
import os
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import mysql.connector
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

load_dotenv()  # Load environment variables from .env file if present

BASE_DIR = Path(__file__).resolve().parent
SCHEMA_PATH = BASE_DIR / "schema.sql"
PREVIEW_OUTPUT_PATH = BASE_DIR / "downtime_preview.json"

INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "speedV7")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "dev")

MACHINE_NAME = os.getenv("MACHINE_NAME", "21rb03_1")
FACTORY_TAG = os.getenv("FACTORY_TAG", "factory")
DEFAULT_FACTORY_NAME = os.getenv("DEFAULT_FACTORY_NAME", "Unknown Factory")
MEASUREMENT_NAME = os.getenv("MEASUREMENT_NAME", "machine_speed")
SPEED_FIELD = os.getenv("SPEED_FIELD", "linespeed")
ORDER_FIELD = os.getenv("ORDER_FIELD", "order")
STOP_THRESHOLD = float(os.getenv("STOP_THRESHOLD", "1")) # ค่าความเร็วต่ำกว่านี้ถือว่าเป็นการหยุด
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "6"))
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "1"))
MAX_SAMPLE_GAP_MINUTES = int(os.getenv("MAX_SAMPLE_GAP_MINUTES", "5"))
SYNC_OVERLAP_MINUTES = int(os.getenv("SYNC_OVERLAP_MINUTES", "1"))
STATE_CONTEXT_MINUTES = int(os.getenv("STATE_CONTEXT_MINUTES", "60"))
QUERY_LIMIT = int(os.getenv("QUERY_LIMIT", "1000"))
REQUIRE_ACTIVE_ORDER = os.getenv("REQUIRE_ACTIVE_ORDER", "false").lower(
) == "true"  # ดึง order ที่ active และ no_order ได้
AUTO_CREATE_TABLES = os.getenv("AUTO_CREATE_TABLES", "false").lower(
) == "true"  # สร้างตารางใน MySQL อัตโนมัติถ้ายังไม่มี
APP_TIMEZONE = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Bangkok"))

# ฟังก์ชันสำหรับตรวจสอบว่ามี environment variables ที่จำเป็นสำหรับการเชื่อมต่อ InfluxDB และ MySQL ถูกตั้งค่าไว้ครบถ้วนหรือไม่ หากมีตัวใดขาดหายไปจะแสดงข้อความแสดงข้อผิดพลาดและหยุดการทำงานของโปรแกรม


def require_env(require_mysql: bool = True) -> None:
    required_env = {
        "INFLUX_URL": INFLUX_URL,
        "INFLUX_TOKEN": INFLUX_TOKEN,
        "INFLUX_ORG": INFLUX_ORG,
    }
    if require_mysql:
        required_env.update(
            {
                "MYSQL_HOST": MYSQL_HOST,
                "MYSQL_USER": MYSQL_USER,
                "MYSQL_PASSWORD": MYSQL_PASSWORD,
            }
        )

# เช็คว่ามี environment variables ที่จำเป็นสำหรับการเชื่อมต่อ InfluxDB และ MySQL ถูกตั้งค่าไว้ครบถ้วนหรือไม่ หากมีตัวใดขาดหายไปจะแสดงข้อความแสดงข้อผิดพลาดและหยุดการทำงานของโปรแกรม
    missing_env = [name for name, value in required_env.items() if not value]
    if missing_env:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_env)}"
        )

# ฟังก์ชันสำหรับสร้าง InfluxDB client โดยใช้ค่าการเชื่อมต่อจาก environment variables ที่กำหนดไว้


def get_influx_client() -> InfluxDBClient:
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)


def run_flux_query(query: str) -> list[dict]:
    client = get_influx_client()
    try:
        result = client.query_api().query(query)
        rows: list[dict] = []
        for table in result:
            for record in table.records:
                row = dict(record.values)
                row["time"] = row.get("_time")
                rows.append(row)
        return rows
    finally:
        client.close()

# ดฟังก์ชันสำหรับสร้าง Flux query ที่ใช้ในการดึงข้อมูลตัวอย่างจาก InfluxDB โดยกรองตาม measurement, field, และเครื่องจักร (ถ้ามี) และคำนวณสถานะการหยุด (STOP/RUN) พร้อมกับระยะเวลาที่หยุด (stateDuration) เพื่อใช้ในการตรวจจับเหตุการณ์ downtime ต่อไป
def build_flux_query(machine_name: Optional[str], range_start: str) -> str:
    machine_filter = f'|> filter(fn: (r) => r["machine"] == "{machine_name.lower()}")' if machine_name else ""

    return f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {range_start})
  |> filter(fn:(r)=>
      r._measurement == "{MEASUREMENT_NAME}" and
      (r._field == "{ORDER_FIELD}" or r._field == "{SPEED_FIELD}")
  )
  {machine_filter}
  // 1. ลดจำนวนจุดข้อมูลก่อน Pivot (ถ้าทำได้ เช่น กรองเอาแค่ช่วงที่มีการเปลี่ยนแปลง)
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  // 2. กรองเอาแถวที่ค่า Speed เป็น Null ออกก่อนคำนวณ
  |> filter(fn: (r) => exists r.{SPEED_FIELD})
  |> map(fn:(r)=>({{
        r with
        state: if r.{SPEED_FIELD} < {STOP_THRESHOLD} then "STOP" else "RUN"
  }}))
  // 3. stateDuration จะทำงานเร็วขึ้นถ้าข้อมูลถูกกรองมาดีแล้ว
  |> stateDuration(fn:(r)=> r.state == "STOP", unit:1m)
  |> sort(columns: ["_time"]) 
"""

# ฟังก์ชันสำหรับสร้าง Flux query ที่ใช้ในการดึงข้อมูลเหตุการณ์ downtime
def build_downtime_events_flux_query(machine_name: Optional[str], range_start: str) -> str:
    machine_filter = f'|> filter(fn: (r) => r["machine"] == "{machine_name.lower()}")' if machine_name else ""
    active_order_filter = (
        f'|> filter(fn:(r)=> exists r.{ORDER_FIELD} and string(v: r.{ORDER_FIELD}) != "0")'
        if REQUIRE_ACTIVE_ORDER
        else ""
    )

    return f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: {range_start})
  |> filter(fn:(r)=>
      r._measurement == "{MEASUREMENT_NAME}" and
      (r._field == "{ORDER_FIELD}" or r._field == "{SPEED_FIELD}")
  )
  {machine_filter}
  |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value")
  |> filter(fn:(r)=> exists r.{SPEED_FIELD})
  {active_order_filter}
  |> map(fn:(r)=>({{
        r with
        state: if r.{SPEED_FIELD} < {STOP_THRESHOLD} then "STOP" else "RUN"
  }}))
  |> stateDuration(fn:(r)=> r.state == "STOP", unit:1m)
  |> difference(columns:["stateDuration"])
  |> filter(fn:(r)=> r.stateDuration < 0)
  |> map(fn:(r)=>({{
        r with
        Start: time(v: uint(v: r._time) + uint(v: r.stateDuration * 60000000000)),
        End: r._time,
        Duration_min: -r.stateDuration,
        Order: if exists r.{ORDER_FIELD} and string(v: r.{ORDER_FIELD}) != "0" then string(v: r.{ORDER_FIELD}) else "NO ORDER",
        Event: "STOP"
  }}))
  |> keep(columns:["machine", "{FACTORY_TAG}", "Start", "End", "Duration_min", "Order", "Event"])
  |> sort(columns:["Start"])
"""


def normalize_order_value(value) -> Optional[str]:
    if value is None:
        return None
    order_no = str(value).strip()
    if not order_no or order_no == "0":
        return None
    return order_no


def normalize_influx_datetime(value) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(microsecond=0)
        return value.astimezone(APP_TIMEZONE).replace(tzinfo=None, microsecond=0)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(
        APP_TIMEZONE
    ).replace(tzinfo=None, microsecond=0)


def format_influx_start_time(value: datetime) -> str:
    localized = value.replace(tzinfo=APP_TIMEZONE)
    utc_value = localized.astimezone(timezone.utc)
    return utc_value.strftime("%Y-%m-%dT%H:%M:%SZ")

# ฟังก์ชันสำหรับดึงเวลา endTime ล่าสุดของเหตุการณ์ downtime แยกตามเครื่องจักรจาก MySQL เพื่อใช้เป็นจุดเริ่มต้นในการดึงข้อมูลใหม่จาก InfluxDB ในการซิงค์ข้อมูลแบบ incremental ซึ่งจะช่วยลดปริมาณข้อมูลที่ต้องดึงมาและเพิ่มประสิทธิภาพในการซิงค์ข้อมูล


def get_last_recorded_end_times() -> dict[str, datetime]:
    """หาเวลา endTime ล่าสุดแยกตามเครื่องจาก MySQL เพื่อใช้เป็น watermark ต่อเครื่อง"""
    connection = get_mysql_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT m.machine_code, MAX(e.endTime) AS latest_end_time
            FROM t_downtime_events e
            INNER JOIN m_machine m ON m.machineId = e.machineId
            GROUP BY m.machine_code
            """
        )
        # ดึงผลลัพธ์ทั้งหมดมาเก็บในตัวแปร result ซึ่งจะเป็น list ของ tuple ที่มี machine_code และ latest_end_time
        result = cursor.fetchall()

        return {
            machine_code.lower(): latest_end_time
            for machine_code, latest_end_time in result
            if machine_code and latest_end_time
        }
    finally:
        cursor.close()
        connection.close()


def get_last_recorded_times(
    last_recorded_end_times: Optional[dict[str, datetime]] = None,
) -> dict[str, str]:
    """แปลง watermark เป็นเวลาเริ่ม query โดยถอยกลับเพื่อให้ Flux เห็น context ของ STOP ที่คาบรอบ sync"""
    end_times = last_recorded_end_times or get_last_recorded_end_times()
    rewind_minutes = max(SYNC_OVERLAP_MINUTES, STATE_CONTEXT_MINUTES)
    return {
        machine_code: format_influx_start_time(
            latest_end_time - timedelta(minutes=rewind_minutes)
        )
        for machine_code, latest_end_time in end_times.items()
    }

# ฟังก์ชันสำหรับดึงรายชื่อเครื่องจักรทั้งหมดที่มีข้อมูลใน InfluxDB โดยใช้ Flux query ที่กรองตาม measurement ที่กำหนดไว้ และแปลงผลลัพธ์ให้เป็น list ของ machine codes ที่ไม่ซ้ำกัน


def list_machine_codes() -> list[str]:
    query = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{HISTORY_DAYS}d)
  |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_NAME}")
  |> keep(columns: ["machine"])
  |> group()
  |> distinct(column: "machine")
  |> sort(columns: ["machine"])
"""
    rows = run_flux_query(query)
    return sorted(
        {
            str(row.get("machine") or row.get("_value") or "")
            for row in rows
            if row.get("machine") or row.get("_value")
        }
    )

# ฟังก์ชันหลักสำหรับดึงข้อมูลตัวอย่างจาก InfluxDB สำหรับเครื่องจักรแต่ละเครื่อง โดยใช้ Flux query ที่สร้างขึ้น และแปลงข้อมูลให้เป็นรูปแบบที่เหมาะสมสำหรับการตรวจจับเหตุการณ์ downtime ต่อไป


def fetch_samples_for_machine(machine_name: str, start_time: str) -> list[dict]:
    # บันทึกเวลาที่เริ่มต้นการดึงข้อมูลเพื่อใช้ในการวัดประสิทธิภาพของการดึงข้อมูลจาก InfluxDB
    start_process_time = time.time()
    client = get_influx_client()
    try:
        result = client.query_api().query(build_flux_query(machine_name, start_time))
        rows: list[dict] = []

        for table in result:
            for record in table.records:
                order_no = normalize_order_value(
                    record.values.get(ORDER_FIELD))
                state_dur = record.values.get("stateDuration", 0)

                # ดึงเวลาดิบจาก Influx
                raw_time = record.get_time().astimezone(APP_TIMEZONE)

                # ปรับ Logic: ถ้ากำลังหยุด (state_duration > 0)
                # ให้ปัดวินาทีทิ้งให้เป็นต้นนาที เพื่อเลียนแบบการนับ unit: 1m
                if state_dur > 0:
                    adjusted_time = raw_time.replace(
                        second=0, microsecond=0, tzinfo=None)
                else:
                    adjusted_time = raw_time.replace(
                        microsecond=0, tzinfo=None)

                rows.append({
                    "machine": str(record.values.get("machine") or machine_name),
                    "factory": str(record.values.get(FACTORY_TAG) or DEFAULT_FACTORY_NAME),
                    "time": adjusted_time,
                    "speed": float(record.values[SPEED_FIELD]),
                    "order_no": order_no,
                    "state_duration": state_dur
                })

        rows.sort(key=lambda row: row["time"])

        end_process_time = time.time()
        # คำนวณเวลาที่ใช้ในการประมวลผลการดึงข้อมูล
        duration = end_process_time - start_process_time
        print(
            f"    Fetched {len(rows)} samples for {machine_name} in {duration:.2f} seconds")

        return rows
    finally:
        client.close()


def fetch_downtime_events_for_machine(machine_name: str, start_time: str) -> list[dict]:
    start_process_time = time.time()
    client = get_influx_client()
    try:
        result = client.query_api().query(
            build_downtime_events_flux_query(machine_name, start_time)
        )
        events: list[dict] = []

        for table in result:
            for record in table.records:
                values = record.values
                duration_min = values.get("Duration_min", 0)
                event = {
                    "machine": str(values.get("machine") or machine_name),
                    "factory": str(values.get(FACTORY_TAG) or DEFAULT_FACTORY_NAME),
                    "order_no": normalize_order_value(values.get("Order")),
                    "start_time": normalize_influx_datetime(values.get("Start")),
                    "end_time": normalize_influx_datetime(values.get("End")),
                    "duration_min": round(float(duration_min)),
                    "event": str(values.get("Event") or "STOP"),
                    "source": "influx",
                }
                if event["duration_min"] >= 0:
                    events.append(event)

        events.sort(key=lambda event: event["start_time"])

        duration = time.time() - start_process_time
        print(
            f"    Fetched {len(events)} downtime events for {machine_name} in {duration:.2f} seconds"
        )

        return events
    finally:
        client.close()


# ดึงข้อมูลจาก InfluxDB โดยใช้ Flux query ที่สร้างขึ้น และแปลงข้อมูลให้เป็นรูปแบบที่เหมาะสมสำหรับการตรวจจับเหตุการณ์ downtime ต่อไป
# ฟังก์ชันนี้จะตรวจสอบว่ามีข้อมูล downtime ล่าสุดของเครื่องจักรนั้นๆ ใน MySQL หรือไม่ ถ้ามีจะใช้เวลานั้นเป็นจุดเริ่มต้นในการดึงข้อมูลใหม่จาก InfluxDB เพื่อให้การซิงค์ข้อมูลเป็นแบบ incremental และลดปริมาณข้อมูลที่ต้องดึงมา


def fetch_machine_downtime_events(
    machine_name: Optional[str],
    last_recorded_end_times: Optional[dict[str, datetime]] = None,
) -> list[dict]:
    last_recorded_end_times = last_recorded_end_times or get_last_recorded_end_times()
    last_recorded_times = get_last_recorded_times(last_recorded_end_times)

    if machine_name:
        machine_key = machine_name.lower()
        range_start = last_recorded_times.get(
            machine_key, f"-{HISTORY_DAYS}d")
        sync_mode = "incremental" if machine_key in last_recorded_times else "historical"
        print(
            f"🔄 Syncing {machine_name} downtime events in {sync_mode} mode from: {range_start}")
        return fetch_downtime_events_for_machine(machine_name, range_start)

    events: list[dict] = []
    machine_codes = list_machine_codes()
    print(f"🔄 Syncing all machines: {len(machine_codes)} found")

    total_start = time.time()
    for current_machine in machine_codes:
        machine_key = current_machine.lower()
        range_start = last_recorded_times.get(
            machine_key, f"-{HISTORY_DAYS}d")
        sync_mode = "incremental" if machine_key in last_recorded_times else "historical"
        print(f"  - {current_machine}: {sync_mode} from {range_start} ...",
              end="", flush=True)

        machine_events = fetch_downtime_events_for_machine(
            current_machine, range_start
        )
        events.extend(machine_events)

    total_end = time.time()
    print(f"\n🚀 All machines synced in {total_end - total_start:.2f} seconds")

    events.sort(key=lambda event: (event["machine"], event["start_time"]))
    return events


def fetch_machine_samples(
    machine_name: Optional[str],
    last_recorded_end_times: Optional[dict[str, datetime]] = None,
) -> list[dict]:
    last_recorded_end_times = last_recorded_end_times or get_last_recorded_end_times()
    last_recorded_times = get_last_recorded_times(last_recorded_end_times)

    if machine_name:
        machine_key = machine_name.lower()
        range_start = last_recorded_times.get(
            machine_key, f"-{HISTORY_DAYS}d")
        sync_mode = "incremental" if machine_key in last_recorded_times else "historical"
        print(
            f"🔄 Syncing {machine_name} in {sync_mode} mode from: {range_start}")
        return fetch_samples_for_machine(machine_name, range_start)

    rows: list[dict] = []
    machine_codes = list_machine_codes()
    print(f"🔄 Syncing all machines: {len(machine_codes)} found")

    total_start = time.time()  # จับเวลาเริ่มของทั้งระบบ
    for current_machine in machine_codes:
        machine_key = current_machine.lower()
        range_start = last_recorded_times.get(
            machine_key, f"-{HISTORY_DAYS}d")
        sync_mode = "incremental" if machine_key in last_recorded_times else "historical"
        print(f"  - {current_machine}: {sync_mode} from {range_start} ...",
              end="", flush=True)  # พิมพ์ค้างไว้ก่อน

        machine_rows = fetch_samples_for_machine(current_machine, range_start)
        rows.extend(machine_rows)

    total_end = time.time()
    print(f"\n🚀 All machines synced in {total_end - total_start:.2f} seconds")

    rows.sort(key=lambda row: (row["machine"], row["time"]))
    return rows


def skip_already_recorded_overlap_events(
    events: list[dict],
    last_recorded_end_times: dict[str, datetime],
) -> list[dict]:
    filtered_events = []
    skipped_count = 0

    for event in events:
        latest_end_time = last_recorded_end_times.get(event["machine"].lower())
        if latest_end_time and event["start_time"] <= latest_end_time:
            skipped_count += 1
            continue
        filtered_events.append(event)

    if skipped_count:
        print(
            f"Skipped overlap events already covered by SQL: {skipped_count}"
        )

    return filtered_events


def print_new_event_log(events: list[dict]) -> None:
    if not events:
        print("New downtime events to write: 0")
        return

    print(f"New downtime events to write: {len(events)}")
    for event in events:
        print(
            f"  - {event['machine']} | ORDER: {event['order_no']} | "
            f"START: {event['start_time']} | END: {event['end_time']} | "
            f"DURATION: {event['duration_min']} min"
        )


def inspect_influx() -> None:
    measurements_query = f"""
import "influxdata/influxdb/schema"

schema.measurements(bucket: "{INFLUX_BUCKET}")
"""
    field_keys_query = f"""
import "influxdata/influxdb/schema"

schema.fieldKeys(
  bucket: "{INFLUX_BUCKET}",
  predicate: (r) => r._measurement == "{MEASUREMENT_NAME}",
  start: -{LOOKBACK_HOURS}h
)
"""
    tag_keys_query = f"""
import "influxdata/influxdb/schema"

schema.tagKeys(
  bucket: "{INFLUX_BUCKET}",
  predicate: (r) => r._measurement == "{MEASUREMENT_NAME}",
  start: -{LOOKBACK_HOURS}h
)
"""
    machines_query = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{LOOKBACK_HOURS}h)
  |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_NAME}")
  |> keep(columns: ["machine"])
  |> group()
  |> distinct(column: "machine")
  |> sort(columns: ["machine"])
"""
    sample_query = f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{LOOKBACK_HOURS}h)
  |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_NAME}")
  |> limit(n: 10)
"""

    measurement_rows = run_flux_query(measurements_query)
    field_rows = run_flux_query(field_keys_query)
    tag_rows = run_flux_query(tag_keys_query)
    machine_rows = run_flux_query(machines_query)
    sample_rows = run_flux_query(sample_query)

    measurements = sorted(
        {
            str(
                row.get("_value")
                or row.get("_measurement")
                or row.get("measurement")
                or ""
            )
            for row in measurement_rows
            if (
                row.get("_value")
                or row.get("_measurement")
                or row.get("measurement")
            )
        }
    )
    field_keys = sorted(
        {str(row.get("_value") or "")
         for row in field_rows if row.get("_value")}
    )
    tag_keys = sorted(
        {str(row.get("_value") or "") for row in tag_rows if row.get("_value")}
    )
    machines = sorted(
        {
            str(row.get("machine") or row.get("_value") or "")
            for row in machine_rows
            if row.get("machine") or row.get("_value")
        }
    )

    print("\n=== INFLUX INSPECT ===")
    print(f"Bucket: {INFLUX_BUCKET}")
    print(f"Lookback hours: {LOOKBACK_HOURS}")
    print(f"Configured measurement: {MEASUREMENT_NAME}")
    print(f"Configured machine: {MACHINE_NAME}")
    print(f"Configured speed field: {SPEED_FIELD}")
    print(f"Configured order field: {ORDER_FIELD}")
    print(f"Measurements found ({len(measurements)}): {measurements}")
    print(
        f"Field keys for configured measurement ({len(field_keys)}): {field_keys}")
    print(f"Tag keys for configured measurement ({len(tag_keys)}): {tag_keys}")
    print(
        f"Machines found for configured measurement ({len(machines)}): {machines[:30]}")
    print(f"Sample rows found for configured measurement: {len(sample_rows)}")

    for index, row in enumerate(sample_rows[:5], start=1):
        preview = {
            key: value for key, value in row.items() if key not in {"result", "table"}
        }
        print(f"Sample {index}: {preview}")


def build_event(
    machine: str,
    factory: str,
    order_no: Optional[str],
    start_time,
    end_time,
    event_type: str,
) -> dict:
    duration_min = round((end_time - start_time).total_seconds() / 60)
    return {
        "machine": machine,
        "factory": factory,
        "order_no": order_no,
        "start_time": start_time,
        "end_time": end_time,
        "duration_min": duration_min,
        "event": event_type,
        "source": "influx",
    }


def detect_downtime_events(samples: list[dict]) -> list[dict]:
    if not samples:
        return []

    events, current_event_by_machine, previous_row_by_machine = [], {}, {}
    max_gap = timedelta(minutes=MAX_SAMPLE_GAP_MINUTES)

    for row in samples:
        machine = row["machine"]
        current_event = current_event_by_machine.get(machine)
        previous_row = previous_row_by_machine.get(machine)

        current_order = row["order_no"]
        is_stop = row["speed"] < STOP_THRESHOLD

        # เงื่อนไขสำคัญ: ถ้า REQUIRE_ACTIVE_ORDER เป็น False จะยอมให้ track แม้ไม่มี Order (None)
        # not REQUIRE_ACTIVE_ORDER คือเช็คว่าไม่ต้องการ Order ก็ให้ track ได้เลย ส่วน current_order is not None คือเช็คว่ามี Order หรือไม่ ถ้าไม่มีจะเป็น None ซึ่งถ้า REQUIRE_ACTIVE_ORDER เป็น False ก็ยังให้ track ได้อยู่ดี

        should_track = (not REQUIRE_ACTIVE_ORDER) or (
            current_order is not None)

        if previous_row and current_event:
            if row["time"] - previous_row["time"] > max_gap:
                current_event = None
            elif REQUIRE_ACTIVE_ORDER and current_order is None:
                current_event = None
            elif current_event["order_no"] != current_order:
                current_event = None
            elif not is_stop:
                events.append(
                    build_event(
                        current_event["machine"],
                        current_event["factory"],
                        current_event["order_no"],
                        current_event["start_time"],
                        row["time"],
                        "STOP",
                    )
                )
                current_event = None

        # ถ้าควรจะ track และกำลังหยุดอยู่ แต่ยังไม่มี Event ที่เปิดอยู่ ให้เริ่มบันทึก Event ใหม่
        if should_track and is_stop and current_event is None:
            # เริ่มบันทึก Event ใหม่
            current_event = {
                "machine": row["machine"],
                "factory": row["factory"],
                "order_no": current_order,
                "start_time": row["time"],
            }

        if current_event is None:
            current_event_by_machine.pop(machine, None)
        else:
            current_event_by_machine[machine] = current_event

        previous_row_by_machine[machine] = row

    for machine, current_event in current_event_by_machine.items():
        previous_row = previous_row_by_machine.get(machine)
        if previous_row:
            events.append(
                build_event(
                    current_event["machine"],
                    current_event["factory"],
                    current_event["order_no"],
                    current_event["start_time"],
                    previous_row["time"],
                    "open",
                )
            )

    # คืนค่าเฉพาะเหตุการณ์ที่จบด้วยสถานะ STOP และมีระยะเวลา
    return [e for e in events if e["event"] == "STOP" and e["duration_min"] >= 0]


def write_preview_file(events: list[dict], output_path: Path) -> None:
    serializable_events = [
        {
            **event,
            "start_time": event["start_time"].isoformat(sep=" "),
            "end_time": event["end_time"].isoformat(sep=" "),
        }
        for event in events
    ]
    output_path.write_text(
        json.dumps(serializable_events, indent=2, ensure_ascii=False)
    )


def print_preview_summary(
    fetched_events: list[dict],
    new_events: list[dict],
    output_path: Path,
    scope_label: str,
) -> None:
    machine_count = len({event["machine"] for event in fetched_events})
    print(f"\n=== DOWNTIME PREVIEW ({scope_label}) ===")
    print(f"Influx downtime rows fetched: {len(fetched_events)}")
    print(f"Machines scanned: {machine_count}")
    print(f"New downtime events after overlap filter: {len(new_events)}")
    print(f"Preview file: {output_path}")
    print(f"Require active order: {REQUIRE_ACTIVE_ORDER}")

    for event in new_events[:20]:
        print(
            f"MACHINE: {event['machine']} | ORDER: {event['order_no']} | START: {event['start_time']} | "
            f"END: {event['end_time']} | DURATION: {event['duration_min']} min | "
            f"STATUS: {event['event']}"
        )


def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def ensure_schema(connection) -> None:
    if not AUTO_CREATE_TABLES:
        return

    schema_sql = SCHEMA_PATH.read_text()
    cursor = connection.cursor()
    try:
        for statement in schema_sql.split(";"):
            sql = statement.strip()
            if sql:
                cursor.execute(sql)
        connection.commit()
    finally:
        cursor.close()


def upsert_factories(connection, factory_names: set[str]) -> None:
    if not factory_names:
        return

    sql = """
    INSERT INTO m_factory (factory_name)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE
        factory_name = VALUES(factory_name)
    """
    rows = [(name,) for name in sorted(factory_names)]
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, rows)
        connection.commit()
    finally:
        cursor.close()


def fetch_factory_map(connection) -> dict[str, int]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT factoryId, factory_name FROM m_factory")
        return {factory_name: factory_id for factory_id, factory_name in cursor.fetchall()}
    finally:
        cursor.close()


def get_table_columns(connection, table_name: str) -> set[str]:
    cursor = connection.cursor()
    try:
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        return {column_name for column_name, *_ in cursor.fetchall()}
    finally:
        cursor.close()


def upsert_machines(connection, machine_rows: set[tuple[str, str]]) -> None:
    if not machine_rows:
        return

    factory_map = fetch_factory_map(connection)
    machine_columns = get_table_columns(connection, "m_machine")
    extra_columns = [
        column
        for column in ("machine_type", "plant")
        if column in machine_columns
    ]
    insert_columns = ["factoryId", "machine_code", *extra_columns]
    placeholders = ", ".join(["%s"] * len(insert_columns))
    update_clause = "factoryId = VALUES(factoryId)"
    sql = f"""
    INSERT INTO m_machine ({", ".join(insert_columns)})
    VALUES ({placeholders})
    ON DUPLICATE KEY UPDATE
        {update_clause}
    """
    rows = [
        (
            factory_map[factory_name],
            machine_code,
            *(["Unknown"] * len(extra_columns)),
        )
        for machine_code, factory_name in sorted(machine_rows)
    ]
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, rows)
        connection.commit()
    finally:
        cursor.close()


def fetch_machine_map(connection) -> dict[str, int]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT machineId, machine_code FROM m_machine")
        return {machine_code.lower(): machine_id for machine_id, machine_code in cursor.fetchall()}
    finally:
        cursor.close()


def upsert_orders(connection, order_numbers: set[str]) -> None:
    if not order_numbers:
        return

    sql = """
    INSERT INTO t_order_number (order_no)
    VALUES (%s)
    ON DUPLICATE KEY UPDATE
        order_no = VALUES(order_no)
    """
    rows = [(order_no,) for order_no in sorted(order_numbers)]
    cursor = connection.cursor()
    try:
        cursor.executemany(sql, rows)
        connection.commit()
    finally:
        cursor.close()


def fetch_order_map(connection) -> dict[str, int]:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT orderId, order_no FROM t_order_number")
        return {order_no: order_id for order_id, order_no in cursor.fetchall()}
    finally:
        cursor.close()


def upsert_downtime_events(connection, events: list[dict]) -> int:
    if not events:
        return 0

    upsert_factories(connection, {event["factory"] for event in events})

    upsert_machines(
        connection, {(event["machine"], event["factory"]) for event in events}
    )

    order_numbers = {event["order_no"]
                     for event in events if event["order_no"]}
    upsert_orders(connection, order_numbers)

    machine_map = fetch_machine_map(connection)
    order_map = fetch_order_map(connection) if order_numbers else {}

    sql = """
    INSERT INTO t_downtime_events (
        machineId,
        orderId,
        startTime,
        endTime,
        duration_min,
        event,
        reason_title,
        reason_sub_title,
        note,
        source
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        orderId = VALUES(orderId),
        endTime = VALUES(endTime),
        duration_min = VALUES(duration_min),
        event = VALUES(event),
        source = VALUES(source),
        updated_at = CURRENT_TIMESTAMP
    """
    rows = [
        (
            machine_map[event["machine"].lower()],
            order_map.get(event["order_no"]),
            event["start_time"],
            event["end_time"],
            event["duration_min"],
            event["event"],
            event.get("reason_title", event.get("reason")),
            event.get("reason_sub_title"),
            event.get("note"),
            event["source"],
        )
        for event in events
    ]

    cursor = connection.cursor()
    try:
        cursor.executemany(sql, rows)
        connection.commit()
        return cursor.rowcount
    finally:
        cursor.close()


def print_summary(influx_row_count: int, event_count: int, affected_rows: int, scope_label: str) -> None:
    print(f"\n=== DOWNTIME SYNC ({scope_label}) ===")
    print(f"Influx downtime rows fetched: {influx_row_count}")
    print(f"New downtime events after overlap filter: {event_count}")
    print(f"Inserted/updated rows in SQL: {affected_rows}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from Influx and calculate downtime preview without writing to MySQL.",
    )
    parser.add_argument(
        "--preview-output",
        default=str(PREVIEW_OUTPUT_PATH),
        help="Path to write preview JSON when using --dry-run.",
    )
    parser.add_argument(
        "--inspect-influx",
        action="store_true",
        help="Inspect measurements, tags, fields, and sample rows from InfluxDB.",
    )
    parser.add_argument(
        "--all-machines",
        action="store_true",
        help="Query all machines and preview only the downtime events found.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    machine_name = None if args.all_machines else MACHINE_NAME
    scope_label = "ALL_MACHINES" if args.all_machines else MACHINE_NAME
    require_env(require_mysql=not (args.dry_run or args.inspect_influx))

    if args.inspect_influx:
        inspect_influx()
        return

    last_recorded_end_times = get_last_recorded_end_times()
    fetched_events = fetch_machine_downtime_events(machine_name, last_recorded_end_times)
    events = skip_already_recorded_overlap_events(
        fetched_events, last_recorded_end_times
    )
    print_new_event_log(events)

    if args.dry_run:
        output_path = Path(args.preview_output)
        write_preview_file(events, output_path)
        print_preview_summary(fetched_events, events, output_path, scope_label)
        return

    connection = get_mysql_connection()
    try:
        ensure_schema(connection)
        affected_rows = upsert_downtime_events(connection, events)
    finally:
        connection.close()

    print_summary(len(fetched_events), len(events), affected_rows, scope_label)


if __name__ == "__main__":
    main()
