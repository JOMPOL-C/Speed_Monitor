import argparse
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import sys
import mysql.connector
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient

load_dotenv() # Load environment variables from .env file if present

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

MACHINE_NAME = os.getenv("MACHINE_NAME", "11bc03")
FACTORY_TAG = os.getenv("FACTORY_TAG", "factory")
DEFAULT_FACTORY_NAME = os.getenv("DEFAULT_FACTORY_NAME", "Unknown Factory")
MEASUREMENT_NAME = os.getenv("MEASUREMENT_NAME", "machine_speed")
SPEED_FIELD = os.getenv("SPEED_FIELD", "linespeed")
ORDER_FIELD = os.getenv("ORDER_FIELD", "order")
STOP_THRESHOLD = float(os.getenv("STOP_THRESHOLD", "1"))
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS", "6"))
MAX_SAMPLE_GAP_MINUTES = int(os.getenv("MAX_SAMPLE_GAP_MINUTES", "5"))
QUERY_LIMIT = int(os.getenv("QUERY_LIMIT", "1000"))
REQUIRE_ACTIVE_ORDER = os.getenv("REQUIRE_ACTIVE_ORDER", "true").lower() == "true"
AUTO_CREATE_TABLES = os.getenv("AUTO_CREATE_TABLES", "true").lower() == "true"
APP_TIMEZONE = ZoneInfo(os.getenv("APP_TIMEZONE", "Asia/Bangkok"))


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


def build_flux_query() -> str:
    return f"""
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{LOOKBACK_HOURS}h)
  |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_NAME}")
  |> filter(fn: (r) => r["machine"] == "{MACHINE_NAME}")
  |> filter(fn: (r) => r["_field"] == "{SPEED_FIELD}" or r["_field"] == "{ORDER_FIELD}")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => exists r["{SPEED_FIELD}"])
  |> sort(columns: ["_time"])
  |> tail(n: {QUERY_LIMIT})
"""


def normalize_order_value(value) -> Optional[str]:
    if value is None:
        return None
    order_no = str(value).strip()
    if not order_no or order_no == "0":
        return None
    return order_no

# การดึงข้อมูลตัวอย่างจาก InfluxDB โดยใช้ Flux query ที่กำหนดไว้ และแปลงผลลัพธ์เป็น list ของ dict ที่มีข้อมูลที่จำเป็นสำหรับการตรวจจับ downtime events
def fetch_machine_samples() -> list[dict]:
    client = get_influx_client()
    try:
        result = client.query_api().query(build_flux_query())
        rows: list[dict] = []
        last_order_no: Optional[str] = None

        for table in result:
            for record in table.records:
                order_no = normalize_order_value(record.values.get(ORDER_FIELD))
                if order_no is not None:
                    last_order_no = order_no

                rows.append(
                    {
                        "machine": str(record.values.get("machine") or MACHINE_NAME),
                        "factory": str(
                            record.values.get(FACTORY_TAG) or DEFAULT_FACTORY_NAME
                        ),
                        "time": record.get_time().astimezone(APP_TIMEZONE).replace(
                            tzinfo=None
                        ),
                        "speed": float(record.values[SPEED_FIELD]),
                        "order_no": order_no or last_order_no,
                    }
                )

        rows.sort(key=lambda row: row["time"])
        return rows
    finally:
        client.close()


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
        {str(row.get("_value") or "") for row in field_rows if row.get("_value")}
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
    print(f"Field keys for configured measurement ({len(field_keys)}): {field_keys}")
    print(f"Tag keys for configured measurement ({len(tag_keys)}): {tag_keys}")
    print(f"Machines found for configured measurement ({len(machines)}): {machines[:30]}")
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
    duration_min = round((end_time - start_time).total_seconds() / 60, 2)
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

    events: list[dict] = []
    current_event: Optional[dict] = None
    previous_row: Optional[dict] = None
    max_gap = timedelta(minutes=MAX_SAMPLE_GAP_MINUTES)

    for row in samples:
        if previous_row is not None and current_event is not None:
            if row["time"] - previous_row["time"] > max_gap:
                events.append(
                    build_event(
                        current_event["machine"],
                        current_event["factory"],
                        current_event["order_no"],
                        current_event["start_time"],
                        previous_row["time"],
                        "closed_by_gap",
                    )
                )
                current_event = None
            elif (
                row["order_no"] is not None
                and current_event["order_no"] is not None
                and row["order_no"] != current_event["order_no"]
            ):
                events.append(
                    build_event(
                        current_event["machine"],
                        current_event["factory"],
                        current_event["order_no"],
                        current_event["start_time"],
                        previous_row["time"],
                        "closed_by_order_change",
                    )
                )
                current_event = None
            elif REQUIRE_ACTIVE_ORDER and row["order_no"] is None:
                events.append(
                    build_event(
                        current_event["machine"],
                        current_event["factory"],
                        current_event["order_no"],
                        current_event["start_time"],
                        previous_row["time"],
                        "closed_by_order_missing",
                    )
                )
                current_event = None

        should_track_row = (not REQUIRE_ACTIVE_ORDER) or (row["order_no"] is not None)

        if (
            should_track_row
            and row["speed"] <= STOP_THRESHOLD
            and current_event is None
        ):
            current_event = {
                "machine": row["machine"],
                "factory": row["factory"],
                "order_no": row["order_no"],
                "start_time": row["time"],
            }
        elif row["speed"] > STOP_THRESHOLD and current_event is not None:
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

        previous_row = row

    if current_event is not None and previous_row is not None:
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

    return [event for event in events if event["duration_min"] >= 0]


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


def print_preview_summary(samples: list[dict], events: list[dict], output_path: Path) -> None:
    print(f"\n=== DOWNTIME PREVIEW ({MACHINE_NAME}) ===")
    print(f"Influx samples fetched: {len(samples)}")
    print(f"Detected downtime events: {len(events)}")
    print(f"Preview file: {output_path}")
    print(f"Require active order: {REQUIRE_ACTIVE_ORDER}")

    for event in events[:20]:
        print(
            f"ORDER: {event['order_no']} | START: {event['start_time']} | "
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
        for _ in cursor.execute(schema_sql, multi=True):
            pass
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


def upsert_machines(connection, machine_rows: set[tuple[str, str]]) -> None:
    if not machine_rows:
        return

    factory_map = fetch_factory_map(connection)
    sql = """
    INSERT INTO m_machine (factoryId, machine_code)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        factoryId = VALUES(factoryId)
    """
    rows = [
        (factory_map[factory_name], machine_code)
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
        return {machine_code: machine_id for machine_id, machine_code in cursor.fetchall()}
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

    order_numbers = {event["order_no"] for event in events if event["order_no"]}
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
        source
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
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
            machine_map[event["machine"]],
            order_map.get(event["order_no"]),
            event["start_time"],
            event["end_time"],
            event["duration_min"],
            event["event"],
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


def print_summary(sample_count: int, event_count: int, affected_rows: int) -> None:
    print(f"\n=== DOWNTIME SYNC ({MACHINE_NAME}) ===")
    print(f"Influx samples fetched: {sample_count}")
    print(f"Detected downtime events: {event_count}")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    require_env(require_mysql=not (args.dry_run or args.inspect_influx))

    if args.inspect_influx:
        inspect_influx()
        return

    samples = fetch_machine_samples()
    events = detect_downtime_events(samples)

    if args.dry_run:
        output_path = Path(args.preview_output)
        write_preview_file(events, output_path)
        print_preview_summary(samples, events, output_path)
        return

    connection = get_mysql_connection()
    try:
        ensure_schema(connection)
        affected_rows = upsert_downtime_events(connection, events)
    finally:
        connection.close()

    print_summary(len(samples), len(events), affected_rows)


if __name__ == "__main__":
    main()
