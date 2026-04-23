from datetime import datetime
from decimal import Decimal
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from main import get_mysql_connection

app = FastAPI(title="Speed Monitor API", version="1.0.0")

class ReasonUpdatePayload(BaseModel): 
    reason_code: Optional[str] = Field(default=None, max_length=50)
    reason: Optional[str] = Field(default=None, max_length=255)


@app.get("/health")
def healthcheck():
    return {"status": "ok"}

@app.get("/api/reasons")
def list_reasons():
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT code, name, is_active
            FROM downtime_reason_master
            WHERE is_active = 1
            ORDER BY name
            """
        )
        return cursor.fetchall()
    finally:
        cursor.close()
        connection.close()


@app.get("/api/downtime-events")
def list_downtime_events(
    machine_code: Optional[str] = None,
    order_no: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
):
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        # 1. กำหนดค่า Lookback เริ่มต้น (เช่น 24 ชั่วโมง) และเตรียม list สำหรับ params
        lookback_hours = 24 
        where_clauses = ["e.startTime >= DATE_SUB(NOW(), INTERVAL %s HOUR)"]
        params = [lookback_hours] # ต้องมีค่าไปแทน %s ตัวแรกเสมอ

        if machine_code:
            where_clauses.append("m.machine_code = %s")
            params.append(machine_code)

        if order_no:
            where_clauses.append("o.order_no = %s")
            params.append(order_no)

        if date_from:
            where_clauses.append("e.startTime >= %s")
            params.append(date_from)

        if date_to:
            where_clauses.append("e.endTime <= %s")
            params.append(date_to)

        sql = f"""
        SELECT
            e.downtimeId,
            f.factory_name,
            m.machine_code,
            o.order_no,
            e.startTime,
            e.endTime,
            e.duration_min,
            e.event,
            e.reason_code,
            e.reason,
            e.source,
            e.updated_at
        FROM t_downtime_events e
        INNER JOIN m_machine m ON m.machineId = e.machineId
        INNER JOIN m_factory f ON f.factoryId = m.factoryId
        LEFT JOIN t_order_number o ON o.orderId = e.orderId
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.downtimeId DESC
        """ 

        # 2. ต้องส่ง params เข้าไปด้วยเสมอเมื่อใน sql มี %s
        cursor.execute(sql, params) 
        rows = cursor.fetchall()
        
        for row in rows:
            duration_value = row.get("duration_min")
            if isinstance(duration_value, Decimal):
                row["duration_min"] = float(duration_value)
        return rows
    finally:
        cursor.close()
        connection.close()

@app.patch("/api/downtime-events/{downtime_id}/reason")
def update_downtime_reason(downtime_id: int, payload: ReasonUpdatePayload):
    connection = get_mysql_connection()
    cursor = connection.cursor()
    try:
        if payload.reason_code:
            cursor.execute(
                """
                SELECT 1
                FROM downtime_reason_master
                WHERE code = %s AND is_active = 1
                """,
                (payload.reason_code,),
            )
            if cursor.fetchone() is None:
                raise HTTPException(status_code=400, detail="Invalid reason_code")

        cursor.execute(
            """
            UPDATE t_downtime_events
            SET
                reason_code = %s,
                reason = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE downtimeId = %s
            """,
            (payload.reason_code, payload.reason, downtime_id),
        )
        connection.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Downtime event not found")

        return {
            "message": "Downtime reason updated",
            "downtimeId": downtime_id,
            "reason_code": payload.reason_code,
            "reason": payload.reason,
        }
    finally:
        cursor.close()
        connection.close()
        
