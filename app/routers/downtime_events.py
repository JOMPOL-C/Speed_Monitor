from datetime import datetime
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.db import get_mysql_connection


router = APIRouter()


class ReasonUpdatePayload(BaseModel):
    reason_title: Optional[str] = Field(default=None, max_length=255)
    reason_sub_title: Optional[str] = Field(default=None, max_length=255)
    note: Optional[str] = None


def normalize_duration(row: dict) -> dict:
    duration_value = row.get("duration_min")
    if isinstance(duration_value, Decimal):
        row["duration_min"] = float(duration_value)
    return row


@router.get("/downtime-events")
def list_downtime_events(
    machine_code: Optional[str] = None,
    order_no: Optional[str] = None,
    date_from: Optional[datetime] = None,
    date_to: Optional[datetime] = None,
):
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        where_clauses = ["1=1"]
        params = []

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
            e.reason_title,
            e.reason_sub_title,
            e.note,
            e.source,
            e.updated_at
        FROM t_downtime_events e
        INNER JOIN m_machine m ON m.machineId = e.machineId
        INNER JOIN m_factory f ON f.factoryId = m.factoryId
        LEFT JOIN t_order_number o ON o.orderId = e.orderId
        WHERE {" AND ".join(where_clauses)}
        ORDER BY e.downtimeId DESC
        """

        cursor.execute(sql, params)
        return [normalize_duration(row) for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()


@router.get("/downtime-events/{downtime_id}")
def get_downtime_event(downtime_id: int):
    connection = get_mysql_connection()
    cursor = connection.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT
                e.downtimeId,
                f.factory_name,
                m.machine_code,
                o.order_no,
                e.startTime,
                e.endTime,
                e.duration_min,
                e.event,
                e.reason_title,
                e.reason_sub_title,
                e.note,
                e.source,
                e.updated_at
            FROM t_downtime_events e
            INNER JOIN m_machine m ON m.machineId = e.machineId
            INNER JOIN m_factory f ON f.factoryId = m.factoryId
            LEFT JOIN t_order_number o ON o.orderId = e.orderId
            WHERE e.downtimeId = %s
            """,
            (downtime_id,),
        )
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail="Downtime event not found")

        return normalize_duration(row)
    finally:
        cursor.close()
        connection.close()


@router.patch("/downtime-events/{downtime_id}/reason")
def update_downtime_reason(downtime_id: int, payload: ReasonUpdatePayload):
    connection = get_mysql_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            UPDATE t_downtime_events
            SET
                reason_title = %s,
                reason_sub_title = %s,
                note = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE downtimeId = %s
            """,
            (
                payload.reason_title,
                payload.reason_sub_title,
                payload.note,
                downtime_id,
            ),
        )
        connection.commit()

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Downtime event not found")

        return {
            "message": "Downtime reason updated",
            "downtimeId": downtime_id,
            "reason_title": payload.reason_title,
            "reason_sub_title": payload.reason_sub_title,
            "note": payload.note,
        }
    finally:
        cursor.close()
        connection.close()
