from fastapi import APIRouter

from app.db import get_mysql_connection


router = APIRouter()


@router.get("/reasons")
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
