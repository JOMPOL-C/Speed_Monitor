# Speed_Monitor

โปรเจกต์นี้ทำ flow แบบ lean ตามนี้:

1. ดึง `linespeed` และ `order` จาก InfluxDB โดยตรง
2. คำนวณ downtime ใน Python
3. บันทึกเฉพาะผลลัพธ์ลง `t_downtime_events`
4. เปิด API ให้ frontend อ่าน downtime และอัปเดต reason ได้

## โครงสร้างข้อมูล

- `m_factory`: master โรงงาน
- `m_machine`: master เครื่องจักร
- `t_order_number`: master order
- `t_downtime_events`: เก็บช่วง downtime ที่ SQL คำนวณได้
- `downtime_reason_master`: รายการ reason ที่ให้ frontend เลือก

แนวทางนี้ตั้งใจไม่เก็บ raw sample จาก Influx ลง MySQL เพื่อลดขนาดฐานข้อมูล

## Environment

สร้างไฟล์ `.env`

```env
INFLUX_URL=http://192.168.23.32:8086
INFLUX_TOKEN=your_token
INFLUX_ORG=your_org
INFLUX_BUCKET=speedV7

MYSQL_HOST=192.168.23.32
MYSQL_PORT=3306
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=dev

MACHINE_NAME=11dw01
FACTORY_TAG=factory
DEFAULT_FACTORY_NAME=Factory A
MEASUREMENT_NAME=machine_speed
SPEED_FIELD=linespeed
ORDER_FIELD=order
STOP_THRESHOLD=1
LOOKBACK_HOURS=6
MAX_SAMPLE_GAP_MINUTES=5
QUERY_LIMIT=5000
REQUIRE_ACTIVE_ORDER=true
AUTO_CREATE_TABLES=true
APP_TIMEZONE=Asia/Bangkok
```

## Install

```bash
pip install influxdb-client mysql-connector-python python-dotenv fastapi uvicorn
```

## Run Sync

```bash
python3 main.py
```

## Run Preview Without SQL

```bash
python3 main.py --dry-run
```

ไฟล์ preview จะถูกสร้างที่ [downtime_preview.json](/Users/sutthiphong/Documents/GitHub/Speed_Monitor/downtime_preview.json) เพื่อใช้เช็ก `start_time`, `end_time`, `duration_min` ก่อนลง SQL

ถ้า `REQUIRE_ACTIVE_ORDER=true` ระบบจะนับ downtime เฉพาะช่วงที่มี `order` เท่านั้น เพื่อไม่ให้เครื่องว่างหรือไม่มีงานถูกตีเป็น downtime production

ถ้าต้องการ preview ทุกเครื่องแล้วให้แสดงเฉพาะ event ที่เจอ:

```bash
python3 main.py --dry-run --all-machines
```

## Inspect Influx

```bash
python3 main.py --inspect-influx
```

โหมดนี้ใช้ดูว่าใน Influx มี `measurement`, `field`, `tag`, และค่า `machine` อะไรจริงบ้าง เพื่อเอาไปตั้ง `.env` ให้ตรง

## Run API

```bash
uvicorn api:app --reload
```

## API ที่ใช้กับ Frontend

- `GET /health`
- `GET /api/reasons`
- `GET /api/downtime-events?machine_code=11dw01`
- `PATCH /api/downtime-events/{downtimeId}/reason`

ตัว sync จะไม่ล้าง `reason_code` และ `reason` ที่ user กรอกไว้ ถ้า downtime event เดิมถูกคำนวณซ้ำในรอบ sync ใหม่ครับ
