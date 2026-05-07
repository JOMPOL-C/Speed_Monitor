# Create the content for api.md
api_docs_content = """# Speed Monitor API Documentation

เอกสารประกอบการใช้งาน API สำหรับระบบ Speed Monitor เพื่อเชื่อมต่อระหว่าง Backend และ Frontend

## Base URL
`http://192.168.21.243:8000`

---

## 1. Get Downtime Events
ใช้สำหรับดึงรายการเหตุการณ์หยุดของเครื่องจักรทั้งหมด หรือกรองตามเงื่อนไข

- **Endpoint:** `/api/downtime-events`
- **Method:** `GET`
- **Query Parameters:**
    - `machine_code` (string, optional): รหัสเครื่องจักร (เช่น `21rb03_2`)
    - `order_no` (string, optional): เลขที่ใบสั่งผลิต
    - `date_from` (datetime, optional): วันที่เริ่ม (รูปแบบ ISO 8601)
    - `date_to` (datetime, optional): วันที่สิ้นสุด
- **Success Response (200 OK):**
    ```json
    [
      {
        "downtimeId": 7584,
        "factory_name": "Factory A",
        "machine_code": "21rb03_2",
        "order_no": "111000096974",
        "startTime": "2026-04-24T09:19:10",
        "endTime": "2026-04-24T09:46:10",
        "duration_min": 27.0,
        "event": "STOP",
        "reason_title": null,
        "reason_sub_title": null,
        "note": null,
        "source": "influx",
        "updated_at": "2026-04-24T14:09:02"
      }
    ]
    ```

---

## 2. Update Downtime Reason
ใช้สำหรับบันทึกหรือแก้ไขเหตุผลการหยุดของเหตุการณ์นั้นๆ

- **Endpoint:** `/api/downtime-events/{downtime_id}/reason`
- **Method:** `PATCH`
- **Path Parameter:**
    - `downtime_id` (integer): ID ของรายการที่ต้องการแก้ไข
- **Request Body:**
    ```json
    {
      "reason_title": "สายพานขาด",
      "reason_sub_title": "รอช่างซ่อม",
      "note": "แจ้งซ่อมแล้ว รออะไหล่"
    }
    ```
- **Success Response (200 OK):**
    ```json
    {
      "message": "Downtime reason updated",
      "downtimeId": 7584,
      "reason_title": "สายพานขาด",
      "reason_sub_title": "รอช่างซ่อม",
      "note": "แจ้งซ่อมแล้ว รออะไหล่"
    }
    ```

---

## 3. List Master Reasons
ดึงรายชื่อเหตุผลมาตรฐานจากฐานข้อมูล (ถ้ามี)

- **Endpoint:** `/api/reasons`
- **Method:** `GET`
- **Success Response (200 OK):** รายการรหัสและชื่อเหตุผล

---

## 4. Health Check
ตรวจสอบสถานะการทำงานของ API

- **Endpoint:** `/health`
- **Method:** `GET`
- **Success Response:** `{"status": "ok"}`

---

การ Run แบบ Service โดยใช้เครื่องมือ launchd


```bash
plutil -lint ~/Library/LaunchAgents/com.speedmonitor.sync.plist // ถ้าขึ้น OK แสดงว่าโครงสร้าง XML ถูกต้อง

nano ~/Library/LaunchAgents/com.speedmonitor.sync.plist
```

```XML
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.speedmonitor.sync</string>

    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/Users/sutthiphong/Documents/GitHub/Speed_Monitor/main.py</string>
        <string>--all-machines</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/Users/sutthiphong/Documents/GitHub/Speed_Monitor</string>

    <key>RunAtLoad</key>
    <true/>

    <key>StartInterval</key>
    <integer>60</integer> // กำหนดเวลาที่จะรันทุกๆกี่วินาที

    <key>StandardOutPath</key>
    <string>/Users/sutthiphong/Documents/GitHub/Speed_Monitor/sync_stdout.log</string>

    <key>StandardErrorPath</key>
    <string>/Users/sutthiphong/Documents/GitHub/Speed_Monitor/sync_stderr.log</string>
</dict>
</plist>
```

# วิธีบันทึก
- กด Control + O (เพื่อสั่ง WriteOut หรือบันทึก)
- กด Enter (เพื่อยืนยันชื่อไฟล์)
- กด Control + X (เพื่อออกจากโปรแกรม nano)

# โหลดไฟล์เข้าไปในระบบ Launchd ( เปิดใช้งาน Service )
- launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.speedmonitor.sync.plist

# ยกเลิกของเก่า ( ปิดใช้งาน Service )
- launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.speedmonitor.sync.plist


# สั่งให้ Service เริ่มทำงานทันที
launchctl kickstart -k gui/$(id -u)/com.speedmonitor.sync


- launchctl print gui/$(id -u)/com.speedmonitor.sync


# โชว์ log
tail -f /Users/sutthiphong/Documents/GitHub/Speed_Monitor/sync_stdout.log

# เป็น background service ของ macOS ผ่าน launchd 
## service นี้ทำหน้าที่: 

1. ดึงเวลาล่าสุดของ downtime แต่ละเครื่องจาก MySQL
2. ส่ง Flux query ไปให้ InfluxDB คำนวณ downtime
3. รับ downtime event ที่คำนวณเสร็จแล้วกลับมา
4. กรอง event ที่เคยบันทึกแล้วออก
5. insert/update event ใหม่ลง MySQL
6. เขียน log การทำงานลงไฟล์ sync_stdout.log และ sync_stderr.log