/***********************************************************************
 * Project      :     ESP32_MQTT_TUYA  (Tuya Zigbee Gateway - MQTT Reader)
 * Description  :     เชื่อม Wi-Fi + MQTT broker แล้วอ่านค่าจาก 2 กลุ่ม topic
 *                    (อ้างอิง extras/MQTT_GUIDE.md)
 *                    A) อุปกรณ์ Tuya ผ่าน  tuya/{device_id}/status
 *                      1) a3ef45f5befe5df4beufrr  เกตเวย์ ZBS06        (เช็คว่ายัง online อยู่ไหม)
 *                      2) a3d114ff516bb0c823oo4r  สมาร์ทปลั๊ก          (V / mA / W / kWh + switch)
 *                      3) a35db147a188ac4004kpk1  สวิตช์ + AM2301      (อุณหภูมิ / ความชื้น + switch)
 *                      4) a369f740c23f4ef8e52qdw  เซนเซอร์ Motion+แสง  (motion / battery / lux)
 *                    B) เซนเซอร์ Xiaomi BLE ผ่าน  sensor/{mac}/reading  (สแกน BLE ตรง ไม่ผ่านเกตเวย์)
 *                      5) A4:C1:38:72:DB:01                             (temp / humidity / battery / rssi)
 *                      6) A4:C1:38:11:A3:D0                             (temp / humidity / battery / rssi)
 *
 *                    เลือกอ่านเฉพาะบางตัว: คอมเมนต์ทั้งบล็อก "DEVICE n" ใน onMqttMessage()
 *                    (โปรแกรมยัง subscribe "tuya/+/status" + "sensor/+/reading" ครบทุกตัวเสมอ)
 *
 *                    Relay1 บนบอร์ด (GPIO12) สัมพันธ์กับเซนเซอร์ Motion+แสง (DEV_MOTION):
 *                      ตรวจจับการเคลื่อนไหว (motion=true) -> Relay1 ON, ไม่พบ -> Relay1 OFF
 *                    หลัง Relay1 เปลี่ยนสถานะ ต้องรออย่างน้อย 1000 ms จึงจะเปลี่ยนสถานะอีกครั้งได้
 *                    (กันสั่งงานรัวๆ ถ้า Motion Sensor รายงานผลสลับไปมาเร็วเกินไป)
 *
 *                    Relay2 บนบอร์ด (GPIO13) สัมพันธ์กับค่าความสว่าง (illuminance_value) ของ
 *                    เซนเซอร์ตัวเดียวกัน (DEV_MOTION): Lux < 100 (มืด) -> Relay2 ON,
 *                    Lux >= 100 (สว่างพอ) -> Relay2 OFF — ประเมินเฉพาะตอนข้อความมีค่า lux มาด้วย
 *                    (เซนเซอร์ไม่ได้ส่งค่าแสงมาทุกข้อความ) หลัง Relay2 เปลี่ยนสถานะ ต้องรออย่าง
 *                    น้อย 1000 ms จึงจะเปลี่ยนสถานะอีกครั้งได้ เช่นเดียวกับ Relay1
 *
 *                    LCD 16x2 I2C (0x27) แสดงอุณหภูมิ/ความชื้นของเซนเซอร์ Xiaomi BLE:
 *                      แถวที่ 1 (row 0) = BLE_SENSOR_1, แถวที่ 2 (row 1) = BLE_SENSOR_2
 *                    อัปเดตเฉพาะแถวของเซนเซอร์ที่เพิ่งส่งค่าเข้ามา ไม่กระทบอีกแถว
 *
 * Author       :     Tenergy Innovation Co., Ltd.
 * Date         :     5 Sep 2026
 * Revision     :     1.3
 * Rev1.0       :     Original
 * Rev1.1       :     เพิ่ม Relay1 (GPIO12) สัมพันธ์กับ Motion Sensor (DEV_MOTION) + ดีเลย์ 1000 ms
 *                    ก่อนเปลี่ยนสถานะ Relay1 ได้อีกครั้ง กันสลับสถานะโดยไม่ตั้งใจ
 * Rev1.2       :     เพิ่ม Relay2 (GPIO13) สัมพันธ์กับค่า Lux ของเซนเซอร์เดียวกัน (< 100 = ON,
 *                    >= 100 = OFF) + ดีเลย์ 1000 ms ก่อนเปลี่ยนสถานะ Relay2 ได้อีกครั้ง
 * Rev1.3       :     เพิ่ม LCD16x2 I2C แสดงอุณหภูมิ/ความชื้นของเซนเซอร์ Xiaomi BLE ทั้ง 2 ตัว
 * website      :     http://www.tenergyinnovation.co.th
 * Facebook     :     https://www.facebook.com/tenergy.innovation
 * Email        :     uten.boonliam@tenergyinnovation.co.th
 * TEL          :     +6689-140-7205
 ***********************************************************************/
#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <Wire.h>
#include <LiquidCrystal_PCF8574.h>

/**************************************/
/*            Wi-Fi config            */
/**************************************/
#define WIFI_SSID   "TENERGYINNOVATION"
#define WIFI_PASS   "L0vemel0vemydog"

/**************************************/
/*            MQTT config             */
/*   (อ้างอิง extras/MQTT_GUIDE.md §2) */
/**************************************/
#define MQTT_HOST       "192.168.1.221"
#define MQTT_PORT       1883
#define MQTT_USER       "tiny32"
#define MQTT_PASS       "tiny32"
#define MQTT_CLIENT_ID  "esp32-tuya-reader-01"   // ⚠️ ต้องไม่ซ้ำกับ client ตัวอื่น (บอร์ด/mosquitto_sub/dashboard)

// TODO: ก่อน push ขึ้น git สาธารณะ ควรย้าย credential ด้านบนไปไว้ใน secrets.h (ไม่ commit)

/**************************************/
/*    Tuya device id (MQTT_GUIDE §4)   */
/**************************************/
#define DEV_GATEWAY     "a3ef45f5befe5df4beufrr"   // เกตเวย์ ZBS06         (read-only)
#define DEV_SMARTPLUG   "a3d114ff516bb0c823oo4r"   // สมาร์ทปลั๊ก
#define DEV_SWITCH_TH   "a35db147a188ac4004kpk1"   // สวิตช์ + AM2301
#define DEV_MOTION      "a369f740c23f4ef8e52qdw"   // เซนเซอร์ Motion + แสง (read-only)

/**************************************/
/*  Xiaomi BLE sensor MAC (GUIDE §4.5) */
/**************************************/
#define BLE_SENSOR_1    "A4:C1:38:72:DB:01"        // เซนเซอร์ Xiaomi BLE ตัวที่ 1 (read-only)
#define BLE_SENSOR_2    "A4:C1:38:11:A3:D0"        // เซนเซอร์ Xiaomi BLE ตัวที่ 2 (read-only)

/**************************************/
/*           GPIO define              */
/*  (ผังขาตาม extras/arduino_learning_kit_v2.0/Arduino_ESP32_Relay ฯลฯ) */
/**************************************/
#define RELAY_1_PIN     12   // Relay1 บนบอร์ด -> สัมพันธ์กับ Motion Sensor (DEV_MOTION)
#define RELAY_2_PIN     13   // Relay2 บนบอร์ด -> สัมพันธ์กับค่า Lux ของเซนเซอร์เดียวกัน
#define RELAY_ON        LOW  // Relay module เป็นแบบ active-LOW
#define RELAY_OFF       HIGH

/**************************************/
/*          constant define           */
/**************************************/
#define STATUS_TOPIC            "tuya/+/status"     // อุปกรณ์ Tuya ทุกตัว (retained เด้งมาทันที)
#define SENSOR_TOPIC            "sensor/+/reading"  // เซนเซอร์ Xiaomi BLE ทุกตัว (retained)
#define MQTT_BUFFER_SIZE        1024              // payload ของสวิตช์ AM2301 ยาว ~400 ไบต์ (default 256 จะโดนตัดทิ้งเงียบ ๆ)
#define MQTT_KEEPALIVE_SEC      60
#define MQTT_RECONNECT_MS       3000
#define WIFI_CONNECT_TIMEOUT_MS 20000             // ต่อ Wi-Fi ไม่ได้ภายในเวลานี้ -> ESP.restart()
#define LIVENESS_EVERY_MS       5000              // พิมพ์ตารางสถานะ online ทุก ๆ กี่ ms
#define DEVICE_STALE_MS         30000             // ไม่ได้ยินจากอุปกรณ์เกินเวลานี้ = ถือว่า OFFLINE
                                                 // (บริดจ์ poll ทุก ~10 วิ -> 30 วิ = พลาด ~3 รอบ)
#define RELAY1_LOCKOUT_MS       1000              // หลัง Relay1 เปลี่ยนสถานะ ต้องรออย่างน้อยเท่านี้จึงเปลี่ยนอีกครั้งได้
#define RELAY2_LOCKOUT_MS       1000              // หลัง Relay2 เปลี่ยนสถานะ ต้องรออย่างน้อยเท่านี้จึงเปลี่ยนอีกครั้งได้
#define LUX_THRESHOLD           100               // Lux < ค่านี้ = มืด (Relay2 ON), >= ค่านี้ = สว่างพอ (Relay2 OFF)

/**************************************/
/*           object define            */
/**************************************/
WiFiClient   net;
PubSubClient mqtt(net);
LiquidCrystal_PCF8574 lcd(0x27);   // LCD16x2 I2C, SDA=21, SCL=22 (address 0x27)

/**************************************/
/*          global variable           */
/**************************************/
// เวลา (millis) ที่ได้รับข้อมูลล่าสุดของแต่ละอุปกรณ์ — 0 = ยังไม่เคยได้รับ
unsigned long lastSeenGateway  = 0;
unsigned long lastSeenPlug     = 0;
unsigned long lastSeenSwitchTh = 0;
unsigned long lastSeenMotion   = 0;
unsigned long lastSeenBle1     = 0;
unsigned long lastSeenBle2     = 0;

// ----- Relay1 <-> Motion Sensor -----
bool          relay1On        = false;  // สถานะปัจจุบันของ Relay1 (false = OFF)
unsigned long relay1LockUntil = 0;      // เปลี่ยนสถานะได้อีกครั้งเมื่อ millis() >= ค่านี้

// ----- Relay2 <-> ค่า Lux ของ Motion Sensor -----
bool          relay2On        = false;  // สถานะปัจจุบันของ Relay2 (false = OFF)
unsigned long relay2LockUntil = 0;      // เปลี่ยนสถานะได้อีกครั้งเมื่อ millis() >= ค่านี้

/**************************************/
/*         function prototype         */
/**************************************/
void setupWiFi();
void reconnectMqtt();
void onMqttMessage(char *topic, byte *payload, unsigned int len);
void printBleReading(const char *label, JsonDocument &doc, uint8_t lcdRow);
void setRelay1(bool on);
void setRelay2(bool on);
void checkLiveness();
void printLiveness(const char *name, unsigned long lastSeen);

/***********************************************************************
 * setup
 ***********************************************************************/
void setup()
{
  Serial.begin(115200);
  delay(300);
  Serial.println();
  Serial.println(F("==================================================="));
  Serial.println(F("  ESP32_MQTT_TUYA  -  Tuya device MQTT reader"));
  Serial.println(F("==================================================="));

  // Relay1/Relay2 บนบอร์ด : เริ่มต้นดับไว้ก่อน (สั่งจริงตามสถานะ Motion Sensor ที่รับมาทาง MQTT)
  pinMode(RELAY_1_PIN, OUTPUT);
  pinMode(RELAY_2_PIN, OUTPUT);
  digitalWrite(RELAY_1_PIN, RELAY_OFF);
  digitalWrite(RELAY_2_PIN, RELAY_OFF);

  // LCD16x2 I2C : แสดงอุณหภูมิ/ความชื้นของเซนเซอร์ Xiaomi BLE 2 ตัว (แถวละ 1 ตัว)
  Wire.begin();
  Serial.print("Info: LCD16x2 initial....");
  lcd.begin(16, 2);
  lcd.setBacklight(255);
  lcd.clear();
  lcd.setCursor(0, 0);
  lcd.print("S1: waiting...");
  lcd.setCursor(0, 1);
  lcd.print("S2: waiting...");
  Serial.println("done");

  setupWiFi();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setKeepAlive(MQTT_KEEPALIVE_SEC);
  mqtt.setBufferSize(MQTT_BUFFER_SIZE);
  mqtt.setCallback(onMqttMessage);
}

/***********************************************************************
 * loop
 ***********************************************************************/
void loop()
{
  if (WiFi.status() != WL_CONNECTED)
    setupWiFi();

  if (!mqtt.connected())
    reconnectMqtt();

  mqtt.loop();

  static unsigned long lastCheck = 0;
  if (millis() - lastCheck >= LIVENESS_EVERY_MS)
  {
    lastCheck = millis();
    checkLiveness();
  }
}

/***********************************************************************
 * setupWiFi : เชื่อมต่อ Wi-Fi (บล็อกจนต่อได้ หรือครบ timeout แล้ว restart)
 ***********************************************************************/
void setupWiFi()
{
  Serial.printf("\n[WiFi] connecting to \"%s\" ", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED)
  {
    delay(300);
    Serial.print('.');
    if (millis() - start > WIFI_CONNECT_TIMEOUT_MS)
    {
      Serial.println(F("\n[WiFi] timeout -> restart"));
      ESP.restart();
    }
  }
  Serial.printf("\n[WiFi] connected, IP = %s\n", WiFi.localIP().toString().c_str());
}

/***********************************************************************
 * reconnectMqtt : ต่อ broker + subscribe (retained status จะเด้งมาทันที)
 ***********************************************************************/
void reconnectMqtt()
{
  while (!mqtt.connected())
  {
    Serial.printf("[MQTT] connecting to %s:%d ... ", MQTT_HOST, MQTT_PORT);
    if (mqtt.connect(MQTT_CLIENT_ID, MQTT_USER, MQTT_PASS))
    {
      Serial.println(F("connected"));
      mqtt.subscribe(STATUS_TOPIC);
      mqtt.subscribe(SENSOR_TOPIC);
      Serial.printf("[MQTT] subscribed \"%s\" + \"%s\"\n", STATUS_TOPIC, SENSOR_TOPIC);
    }
    else
    {
      Serial.printf("failed rc=%d, retry in %d ms\n", mqtt.state(), MQTT_RECONNECT_MS);
      delay(MQTT_RECONNECT_MS);
    }
  }
}

/***********************************************************************
 * onMqttMessage : callback ทุกครั้งที่มี message เข้ามา
 *   - แยก id + kind จาก topic  (tuya/{id}/status  หรือ  sensor/{mac}/reading)
 *   - สนใจเฉพาะ kind == "status" (Tuya) และ kind == "reading" (Xiaomi BLE)
 *   - แต่ละอุปกรณ์แยกเป็น 1 บล็อก : คอมเมนต์ทั้งบล็อกเพื่อ "หยุดอ่าน" อุปกรณ์นั้น
 ***********************************************************************/
void onMqttMessage(char *topic, byte *payload, unsigned int len)
{
  JsonDocument doc;
  if (deserializeJson(doc, payload, len))
    return; // payload parse ไม่ได้ -> ทิ้ง

  String t(topic);
  int a = t.indexOf('/');
  int b = t.lastIndexOf('/');
  if (a < 0 || b <= a)
    return;
  String id   = t.substring(a + 1, b);   // device_id (Tuya) หรือ MAC (BLE)
  String kind = t.substring(b + 1);      // "status" หรือ "reading"

  if (kind != "status" && kind != "reading")
    return;

  // ============================================================
  // DEVICE 1 : เกตเวย์ ZBS06  (a3ef45f5befe5df4beufrr)
  //   payload ว่าง {} เป็นปกติ — มี message เข้ามา = บริดจ์<->Tuya ยังเชื่อมกันอยู่
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านเกตเวย์ <<<
  // ============================================================
  if (id == DEV_GATEWAY)
  {
    lastSeenGateway = millis();
    Serial.println(F("\n[ZBS06 Gateway] heartbeat  (payload ว่าง = ปกติ, บริดจ์<->Tuya ยังเชื่อมอยู่)"));
  }

  // ============================================================
  // DEVICE 2 : สมาร์ทปลั๊ก  (a3d114ff516bb0c823oo4r)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านสมาร์ทปลั๊ก <<<
  // ============================================================
  if (id == DEV_SMARTPLUG)
  {
    lastSeenPlug = millis();
    bool        sw   = doc["switch_1"]     | false;
    float       volt = doc["cur_voltage"]  | 0.0f;   // V   (บริดจ์หาร 10 แล้ว)
    float       curr = doc["cur_current"]  | 0.0f;   // mA  (ไม่หาร)
    float       pwr  = doc["cur_power"]    | 0.0f;   // W   (บริดจ์หาร 10 แล้ว)
    float       ele  = doc["add_ele"]      | 0.0f;   // kWh (บริดจ์หาร 1000 แล้ว)
    bool        lock = doc["child_lock"]   | false;
    int         cd   = doc["countdown_1"]  | 0;      // วินาที
    const char *rst  = doc["relay_status"] | "-";    // เช่น last / memory / power_on / power_off
    const char *lm   = doc["light_mode"]   | "-";    // โหมดไฟแสดงสถานะที่ตัวเครื่อง (เช่น relay)

    Serial.println(F("\n---- [Smart Plug  a3d114...] --------------------"));
    Serial.printf("  relay        : %s\n", sw ? "ON" : "OFF");
    Serial.printf("  voltage      : %.1f V\n", volt);
    Serial.printf("  current      : %.0f mA\n", curr);
    Serial.printf("  power        : %.1f W\n", pwr);
    Serial.printf("  energy (add) : %.3f kWh\n", ele);
    Serial.printf("  child_lock   : %s\n", lock ? "on" : "off");
    Serial.printf("  countdown_1  : %d s\n", cd);
    Serial.printf("  relay_status : %s\n", rst);
    Serial.printf("  light_mode   : %s\n", lm);
  }

  // ============================================================
  // DEVICE 3 : สวิตช์ + AM2301 วัดอุณหภูมิ/ความชื้น  (a35db147a188ac4004kpk1)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านสวิตช์ตัวนี้ <<<
  // ============================================================
  if (id == DEV_SWITCH_TH)
  {
    lastSeenSwitchTh = millis();
    bool        sw   = doc["switch_1"]       | false;
    float       temp = doc["temp_current"]   | 0.0f;   // °C (บริดจ์หาร 10 แล้ว)
    int         hum  = doc["humidity_value"] | 0;      // %  (ไม่หาร)
    const char *wm   = doc["work_mode"]      | "-";    // โหมดทำงาน (read-only): hot / dehumidify / colding / wet
    JsonArray faults = doc["fault"].as<JsonArray>();

    Serial.println(F("\n---- [Switch + AM2301  a35db1...] --------------"));
    Serial.printf("  relay        : %s\n", sw ? "ON" : "OFF");
    Serial.printf("  temperature  : %.1f C\n", temp);
    Serial.printf("  humidity     : %d %%\n", hum);
    Serial.printf("  work_mode    : %s\n", wm);
    if (faults.size() == 0)
    {
      Serial.println(F("  fault        : []  (ปกติ)"));
    }
    else
    {
      Serial.print(F("  fault        : "));
      for (JsonVariant f : faults)
      {
        Serial.print(f.as<const char *>());
        Serial.print(' ');
      }
      Serial.println();
    }
  }

  // ============================================================
  // DEVICE 4 : เซนเซอร์ Motion + แสง  (a369f740c23f4ef8e52qdw)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านเซนเซอร์ตัวนี้ <<<
  // ============================================================
  if (id == DEV_MOTION)
  {
    lastSeenMotion = millis();
    bool        moving = doc["motion"]             | false;
    const char *pirRaw = doc["pir_state"]         | "-";   // none / pir / presence
    int         batt   = doc["battery_percentage"] | -1;    // %
    const char *sens   = doc["pir_sensitivity"]    | "-";   // low / middle / high
    const char *ptime  = doc["pir_time"]           | "-";   // เวลาหน่วงหลังตรวจไม่พบ (เช่น "30S")
    int         itime  = doc["interval_time"]      | -1;    // ช่วงเวลารายงานผล (วินาที)

    Serial.println(F("\n---- [Motion + Light  a369f7...] --------------"));
    Serial.printf("  motion        : %s\n", moving ? "DETECTED" : "none");
    Serial.printf("  pir_state     : %s\n", pirRaw);
    Serial.printf("  battery       : %d %%\n", batt);
    Serial.printf("  sensitivity   : %s\n", sens);
    Serial.printf("  pir_time      : %s\n", ptime);
    Serial.printf("  interval_time : %d s\n", itime);
    bool hasLux = !doc["illuminance_value"].isNull();
    int  lux    = hasLux ? doc["illuminance_value"].as<int>() : -1;
    if (hasLux)
      Serial.printf("  illuminance   : %d lux\n", lux);
    else
      Serial.println(F("  illuminance   : (ไม่มีในข้อความนี้)"));

    // Relay1 <-> Motion Sensor : เปลี่ยนสถานะเฉพาะตอนค่าจริงต่างจาก Relay1 ปัจจุบัน
    // และพ้นช่วงล็อก 1000 ms แล้วเท่านั้น (กันสลับสถานะถี่เกินไปโดยไม่ตั้งใจ)
    if (moving != relay1On && millis() >= relay1LockUntil)
    {
      setRelay1(moving);
      relay1LockUntil = millis() + RELAY1_LOCKOUT_MS;
    }

    // Relay2 <-> ค่า Lux : ประเมินเฉพาะตอนข้อความนี้มีค่า lux มาด้วย (ไม่ใช่ทุกข้อความ)
    // Lux < LUX_THRESHOLD (มืด) -> ON, Lux >= LUX_THRESHOLD (สว่างพอ) -> OFF
    // เปลี่ยนสถานะเฉพาะตอนต่างจาก Relay2 ปัจจุบัน และพ้นช่วงล็อก 1000 ms แล้วเท่านั้น
    if (hasLux)
    {
      bool wantDark = (lux < LUX_THRESHOLD);
      if (wantDark != relay2On && millis() >= relay2LockUntil)
      {
        setRelay2(wantDark);
        relay2LockUntil = millis() + RELAY2_LOCKOUT_MS;
      }
    }
  }

  // ============================================================
  // DEVICE 5 : เซนเซอร์ Xiaomi BLE #1  (sensor/A4:C1:38:72:DB:01/reading)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านเซนเซอร์ BLE ตัวนี้ <<<
  // ============================================================
  if (id == BLE_SENSOR_1)
  {
    lastSeenBle1 = millis();
    printBleReading("Xiaomi BLE #1  A4:C1:38:72:DB:01", doc, 0);   // LCD แถวที่ 1
  }

  // ============================================================
  // DEVICE 6 : เซนเซอร์ Xiaomi BLE #2  (sensor/A4:C1:38:11:A3:D0/reading)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านเซนเซอร์ BLE ตัวนี้ <<<
  // ============================================================
  if (id == BLE_SENSOR_2)
  {
    lastSeenBle2 = millis();
    printBleReading("Xiaomi BLE #2  A4:C1:38:11:A3:D0", doc, 1);   // LCD แถวที่ 2
  }
}

/***********************************************************************
 * printBleReading : พิมพ์ค่าจาก payload ของ sensor/{mac}/reading ออก Serial
 *   และแสดงอุณหภูมิ/ความชื้นบน LCD16x2 แถวที่กำหนด (lcdRow: 0 หรือ 1)
 *   payload : {"temperature_c","humidity_pct","battery_pct","battery_mv","rssi"}
 ***********************************************************************/
void printBleReading(const char *label, JsonDocument &doc, uint8_t lcdRow)
{
  float tempC = doc["temperature_c"] | 0.0f;   // °C
  int   hum   = doc["humidity_pct"]  | 0;      // %
  int   batt  = doc["battery_pct"]   | -1;     // %
  int   mv    = doc["battery_mv"]    | -1;     // mV
  int   rssi  = doc["rssi"]          | 0;      // dBm (ยิ่งเข้าใกล้ 0 = สัญญาณแรง)

  Serial.printf("\n---- [%s] ----\n", label);
  Serial.printf("  temperature : %.1f C\n", tempC);
  Serial.printf("  humidity    : %d %%\n", hum);
  Serial.printf("  battery     : %d %% (%d mV)\n", batt, mv);
  Serial.printf("  rssi        : %d dBm\n", rssi);

  // LCD16x2 : "S1:30.8C H:67%" เขียนทับเฉพาะแถวของเซนเซอร์ตัวนี้ ไม่กระทบแถวของอีกตัว
  char line[17];
  snprintf(line, sizeof(line), "S%d:%4.1fC H:%2d%%", lcdRow + 1, tempC, hum);
  lcd.setCursor(0, lcdRow);
  lcd.print(line);
}

/***********************************************************************
 * setRelay1 : สั่ง Relay1 บนบอร์ด ON/OFF (active-LOW) + log
 ***********************************************************************/
void setRelay1(bool on)
{
  relay1On = on;
  digitalWrite(RELAY_1_PIN, on ? RELAY_ON : RELAY_OFF);
  Serial.printf("[RELAY1] %s  (motion sensor %s)\n", on ? "ON" : "OFF", on ? "DETECTED" : "no motion");
}

/***********************************************************************
 * setRelay2 : สั่ง Relay2 บนบอร์ด ON/OFF (active-LOW) + log
 ***********************************************************************/
void setRelay2(bool on)
{
  relay2On = on;
  digitalWrite(RELAY_2_PIN, on ? RELAY_ON : RELAY_OFF);
  Serial.printf("[RELAY2] %s  (lux %s %d)\n", on ? "ON" : "OFF", on ? "<" : ">=", LUX_THRESHOLD);
}

/***********************************************************************
 * checkLiveness : พิมพ์ตารางว่าอุปกรณ์แต่ละตัวยัง online อยู่ไหม
 *   (นี่คือคำตอบของ "เกตเวย์ ZBS06 ยังทำงานอยู่หรือไม่" + ความสดของ status อีก 3 ตัว)
 *   หมายเหตุ: ถ้าคอมเมนต์บล็อกอุปกรณ์ใน onMqttMessage() ตัวนั้นจะขึ้น
 *   "ยังไม่เคยได้รับข้อมูล" ที่นี่เอง (จุดคอมเมนต์เดียว ไม่ต้องแก้ 2 ที่)
 ***********************************************************************/
void checkLiveness()
{
  Serial.printf("\n===== Device liveness (stale > %lu s = OFFLINE) =====\n",
                (unsigned long)(DEVICE_STALE_MS / 1000));
  printLiveness("ZBS06 Gateway  ", lastSeenGateway);
  printLiveness("Smart Plug     ", lastSeenPlug);
  printLiveness("Switch + AM2301", lastSeenSwitchTh);
  printLiveness("Motion + Light ", lastSeenMotion);
  printLiveness("Xiaomi BLE #1  ", lastSeenBle1);
  printLiveness("Xiaomi BLE #2  ", lastSeenBle2);
  Serial.println(F("===================================================="));
}

/***********************************************************************
 * printLiveness : ช่วยพิมพ์ 1 บรรทัดของ checkLiveness()
 ***********************************************************************/
void printLiveness(const char *name, unsigned long lastSeen)
{
  if (lastSeen == 0)
  {
    Serial.printf("  %s : -- ยังไม่เคยได้รับข้อมูล --\n", name);
    return;
  }
  unsigned long ageSec = (millis() - lastSeen) / 1000;
  bool online = (millis() - lastSeen) <= DEVICE_STALE_MS;
  Serial.printf("  %s : %-7s (last seen %lu s ago)\n", name, online ? "ONLINE" : "OFFLINE", ageSec);
}
