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
 * Author       :     Tenergy Innovation Co., Ltd.
 * Date         :     4 Sep 2026
 * Revision     :     1.0
 * Rev1.0       :     Original
 * website      :     http://www.tenergyinnovation.co.th
 * Facebook     :     https://www.facebook.com/tenergy.innovation
 * Email        :     uten.boonliam@tenergyinnovation.co.th
 * TEL          :     +6689-140-7205
 ***********************************************************************/
#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

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

/**************************************/
/*           object define            */
/**************************************/
WiFiClient   net;
PubSubClient mqtt(net);

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

/**************************************/
/*         function prototype         */
/**************************************/
void setupWiFi();
void reconnectMqtt();
void onMqttMessage(char *topic, byte *payload, unsigned int len);
void printBleReading(const char *label, JsonDocument &doc);
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
    if (!doc["illuminance_value"].isNull())
      Serial.printf("  illuminance   : %d lux\n", doc["illuminance_value"].as<int>());
    else
      Serial.println(F("  illuminance   : (ไม่มีในข้อความนี้)"));
  }

  // ============================================================
  // DEVICE 5 : เซนเซอร์ Xiaomi BLE #1  (sensor/A4:C1:38:72:DB:01/reading)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านเซนเซอร์ BLE ตัวนี้ <<<
  // ============================================================
  if (id == BLE_SENSOR_1)
  {
    lastSeenBle1 = millis();
    printBleReading("Xiaomi BLE #1  A4:C1:38:72:DB:01", doc);
  }

  // ============================================================
  // DEVICE 6 : เซนเซอร์ Xiaomi BLE #2  (sensor/A4:C1:38:11:A3:D0/reading)
  //   >>> คอมเมนต์ทั้งบล็อกนี้เพื่อหยุดอ่านเซนเซอร์ BLE ตัวนี้ <<<
  // ============================================================
  if (id == BLE_SENSOR_2)
  {
    lastSeenBle2 = millis();
    printBleReading("Xiaomi BLE #2  A4:C1:38:11:A3:D0", doc);
  }
}

/***********************************************************************
 * printBleReading : พิมพ์ค่าจาก payload ของ sensor/{mac}/reading
 *   payload : {"temperature_c","humidity_pct","battery_pct","battery_mv","rssi"}
 ***********************************************************************/
void printBleReading(const char *label, JsonDocument &doc)
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
