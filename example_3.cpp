/***********************************************************************
 * Project      : ESP32_MQTT_TUYA (Xiaomi BLE temperature auto control)
 * Description  : ควบคุมอุปกรณ์ Tuya อัตโนมัติด้วยอุณหภูมิจากเซนเซอร์ Xiaomi BLE
 *
 *   BLE sensor #1 (A4:C1:38:72:DB:01) -> Smart Plug (DEV_SMARTPLUG)
 *   BLE sensor #2 (A4:C1:38:11:A3:D0) -> Switch + AM2301 (DEV_SWITCH_TH)
 *
 *   กติกาของแต่ละคู่:
 *     temperature_c > ค่า threshold  : สั่งเปิด switch_1
 *     temperature_c < ค่า threshold  : สั่งปิด switch_1
 *     temperature_c = ค่า threshold  : คงสถานะเดิม ไม่ส่งคำสั่งใหม่
 *
 *   การคงสถานะเมื่อค่าเท่ากับ threshold ป้องกัน relay สลับไปมาเมื่ออุณหภูมิ
 *   แกว่งพอดีกับจุดตั้งค่า ตัวโปรแกรมยังตรวจ ack/status ทุกครั้งที่สั่งงาน
 *   เพื่อยืนยันว่าอุปกรณ์ Tuya ทำงานจริง และจะไม่ subscribe wildcard ที่ไม่จำเป็น
 *
 * MQTT topics ที่ subscribe (ไม่มี wildcard):
 *   sensor/{BLE_SENSOR_1}/reading      : อุณหภูมิสำหรับสมาร์ทปลั๊ก
 *   sensor/{BLE_SENSOR_2}/reading      : อุณหภูมิสำหรับสวิตช์ + AM2301
 *   tuya/{device}/ack                  : ผลลัพธ์คำสั่ง switch_1
 *   tuya/{device}/status               : สถานะจริงล่าสุดของ relay
 *
 * อ้างอิง: extras/MQTT_GUIDE.md §3, §4 และ §4.5
 * Author       : Tenergy Innovation Co., Ltd.
 * Date         : 4 Sep 2026
 * Revision     : 2.0
 * Rev2.0       : เปลี่ยนจากปุ่ม TM1638 เป็นควบคุมอัตโนมัติด้วย Xiaomi BLE 2 ตัว
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

// client ID ต้องไม่ซ้ำกับ ESP32/โปรแกรม MQTT ตัวอื่นในเครือข่ายเดียวกัน
#define MQTT_CLIENT_ID  "esp32-tuya-temp-auto-01"

/**************************************/
/*    Tuya device id (MQTT_GUIDE §4)   */
/**************************************/
#define DEV_GATEWAY     "a3ef45f5befe5df4beufrr"   // เกตเวย์ ZBS06         (read-only)
#define DEV_SMARTPLUG   "a3d114ff516bb0c823oo4r"   // สมาร์ทปลั๊ก           (สั่งได้: switch_1)
#define DEV_SWITCH_TH   "a35db147a188ac4004kpk1"   // สวิตช์ + AM2301       (สั่งได้: switch_1)
#define DEV_MOTION      "a369f740c23f4ef8e52qdw"   // Motion + แสง          (read-only)

/**************************************/
/*  Xiaomi BLE sensor MAC (GUIDE §4.5) */
/**************************************/
#define BLE_SENSOR_1    "A4:C1:38:72:DB:01"        // ควบคุม DEV_SMARTPLUG
#define BLE_SENSOR_2    "A4:C1:38:11:A3:D0"        // ควบคุม DEV_SWITCH_TH

/**************************************/
/*     Temperature control settings    */
/**************************************/
// เปลี่ยนตัวเลข 30.0f เพื่อเปลี่ยนอุณหภูมิที่ต้องการควบคุม (หน่วย °C)
#define SMARTPLUG_ON_TEMP_C  30.0f  // BLE #1 > ค่านี้ = เปิดสมาร์ทปลั๊ก
#define SWITCH_TH_ON_TEMP_C  30.0f  // BLE #2 > ค่านี้ = เปิดสวิตช์ + AM2301

/**************************************/
/*          MQTT topic define          */
/**************************************/
// ใช้ topic แบบระบุอุปกรณ์ตายตัว แทน tuya/# หรือ sensor/# เพื่อลด traffic บน ESP32
#define BLE1_READING_TOPIC      "sensor/" BLE_SENSOR_1 "/reading"
#define BLE2_READING_TOPIC      "sensor/" BLE_SENSOR_2 "/reading"
#define PLUG_ACK_TOPIC          "tuya/" DEV_SMARTPLUG "/ack"
#define SWITCH_TH_ACK_TOPIC     "tuya/" DEV_SWITCH_TH "/ack"
#define PLUG_STATUS_TOPIC       "tuya/" DEV_SMARTPLUG "/status"
#define SWITCH_TH_STATUS_TOPIC  "tuya/" DEV_SWITCH_TH "/status"

/**************************************/
/*          Constant settings         */
/**************************************/
// status ของ AM2301 มีขนาดประมาณ 400 byte; ค่า PubSubClient ปกติ 256 byte ไม่พอ
#define MQTT_BUFFER_SIZE        1024
#define MQTT_KEEPALIVE_SEC      60
#define MQTT_RECONNECT_MS       3000
#define WIFI_CONNECT_TIMEOUT_MS 20000

// หากไม่มี ack/status ยืนยันภายในเวลานี้ จะยกเลิกสถานะ "กำลังรอ" เพื่อให้
// reading BLE รอบถัดไปสามารถสั่งซ้ำได้
#define COMMAND_CONFIRM_TIMEOUT_MS 15000

/**************************************/
/*           Object define            */
/**************************************/
WiFiClient   net;
PubSubClient mqtt(net);

/**************************************/
/*         Data structure             */
/**************************************/
/*
 * เก็บสถานะของอุปกรณ์ Tuya หนึ่งตัว เพื่อใช้โค้ดชุดเดียวกันได้ทั้งปลั๊กและสวิตช์
 *
 * reportedOn       : สถานะล่าสุดที่อุปกรณ์รายงานใน status/ack
 * targetOn         : สถานะที่กฎอุณหภูมิต้องการ
 * commandPending   : ส่งคำสั่งแล้ว กำลังรอ ack หรือ status ยืนยัน
 *
 * has... เป็น flag สำคัญ เพราะค่า false อาจหมายถึง "ปิดจริง" ไม่ใช่ "ยังไม่เคยรู้ค่า".
 */
struct DeviceControl
{
  const char *deviceId;
  const char *label;
  bool hasReportedState;
  bool reportedOn;
  bool hasTarget;
  bool targetOn;
  bool commandPending;
  unsigned long commandSentAt;
};

DeviceControl smartPlug = {
  DEV_SMARTPLUG, "Smart Plug (BLE #1)",
  false, false, false, false, false, 0
};

DeviceControl switchTh = {
  DEV_SWITCH_TH, "Switch + AM2301 (BLE #2)",
  false, false, false, false, false, 0
};

/**************************************/
/*         Function prototype         */
/**************************************/
void setupWiFi();
void reconnectMqtt();
void onMqttMessage(char *topic, byte *payload, unsigned int length);
void handleBleReading(const char *sensorName, JsonDocument &doc,
                      float thresholdC, DeviceControl &device);
void applyTemperatureRule(float temperatureC, float thresholdC, DeviceControl &device);
void handleDeviceStatus(JsonDocument &doc, DeviceControl &device);
void handleDeviceAck(JsonDocument &doc, DeviceControl &device);
void sendCommandIfNeeded(DeviceControl &device);
bool publishSwitchSet(const char *deviceId, bool value);
void checkCommandTimeout(DeviceControl &device);

/***********************************************************************
 * setup : เริ่ม Serial, Wi-Fi และ MQTT client
 ***********************************************************************/
void setup()
{
  Serial.begin(115200);
  delay(300);

  Serial.println();
  Serial.println(F("========================================================"));
  Serial.println(F(" ESP32_MQTT_TUYA - Xiaomi BLE temperature auto control"));
  Serial.println(F("========================================================"));
  Serial.printf("[RULE] BLE #1 > %.1f C -> Smart Plug ON,  < -> OFF\n", SMARTPLUG_ON_TEMP_C);
  Serial.printf("[RULE] BLE #2 > %.1f C -> Switch+AM2301 ON, < -> OFF\n", SWITCH_TH_ON_TEMP_C);
  Serial.println(F("[RULE] temperature equal to threshold -> keep current state\n"));

  setupWiFi();

  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  mqtt.setKeepAlive(MQTT_KEEPALIVE_SEC);
  mqtt.setBufferSize(MQTT_BUFFER_SIZE);
  mqtt.setCallback(onMqttMessage);
}

/***********************************************************************
 * loop : รักษาการเชื่อมต่อ MQTT, รับ message และตรวจ timeout การยืนยันคำสั่ง
 *
 * mqtt.loop() ต้องถูกเรียกถี่ ๆ เพื่อให้ PubSubClient รับ retained reading,
 * ack และ status ได้ทันเวลา จึงไม่มี delay ยาวใน loop นี้
 ***********************************************************************/
void loop()
{
  if (WiFi.status() != WL_CONNECTED)
    setupWiFi();

  if (!mqtt.connected())
    reconnectMqtt();

  mqtt.loop();

  checkCommandTimeout(smartPlug);
  checkCommandTimeout(switchTh);
}

/***********************************************************************
 * setupWiFi : ต่อ Wi-Fi; หากต่อไม่สำเร็จภายในเวลาที่กำหนดจะ restart
 ***********************************************************************/
void setupWiFi()
{
  Serial.printf("\n[WiFi] connecting to \"%s\" ", WIFI_SSID);
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  unsigned long startedAt = millis();
  while (WiFi.status() != WL_CONNECTED)
  {
    delay(300);
    Serial.print('.');

    if (millis() - startedAt > WIFI_CONNECT_TIMEOUT_MS)
    {
      Serial.println(F("\n[WiFi] timeout -> restart"));
      ESP.restart();
    }
  }

  Serial.printf("\n[WiFi] connected, IP = %s\n", WiFi.localIP().toString().c_str());
}

/***********************************************************************
 * reconnectMqtt : ต่อ MQTT แล้ว subscribe เฉพาะ 6 topic ที่จำเป็น
 *
 * retained message ของ BLE/status จะถูกส่งกลับทันทีหลัง subscribe ดังนั้น
 * ระบบจะได้ค่าล่าสุดโดยไม่ต้องรอให้เซนเซอร์วัดรอบใหม่
 ***********************************************************************/
void reconnectMqtt()
{
  while (!mqtt.connected())
  {
    Serial.printf("[MQTT] connecting to %s:%d ... ", MQTT_HOST, MQTT_PORT);

    if (mqtt.connect(MQTT_CLIENT_ID, MQTT_USER, MQTT_PASS))
    {
      Serial.println(F("connected"));

      // Subscribe status ก่อน เพื่อให้ retained state ของ relay มาก่อน BLE reading
      // และช่วยลดการส่ง set ซ้ำโดยไม่จำเป็นทันทีหลัง ESP32 เพิ่งบูต
      mqtt.subscribe(PLUG_STATUS_TOPIC);
      mqtt.subscribe(SWITCH_TH_STATUS_TOPIC);
      mqtt.subscribe(PLUG_ACK_TOPIC);
      mqtt.subscribe(SWITCH_TH_ACK_TOPIC);
      mqtt.subscribe(BLE1_READING_TOPIC);
      mqtt.subscribe(BLE2_READING_TOPIC);

      Serial.println(F("[MQTT] subscribed: BLE #1/#2 + ack/status of both controlled devices"));
    }
    else
    {
      Serial.printf("failed rc=%d, retry in %d ms\n", mqtt.state(), MQTT_RECONNECT_MS);
      delay(MQTT_RECONNECT_MS);
    }
  }
}

/***********************************************************************
 * onMqttMessage : แยก topic แล้วส่งต่อไปยัง handler ที่เหมาะสม
 *
 * ArduinoJson แปลง payload JSON เป็น doc เพียงครั้งเดียวต่อ message. ถ้า JSON
 * เสีย/ไม่ครบ จะหยุดทันทีเพื่อป้องกันสั่ง relay จากข้อมูลที่เชื่อถือไม่ได้.
 ***********************************************************************/
void onMqttMessage(char *topic, byte *payload, unsigned int length)
{
  JsonDocument doc;
  DeserializationError error = deserializeJson(doc, payload, length);
  if (error)
  {
    Serial.printf("[MQTT] ignore invalid JSON on %s: %s\n", topic, error.c_str());
    return;
  }

  if (strcmp(topic, BLE1_READING_TOPIC) == 0)
  {
    handleBleReading("Xiaomi BLE #1", doc, SMARTPLUG_ON_TEMP_C, smartPlug);
  }
  else if (strcmp(topic, BLE2_READING_TOPIC) == 0)
  {
    handleBleReading("Xiaomi BLE #2", doc, SWITCH_TH_ON_TEMP_C, switchTh);
  }
  else if (strcmp(topic, PLUG_STATUS_TOPIC) == 0)
  {
    handleDeviceStatus(doc, smartPlug);
  }
  else if (strcmp(topic, SWITCH_TH_STATUS_TOPIC) == 0)
  {
    handleDeviceStatus(doc, switchTh);
  }
  else if (strcmp(topic, PLUG_ACK_TOPIC) == 0)
  {
    handleDeviceAck(doc, smartPlug);
  }
  else if (strcmp(topic, SWITCH_TH_ACK_TOPIC) == 0)
  {
    handleDeviceAck(doc, switchTh);
  }
}

/***********************************************************************
 * handleBleReading : อ่าน temperature_c จาก Xiaomi BLE และใช้กฎของอุปกรณ์คู่กัน
 *
 * payload ที่คาดหวัง (MQTT_GUIDE §4.5):
 * {"temperature_c":30.1,"humidity_pct":69,"battery_pct":68,...}
 ***********************************************************************/
void handleBleReading(const char *sensorName, JsonDocument &doc,
                      float thresholdC, DeviceControl &device)
{
  // ห้ามใช้ "| 0.0" ตรงนี้ เพราะ field ที่หายไปจะกลายเป็น 0°C และอาจสั่งปิดผิดพลาด
  if (doc["temperature_c"].isNull())
  {
    Serial.printf("[BLE] %s: missing temperature_c -> no command\n", sensorName);
    return;
  }

  float temperatureC = doc["temperature_c"].as<float>();
  int humidityPct = doc["humidity_pct"] | -1;
  int batteryPct  = doc["battery_pct"]  | -1;

  Serial.printf("[BLE] %s: %.1f C, humidity=%d%%, battery=%d%% -> %s\n",
                sensorName, temperatureC, humidityPct, batteryPct, device.label);

  applyTemperatureRule(temperatureC, thresholdC, device);
}

/***********************************************************************
 * applyTemperatureRule : แปลงอุณหภูมิเป็นสถานะ ON/OFF ที่ต้องการ
 *
 * ใช้ > และ < ตามโจทย์โดยตรง ไม่ใช้ >= หรือ <=.
 * เมื่อเท่ากับ threshold จะ return โดยไม่เปลี่ยน target เดิมหรือส่งคำสั่งใด ๆ.
 ***********************************************************************/
void applyTemperatureRule(float temperatureC, float thresholdC, DeviceControl &device)
{
  if (temperatureC == thresholdC)
  {
    Serial.printf("[RULE] %s: %.1f C equals threshold %.1f C -> keep state\n",
                  device.label, temperatureC, thresholdC);
    return;
  }

  bool wantedOn = temperatureC > thresholdC;
  bool targetChanged = !device.hasTarget || (device.targetOn != wantedOn);

  device.hasTarget = true;
  device.targetOn = wantedOn;

  Serial.printf("[RULE] %s: %.1f C %c %.1f C -> target %s%s\n",
                device.label, temperatureC, wantedOn ? '>' : '<', thresholdC,
                wantedOn ? "ON" : "OFF", targetChanged ? " (new target)" : "");

  // ส่งเฉพาะเมื่อสถานะจริงยังต่างจาก target; ช่วยลดการ publish ซ้ำทุก BLE reading.
  sendCommandIfNeeded(device);
}

/***********************************************************************
 * handleDeviceStatus : status คือสถานะจริงล่าสุดของ relay จาก Tuya bridge
 *
 * status ยังช่วยยืนยันคำสั่งได้ แม้ ack หายระหว่างเครือข่ายมีปัญหา.
 ***********************************************************************/
void handleDeviceStatus(JsonDocument &doc, DeviceControl &device)
{
  if (doc["switch_1"].isNull())
  {
    Serial.printf("[STATUS] %s: missing switch_1 -> ignore\n", device.label);
    return;
  }

  device.reportedOn = doc["switch_1"].as<bool>();
  device.hasReportedState = true;
  Serial.printf("[STATUS] %s is %s\n", device.label, device.reportedOn ? "ON" : "OFF");

  if (device.hasTarget && device.reportedOn == device.targetOn)
  {
    if (device.commandPending)
      Serial.printf("[OK] %s confirmed by status\n", device.label);
    device.commandPending = false;
  }

  // ถ้ามี target แล้ว แต่ถูกเปลี่ยนจากแอป/สวิตช์ผนังจนต่างจาก target ให้กฎ BLE ควบคุมกลับ
  sendCommandIfNeeded(device);
}

/***********************************************************************
 * handleDeviceAck : ตรวจผลตอบกลับของคำสั่ง set
 *
 * คำสั่งสำเร็จจริงต้องมีทั้ง result.success และ result.result เป็น true
 * ตาม MQTT_GUIDE.md §5. รับเฉพาะ ack ของ switch_1 เพื่อไม่ให้ ack DP อื่น
 * มายืนยันคำสั่งควบคุม relay ผิดตัว.
 ***********************************************************************/
void handleDeviceAck(JsonDocument &doc, DeviceControl &device)
{
  const char *code = doc["code"] | "";
  if (strcmp(code, "switch_1") != 0)
    return;

  bool accepted = doc["result"]["success"] | false;
  bool delivered = doc["result"]["result"]  | false;
  bool ok = accepted && delivered;

  if (!ok)
  {
    Serial.printf("[ACK] %s FAILED (success=%s, result=%s)\n", device.label,
                  accepted ? "true" : "false", delivered ? "true" : "false");
    device.commandPending = false;
    return;
  }

  // ack ที่สำเร็จมี value เป็นสถานะที่ bridge ส่งให้อุปกรณ์
  if (!doc["value"].isNull())
  {
    device.reportedOn = doc["value"].as<bool>();
    device.hasReportedState = true;
  }
  device.commandPending = false;

  Serial.printf("[ACK] %s confirmed %s\n", device.label,
                device.reportedOn ? "ON" : "OFF");

  // กรณีอุณหภูมิเปลี่ยนเร็วและ target ใหม่เกิดระหว่างรอ ack เก่า ให้ส่ง target ล่าสุดต่อทันที
  sendCommandIfNeeded(device);
}

/***********************************************************************
 * sendCommandIfNeeded : ส่ง tuya/{device_id}/set เฉพาะเมื่อจำเป็น
 *
 * กรอง 3 กรณีที่ไม่ต้องส่ง:
 *   1) ยังไม่มี reading BLE จึงไม่มี target
 *   2) status/ack บอกว่า relay อยู่ใน target แล้ว
 *   3) มีคำสั่งเดียวกันกำลังรอการยืนยันอยู่
 ***********************************************************************/
void sendCommandIfNeeded(DeviceControl &device)
{
  if (!device.hasTarget)
    return;

  if (device.hasReportedState && device.reportedOn == device.targetOn)
    return;

  if (device.commandPending)
    return;

  if (publishSwitchSet(device.deviceId, device.targetOn))
  {
    device.commandPending = true;
    device.commandSentAt = millis();
  }
}

/***********************************************************************
 * publishSwitchSet : ส่งได้เพียง DP switch_1 ตามข้อจำกัดใน MQTT_GUIDE §4
 *
 * ตัวอย่าง payload: {"code":"switch_1","value":true}
 ***********************************************************************/
bool publishSwitchSet(const char *deviceId, bool value)
{
  char topic[64];
  char payload[48];

  snprintf(topic, sizeof(topic), "tuya/%s/set", deviceId);
  snprintf(payload, sizeof(payload), "{\"code\":\"switch_1\",\"value\":%s}",
           value ? "true" : "false");

  bool published = mqtt.publish(topic, payload);
  Serial.printf("[SET] %s -> %s (%s)\n", topic, payload,
                published ? "published" : "PUBLISH FAILED");
  return published;
}

/***********************************************************************
 * checkCommandTimeout : ไม่ให้ commandPending ค้างตลอดไปหาก broker/bridge
 * ไม่ส่ง ack/status กลับมา เมื่อ timeout แล้ว BLE reading รอบถัดไปจะลองสั่งอีกครั้ง
 ***********************************************************************/
void checkCommandTimeout(DeviceControl &device)
{
  if (!device.commandPending)
    return;

  if (millis() - device.commandSentAt > COMMAND_CONFIRM_TIMEOUT_MS)
  {
    Serial.printf("[WARN] %s: no ack/status within %lu ms; will retry on next BLE reading\n",
                  device.label, (unsigned long)COMMAND_CONFIRM_TIMEOUT_MS);
    device.commandPending = false;
  }
}
