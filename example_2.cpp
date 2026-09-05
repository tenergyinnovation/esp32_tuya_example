/***********************************************************************
 * Project      :     ESP32_MQTT_TUYA  (Tuya Zigbee Gateway - button control)
 * Description  :     ESP32 + บอร์ด TM1638 : กดปุ่มสั่งเปิด/ปิดอุปกรณ์ Tuya ผ่าน MQTT
 *                      S1 -> toggle สมาร์ทปลั๊ก    (a3d114ff516bb0c823oo4r)
 *                      S2 -> toggle สวิตช์ + AM2301 (a35db147a188ac4004kpk1)
 *                    กด -> beep 1 ครั้ง -> publish tuya/{id}/set -> รอ ack ยืนยันผล
 *                      -> LED บนบอร์ด TM1638 (ดวงที่ 1 = ปลั๊ก, ดวงที่ 2 = สวิตช์) ตามผลที่ยืนยันแล้ว
 *                      -> ล็อกปุ่ม 500 ms กันกดซ้ำ
 *
 *                    *** ไม่ subscribe tuya/+/status และ sensor/+/reading (wildcard) อีกต่อไป ***
 *                    subscribe เฉพาะ 4 topic ของ 2 อุปกรณ์นี้เท่านั้น:
 *                      tuya/{plug}/ack , tuya/{switch}/ack       -> ผลของคำสั่ง set
 *                      tuya/{plug}/status , tuya/{switch}/status -> สถานะจริง (ไว้ sync LED
 *                        + ยืนยันคำสั่งเป็น fallback ของ ack) — traffic ต่ำ ~1 msg/10s ต่อตัว
 *                    เหตุผลที่ตัด wildcard: retained status ของอุปกรณ์ Tuya ครบ 4 ตัว + เซนเซอร์
 *                    Xiaomi BLE (publish ทุกไม่กี่วินาที) ทำให้ callback ทำงานถี่มาก จน
 *                    mqtt.loop() คืนช้าและ publish คำสั่งจากปุ่มไม่ทันเวลา -> สั่งงานไม่สำเร็จ
 *
 * Author       :     Tenergy Innovation Co., Ltd.
 * Date         :     4 Sep 2026
 * Revision     :     1.5
 * Rev1.0       :     Original (อ่าน status อุปกรณ์ Tuya 4 ตัว + เซนเซอร์ Xiaomi BLE 2 ตัว)
 * Rev1.1       :     เพิ่มปุ่ม toggle สั่งปลั๊ก/สวิตช์ AM2301 + buzzer beep + รอ ack
 * Rev1.2       :     เพิ่มไฟแสดงสถานะปลั๊ก/สวิตช์ AM2301 ตามสถานะจริงของอุปกรณ์
 * Rev1.3       :     ย้ายปุ่ม + ไฟแสดงสถานะไปใช้บอร์ด TM1638 (ErriezTM1638), buzzer ย้ายมา GPIO27
 * Rev1.4       :     เลิก subscribe wildcard status/reading -> subscribe เฉพาะ ack ของ 2 อุปกรณ์
 *                    (แก้ปัญหา message ท่วมจน publish คำสั่งปุ่มไม่ทัน), ตัดโค้ดอ่าน/ตารางสถานะออก
 * Rev1.5       :     เพิ่ม subscribe status เฉพาะ 2 อุปกรณ์ที่สั่งงาน -> LED สะท้อนสถานะจริง
 *                    (รวมกรณีเปิด/ปิดจากสวิตช์ผนัง/แอป) + ใช้ status ยืนยันคำสั่งเป็น fallback ของ ack
 * website      :     http://www.tenergyinnovation.co.th
 * Facebook     :     https://www.facebook.com/tenergy.innovation
 * Email        :     uten.boonliam@tenergyinnovation.co.th
 * TEL          :     +6689-140-7205
 *
 * หมายเหตุฮาร์ดแวร์ (ผังขาอ้างอิงจาก extras/arduino_learning_kit_v2.0/):
 *   - TM1638  : CLK(clock)=GPIO18, DIO(data)=GPIO5, STB(strobe)=GPIO19
 *               (ตาม Arduino_ESP32_TM1638_demo.ino)
 *               ปุ่ม S1-S8 อ่านผ่าน tm1638.getKeys(), LED 8 ดวงเขียนผ่าน tm1638.writeData()
 *   - BUZZER  : GPIO27 (ledc PWM — ตาม Arduino_ESP32_Buzzer.ino / example_1..4.cpp)
 ***********************************************************************/
#include <Arduino.h>
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <ErriezTM1638.h>   // ไดรเวอร์บอร์ด TM1638 (ปุ่ม S1-S8 + LED 8 ดวง) — มีใน lib/ErriezTM1638

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
#define MQTT_CLIENT_ID  "esp32-tuya-ctrl-01"   // ⚠️ ต้องไม่ซ้ำกับ client ตัวอื่น (บอร์ด/mosquitto_sub/dashboard)

// TODO: ก่อน push ขึ้น git สาธารณะ ควรย้าย credential ด้านบนไปไว้ใน secrets.h (ไม่ commit)

/**************************************/
/*  อุปกรณ์ Tuya ที่สั่งงาน (MQTT_GUIDE §4) */
/**************************************/
#define DEV_SMARTPLUG   "a3d114ff516bb0c823oo4r"   // สมาร์ทปลั๊ก      (สั่งได้: switch_1)
#define DEV_SWITCH_TH   "a35db147a188ac4004kpk1"   // สวิตช์ + AM2301   (สั่งได้: switch_1)

// topic ที่ subscribe — เฉพาะ 2 อุปกรณ์นี้ (ack = ผลคำสั่ง, status = สถานะจริงไว้ sync LED)
#define ACK_TOPIC_PLUG       "tuya/" DEV_SMARTPLUG "/ack"
#define ACK_TOPIC_SWITCH     "tuya/" DEV_SWITCH_TH "/ack"
#define STATUS_TOPIC_PLUG    "tuya/" DEV_SMARTPLUG "/status"
#define STATUS_TOPIC_SWITCH  "tuya/" DEV_SWITCH_TH "/status"

/**************************************/
/*           GPIO define              */
/**************************************/
// ผังขาตาม extras/arduino_learning_kit_v2.0/Arduino_ESP32_TM1638_demo.ino
#define TM1638_CLK     18    // TM1638 Clock  (clock)
#define TM1638_DIO     5     // TM1638 Data I/O (data)
#define TM1638_STB     19    // TM1638 Strobe (strobe)
#define BUZZER_PIN     27    // Buzzer (ledc PWM — ตาม Arduino_ESP32_Buzzer.ino / example_1..4.cpp)
#define BUZZER_CH      0     // ledc PWM channel ของ buzzer
#define BUZZER_FREQ    2700  // ความถี่เสียง beep (Hz)

// ตำแหน่ง LED บนบอร์ด TM1638 ที่ใช้เป็นไฟแสดงสถานะ (นับตามที่พิมพ์บนบอร์ดจริง LED1-LED8)
#define TM1638_LED_PLUG    1    // LED1 บนบอร์ด = สมาร์ทปลั๊ก
#define TM1638_LED_SWITCH  2    // LED2 บนบอร์ด = สวิตช์ AM2301

// ปุ่ม S1-S8 บน TM1638 ที่ใช้สั่งงาน (จาก tm1638.getKeys())
#define TM1638_BTN_PLUG    1    // S1 -> toggle สมาร์ทปลั๊ก
#define TM1638_BTN_SWITCH  2    // S2 -> toggle สวิตช์ AM2301

/**************************************/
/*          constant define           */
/**************************************/
#define MQTT_BUFFER_SIZE        1024    // payload status ของสวิตช์ AM2301 ยาว ~400 ไบต์ (default 256 จะโดนตัดทิ้งเงียบ ๆ)
#define MQTT_KEEPALIVE_SEC      60
#define MQTT_RECONNECT_MS       3000
#define WIFI_CONNECT_TIMEOUT_MS 20000   // ต่อ Wi-Fi ไม่ได้ภายในเวลานี้ -> ESP.restart()
#define BUTTON_LOCKOUT_MS       500     // หลังกดปุ่ม 1 ครั้ง ต้องรออย่างน้อยเท่านี้จึงกดซ้ำได้
#define BEEP_MS                 60      // ความยาวเสียง beep ยืนยันการกดปุ่ม
#define CMD_ACK_TIMEOUT_MS      15000   // รอผล ack/status ยืนยันคำสั่ง set นานสุดเท่านี้ ก่อนแจ้ง "ไม่ยืนยัน"

/**************************************/
/*           object define            */
/**************************************/
WiFiClient   net;
PubSubClient mqtt(net);
TM1638       tm1638(TM1638_CLK, TM1638_DIO, TM1638_STB);

/**************************************/
/*          global variable           */
/**************************************/
// ----- ปุ่มกด TM1638 (toggle) + การติดตามผลคำสั่ง set -----
uint8_t       tm1638PrevBtn      = 0;   // เลขปุ่มที่กดค้างอยู่ครั้งก่อน (0 = ไม่กด) ใช้จับ "ขอบขาลง"
unsigned long tm1638LockoutUntil = 0;   // กดซ้ำได้เมื่อ millis() >= ค่านี้

bool          plugDesiredOn   = false;  // สถานะที่ "ตั้งใจ" ของปลั๊ก (สลับทุกครั้งที่กด S1)
bool          thDesiredOn     = false;  // สถานะที่ "ตั้งใจ" ของสวิตช์ AM2301 (สลับทุกครั้งที่กด S2)
bool          plugCmdPending  = false;  // กำลังรอผล ack ของคำสั่งปลั๊กอยู่
bool          thCmdPending    = false;
unsigned long plugCmdSince    = 0;      // เวลาที่ส่งคำสั่งปลั๊กล่าสุด
unsigned long thCmdSince      = 0;

bool          plugReportedOn  = false;  // สถานะจริงล่าสุดของปลั๊ก (จาก status / ack ที่สำเร็จ) -> ขับ LED
bool          thReportedOn    = false;  // สถานะจริงล่าสุดของสวิตช์ AM2301 -> ขับ LED

/**************************************/
/*         function prototype         */
/**************************************/
void setupWiFi();
void reconnectMqtt();
void onMqttMessage(char *topic, byte *payload, unsigned int len);
void beep(uint16_t ms);
void publishSwitchSet(const char *deviceId, bool value);
uint8_t readTm1638Button();
void handleButtons();
void checkCmdTimeout();
void updateLeds();

/***********************************************************************
 * setup
 ***********************************************************************/
void setup()
{
  Serial.begin(115200);
  delay(300);
  Serial.println();
  Serial.println(F("==================================================="));
  Serial.println(F("  ESP32_MQTT_TUYA  -  TM1638 button control (ack only)"));
  Serial.println(F("==================================================="));

  // บอร์ด TM1638 (ปุ่ม S1-S8 + LED 8 ดวง)
  tm1638.begin();
  tm1638.clear();
  tm1638.setBrightness(1);   // 0..7
  tm1638.displayOn();

  // Buzzer : ledc PWM
  ledcSetup(BUZZER_CH, BUZZER_FREQ, 10);
  ledcAttachPin(BUZZER_PIN, BUZZER_CH);
  ledcWrite(BUZZER_CH, 0);

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

  mqtt.loop();          // subscribe แค่ ack + status ของ 2 อุปกรณ์ (~0.2 msg/s) -> คืนเร็ว

  handleButtons();      // อ่านปุ่ม TM1638 S1/S2 -> toggle -> publish set + beep  (สำคัญสุด)
  checkCmdTimeout();    // แจ้งเตือนถ้ารอ ack/status ยืนยันนานเกินไป
  updateLeds();         // LED TM1638 ดวงที่ 1/2 ตามสถานะจริงของปลั๊ก/สวิตช์

  delay(20);            // poll interval สั้น ๆ (debounce ปุ่ม + ไม่กิน CPU)
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
 * reconnectMqtt : ต่อ broker + subscribe เฉพาะ ack + status ของ 2 อุปกรณ์ที่สั่งงาน
 *   (ไม่ subscribe wildcard -> ไม่มี message ท่วมมารบกวนการ publish คำสั่งปุ่ม)
 ***********************************************************************/
void reconnectMqtt()
{
  while (!mqtt.connected())
  {
    Serial.printf("[MQTT] connecting to %s:%d ... ", MQTT_HOST, MQTT_PORT);
    if (mqtt.connect(MQTT_CLIENT_ID, MQTT_USER, MQTT_PASS))
    {
      Serial.println(F("connected"));
      mqtt.subscribe(ACK_TOPIC_PLUG);
      mqtt.subscribe(ACK_TOPIC_SWITCH);
      mqtt.subscribe(STATUS_TOPIC_PLUG);
      mqtt.subscribe(STATUS_TOPIC_SWITCH);
      Serial.println(F("[MQTT] subscribed: ack + status ของ smartplug และ switch+AM2301"));
    }
    else
    {
      Serial.printf("failed rc=%d, retry in %d ms\n", mqtt.state(), MQTT_RECONNECT_MS);
      delay(MQTT_RECONNECT_MS);
    }
  }
}

/***********************************************************************
 * onMqttMessage : callback — รับ tuya/{id}/ack และ tuya/{id}/status ของ 2 อุปกรณ์
 *   - ack    : ผลของคำสั่ง set (สำเร็จ = result.success && result.result — MQTT_GUIDE.md §5)
 *   - status : สถานะจริง -> อัปเดต LED, sync ปุ่ม, ยืนยันคำสั่งเป็น fallback ของ ack
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
  String id   = t.substring(a + 1, b);   // device_id
  String kind = t.substring(b + 1);      // "ack" | "status"

  // ----- ack : ผลของคำสั่ง set -----
  if (kind == "ack")
  {
    bool ok = (doc["result"]["success"] | false) && (doc["result"]["result"] | false);
    if (id == DEV_SMARTPLUG && plugCmdPending)
    {
      Serial.printf("\n[ACK] Smart Plug -> %s\n", ok ? "SUCCESS" : "FAIL");
      if (ok)
        plugReportedOn = doc["value"] | plugReportedOn; // ปรับ LED ทันทีเมื่อสั่งสำเร็จ
      plugCmdPending = false;
    }
    else if (id == DEV_SWITCH_TH && thCmdPending)
    {
      Serial.printf("\n[ACK] Switch+AM2301 -> %s\n", ok ? "SUCCESS" : "FAIL");
      if (ok)
        thReportedOn = doc["value"] | thReportedOn;
      thCmdPending = false;
    }
    return;
  }

  // ----- status : สถานะจริงของอุปกรณ์ (รวมกรณีถูกสั่งจากสวิตช์ผนัง/แอป) -----
  if (kind == "status")
  {
    if (id == DEV_SMARTPLUG)
    {
      bool sw = doc["switch_1"] | false;
      plugReportedOn = sw;                 // -> ขับ LED ให้ตรงของจริงเสมอ
      if (!plugCmdPending)
        plugDesiredOn = sw;               // ไม่มีคำสั่งค้าง -> ให้ปุ่มถัดไป toggle จากค่าจริง
      else if (sw == plugDesiredOn)
      {
        Serial.println(F("[OK] คำสั่ง S1 (ปลั๊ก) ยืนยันจาก status แล้ว"));
        plugCmdPending = false;
      }
    }
    else if (id == DEV_SWITCH_TH)
    {
      bool sw = doc["switch_1"] | false;
      thReportedOn = sw;
      if (!thCmdPending)
        thDesiredOn = sw;
      else if (sw == thDesiredOn)
      {
        Serial.println(F("[OK] คำสั่ง S2 (สวิตช์ AM2301) ยืนยันจาก status แล้ว"));
        thCmdPending = false;
      }
    }
  }
}

/***********************************************************************
 * beep : เสียงยืนยันสั้น ๆ 1 ครั้ง
 ***********************************************************************/
void beep(uint16_t ms)
{
  ledcWriteTone(BUZZER_CH, BUZZER_FREQ);
  delay(ms);
  ledcWriteTone(BUZZER_CH, 0);
  ledcWrite(BUZZER_CH, 0);
}

/***********************************************************************
 * publishSwitchSet : ส่งคำสั่งเปิด/ปิด 1 DP (switch_1) ไปที่ tuya/{id}/set
 *   payload ตาม MQTT_GUIDE.md §3 : {"code":"switch_1","value":<bool>}  (ห้ามใส่ field อื่น)
 ***********************************************************************/
void publishSwitchSet(const char *deviceId, bool value)
{
  char topic[64];
  char payload[48];
  snprintf(topic, sizeof(topic), "tuya/%s/set", deviceId);
  snprintf(payload, sizeof(payload), "{\"code\":\"switch_1\",\"value\":%s}", value ? "true" : "false");
  bool ok = mqtt.publish(topic, payload);
  Serial.printf("[SET] %s  %s  (publish %s)\n", topic, payload, ok ? "ok" : "FAILED");
}

/***********************************************************************
 * readTm1638Button : อ่านปุ่มบนบอร์ด TM1638
 *   คืน 0 = ไม่มีการกด, 1-8 = ตำแหน่งปุ่ม S1-S8 (รองรับกดทีละปุ่ม)
 *   ตรรกะ map เดียวกับ button_sw_tm1638() ใน tm1638_button_switch.ino
 ***********************************************************************/
uint8_t readTm1638Button()
{
  switch (tm1638.getKeys())
  {
  case 0x00000001: return 1;
  case 0x00000100: return 2;
  case 0x00010000: return 3;
  case 0x01000000: return 4;
  case 0x00000010: return 5;
  case 0x00001000: return 6;
  case 0x00100000: return 7;
  case 0x10000000: return 8;
  default:         return 0;
  }
}

/***********************************************************************
 * handleButtons : อ่านปุ่ม TM1638 -> toggle -> beep -> publish set
 *   ตรวจ "ขอบขาลง" (จาก 0 -> เลขปุ่ม) + ล็อก BUTTON_LOCKOUT_MS กันกดซ้ำ
 ***********************************************************************/
void handleButtons()
{
  uint8_t btn = readTm1638Button();

  bool justPressed = (btn != 0) && (tm1638PrevBtn == 0) && (millis() >= tm1638LockoutUntil);
  tm1638PrevBtn = btn;
  if (!justPressed)
    return;
  tm1638LockoutUntil = millis() + BUTTON_LOCKOUT_MS;

  // ---- S1 : toggle สมาร์ทปลั๊ก ----
  if (btn == TM1638_BTN_PLUG)
  {
    plugDesiredOn = !plugDesiredOn;
    Serial.printf("\n[S1] กดปุ่ม -> สั่งปลั๊กเป็น %s\n", plugDesiredOn ? "ON" : "OFF");
    beep(BEEP_MS);
    if (mqtt.connected())
    {
      publishSwitchSet(DEV_SMARTPLUG, plugDesiredOn);
      plugCmdPending = true;
      plugCmdSince   = millis();
    }
    else
      Serial.println(F("[S1] ข้าม: MQTT ยังไม่ได้เชื่อมต่อ"));
  }

  // ---- S2 : toggle สวิตช์ + AM2301 ----
  else if (btn == TM1638_BTN_SWITCH)
  {
    thDesiredOn = !thDesiredOn;
    Serial.printf("\n[S2] กดปุ่ม -> สั่งสวิตช์ AM2301 เป็น %s\n", thDesiredOn ? "ON" : "OFF");
    beep(BEEP_MS);
    if (mqtt.connected())
    {
      publishSwitchSet(DEV_SWITCH_TH, thDesiredOn);
      thCmdPending = true;
      thCmdSince   = millis();
    }
    else
      Serial.println(F("[S2] ข้าม: MQTT ยังไม่ได้เชื่อมต่อ"));
  }
}

/***********************************************************************
 * checkCmdTimeout : ถ้ารอผล ack ยืนยันนานเกิน CMD_ACK_TIMEOUT_MS ให้แจ้งเตือน
 ***********************************************************************/
void checkCmdTimeout()
{
  if (plugCmdPending && millis() - plugCmdSince > CMD_ACK_TIMEOUT_MS)
  {
    Serial.println(F("[WARN] คำสั่ง S1 (ปลั๊ก) ไม่ได้รับ ack ยืนยันภายในเวลาที่กำหนด"));
    plugCmdPending = false;
  }
  if (thCmdPending && millis() - thCmdSince > CMD_ACK_TIMEOUT_MS)
  {
    Serial.println(F("[WARN] คำสั่ง S2 (สวิตช์ AM2301) ไม่ได้รับ ack ยืนยันภายในเวลาที่กำหนด"));
    thCmdPending = false;
  }
}

/***********************************************************************
 * updateLeds : LED บนบอร์ด TM1638 ให้ตรงกับสถานะจริงของอุปกรณ์ (จาก status / ack)
 *   LED1 บนบอร์ด ติด = สมาร์ทปลั๊ก ON
 *   LED2 บนบอร์ด ติด = สวิตช์ AM2301 ON
 *
 *   ที่อยู่ LED ของ LEDn บนบอร์ด = 2*n - 1  (n = 1..8 -> address 1,3,5,...,15 = ไบต์ LED)
 *   หมายเหตุ: สูตรใน led_tm1638() ของ tm1638_led.ino คือ 15-(n-1)*2 ซึ่งเรียงกลับด้าน
 *   (โปรแกรม LED1 -> บอร์ด LED8) จึงต้องใช้ 2*n-1 แทน เพื่อให้ตรงกับที่พิมพ์บนบอร์ดจริง
 ***********************************************************************/
void updateLeds()
{
  tm1638.writeData(2 * TM1638_LED_PLUG   - 1, plugReportedOn ? 1 : 0);
  tm1638.writeData(2 * TM1638_LED_SWITCH - 1, thReportedOn   ? 1 : 0);
}
