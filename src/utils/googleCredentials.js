const fs = require('fs')

// รวม logic โหลด Google credentials ให้ Sheets/STT/TTS ใช้ตัวเดียวกัน
// เดิมแต่ละไฟล์เช็คคนละแบบ (Sheets มี fallback keyFile เฉพาะของตัวเอง, STT/TTS ไม่มี)
// ทำให้ deploy บาง env auth service นึงผ่านแต่อีก service ไม่ผ่านแบบเงียบๆ
const FALLBACK_KEY_FILE = '/etc/secrets/google-credentials.json' // Render secret file — มีจริงเฉพาะบน Render

// ใช้ได้ทั้ง googleapis GoogleAuth และ @google-cloud/* client libraries (ทั้งคู่รับ keyFilename)
function getGoogleClientOptions() {
  if (process.env.GOOGLE_CREDENTIALS_JSON) {
    return { credentials: JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON) }
  }
  if (process.env.GOOGLE_APPLICATION_CREDENTIALS) {
    return {} // ปล่อยให้ client library อ่าน path จาก env ตัวนี้เอง (มาตรฐาน ADC)
  }
  // ใช้ path ของ Render ได้ก็ต่อเมื่อไฟล์มีอยู่จริง — เช็คก่อนกัน dev เครื่อง local (ที่ใช้ gcloud ADC แทน)
  // พังเพราะ keyFilename ชี้ไปที่ไฟล์ที่ไม่มีจริง ซึ่งจะบล็อกไม่ให้ SDK fallback ไป ADC เอง
  if (fs.existsSync(FALLBACK_KEY_FILE)) {
    return { keyFilename: FALLBACK_KEY_FILE }
  }
  return {} // ปล่อยให้ ADC จัดการเอง (เช่น gcloud auth application-default login ตอน dev)
}

module.exports = { getGoogleClientOptions }
