const axios = require('axios')

const API_URL = 'https://api-v2.thaibulksms.com/sms'
const API_KEY = process.env.THAIBULKSMS_API_KEY
const API_SECRET = process.env.THAIBULKSMS_API_SECRET
const DEFAULT_SENDER = process.env.THAIBULKSMS_SENDER

// ThaiBulkSMS รับเบอร์ได้หลายรูปแบบ (0812345678, 66812345678, +66812345678)
// เบอร์ที่ normalizePhone() แปลงเป็น +66... ไว้แล้วในระบบเรา ใช้ส่งตรงๆ ได้เลย ไม่ต้องแปลงซ้ำ
// sender ระบุได้ต่อครั้ง (แต่ละ campaign เลือกชื่อผู้ส่งของตัวเองได้) — ไม่ระบุจะ fallback ไปใช้ค่า default ใน .env
async function sendSms(to, body, sender) {
  try {
    const response = await axios.post(
      API_URL,
      { msisdn: to, message: body, sender: sender || DEFAULT_SENDER },
      { auth: { username: API_KEY, password: API_SECRET } }
    )
    const data = response.data
    if (data.bad_phone_number_list?.length) {
      console.warn(`[ThaiBulkSMS] เบอร์ส่งไม่ได้: ${JSON.stringify(data.bad_phone_number_list)}`)
    }
    console.log(`[ThaiBulkSMS] Sent to ${to} — เครดิตเหลือ ${data.remaining_credit} (${data.credit_type})`)
    return data
  } catch (err) {
    // ThaiBulkSMS ส่ง error กลับมาเป็น { error: { code, name, description } } — ดึงมาแสดงให้อ่านง่ายกว่า axios message เฉยๆ
    const apiError = err.response?.data?.error
    const message = apiError ? `${apiError.name} (${apiError.code}): ${apiError.description}` : err.message
    throw new Error(`[ThaiBulkSMS] Send failed: ${message}`)
  }
}

module.exports = { sendSms }
