// เกณฑ์เดียวกับฝั่ง client ใน admin.html (parsePastedContacts) — กันเบอร์รูปแบบผิดที่หลุด validation
// ฝั่ง browser ไปแล้ว ไม่ให้ไปพังตอนยิงจริงที่ Twilio แทน
function isValidPhone(phone) {
  if (!phone) return false
  const digits = String(phone).replace(/[^\d+]/g, '')
  return digits.length >= 9
}

// แปลงเบอร์รูปแบบไทย (0812345678) เป็น E.164 (+66812345678) — Twilio ต้องการ E.164 ถึงจะโทรออกได้ถูกต้อง
// เบอร์ที่มี + นำหน้าอยู่แล้ว หรือไม่ตรงรูปแบบเบอร์ไทย จะปล่อยผ่านตามเดิมโดยไม่แตะ
function normalizePhone(phone) {
  if (!phone) return phone
  const trimmed = String(phone).trim().replace(/[\s\-()]/g, '')
  if (trimmed.startsWith('+')) return trimmed
  if (/^0\d{8,9}$/.test(trimmed)) return '+66' + trimmed.slice(1)
  if (/^66\d{8,9}$/.test(trimmed)) return '+' + trimmed
  return trimmed
}

module.exports = { isValidPhone, normalizePhone }
