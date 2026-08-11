// เกณฑ์เดียวกับฝั่ง client ใน admin.html (parsePastedContacts) — กันเบอร์รูปแบบผิดที่หลุด validation
// ฝั่ง browser ไปแล้ว ไม่ให้ไปพังตอนยิงจริงที่ Twilio แทน
function isValidPhone(phone) {
  if (!phone) return false
  const digits = String(phone).replace(/[^\d+]/g, '')
  return digits.length >= 9
}

module.exports = { isValidPhone }
