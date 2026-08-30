// เก็บ session ของแต่ละสาย (in-memory)
const sessions = new Map()

// Track 7 (defensive fix, 2026-08-30) — จุดลบ session จุดเดียวในระบบคือ webhook.js ตอน Twilio ส่ง /webhook/status
// มาแจ้งว่าสายจบแล้ว (สายโทรออกตั้งค่านี้ไว้ในโค้ดชัดเจนผ่าน twilio.js แต่สายโทรเข้าไม่มีการตั้งค่าใดๆ ในโค้ดเลย
// ต้องพึ่งการตั้งค่าฝั่ง Twilio Console แทน) — ถ้า statusCallback ไม่มาถึงด้วยเหตุผลใดก็ตาม session จะค้างอยู่ใน
// Map นี้ตลอดไป เป็น memory leak ไม่มีขอบเขต แก้แบบเดียวกับที่ callQueue.js ใช้กับ RELEASE_SAFETY_TIMEOUT_MS —
// arm safety timer ทุกครั้งที่ set() แล้วบังคับลบเองถ้ายังไม่ถูกลบตามปกติภายในเวลาที่ควร ไม่ต้องรู้/แก้ config
// ภายนอกเลย แค่ bound ความเสียหายสูงสุดไว้เท่านั้น
const SESSION_REAP_SAFETY_TIMEOUT_MS = ((parseInt(process.env.MAX_CALL_DURATION_SECONDS) || 300) + 120) * 1000
const reapTimers = new Map() // callSid -> timer

function clearReapTimer(callSid) {
  const timer = reapTimers.get(callSid)
  if (timer) { clearTimeout(timer); reapTimers.delete(callSid) }
}

module.exports = {
  get: (callSid) => sessions.get(callSid),
  set: (callSid, data) => {
    sessions.set(callSid, data)
    clearReapTimer(callSid) // เผื่อ set() ซ้ำ callSid เดิม (ไม่ควรเกิดปกติ) กัน timer เก่าค้าง
    const timer = setTimeout(() => {
      if (!sessions.has(callSid)) return
      console.warn(`[CallSessions] session ${callSid} ยังไม่ถูกลบภายในเวลาที่ควร (ไม่ได้รับ /webhook/status?) — ลบเองกัน memory leak`)
      sessions.delete(callSid)
      reapTimers.delete(callSid)
    }, SESSION_REAP_SAFETY_TIMEOUT_MS)
    timer.unref?.()
    reapTimers.set(callSid, timer)
  },
  delete: (callSid) => {
    clearReapTimer(callSid)
    return sessions.delete(callSid)
  },
  has: (callSid) => sessions.has(callSid),
  entries: () => sessions.entries(),
  size: () => sessions.size
}
