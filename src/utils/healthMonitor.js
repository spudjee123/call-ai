// เก็บ error ล่าสุด (TTS/STT/AI) ไว้ในหน้าต่างเวลาสั้นๆ ให้ /admin เช็คได้ว่าระบบมีปัญหาอยู่ไหม
// ไม่ต้องพึ่งการอ่าน log มือ — จุดเริ่มมาจากเคส ElevenLabs 401 ที่กว่าจะรู้ว่าเสียงเงียบทั้งสายก็ตอนมีคนถามแล้ว

const WINDOW_MS = 10 * 60 * 1000 // 10 นาที

let events = []

function reportError(type, message) {
  events.push({ type, message, time: Date.now() })
  const cutoff = Date.now() - WINDOW_MS
  events = events.filter(e => e.time >= cutoff)
}

function getStatus() {
  const cutoff = Date.now() - WINDOW_MS
  const recent = events.filter(e => e.time >= cutoff)
  return {
    ok: recent.length === 0,
    count: recent.length,
    events: recent.slice(-10).map(e => ({ type: e.type, message: e.message, time: new Date(e.time).toISOString() })),
  }
}

module.exports = { reportError, getStatus }
