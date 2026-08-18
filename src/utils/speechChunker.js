// เป้าหมาย: หา "ก้อนข้อความที่พูดแล้วฟังเป็นธรรมชาติ" จาก buffer ที่สะสมมาจาก Claude streaming
// ไม่ใช่ "ประโยคสมบูรณ์ทางไวยากรณ์" — เพราะบทสนทนาไทยจริงจบด้วยคำหลากหลายมาก
// (ได้เลย/โอเค/ไม่เป็นไร/ตัวเลข/URL) ไม่ใช่แค่ ค่ะ/คะ/ครับ
//
// escalation ตามเวลา (elapsedMs = เวลานับจากตัวอักษรแรกของ buffer ปัจจุบันมาถึง):
//   ทันที          → strong boundary (. ? ! \n) หรือ Thai soft boundary (ค่ะ/คะ/ครับ ฯลฯ)
//   >= SOFT_TIMEOUT → ผ่อนลงมาใช้ natural boundary (ช่องว่างระหว่างคำ) ได้ ถ้า buffer ยาวพอ
//   >= HARD_MAX     → ยังต้องหา natural boundary อยู่ดี "ห้ามตัดกลางคำเด็ดขาด"
//                     ถ้าไม่มีช่องว่างเลยสักตัว (คำเดียวยาวผิดปกติ) ก็ยังคืน null รอต่อไป
//
// END_CALL ไม่ต้องกันในนี้ — ย้ายไปเป็น Claude tool call แยกจาก text channel แล้ว (Checkpoint B)
// ไม่มีทาง string marker หลุดมาปนใน buffer ข้อความได้อีก

const MIN_CHUNK_LENGTH = 6          // ตัวอักษรขั้นต่ำ กันตัดก้อนสั้นเกินไปแม้จะเจอ boundary ก็ตาม (เช่น "ค่ะ" เดี่ยวๆ ไม่ควรตัด แต่ "ยินดีค่ะ" 8 ตัวอักษรเป็นวลีสมบูรณ์ที่ควรตัดได้)
const FALLBACK_MIN_LENGTH = 25     // ความยาวขั้นต่ำก่อนจะยอมใช้ natural boundary แทน strong/soft
const SOFT_TIMEOUT_MS = 300
const HARD_MAX_MS = 800

const STRONG_BOUNDARY_RE = /[.?!\n]/
// เรียงจากยาว→สั้นตั้งใจ (นะครับ/นะคะ ต้องเช็คก่อน ครับ/คะ ไม่งั้น regex ตัดสั้นไปจับ "นะครับ" เป็นแค่ "ครับ" ผิดตำแหน่ง)
const THAI_SOFT_BOUNDARY_RE = /(นะครับ|นะคะ|ได้เลย|ไม่เป็นไร|ครับ|ค่ะ|คะ|โอเค)(?=\s|$)/

// หา match ที่ตำแหน่งเร็วสุด (leftmost) ของ regex ใน buffer โดยจุดตัดต้อง >= MIN_CHUNK_LENGTH
function findEarliestMatch(buffer, re) {
  const match = buffer.match(re)
  if (!match) return -1
  const cutAt = match.index + match[0].length
  return cutAt >= MIN_CHUNK_LENGTH ? cutAt : -1
}

// หาตำแหน่งช่องว่าง "ปลอดภัยที่สุด" ที่ใกล้เคียงจุดปัจจุบันที่สุด (ตัดได้โดยไม่ขาดกลางคำ)
// คืนตำแหน่งท้ายสุดที่เจอ (ยิ่งใกล้ปัจจุบันยิ่งได้ก้อนที่ยาวขึ้น ลด latency ของก้อนถัดไป)
function findLastSafeBoundary(buffer, minLength) {
  let lastSpace = -1
  for (let i = buffer.length - 1; i >= minLength; i--) {
    if (buffer[i] === ' ' || buffer[i] === '\n') { lastSpace = i + 1; break }
  }
  return lastSpace
}

// buffer: ข้อความที่สะสมมาตั้งแต่ flush ครั้งล่าสุด
// elapsedMs: เวลาผ่านไปนับจากตัวอักษรแรกของ buffer นี้มาถึง
// คืน { chunk, remainder } ถ้าพร้อม flush หรือ null ถ้ายังไม่ควรตัด
function findChunkBoundary(buffer, elapsedMs) {
  if (!buffer || buffer.length < MIN_CHUNK_LENGTH) return null

  const strongCut = findEarliestMatch(buffer, STRONG_BOUNDARY_RE)
  const softCut = findEarliestMatch(buffer, THAI_SOFT_BOUNDARY_RE)

  // ใช้ตัวที่เจอเร็วสุด (ตำแหน่งน้อยสุด) ระหว่าง strong กับ soft ถ้าเจอทั้งคู่
  const candidates = [strongCut, softCut].filter(c => c !== -1)
  if (candidates.length) {
    const cutAt = Math.min(...candidates)
    return split(buffer, cutAt)
  }

  // ยังไม่ครบ soft timeout — ไม่ผ่อนเกณฑ์ รอต่อ
  if (elapsedMs < SOFT_TIMEOUT_MS) return null

  // ครบ soft timeout แล้ว — ผ่อนเป็น natural boundary ได้ แต่ buffer ต้องยาวพอสมควรก่อน
  if (buffer.length >= FALLBACK_MIN_LENGTH) {
    const naturalCut = findLastSafeBoundary(buffer, MIN_CHUNK_LENGTH)
    if (naturalCut !== -1) return split(buffer, naturalCut)
  }

  // ครบ hard max แล้ว — ยังต้องหา natural boundary อยู่ดี ห้ามตัดกลางคำเด็ดขาด
  if (elapsedMs >= HARD_MAX_MS) {
    const naturalCut = findLastSafeBoundary(buffer, MIN_CHUNK_LENGTH)
    if (naturalCut !== -1) return split(buffer, naturalCut)
    // ไม่มีช่องว่างเลยแม้แต่ตัวเดียวใน buffer (คำเดียวยาวผิดปกติ) — รอต่อไป ไม่มีทางเลือกที่ปลอดภัย
  }

  return null
}

function split(buffer, cutAt) {
  return { chunk: buffer.slice(0, cutAt).trim(), remainder: buffer.slice(cutAt) }
}

module.exports = { findChunkBoundary }
