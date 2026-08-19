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
//
// L1c1 (production defect 2026-08-19) — natural boundary (ที่ elapsedMs >= SOFT_TIMEOUT_MS, ก่อน HARD_MAX_MS)
// เคยตัดกลาง "ตัวเลข + หน่วยนับ" ได้ (เช่น "รับ 2,000" | "พอยต์นะคะ" เป็นคนละ ElevenLabs request กัน ฟังเป็นคนละ
// โทนเสียงเพราะ synthesis แต่ละก้อนไม่มี continuity context ต่อกัน) — ตัวเลขกับหน่วยนับเป็น semantic unit เดียวกัน
// ควรหลีกเลี่ยงการแยก chunk จนถึง HARD_MAX_MS (ไม่ใช่ห้ามเด็ดขาด — ที่ ceiling เรายังยอม natural cut ตามเดิม
// เพื่อกัน indefinite wait ถ้า Claude เงียบผิดปกติจริงระหว่างพูดตัวเลข)
//
// แก้ด้วยการกัน natural-boundary cut ที่ candidate chunk ลงท้ายด้วยตัวเลข จนกว่าจะครบ HARD_MAX_MS (reuse ceiling
// เดิม ไม่เพิ่ม constant ใหม่ — ceiling นี้ยังห่าง CHUNK_READY_TIMEOUT_MS (2000ms) ของ Watchdog B อยู่มาก และ
// Watchdog B เองก็ disarm ไปแล้วตั้งแต่ chunk แรกของเทิร์น ไม่ใช่ per-chunk อยู่แล้ว) — เมื่อ HARD_MAX_MS หมดเวลา
// (elapsedMs >= HARD_MAX_MS) protection หมดอายุ ยอมตัดกลางเหมือนเดิม ไม่กระทบ strong/soft boundary หรือ
// final-flush ตอน stream จบเลย (คนละ code path)
//
// L1c1 follow-up (commit-gate review) — findChunkBoundary() ถูกเรียกเฉพาะตอนมี delta ใหม่มาถึงเท่านั้น ไม่มี
// wall-clock polling ในตัวเอง ถ้า Claude เงียบเกินไประหว่างที่ buffer กำลังถูก numeric protection กันอยู่ (ไม่มี
// delta ใหม่มาปลุก chunker เลย) จะไม่มีใครมาตรวจว่า protection หมดอายุแล้วจริง จนกว่า stream จะจบ (final flush)
// หรือ delta ถัดไปมาถึง — เสี่ยงชน CHUNK_READY_TIMEOUT (Watchdog B, 2000ms) ทั้งที่สั้นกว่ามาก ให้ caller
// (createChunkedProducer ใน chunkedTurn.js) เป็นคน arm wall-clock timer เองผ่าน getNumericProtectionRemainingMs()
// ด้านล่าง — ฟังก์ชันนี้เป็น pure function ตอบแค่ "ตอนนี้ถูก protect อยู่ไหม เหลืออีกกี่ ms" ไม่รู้จัก setTimeout/
// producer lifecycle เลย เหมือนที่ attemptWithWatchdog.js แยก timing primitive ออกจาก caller's cancellation logic
const MIN_CHUNK_LENGTH = 6          // ตัวอักษรขั้นต่ำ กันตัดก้อนสั้นเกินไปแม้จะเจอ boundary ก็ตาม (เช่น "ค่ะ" เดี่ยวๆ ไม่ควรตัด แต่ "ยินดีค่ะ" 8 ตัวอักษรเป็นวลีสมบูรณ์ที่ควรตัดได้)
const FALLBACK_MIN_LENGTH = 25     // ความยาวขั้นต่ำก่อนจะยอมใช้ natural boundary แทน strong/soft
const SOFT_TIMEOUT_MS = 300
const HARD_MAX_MS = 800

const STRONG_BOUNDARY_RE = /[.?!\n]/
// เรียงจากยาว→สั้นตั้งใจ (นะครับ/นะคะ ต้องเช็คก่อน ครับ/คะ ไม่งั้น regex ตัดสั้นไปจับ "นะครับ" เป็นแค่ "ครับ" ผิดตำแหน่ง)
const THAI_SOFT_BOUNDARY_RE = /(นะครับ|นะคะ|ได้เลย|ไม่เป็นไร|ครับ|ค่ะ|คะ|โอเค)(?=\s|$)/

// L1c1 — candidate chunk (ข้อความที่กำลังจะถูก flush ถ้าไม่ถูกกัน) ลงท้ายด้วยเลขอารบิกหรือเลขไทยไหม — ตั้งใจกว้าง
// (ไม่ whitelist หน่วยเฉพาะ เช่น บาท/พอยต์/เปอร์เซ็นต์) เพราะเจอ pattern แบบนี้ได้หลายแบบ (100 บาท, 30%, 15.30 น.,
// เบอร์โทร) — เช็คแค่ "ท้าย candidate เป็นตัวเลขไหม" พอ ถ้าหน่วย/สัญลักษณ์ตามมาแล้วจริง (เช่น "2,000 พอยต์") ท้าย
// candidate จะไม่ใช่ตัวเลขอีกต่อไป ไม่โดน protect ซ้ำ
function endsWithNumericToken(text) {
  return /[0-9๐-๙]$/.test(text.trim())
}

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
    if (naturalCut !== -1) {
      // L1c1: candidate ลงท้ายด้วยตัวเลข + ยังไม่ครบ HARD_MAX_MS → กันไว้ก่อน รอ delta ถัดไปที่น่าจะเป็นหน่วยนับ
      // ตามมา (ถ้า Claude เงียบผิดปกติจริง protection จะหมดอายุเองที่ HARD_MAX_MS ด้านล่าง ไม่รอไม่มีที่สิ้นสุด)
      const candidate = buffer.slice(0, naturalCut).trim()
      const protectedByNumericTail = elapsedMs < HARD_MAX_MS && endsWithNumericToken(candidate)
      if (!protectedByNumericTail) return split(buffer, naturalCut)
    }
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

// L1c1 follow-up — คืนค่า:
//   null       — ไม่ได้ถูก numeric protection กันอยู่ตอนนี้ (ไม่มี natural-boundary candidate เลย, ตัดได้ทันที
//                ผ่าน strong/soft boundary อยู่แล้ว, หรือ protection หมดอายุไปแล้วที่ elapsedMs >= HARD_MAX_MS)
//   number > 0 — จำนวน ms ที่เหลือจนถึง HARD_MAX_MS (ไม่มีทางเป็น 0 พอดี เพราะ elapsedMs >= HARD_MAX_MS คืน
//                null ไปก่อนแล้วด้านล่าง) — caller ควร arm wall-clock timer ไว้เท่านี้ แล้วเรียก
//                findChunkBoundary() ซ้ำตอน timer fire จริง (ฟังก์ชันนี้เอง "ไม่ตัด" อะไรทั้งสิ้น แค่บอกเวลา)
//
// ไม่ gate ด้วย elapsedMs < SOFT_TIMEOUT_MS เหมือน findChunkBoundary() เพราะ buffer ที่ยังไม่ครบ soft timeout
// ไม่มีทางเปลี่ยนแปลงเองได้ถ้าไม่มี delta ใหม่ (ซึ่งถ้ามี delta ใหม่จริงก็จะเรียกฟังก์ชันนี้ใหม่จาก state ล่าสุดอยู่
// แล้ว) — arm timer ไปที่ HARD_MAX_MS ตรงๆ ครั้งเดียวครอบคลุมทั้งสองช่วงเวลาได้พอ ไม่ต้องมี timer สองชั้น
function getNumericProtectionRemainingMs(buffer, elapsedMs) {
  if (!buffer) return null

  const strongCut = findEarliestMatch(buffer, STRONG_BOUNDARY_RE)
  const softCut = findEarliestMatch(buffer, THAI_SOFT_BOUNDARY_RE)
  if (strongCut !== -1 || softCut !== -1) return null // ตัดได้เลยผ่าน strong/soft path อยู่แล้ว ไม่ใช่ numeric-protected case

  if (buffer.length < FALLBACK_MIN_LENGTH) return null // ยังไม่ถึงเกณฑ์ natural-boundary เลย ไม่มีอะไรให้ protect

  const naturalCut = findLastSafeBoundary(buffer, MIN_CHUNK_LENGTH)
  if (naturalCut === -1) return null // ไม่มี natural boundary ให้ตัดเลย (คำเดียวยาวผิดปกติ) — ไม่ใช่ numeric-protection case

  const candidate = buffer.slice(0, naturalCut).trim()
  if (!endsWithNumericToken(candidate)) return null // ไม่ได้ลงท้ายตัวเลข ไม่ถูก protect

  if (elapsedMs >= HARD_MAX_MS) return null // protection หมดอายุไปแล้ว — findChunkBoundary() ตัดได้เองอยู่แล้วตอนนี้

  return HARD_MAX_MS - elapsedMs
}

module.exports = { findChunkBoundary, getNumericProtectionRemainingMs }
