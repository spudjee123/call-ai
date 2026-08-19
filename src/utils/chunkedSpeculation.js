// L1b — chunked speculative prewarm: pure matching/classification logic, แยกจาก audioStream.js เพื่อ
// unit-test ได้อิสระจาก WS harness ทั้งชุด (เหมือน generationGuard.js/turnState.js)
//
// Conservative adoption rule (ล็อกไว้ตอน design review): normalized exact match เท่านั้น — ถ้า final
// transcript เพิ่มเนื้อหาเกินกว่า interim ที่ speculation เริ่มจาก (semantic extension) ถือเป็น MISS เสมอ
// เหตุผล: คำตอบ speculative ถูก generate จาก user message ที่เป็น interim (ยังไม่จบประโยคจริง) — ถ้า final
// มีเนื้อหาเพิ่ม ไม่ควรเอาคำตอบจากคำถามที่ยังไม่จบมาใช้เพียงเพื่อ latency
function normalizeForMatch(text) {
  return (text || '')
    .trim()
    .replace(/\s+/g, ' ')
    .replace(/[.?!,๚๛…]+$/g, '')
}

function isSpeculationMatch(interimText, finalText) {
  return normalizeForMatch(interimText) === normalizeForMatch(finalText)
}

// classifyForAdoption(handle) — ตัดสินใจจาก producer state จริง ณ ตอนนี้ (ไม่ใช้ producerDone แทนความหมาย
// "มี delta อยู่" เพราะ producerDone เพียวๆ ครอบคลุมทั้ง empty response และ control-only response ด้วย)
//
// รอบ 3 (design lock): "has delta, no chunk, still running" ADOPT_NOW ทันที (ไม่รอ grace 2000ms ก่อน adopt
// อีกต่อไป) แล้วให้ Watchdog B ตัวจริงของเทิร์นเป็นคนคุมต่อ — grace 150ms เหลือใช้เฉพาะ "ไม่มี progress เลย"
// เท่านั้น (invariant: speculative optimization must never become a blocking dependency)
//
// คืนค่า:
//   { decision: 'ADOPT_NOW', outcome }  — outcome หนึ่งใน BUFFERED_HIT/READY_HIT/CONTROL_ONLY_HIT/DELTA_ONLY_HIT
//   { decision: 'DROP', outcome }       — outcome หนึ่งใน ERROR_FRESH/EMPTY_FRESH
//   { decision: 'GRACE' }               — ต้องรอ producer.waitForFirstProgress() สูงสุด PREWARM_GRACE_MS
//                                          ก่อน re-classify (ผู้เรียกเป็นคนกำหนด timeout จริง)
function classifyForAdoption(handle) {
  const p = handle.producer
  if (p.producerError) return { decision: 'DROP', outcome: 'ERROR_FRESH' }
  if (p.queue.length > 0) return { decision: 'ADOPT_NOW', outcome: p.producerDone ? 'READY_HIT' : 'BUFFERED_HIT' }
  if (p.producerDone) {
    return p.controlEvent
      ? { decision: 'ADOPT_NOW', outcome: 'CONTROL_ONLY_HIT' }
      : { decision: 'DROP', outcome: 'EMPTY_FRESH' }
  }
  if (p.firstDeltaAt != null) return { decision: 'ADOPT_NOW', outcome: 'DELTA_ONLY_HIT' }
  return { decision: 'GRACE' }
}

module.exports = { normalizeForMatch, isSpeculationMatch, classifyForAdoption }
