// B.5.3 — AUDIO_COMMITTED state machine (per generation/per เทิร์น ไม่ใช่ per สาย)
// GENERATING → TTS_PENDING → AUDIO_COMMITTED → DONE
//
// นิยาม AUDIO_COMMITTED ต้องเป๊ะ: "audio chunk แรกของ generation นี้ถูกส่งออกไปทาง Twilio สำเร็จแล้ว"
// เท่านั้น — ไม่ใช่ Claude ตอบเสร็จ, ไม่ใช่ chunker emit แล้ว, ไม่ใช่ส่ง request ไป ElevenLabs แล้ว,
// ไม่ใช่ ElevenLabs ตอบ audio กลับมาแล้ว เพราะจุดนี้คือ boundary เดียวที่ใช้ตัดสิน fallback ได้/ไม่ได้ —
// ถ้า mark เร็วเกินไปจะเสี่ยง fallback ทั้งที่ลูกค้ายังไม่เคยได้ยินอะไรเลย (เสียโอกาสกู้สายเปล่าๆ)
// ถ้า mark ช้าเกินไปจะเสี่ยงปล่อยให้ fallback หลังลูกค้าได้ยินไปแล้ว (พูดซ้ำ)

function createTurnState(generationId) {
  return { generationId, phase: 'GENERATING', audioCommitted: false, fallbackTriggered: false }
}

function markTtsPending(turnState) {
  if (turnState.phase === 'GENERATING') turnState.phase = 'TTS_PENDING'
}

function markAudioCommitted(turnState) {
  if (!turnState.audioCommitted) {
    turnState.audioCommitted = true
    turnState.phase = 'AUDIO_COMMITTED'
  }
}

function markDone(turnState) {
  turnState.phase = 'DONE'
}

// invariant หลักของทั้งระบบ: fallback กลับไปใช้ path เดิมได้ก็ต่อเมื่อยังไม่เคยส่งเสียงออกไปเลยเท่านั้น
// เก็บเป็น field แยก (ไม่ใช่เทียบ phase string ตรงๆ) เพราะนี่คือ safety invariant ที่สำคัญที่สุดของ B.5
// อยากให้เห็นตรงๆ ในโค้ดว่าคือเงื่อนไขนี้ ไม่ใช่กระจายเป็น if (phase === 'X' || phase === 'Y') ที่ต้องตามแก้
// ทุกจุดถ้าเพิ่ม phase ใหม่ทีหลัง
function canFallback(turnState) {
  return !turnState.audioCommitted
}

// จองสิทธิ์ทำ fallback แบบ single-fire ต่อเทิร์น (C4a) — แยกจาก attemptFallback ด้านล่างเพราะ caller บางที่
// (เช่น chunked→legacy fallback ใน audioStream.js) ต้อง await งานจริงเอง ไม่ใช่ยัด async ผ่าน callback แบบเดิม
// คืน true ถ้าจองสิทธิ์ได้ (ยังไม่เคย fallback มาก่อน และยังไม่ commit เสียง) / false ถ้าจองไม่ได้
function claimFallback(turnState) {
  if (!canFallback(turnState)) return false
  if (turnState.fallbackTriggered) return false
  turnState.fallbackTriggered = true
  return true
}

// ห่อ fallback ไว้ให้ยิงได้แค่ครั้งเดียวต่อเทิร์น แม้ error callback จะยิงซ้ำมาหลายรอบก่อนถึงจุด commit ก็ตาม
// คืนค่า true ถ้า fallback ถูกเรียกจริงรอบนี้ / false ถ้าไม่เข้าเงื่อนไข (commit ไปแล้ว หรือ fallback ไปแล้ว)
function attemptFallback(turnState, doFallback) {
  if (!claimFallback(turnState)) return false
  doFallback()
  return true
}

module.exports = { createTurnState, markTtsPending, markAudioCommitted, markDone, canFallback, claimFallback, attemptFallback }
