// รวมเทส race condition ที่ generationGuard.js + turnState.js ต้องป้องกันร่วมกัน
// จำลอง callback ที่มาช้า (มาหลัง generation ถัดไปเริ่มไปแล้ว, มาหลัง barge-in, มาหลัง commit) ไม่ใช่แค่ happy path
const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createCallState, bumpGeneration, isCurrentGeneration, endCall } = require('../src/utils/generationGuard')
const { createTurnState, markTtsPending, markAudioCommitted, canFallback, attemptFallback } = require('../src/utils/turnState')

// helper: จำลอง side-effect boundary หนึ่งจุด (เช่น "ส่งเข้า Twilio") ที่ต้องเช็ค guard ก่อนทำงานจริงเสมอ
function guardedSideEffect(callState, generationId, effectFn) {
  if (!isCurrentGeneration(callState, generationId)) return { ran: false }
  effectFn()
  return { ran: true }
}

test('1) Claude text delta ของ gen1 มาถึงหลัง gen2 เริ่มไปแล้ว → ต้องถูกดรอป', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  const gen2 = bumpGeneration(callState) // gen2 เริ่มก่อน delta ของ gen1 จะมาถึง (เช่น barge-in เร็วมาก)
  let deltasApplied = []
  const r = guardedSideEffect(callState, gen1, () => deltasApplied.push('gen1-delta'))
  assert.equal(r.ran, false)
  assert.deepEqual(deltasApplied, [])
  assert.equal(isCurrentGeneration(callState, gen2), true)
})

test('2) speechChunker emit ของ gen1 มาถึงหลัง gen2 เริ่มไปแล้ว → ต้องถูกดรอป ไม่ส่งต่อไป TTS', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  bumpGeneration(callState) // gen2
  let ttsRequestsQueued = []
  const r = guardedSideEffect(callState, gen1, () => ttsRequestsQueued.push('chunk-from-gen1'))
  assert.equal(r.ran, false)
  assert.deepEqual(ttsRequestsQueued, [])
})

test('3) ElevenLabs audio ของ gen1 ตอบกลับมาช้าหลัง barge-in (gen2 เริ่มแล้ว) → ห้ามส่งเข้า Twilio', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  bumpGeneration(callState) // barge-in → gen2
  let audioSentToTwilio = []
  const r = guardedSideEffect(callState, gen1, () => audioSentToTwilio.push('gen1-audio'))
  assert.equal(r.ran, false)
  assert.deepEqual(audioSentToTwilio, [], 'เสียงผีจาก gen1 ต้องไม่มีทางไปถึงลูกค้า')
})

test('4) จุดสุดท้ายก่อนส่งเข้า Twilio (จุดสำคัญที่สุด): gen เปลี่ยนไปแล้วระหว่างรอ ต้องเช็คซ้ำตรงนี้ด้วย ไม่ใช่เชื่อผลเช็คจุดก่อนหน้า', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  // สมมติผ่านการเช็คที่จุด ElevenLabs-audio-arrival มาแล้วตอนที่ gen1 ยัง current อยู่
  assert.equal(isCurrentGeneration(callState, gen1), true)
  // แต่ระหว่างที่กำลังจะยิง Twilio send จริงๆ (async gap) gen เปลี่ยนไปก่อน
  bumpGeneration(callState) // gen2
  let sent = false
  const r = guardedSideEffect(callState, gen1, () => { sent = true })
  assert.equal(r.ran, false)
  assert.equal(sent, false, 'ต้องเช็ค guard อีกครั้ง ณ จุดส่งจริง ไม่ใช่เชื่อผลเช็คที่จุดก่อนหน้าเพียงจุดเดียว')
})

test('5) error เกิดก่อน commit เสียง → fallback ต้องยิงหนึ่งครั้ง', () => {
  const t = createTurnState(1)
  markTtsPending(t)
  let calls = 0
  const ok = attemptFallback(t, () => { calls++ })
  assert.equal(ok, true)
  assert.equal(calls, 1)
})

test('6) error เกิดหลัง commit เสียงไปแล้ว → fallback ต้องไม่ยิงเลย (ศูนย์ครั้ง)', () => {
  const t = createTurnState(1)
  markTtsPending(t)
  markAudioCommitted(t)
  let calls = 0
  const ok = attemptFallback(t, () => { calls++ })
  assert.equal(ok, false)
  assert.equal(calls, 0, 'ลูกค้าได้ยินเสียงไปแล้ว ห้ามพูดซ้ำด้วย fallback')
})

test('7) error ซ้ำหลายครั้งก่อน commit (เช่น ElevenLabs timeout ยิง error 2 รอบ) → fallback ยังยิงแค่ครั้งเดียว', () => {
  const t = createTurnState(1)
  markTtsPending(t)
  let calls = 0
  const first = attemptFallback(t, () => { calls++ })
  const second = attemptFallback(t, () => { calls++ }) // error ซ้ำมาอีกรอบก่อน turn จะถูกเปลี่ยน phase
  assert.equal(first, true)
  assert.equal(second, false)
  assert.equal(calls, 1)
})

test('8) barge-in ระหว่าง TTS_PENDING (ยังไม่ commit เสียง) → gen เก่าถูก invalidate ทันที ไม่มีเสียงผีหลุดออกไป', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  const t1 = createTurnState(gen1)
  markTtsPending(t1)
  assert.equal(canFallback(t1), true)

  bumpGeneration(callState) // barge-in
  let audioSent = []
  const r = guardedSideEffect(callState, gen1, () => audioSent.push('late-audio'))
  assert.equal(r.ran, false, 'gen1 ถูก invalidate แล้ว เสียงที่กำลังจะมาไม่ควรหลุดออกไป')
  assert.deepEqual(audioSent, [])
})

test('9) barge-in หลัง AUDIO_COMMITTED แล้ว → เสียงที่มาช้าของ gen เก่าถูก drop ด้วย guard (ไม่ใช่ turnState เป็นคนกันตรงนี้)', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  const t1 = createTurnState(gen1)
  markTtsPending(t1)
  markAudioCommitted(t1)
  assert.equal(canFallback(t1), false)

  bumpGeneration(callState) // barge-in เกิดหลัง commit (เช่นลูกค้าพูดแทรกทันทีที่ได้ยินคำแรก)
  let audioSent = []
  const r = guardedSideEffect(callState, gen1, () => audioSent.push('late-audio-after-commit'))
  assert.equal(r.ran, false, 'audio chunk ที่เหลือของ gen1 (chunk 2, 3, ...) ต้องถูก drop โดย generationId guard')
  assert.deepEqual(audioSent, [])
})

test('10) callback ของ gen1 มาช้ามากๆ (มาถึงตอนที่ gen อยู่ที่ gen3 แล้ว) → ต้องไม่มีทางแก้ state ของ gen3 ได้เลย', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  bumpGeneration(callState) // gen2
  const gen3 = bumpGeneration(callState)
  const t3 = createTurnState(gen3)
  markTtsPending(t3)

  // callback แก่ๆ ของ gen1 หลุดมาถึงตอนนี้ พยายามจะ mutate อะไรบางอย่าง
  let gen3Mutated = false
  const r = guardedSideEffect(callState, gen1, () => {
    gen3Mutated = true
    markAudioCommitted(t3) // สมมติ callback เก่าพยายามยุ่งกับ turnState ของ gen ปัจจุบัน
  })
  assert.equal(r.ran, false)
  assert.equal(gen3Mutated, false)
  assert.equal(t3.phase, 'TTS_PENDING', 'state ของ gen3 ต้องไม่ถูกแตะต้องโดย callback แก่ของ gen1 เลย')
})

test('endCall ระหว่างที่ callback ค้างอยู่ → ไม่ว่า generationId จะตรงแค่ไหนก็ต้องถูก drop เพราะสายจบแล้ว', () => {
  const callState = createCallState()
  const gen1 = bumpGeneration(callState)
  endCall(callState) // สายจบไปแล้ว (เช่น ลูกค้าวางสาย) ก่อน callback สุดท้ายจะมาถึง
  let ran = false
  const r = guardedSideEffect(callState, gen1, () => { ran = true })
  assert.equal(r.ran, false)
  assert.equal(ran, false)
})
