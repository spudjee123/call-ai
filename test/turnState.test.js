const { test } = require('node:test')
const assert = require('node:assert/strict')
const {
  createTurnState,
  markTtsPending,
  markAudioCommitted,
  markDone,
  canFallback,
  attemptFallback,
} = require('../src/utils/turnState')

test('initial: phase = GENERATING, audioCommitted/fallbackTriggered = false', () => {
  const t = createTurnState(1)
  assert.equal(t.generationId, 1)
  assert.equal(t.phase, 'GENERATING')
  assert.equal(t.audioCommitted, false)
  assert.equal(t.fallbackTriggered, false)
})

test('markTtsPending: GENERATING → TTS_PENDING', () => {
  const t = createTurnState(1)
  markTtsPending(t)
  assert.equal(t.phase, 'TTS_PENDING')
})

test('markTtsPending: ไม่ทำอะไรถ้า phase ไม่ใช่ GENERATING แล้ว (ไม่ถอยหลัง)', () => {
  const t = createTurnState(1)
  markAudioCommitted(t)
  markTtsPending(t)
  assert.equal(t.phase, 'AUDIO_COMMITTED', 'ห้ามถอยจาก AUDIO_COMMITTED กลับไป TTS_PENDING')
})

test('markAudioCommitted: transition ไป AUDIO_COMMITTED และ set audioCommitted=true', () => {
  const t = createTurnState(1)
  markTtsPending(t)
  markAudioCommitted(t)
  assert.equal(t.phase, 'AUDIO_COMMITTED')
  assert.equal(t.audioCommitted, true)
})

test('markAudioCommitted: idempotent — เรียกซ้ำไม่พังอะไร', () => {
  const t = createTurnState(1)
  markAudioCommitted(t)
  markAudioCommitted(t)
  markAudioCommitted(t)
  assert.equal(t.phase, 'AUDIO_COMMITTED')
  assert.equal(t.audioCommitted, true)
})

test('markDone: ไปที่ DONE ได้จากทุก phase', () => {
  const t = createTurnState(1)
  markDone(t)
  assert.equal(t.phase, 'DONE')
})

test('canFallback: true ก่อน commit', () => {
  const t = createTurnState(1)
  assert.equal(canFallback(t), true)
  markTtsPending(t)
  assert.equal(canFallback(t), true, 'TTS_PENDING ยังไม่ commit เสียงจริง ยัง fallback ได้')
})

test('canFallback: false หลัง commit', () => {
  const t = createTurnState(1)
  markAudioCommitted(t)
  assert.equal(canFallback(t), false)
})

test('attemptFallback: ยิงได้ครั้งเดียวก่อน commit, คืน true ตอนยิงสำเร็จ', () => {
  const t = createTurnState(1)
  let calls = 0
  const ok = attemptFallback(t, () => { calls++ })
  assert.equal(ok, true)
  assert.equal(calls, 1)
  assert.equal(t.fallbackTriggered, true)
})

test('attemptFallback: เรียกซ้ำหลังยิงไปแล้วรอบแรก ต้องไม่ยิงซ้ำ (คืน false)', () => {
  const t = createTurnState(1)
  let calls = 0
  attemptFallback(t, () => { calls++ })
  const second = attemptFallback(t, () => { calls++ })
  assert.equal(second, false)
  assert.equal(calls, 1, 'ต้องยิง doFallback แค่ครั้งเดียวเท่านั้น แม้เรียก attemptFallback ซ้ำ')
})

test('attemptFallback: หลัง commit แล้ว ต้องไม่ยิงเลย (คืน false, ไม่เรียก doFallback)', () => {
  const t = createTurnState(1)
  markAudioCommitted(t)
  let calls = 0
  const ok = attemptFallback(t, () => { calls++ })
  assert.equal(ok, false)
  assert.equal(calls, 0)
})
