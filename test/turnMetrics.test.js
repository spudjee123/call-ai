const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createTurnMetrics, markOnce, duration, computeDerivedMetrics } = require('../src/utils/turnMetrics')

test('createTurnMetrics: field ที่ให้มาถูกเก็บครบ, t1-t7 เป็น null ทั้งหมด, fallback fields ค่าเริ่มต้นถูกต้อง', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 3, path: 'legacy', rolloutBucket: 42, rolloutPercent: 0 })
  assert.equal(m.callSid, 'CA1')
  assert.equal(m.generationId, 3)
  assert.equal(m.path, 'legacy')
  assert.equal(m.rolloutBucket, 42)
  assert.equal(m.rolloutPercent, 0)
  for (const key of ['t0', 't1', 't2', 't3', 't4', 't5', 't6', 't7']) {
    assert.equal(m[key], null, `${key} ต้องเริ่มต้นเป็น null`)
  }
  assert.equal(m.fallbackTriggered, false)
  assert.equal(m.fallbackReason, null)
  assert.equal(m.fallbackStartedAt, null)
  assert.equal(m.fallbackOutcome, null)
  assert.equal(m.audioCommitted, false)
})

test('createTurnMetrics: startedAt เป็น ISO string ที่ parse กลับมาเป็นวันเวลาได้จริง', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 0, rolloutPercent: 0 })
  assert.equal(typeof m.startedAt, 'string')
  assert.ok(!Number.isNaN(new Date(m.startedAt).getTime()))
})

test('markOnce: set ค่าครั้งแรกให้เป็นตัวเลข (monotonic clock)', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 0, rolloutPercent: 0 })
  markOnce(m, 't1')
  assert.equal(typeof m.t1, 'number')
})

test('markOnce: เรียกซ้ำไม่ทับค่าเดิม — ค่ายังคงเป็นของครั้งแรกเสมอ (กัน chunk สุดท้ายทับ chunk แรก)', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 0, rolloutPercent: 0 })
  markOnce(m, 't6')
  const firstValue = m.t6
  markOnce(m, 't6')
  markOnce(m, 't6')
  assert.equal(m.t6, firstValue)
})

test('duration: คืนค่าผลต่างถ้ามีทั้งสองค่า', () => {
  assert.equal(duration(100, 250), 150)
})

test('duration: คืน null ถ้าฝั่งใดฝั่งหนึ่งเป็น null (ไม่ใช่ NaN)', () => {
  assert.equal(duration(null, 250), null)
  assert.equal(duration(100, null), null)
  assert.equal(duration(null, null), null)
})

test('computeDerivedMetrics: ครบทุกค่า → คำนวณ derived ทุกตัวถูกต้อง (จำลอง path chunked ในอนาคต)', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 1, path: 'chunked', rolloutBucket: 3, rolloutPercent: 100 })
  m.t1 = 0
  m.t2 = 10
  m.t3 = 210
  m.t4 = 260
  m.t5 = 270
  m.t6 = 470
  m.t7 = 480
  const d = computeDerivedMetrics(m)
  assert.equal(d.sttToTwilio, 480)
  assert.equal(d.claudeTTFT, 200)
  assert.equal(d.chunkDelay, 50)
  assert.equal(d.ttsTTFB, 200)
  assert.equal(d.requestToAudio, 470)
})

test('computeDerivedMetrics: path=legacy ที่ไม่มี t3/t4 จริง → claudeTTFT/chunkDelay เป็น null ไม่ใช่ NaN แต่ตัวอื่นที่มีค่าครบยังคำนวณได้ปกติ', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 7, rolloutPercent: 0 })
  m.t1 = 0
  m.t2 = 5
  // t3, t4 ไม่มีความหมายบน legacy path — ปล่อย null ไว้ตามจริง ไม่ fabricate
  m.t5 = 300
  m.t6 = 500
  m.t7 = 510
  const d = computeDerivedMetrics(m)
  assert.equal(d.claudeTTFT, null)
  assert.equal(d.chunkDelay, null)
  assert.equal(d.sttToTwilio, 510)
  assert.equal(d.ttsTTFB, 200)
  assert.equal(d.requestToAudio, 505)
  for (const v of Object.values(d)) assert.ok(!Number.isNaN(v), 'ห้ามมี NaN หลุดออกมาเด็ดขาด')
})

test('computeDerivedMetrics: turn ที่จบก่อนถึง TTS เลย (ไม่มี t5-t7) → derived ที่เกี่ยวข้องเป็น null หมด ไม่ throw', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 7, rolloutPercent: 0 })
  m.t1 = 0
  m.t2 = 5
  const d = computeDerivedMetrics(m)
  assert.equal(d.sttToTwilio, null)
  assert.equal(d.ttsTTFB, null)
  assert.equal(d.requestToAudio, null)
})

// Track 6 (2026-08-30) — sttProcessingMs ปิด blind spot latency ต้นทาง (t0 = frame เสียงแรกที่เห็นตั้งแต่เริ่มฟังรอบล่าสุด)
test('computeDerivedMetrics: sttProcessingMs = t1 - t0 เมื่อมีค่าทั้งคู่', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 0, rolloutPercent: 0 })
  m.t0 = 50
  m.t1 = 1350
  const d = computeDerivedMetrics(m)
  assert.equal(d.sttProcessingMs, 1300)
})

test('computeDerivedMetrics: sttProcessingMs เป็น null ถ้าไม่เคย mark t0 (เช่น deploy ใหม่ยังไม่มี media frame มาก่อน final)', () => {
  const m = createTurnMetrics({ callSid: 'CA1', generationId: 0, path: 'legacy', rolloutBucket: 0, rolloutPercent: 0 })
  m.t1 = 1350
  const d = computeDerivedMetrics(m)
  assert.equal(d.sttProcessingMs, null)
})
