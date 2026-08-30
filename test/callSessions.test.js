// Track 7 (defensive fix, 2026-08-30) — session-reap safety timer, mirror ของ callQueue.js's RELEASE_SAFETY_TIMEOUT_MS
// ใช้ mock.timers (pattern เดียวกับ claudeConditional.test.js) กัน test ต้องรอจริงหลายนาที
const { test, beforeEach, mock } = require('node:test')
const assert = require('node:assert/strict')

process.env.MAX_CALL_DURATION_SECONDS = '300' // SESSION_REAP_SAFETY_TIMEOUT_MS = (300+120)*1000 = 420000ms

const sessionsPath = require.resolve('../src/utils/callSessions')
delete require.cache[sessionsPath] // บังคับ re-require หลังตั้ง env var ข้างบนเสมอ (module-level const อ่านตอน require ครั้งแรกเท่านั้น)
const callSessions = require('../src/utils/callSessions')

beforeEach(() => {
  mock.timers.reset()
})

test('Track 7: session ที่ไม่ถูกลบตามปกติ ต้องถูกลบเองโดย safety timer หลังครบเวลา (กัน memory leak ถ้าไม่ได้รับ /webhook/status)', () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    callSessions.set('CA_leak_test', { callSid: 'CA_leak_test' })
    assert.equal(callSessions.has('CA_leak_test'), true)

    mock.timers.tick(419999) // ยังไม่ครบ 420000ms
    assert.equal(callSessions.has('CA_leak_test'), true, 'ยังไม่ครบเวลา ต้องยังไม่ถูกลบ')

    mock.timers.tick(1) // ครบ 420000ms พอดี
    assert.equal(callSessions.has('CA_leak_test'), false, 'ครบเวลาแล้วต้องถูกลบเองอัตโนมัติ')
  } finally {
    mock.timers.reset()
  }
})

test('Track 7: session ที่ถูก delete() ตามปกติ (เหมือน webhook/status มาถึงจริง) ต้อง clear safety timer ไปด้วย ไม่ยิงซ้ำทีหลัง', () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    callSessions.set('CA_normal_flow', { callSid: 'CA_normal_flow' })
    callSessions.delete('CA_normal_flow')
    assert.equal(callSessions.has('CA_normal_flow'), false)

    // เลยเวลา safety timer เดิมไปมาก — ต้องไม่มี error/side effect อะไรเกิดขึ้นอีก (timer ถูก clear ไปแล้วจริง)
    mock.timers.tick(420000)
    assert.equal(callSessions.has('CA_normal_flow'), false)
  } finally {
    mock.timers.reset()
  }
})

test('Track 7: set() ซ้ำ callSid เดิม (ไม่ควรเกิดปกติ) ต้อง reset timer ไม่ใช่มี timer สองตัวค้างซ้อนกัน', () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    callSessions.set('CA_double_set', { v: 1 })
    mock.timers.tick(200000) // ผ่านไปครึ่งทาง
    callSessions.set('CA_double_set', { v: 2 }) // set ซ้ำ — ต้อง reset นาฬิกาใหม่จากศูนย์

    mock.timers.tick(419999) // นับจาก set() ครั้งที่สอง = 419999ms ยังไม่ครบ 420000ms ของรอบใหม่ (ถ้านับจากครั้งแรกจะครบไปแล้วตั้งแต่ tick(200000)+tick(219999)=419999 ก็จริง แต่ต้อง "ยังไม่ตาย" เพราะรอบถูก reset ใหม่)
    assert.equal(callSessions.has('CA_double_set'), true, 'ต้องนับเวลาใหม่จาก set() ครั้งหลังสุด ไม่ใช่ครั้งแรก')

    mock.timers.tick(1)
    assert.equal(callSessions.has('CA_double_set'), false)
  } finally {
    mock.timers.reset()
  }
})
