const { test } = require('node:test')
const assert = require('node:assert/strict')
const { normalizeForMatch, isSpeculationMatch, classifyForAdoption } = require('../src/utils/chunkedSpeculation')

test('normalizeForMatch: trim + collapse whitespace + strip trailing punctuation', () => {
  assert.equal(normalizeForMatch('  สวัสดีครับ   ขอสอบถาม  '), 'สวัสดีครับ ขอสอบถาม')
  assert.equal(normalizeForMatch('จบประโยคแล้ว.'), 'จบประโยคแล้ว')
  assert.equal(normalizeForMatch('จบประโยคแล้ว...'), 'จบประโยคแล้ว')
  assert.equal(normalizeForMatch(null), '')
  assert.equal(normalizeForMatch(undefined), '')
})

test('isSpeculationMatch: exact match (หลัง normalize) → true', () => {
  assert.equal(isSpeculationMatch('ขอสอบถามโปรโมชั่น', 'ขอสอบถามโปรโมชั่น'), true)
  assert.equal(isSpeculationMatch('ขอสอบถามโปรโมชั่น ', ' ขอสอบถามโปรโมชั่น'), true)
  assert.equal(isSpeculationMatch('ขอสอบถามโปรโมชั่น.', 'ขอสอบถามโปรโมชั่น'), true)
})

test('isSpeculationMatch: semantic extension (final ยาวกว่า interim) → MISS แม้ final จะขึ้นต้นด้วย interim ก็ตาม', () => {
  assert.equal(isSpeculationMatch('ขอสอบถาม', 'ขอสอบถามโปรโมชั่นสมาชิกใหม่'), false)
})

test('isSpeculationMatch: คนละเรื่องกันเลย → MISS', () => {
  assert.equal(isSpeculationMatch('สวัสดีครับ', 'ไม่มีอะไรตรงกับ interim เลยครับ'), false)
})

function producer(overrides) {
  return { producer: { queue: [], producerDone: false, producerError: null, controlEvent: null, firstDeltaAt: null, ...overrides } }
}

test('classifyForAdoption: มี chunk ใน queue, producer ยังไม่จบ → ADOPT_NOW / BUFFERED_HIT', () => {
  const handle = producer({ queue: ['chunk1'] })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'ADOPT_NOW', outcome: 'BUFFERED_HIT' })
})

test('classifyForAdoption: มี chunk ใน queue, producer จบแล้ว → ADOPT_NOW / READY_HIT', () => {
  const handle = producer({ queue: ['chunk1'], producerDone: true })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'ADOPT_NOW', outcome: 'READY_HIT' })
})

test('classifyForAdoption: ไม่มี chunk, มี delta, ยังไม่จบ → ADOPT_NOW / DELTA_ONLY_HIT (ไม่ใช่ GRACE — แก้จาก design รอบ 3)', () => {
  const handle = producer({ firstDeltaAt: 123.45 })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'ADOPT_NOW', outcome: 'DELTA_ONLY_HIT' })
})

test('classifyForAdoption: จบแล้วไม่มี chunk แต่มี control event (end_call ล้วนๆ ไม่มี text) → ADOPT_NOW / CONTROL_ONLY_HIT', () => {
  const handle = producer({ producerDone: true, controlEvent: { type: 'end_call' } })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'ADOPT_NOW', outcome: 'CONTROL_ONLY_HIT' })
})

test('classifyForAdoption: จบแล้วไม่มี chunk ไม่มี control เลย (response ว่างเปล่าจริง) → DROP / EMPTY_FRESH', () => {
  const handle = producer({ producerDone: true })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'DROP', outcome: 'EMPTY_FRESH' })
})

test('classifyForAdoption: ไม่มี delta เลย ยังไม่จบ → GRACE', () => {
  const handle = producer({})
  assert.deepEqual(classifyForAdoption(handle), { decision: 'GRACE' })
})

test('classifyForAdoption: producerError set → DROP / ERROR_FRESH เสมอ ไม่ว่า queue จะมี partial chunk ค้างอยู่ก็ตาม', () => {
  const handle = producer({ queue: ['partial before error'], producerError: new Error('boom') })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'DROP', outcome: 'ERROR_FRESH' })
})

test('classifyForAdoption: producerError ชนะ CONTROL_ONLY_HIT ด้วย (error มาทีหลัง control event ก็ยังต้อง DROP)', () => {
  const handle = producer({ producerDone: true, controlEvent: { type: 'end_call' }, producerError: new Error('boom') })
  assert.deepEqual(classifyForAdoption(handle), { decision: 'DROP', outcome: 'ERROR_FRESH' })
})
