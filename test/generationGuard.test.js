const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createCallState, bumpGeneration, isCurrentGeneration, endCall } = require('../src/utils/generationGuard')

test('initial state: generationId = 0, ended = false', () => {
  const callState = createCallState()
  assert.equal(callState.generationId, 0)
  assert.equal(callState.ended, false)
})

test('bumpGeneration: เรียกซ้ำๆ ได้เลขที่ต่างกันเสมอ เรียงขึ้นทีละ 1', () => {
  const callState = createCallState()
  const g1 = bumpGeneration(callState)
  const g2 = bumpGeneration(callState)
  const g3 = bumpGeneration(callState)
  assert.equal(g1, 1)
  assert.equal(g2, 2)
  assert.equal(g3, 3)
})

test('isCurrentGeneration: generation ปัจจุบันต้องผ่าน', () => {
  const callState = createCallState()
  const g1 = bumpGeneration(callState)
  assert.equal(isCurrentGeneration(callState, g1), true)
})

test('isCurrentGeneration: generation เก่าหลัง bump ต้องไม่ผ่าน (stale)', () => {
  const callState = createCallState()
  const g1 = bumpGeneration(callState)
  const g2 = bumpGeneration(callState)
  assert.equal(isCurrentGeneration(callState, g1), false, 'gen1 ต้อง stale หลัง gen2 มา')
  assert.equal(isCurrentGeneration(callState, g2), true)
})

test('endCall: ทำให้ generation ปัจจุบันก็ไม่ current อีกต่อไป แม้เลขจะตรงกันก็ตาม', () => {
  const callState = createCallState()
  const g1 = bumpGeneration(callState)
  assert.equal(isCurrentGeneration(callState, g1), true)
  endCall(callState)
  assert.equal(isCurrentGeneration(callState, g1), false, 'สายจบแล้ว ไม่มี generation ไหน current อีกต่อไป')
})

test('invalidate-before-abort ordering: bump ต้องทำให้ gen เก่า stale ได้ทันที ก่อนจะสั่ง abort จริง', () => {
  const callState = createCallState()
  const staleGen = bumpGeneration(callState)
  // จำลอง controller.abort() เป็นขั้นตอนถัดไปหลังบรรทัดนี้ — ณ จุดนี้ (ก่อน abort ถูกเรียกจริงด้วยซ้ำ)
  // callback เก่าที่หลุดเข้ามาระหว่างนั้นต้องเจอว่า generation ของตัวเองเป็นโมฆะไปแล้ว
  const newGen = bumpGeneration(callState)
  assert.equal(isCurrentGeneration(callState, staleGen), false, 'ต้อง stale ทันทีที่ bump แล้ว ไม่ต้องรอ abort() เสร็จก่อน')
  assert.equal(isCurrentGeneration(callState, newGen), true)
})
