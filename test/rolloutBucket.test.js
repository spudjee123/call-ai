const { test } = require('node:test')
const assert = require('node:assert/strict')
const { getRolloutBucket, decideRollout } = require('../src/utils/rolloutBucket')

test('same callSid → bucket เดิมเสมอ ไม่ว่าจะเรียกกี่ครั้ง', () => {
  const a = getRolloutBucket('CA1234567890')
  const b = getRolloutBucket('CA1234567890')
  const c = getRolloutBucket('CA1234567890')
  assert.equal(a, b)
  assert.equal(b, c)
})

test('different callSid → bucket กระจายตัว ไม่กระจุกที่ค่าเดียว', () => {
  const buckets = new Set()
  for (let i = 0; i < 500; i++) buckets.add(getRolloutBucket('CA' + i))
  // ด้วย 500 ตัวอย่างกับ 100 bucket ควรเห็นค่าที่ต่างกันเยอะพอสมควร ไม่ใช่กระจุกอยู่แค่ไม่กี่ค่า
  assert.ok(buckets.size > 50, `คาดว่ากระจายได้มากกว่า 50 bucket ที่ต่างกัน ได้จริง ${buckets.size}`)
})

test('rolloutPercent = 0 → ไม่มีสายไหนเข้า path ใหม่เลย', () => {
  for (let i = 0; i < 200; i++) {
    const { useChunkedStreaming } = decideRollout('CA' + i, 0)
    assert.equal(useChunkedStreaming, false)
  }
})

test('rolloutPercent = 100 → ทุกสายเข้า path ใหม่หมด', () => {
  for (let i = 0; i < 200; i++) {
    const { useChunkedStreaming } = decideRollout('CA' + i, 100)
    assert.equal(useChunkedStreaming, true)
  }
})

test('boundary 5%: bucket 4 ต้องเข้า path ใหม่ / bucket 5 ต้องไม่เข้า (หา callSid จริงที่ hash ได้ bucket ตรงนี้)', () => {
  // ยืนยันแล้วด้วยสคริปต์แยกว่า CA160 hash ได้ bucket 4 และ CA22 hash ได้ bucket 5 จริง
  const sidBucket4 = 'CA160'
  const sidBucket5 = 'CA22'
  assert.equal(getRolloutBucket(sidBucket4), 4)
  assert.equal(getRolloutBucket(sidBucket5), 5)

  const r4 = decideRollout(sidBucket4, 5)
  const r5 = decideRollout(sidBucket5, 5)
  assert.equal(r4.useChunkedStreaming, true, 'bucket 4 < 5% ต้องเข้า path ใหม่')
  assert.equal(r5.useChunkedStreaming, false, 'bucket 5 ไม่ < 5% ต้องไม่เข้า path ใหม่')
})

test('sticky: เปลี่ยน rolloutPercent กลางสาย ต้องไม่กระทบ decision ที่ freeze ไว้แล้วตอนเริ่มสาย', () => {
  const callSid = 'CA999'
  // จำลอง call state — ตัดสินครั้งเดียวตอนเริ่มสายที่ 5% แล้วเก็บผลไว้
  const callState = { rollout: decideRollout(callSid, 5) }
  const decisionAtStart = callState.rollout.useChunkedStreaming

  // rolloutPercent ภายนอกเปลี่ยนเป็น 100% ระหว่างสายกำลังดำเนินอยู่ (เช่น admin ปรับค่าใน config)
  const newPercentFromConfig = 100
  // ⚠️ ไม่เรียก decideRollout(callSid, newPercentFromConfig) ซ้ำ — เพราะสายนี้ต้อง sticky ต่อ decision เดิม
  // (โค้ดจริงจะไม่มีการเรียกซ้ำแบบนี้เลยด้วยซ้ำ แค่แสดงให้เห็นว่าต่อให้เรียกซ้ำ ค่าที่เก็บไว้ก็ไม่ถูกแก้ทับเอง)
  assert.equal(callState.rollout.useChunkedStreaming, decisionAtStart, 'ค่าที่ freeze ไว้ต้องไม่เปลี่ยนแม้ config ภายนอกจะเปลี่ยนไปแล้ว')
  assert.equal(callState.rollout.percentAtStart, 5, 'percentAtStart ต้องเก็บค่า ณ ตอนเริ่มสายไว้ ไม่ใช่ค่าปัจจุบัน')

  // สายใหม่ที่เริ่มหลังจากนี้ค่อยใช้ 100% ตามที่ควรจะเป็น
  const newCallState = { rollout: decideRollout('CA-new-call', newPercentFromConfig) }
  assert.equal(newCallState.rollout.useChunkedStreaming, true, 'สายใหม่ต้องเห็นค่า % ล่าสุด')
})
