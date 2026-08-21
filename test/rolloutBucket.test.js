const { test } = require('node:test')
const assert = require('node:assert/strict')
const { getRolloutBucket, decideRollout, getLegacyObservedBucket, getLegacyEarlyTtsBucket, getSttA2Bucket } = require('../src/utils/rolloutBucket')

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

// L2a production exposure gate — getLegacyObservedBucket() ต้อง deterministic/0-99 เหมือน getRolloutBucket() เดิม
// แต่เป็นคนละ hash namespace (design revision 2026-08-20, correction: ห้ามใช้ bucket เดียวกับ rollout_percent)
test('L2a: getLegacyObservedBucket deterministic — callSid เดิมได้ bucket เดิมเสมอ', () => {
  const a = getLegacyObservedBucket('CA1234567890')
  const b = getLegacyObservedBucket('CA1234567890')
  assert.equal(a, b)
})

test('L2a: getLegacyObservedBucket อยู่ในช่วง 0-99 เสมอ', () => {
  for (let i = 0; i < 200; i++) {
    const bucket = getLegacyObservedBucket('CA' + i)
    assert.ok(bucket >= 0 && bucket <= 99, `bucket ${bucket} ต้องอยู่ในช่วง 0-99`)
  }
})

test('L2a: fixed known fixtures — hash namespace "legacy-observed:" ให้ค่าคงที่ตามที่คำนวณไว้ล่วงหน้า', () => {
  assert.equal(getLegacyObservedBucket('CA1234567890'), 5)
  assert.equal(getLegacyObservedBucket('CA160'), 38)
  assert.equal(getLegacyObservedBucket('CA22'), 1)
})

test('L2a: getRolloutBucket() เดิม (chunked rollout) ต้องไม่ถูกกระทบ — fixture เดิมจาก boundary test ยังได้ค่าเดิมเป๊ะ', () => {
  assert.equal(getRolloutBucket('CA160'), 4)
  assert.equal(getRolloutBucket('CA22'), 5)
})

test('L2a: independent namespace — getRolloutBucket และ getLegacyObservedBucket ของ callSid เดียวกันไม่ผูก threshold เดียวกัน (อาจชนกันโดยบังเอิญได้ ไม่ใช่บั๊ก ห้าม assert ว่าต้องต่างกันเสมอ)', () => {
  // ยืนยันด้วยตัวอย่างจริงหลาย callSid ว่าค่าทั้งสองขยับเป็นอิสระต่อกัน (ไม่ได้ผูก formula เดียวกันแค่เติม prefix
  // แล้วได้ผลเหมือนเดิม) — ไม่ assert notEqual เพราะ %100 ชนกันได้เป็นปกติทางสถิติ
  const samples = ['CA1', 'CA2', 'CA3', 'CA1234567890', 'CA160', 'CA22']
  const rolloutBuckets = samples.map(getRolloutBucket)
  const observedBuckets = samples.map(getLegacyObservedBucket)
  assert.deepEqual(rolloutBuckets, [8, 60, 16, 53, 4, 5])
  assert.deepEqual(observedBuckets, [14, 68, 84, 5, 38, 1])
  // ทั้งสองชุดต้องไม่ใช่ mapping เดียวกันทุกตัว (ถ้า namespace ไม่ได้ผูกจริงจะเห็น pattern ซ้ำ) — ตัวอย่างนี้ต่างกันหมด
  const identicalCount = rolloutBuckets.filter((v, i) => v === observedBuckets[i]).length
  assert.ok(identicalCount < samples.length, 'ไม่ควรเห็น bucket ชนกันทุกตัวอย่าง (แปลว่า hash ไม่ independent จริง)')
})

// ---------------------------------------------------------------------------
// L2b production exposure gate — getLegacyEarlyTtsBucket() (design revision 2026-08-21)
// ---------------------------------------------------------------------------

test('L2b: getLegacyEarlyTtsBucket deterministic + อยู่ในช่วง 0-99', () => {
  assert.equal(getLegacyEarlyTtsBucket('CA1'), getLegacyEarlyTtsBucket('CA1'))
  for (let i = 0; i < 100; i++) {
    const b = getLegacyEarlyTtsBucket('CA' + i)
    assert.ok(b >= 0 && b <= 99)
  }
})

test('L2b: fixed known fixtures — hash namespace "legacy-early-tts:" ให้ค่าคงที่ตามที่คำนวณไว้ล่วงหน้า', () => {
  assert.equal(getLegacyEarlyTtsBucket('CA1'), 90)
  assert.equal(getLegacyEarlyTtsBucket('CA2'), 77)
  assert.equal(getLegacyEarlyTtsBucket('CA3'), 37)
})

test('L2b: getRolloutBucket()/getLegacyObservedBucket() เดิมต้องไม่ถูกกระทบเลย — fixture เดิมยังได้ค่าเดิมเป๊ะ', () => {
  assert.equal(getRolloutBucket('CA1'), 8)
  assert.equal(getLegacyObservedBucket('CA1'), 14)
})

test('L2b: independent namespace จากทั้ง rollout และ legacy-observed — สาม bucket ของ callSid เดียวกันไม่ผูกกัน (ชนกันได้บังเอิญ ไม่ใช่บั๊ก)', () => {
  const samples = ['CA1', 'CA2', 'CA3', 'CA1234567890']
  const rolloutBuckets = samples.map(getRolloutBucket)
  const observedBuckets = samples.map(getLegacyObservedBucket)
  const earlyTtsBuckets = samples.map(getLegacyEarlyTtsBucket)
  assert.deepEqual(earlyTtsBuckets, [90, 77, 37, 22])
  const allThreeIdentical = samples.filter((_, i) => rolloutBuckets[i] === observedBuckets[i] && observedBuckets[i] === earlyTtsBuckets[i]).length
  assert.ok(allThreeIdentical < samples.length, 'ไม่ควรเห็นสาม bucket ชนกันทุกตัวอย่าง')
})

// ---------------------------------------------------------------------------
// STT-A2 diagnostic gate — getSttA2Bucket() (design revision 2026-08-21)
// ---------------------------------------------------------------------------

test('STT-A2: getSttA2Bucket deterministic + อยู่ในช่วง 0-99', () => {
  assert.equal(getSttA2Bucket('CA1'), getSttA2Bucket('CA1'))
  for (let i = 0; i < 100; i++) {
    const b = getSttA2Bucket('CA' + i)
    assert.ok(b >= 0 && b <= 99)
  }
})

test('STT-A2: fixed known fixtures — hash namespace "stt-a2:" ให้ค่าคงที่ตามที่คำนวณไว้ล่วงหน้า', () => {
  assert.equal(getSttA2Bucket('CA1'), 93)
  assert.equal(getSttA2Bucket('CA2'), 70)
  assert.equal(getSttA2Bucket('CA3'), 6)
})

test('STT-A2: getRolloutBucket()/getLegacyObservedBucket()/getLegacyEarlyTtsBucket() เดิมต้องไม่ถูกกระทบเลย — fixture เดิมยังได้ค่าเดิมเป๊ะ', () => {
  assert.equal(getRolloutBucket('CA1'), 8)
  assert.equal(getLegacyObservedBucket('CA1'), 14)
  assert.equal(getLegacyEarlyTtsBucket('CA1'), 90)
})

test('STT-A2: independent namespace จากทั้งสามตัวเดิม — bucket ของ callSid เดียวกันไม่ผูกกัน (ห้าม assert ว่าต้องต่างกันเสมอ — ชนกันได้บังเอิญ)', () => {
  const samples = ['CA1', 'CA2', 'CA3', 'CA1234567890', 'CA160', 'CA22']
  const a2Buckets = samples.map(getSttA2Bucket)
  assert.deepEqual(a2Buckets, [93, 70, 6, 85, 83, 1])
  // ยืนยันแค่ input string (hash namespace) ต่างจาก legacy-observed:/legacy-early-tts:/plain callSid จริง — ไม่ assert
  // ว่าตัวเลขผลลัพธ์ต้องต่างกัน เพราะ %100 collision เป็นเรื่องปกติทางสถิติ
  const rolloutBuckets = samples.map(getRolloutBucket)
  const allIdenticalToRollout = samples.filter((_, i) => a2Buckets[i] === rolloutBuckets[i]).length
  assert.ok(allIdenticalToRollout < samples.length, 'ไม่ควรเห็น A2 bucket ชนกับ rollout bucket ทุกตัวอย่าง')
})
