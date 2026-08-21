// B.5.1 — Sticky rollout: ตัดสินครั้งเดียวต่อสาย ไม่คำนวณใหม่ทุกเทิร์น
// ใช้ SHA-256 แทน Math.random() เพราะต้อง deterministic เป๊ะ — callSid เดิมต้องได้ bucket เดิมเสมอ
// ไม่ว่าจะเรียกกี่ครั้ง กี่เครื่อง กี่ process ก็ตาม (Math.random() ทำแบบนี้ไม่ได้)
const crypto = require('crypto')

function getRolloutBucket(callSid) {
  const hex = crypto.createHash('sha256').update(String(callSid)).digest('hex').slice(0, 8)
  return parseInt(hex, 16) % 100
}

// เรียกครั้งเดียวตอนเริ่มสาย แล้วเก็บผลไว้ใน call state (เช่น callState.rollout = decideRollout(...))
// ห้ามเรียกซ้ำระหว่างสายเดียวกัน แม้ rolloutPercent จากภายนอกจะเปลี่ยนไปแล้วก็ตาม — สายที่เริ่มไปแล้วต้องเดินตาม
// เส้นทางเดิมจนจบ ไม่งั้นเทิร์นแรกๆ กับเทิร์นหลังๆ ของสายเดียวกันจะสลับ path กันเองมั่วไปหมด debug ไม่ได้เลย
function decideRollout(callSid, rolloutPercent) {
  const bucket = getRolloutBucket(callSid)
  return {
    bucket,
    percentAtStart: rolloutPercent,
    useChunkedStreaming: bucket < rolloutPercent,
  }
}

// L2a exposure gate — dedicated helper (ไม่ใช่ generic getNamespacedBucket) ตั้งใจลดโอกาส caller ใส่ namespace
// ผิดใน production โดยรวม "legacy-observed:" prefix เข้าไปใน hash input โดยตรง ทำให้ bucket assignment ของ
// experiment นี้ independent จาก getRolloutBucket() (chunked rollout) อย่างสมบูรณ์ แม้ callSid เดียวกัน — สอง
// ค่านี้อาจบังเอิญเท่ากันได้ (%100 ชนกันได้เป็นปกติ) นั่นไม่ได้แปลว่า independence เสีย ห้าม assert ว่าต้องต่างกัน
// getRolloutBucket() เดิมไม่ถูกแก้แม้แต่บรรทัดเดียว — chunked production bucket assignment ไม่เปลี่ยน
function getLegacyObservedBucket(callSid) {
  const hex = crypto.createHash('sha256').update(`legacy-observed:${String(callSid)}`).digest('hex').slice(0, 8)
  return parseInt(hex, 16) % 100
}

// L2b exposure gate — dedicated helper, same reasoning as getLegacyObservedBucket() above: own namespace
// prefix so legacy_early_tts assignment is independent of both getRolloutBucket() (chunked) and
// getLegacyObservedBucket() (L2a) even for the same callSid. A collision between any two of the three is
// statistically normal (%100 buckets), not a sign that independence is broken — never assert they differ.
function getLegacyEarlyTtsBucket(callSid) {
  const hex = crypto.createHash('sha256').update(`legacy-early-tts:${String(callSid)}`).digest('hex').slice(0, 8)
  return parseInt(hex, 16) % 100
}

// STT-A2 diagnostic gate — dedicated helper, same reasoning as getLegacyObservedBucket()/getLegacyEarlyTtsBucket()
// above: own namespace prefix so A2 assignment is independent of chunked/L2a/L2b even for the same callSid.
// A collision between any two namespaces is statistically normal (%100 buckets) — never assert they differ.
function getSttA2Bucket(callSid) {
  const hex = crypto.createHash('sha256').update(`stt-a2:${String(callSid)}`).digest('hex').slice(0, 8)
  return parseInt(hex, 16) % 100
}

module.exports = { getRolloutBucket, decideRollout, getLegacyObservedBucket, getLegacyEarlyTtsBucket, getSttA2Bucket }
