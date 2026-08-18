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

module.exports = { getRolloutBucket, decideRollout }
