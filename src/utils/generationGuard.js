// B.5.2 — generationId guard: กัน "เสียงผี"/state หลุดจาก generation เก่าที่ถูกยกเลิกไปแล้ว
// (เช่น Claude delta, chunker output, TTS request, TTS audio, หรือ Twilio send ที่มาช้าหลังจาก
// barge-in หรือเทิร์นถัดไปเริ่มไปแล้ว) — ทุก side effect ที่ปล่อยเสียง/เปลี่ยน state ต้องเช็ค
// isCurrentGeneration() ก่อนเสมอ โดยเฉพาะจุดสุดท้ายก่อนส่งเข้า Twilio ซึ่งสำคัญที่สุด เพราะ ElevenLabs
// อาจตอบช้ากว่า Claude ที่ถูก abort ไปแล้วก็ได้

function createCallState() {
  return { generationId: 0, ended: false }
}

// ใช้ทั้งตอนเริ่ม generation ใหม่จริงๆ (เก็บเลขที่คืนมาไว้เป็น "ของฉัน" สำหรับ guard ทุกจุดของเทิร์นนี้)
// และตอน invalidate ก่อน abort ตอน barge-in (ไม่ต้องสนใจเลขที่คืนมา แค่ต้องการให้ generation เดิมเป็นโมฆะทันที)
// ต้องเรียกก่อนสั่ง abort() เสมอ (invalidate → abort → clear) ไม่ใช่ abort ก่อนแล้วค่อย invalidate
// เพราะระหว่างสองคำสั่งนั้นอาจมี callback เก่าหลุดเข้ามาทำงานได้ทัน แม้ window จะเล็กมากก็ไม่ควรเปิดไว้
function bumpGeneration(callState) {
  callState.generationId += 1
  return callState.generationId
}

function isCurrentGeneration(callState, generationId) {
  return !callState.ended && callState.generationId === generationId
}

// สายจบแล้ว (ไม่ว่าเหตุผลอะไร) — generation ไหนก็ไม่ถือว่า current อีกต่อไป แม้จะเลขตรงกับปัจจุบันก็ตาม
function endCall(callState) {
  callState.ended = true
}

module.exports = { createCallState, bumpGeneration, isCurrentGeneration, endCall }
