const { test } = require('node:test')
const assert = require('node:assert/strict')
const {
  createAudioContinuity,
  recordFrameSent,
  finalizeAudioContinuity,
  truncateForLog,
} = require('../src/utils/audioContinuity')

test('createAudioContinuity: เก็บ callSid/generationId/pipelineId ครบ, field อื่นเริ่มต้นถูกต้อง (ไม่ fabricate)', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 5, pipelineId: 2 })
  assert.equal(ac.callSid, 'CA1')
  assert.equal(ac.generationId, 5)
  assert.equal(ac.pipelineId, 2)
  assert.equal(ac.candidateFirstAt, null)
  assert.equal(ac.bargeTrigger, null)
  assert.equal(ac.providerFirstAudioAt, null)
  assert.equal(ac.framesSent, 0)
  assert.equal(ac.bytesSent, 0)
  assert.equal(ac.clearSentAt, null)
})

test('recordFrameSent: เฟรมแรก set providerFirstAudioAt/twilioFirstMediaAt (markOnce — ไม่ขยับตามเฟรมหลัง)', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  recordFrameSent(ac, Buffer.alloc(160))
  const firstAt = ac.providerFirstAudioAt
  assert.ok(typeof firstAt === 'number')
  assert.equal(ac.twilioFirstMediaAt, firstAt)
  recordFrameSent(ac, Buffer.alloc(160))
  assert.equal(ac.providerFirstAudioAt, firstAt, 'ห้ามขยับตามเฟรมที่สอง')
  assert.equal(ac.twilioFirstMediaAt, firstAt, 'ห้ามขยับตามเฟรมที่สอง')
})

test('recordFrameSent: framesSent/bytesSent สะสมถูกต้องจาก buffer.length จริง (ไม่ใช่ base64 length)', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  recordFrameSent(ac, Buffer.alloc(160))
  recordFrameSent(ac, Buffer.alloc(160))
  recordFrameSent(ac, Buffer.alloc(80))
  assert.equal(ac.framesSent, 3)
  assert.equal(ac.bytesSent, 400)
})

test('recordFrameSent: เฟรมแรกไม่มี gap sample (ไม่มีเฟรมก่อนหน้าให้เทียบ), twilioLastMediaAt ขยับทุกเฟรม', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  recordFrameSent(ac, Buffer.alloc(160))
  assert.equal(ac._frameGapSamples.length, 0)
  const afterFirst = ac.twilioLastMediaAt
  recordFrameSent(ac, Buffer.alloc(160))
  assert.equal(ac._frameGapSamples.length, 1, 'เฟรมที่สองต้องมี gap sample 1 ตัว')
  assert.ok(ac.twilioLastMediaAt >= afterFirst, 'twilioLastMediaAt ต้องขยับไปข้างหน้าเสมอ (ไม่เหมือน providerFirstAudioAt/twilioFirstMediaAt ที่ markOnce)')
  assert.ok(ac._frameGapMax != null && ac._frameGapMax >= 0)
})

test('finalizeAudioContinuity: ตัด internal accumulator (_lastFrameAt/_frameGapMax/_frameGapSamples) ทิ้งก่อน log', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  recordFrameSent(ac, Buffer.alloc(160))
  recordFrameSent(ac, Buffer.alloc(160))
  const result = finalizeAudioContinuity(ac)
  assert.equal(result._lastFrameAt, undefined)
  assert.equal(result._frameGapMax, undefined)
  assert.equal(result._frameGapSamples, undefined)
})

test('finalizeAudioContinuity: ไม่มีเสียงเลย ไม่ถูก barge → outcome=NO_AUDIO', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  const result = finalizeAudioContinuity(ac)
  assert.equal(result.outcome, 'NO_AUDIO')
  assert.equal(result.candidateBeforeFirstAudio, null)
})

test('finalizeAudioContinuity: มีเสียงส่งจริง ไม่ถูก barge → outcome=COMPLETED_NO_BARGE', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  recordFrameSent(ac, Buffer.alloc(160))
  const result = finalizeAudioContinuity(ac)
  assert.equal(result.outcome, 'COMPLETED_NO_BARGE')
})

test('finalizeAudioContinuity: barge เกิดก่อนมีเสียงส่งเลยสักเฟรม (precommit — ระหว่าง Claude กำลังคิด) → outcome=PRECOMMIT_BARGE ไม่ใช่ POST_AUDIO_BARGE', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  ac.bargeTrigger = 'INTERIM_CONFIRM'
  ac.candidateFirstAt = 100
  ac.candidateConfirmAt = 250
  ac.clearSentAt = 260
  // providerFirstAudioAt ไม่เคยถูก set เลย — เทิร์นนี้ไม่เคยส่งเสียงออกไปจริง
  const result = finalizeAudioContinuity(ac)
  assert.equal(result.outcome, 'PRECOMMIT_BARGE', 'ต้องแยกจาก POST_AUDIO_BARGE เพราะไม่เคยมีเสียงให้ "หลุด" ออกมาเลย')
  assert.equal(result.candidateBeforeFirstAudio, null, 'ไม่มี providerFirstAudioAt ให้เทียบ ต้องเป็น null ไม่ใช่ true/false ปลอม')
})

test('finalizeAudioContinuity: candidate เปิดก่อน first audio จริง แล้วโดน clear → outcome=PRE_AUDIO_OVERLAP', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  ac.bargeTrigger = 'INTERIM_CONFIRM'
  ac.candidateFirstAt = 100
  ac.candidateConfirmAt = 250
  ac.providerFirstAudioAt = 260 // จำลองว่า audio มาถึงหลัง candidate เปิด แต่ก่อน confirm/clear
  ac.clearSentAt = 300
  const result = finalizeAudioContinuity(ac)
  assert.equal(result.candidateBeforeFirstAudio, true)
  assert.equal(result.outcome, 'PRE_AUDIO_OVERLAP')
  assert.equal(result.candidateToFirstAudioMs, 160)
  assert.equal(result.firstAudioToClearMs, 40)
})

test('finalizeAudioContinuity: audio เล่นไปพักหนึ่งแล้วค่อยโดน barge จริง (candidate เปิดหลัง audio) → outcome=POST_AUDIO_BARGE', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  ac.providerFirstAudioAt = 100
  ac.bargeTrigger = 'INTERIM_CONFIRM'
  ac.candidateFirstAt = 500 // เปิด candidate หลัง audio เริ่มไปแล้วนาน
  ac.candidateConfirmAt = 650
  ac.clearSentAt = 660
  const result = finalizeAudioContinuity(ac)
  assert.equal(result.candidateBeforeFirstAudio, false)
  assert.equal(result.outcome, 'POST_AUDIO_BARGE')
})

test('finalizeAudioContinuity: FINAL/FINAL_TIER1 trigger ไม่มี candidate เลย (ไม่ผ่าน interim 2-signal) แต่มีเสียงเล่นอยู่ก่อนแล้ว → ยังเป็น POST_AUDIO_BARGE', () => {
  const ac = createAudioContinuity({ callSid: 'CA1', generationId: 1, pipelineId: 1 })
  ac.providerFirstAudioAt = 100
  ac.bargeTrigger = 'FINAL_TIER1'
  ac.clearSentAt = 400
  // candidateFirstAt/candidateConfirmAt ไม่ถูก set เลย (FINAL path ไม่ผ่าน bargeCandidate)
  const result = finalizeAudioContinuity(ac)
  assert.equal(result.candidateBeforeFirstAudio, null, 'ไม่มี candidateFirstAt ให้เทียบ')
  assert.equal(result.outcome, 'POST_AUDIO_BARGE', 'มีเสียงส่งไปแล้วจริงก่อนโดน clear ต่อให้ไม่มี candidate ก็ยังนับเป็น post-audio')
})

test('truncateForLog: null/undefined/empty string → null, ไม่ throw', () => {
  assert.equal(truncateForLog(null), null)
  assert.equal(truncateForLog(undefined), null)
  assert.equal(truncateForLog(''), null)
})

test('truncateForLog: ข้อความสั้นกว่า limit ผ่านตรงๆ ไม่ถูกตัด', () => {
  assert.equal(truncateForLog('สวัสดีครับ'), 'สวัสดีครับ')
})

test('truncateForLog: ข้อความยาวเกิน 80 ตัวอักษร ถูกตัดเหลือ 80 พอดี', () => {
  const long = 'ก'.repeat(120)
  const result = truncateForLog(long)
  assert.equal(result.length, 80)
  assert.equal(result, 'ก'.repeat(80))
})
