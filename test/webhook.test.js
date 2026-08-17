const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// stub googleSheets ก่อน require webhook.js — คุม saveRecordingUrl เองได้ (จำลอง race: หาแถวไม่เจอ N ครั้งแรกแล้วเจอ)
const state = { failNTimes: 0, calls: 0, saved: null, throwOnAttempt: null }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      saveRecordingUrl: async (callSid, recordingUrl) => {
        state.calls++
        if (state.throwOnAttempt && state.calls === state.throwOnAttempt) throw new Error('Quota exceeded')
        if (state.calls <= state.failNTimes) return false
        state.saved = { callSid, recordingUrl }
        return true
      },
    },
  },
}

const { saveRecordingWithRetry } = require('../src/routes/webhook')

beforeEach(() => {
  state.failNTimes = 0; state.calls = 0; state.saved = null; state.throwOnAttempt = null
})

test('saveRecordingWithRetry: เจอแถวตั้งแต่ความพยายามแรก ไม่ต้องลองรอบต่อไปอีก', async () => {
  await saveRecordingWithRetry('CA1', 'https://api.twilio.com/rec1', [1, 1, 1])
  assert.equal(state.calls, 1)
  assert.deepEqual(state.saved, { callSid: 'CA1', recordingUrl: 'https://api.twilio.com/rec1' })
})

test('saveRecordingWithRetry: แถวยังไม่มา 2 ครั้งแรก (race กับ postCallHandler) แล้วสำเร็จตอนลองรอบที่ 3', async () => {
  state.failNTimes = 2
  await saveRecordingWithRetry('CA2', 'https://api.twilio.com/rec2', [1, 1, 1])
  assert.equal(state.calls, 3)
  assert.deepEqual(state.saved, { callSid: 'CA2', recordingUrl: 'https://api.twilio.com/rec2' })
})

test('saveRecordingWithRetry: ทุกความพยายามต้องรอตาม delay ก่อนเสมอ แม้แต่ครั้งแรก (ไม่ลองทันทีที่ t=0 อีกต่อไป — กันอ่านชีตเสียเปล่า)', async () => {
  const waitedBeforeCall = []
  const start = Date.now()
  require.cache[googleSheetsPath].exports.sheetsService.saveRecordingUrl = async () => {
    waitedBeforeCall.push(Date.now() - start)
    return true
  }
  await saveRecordingWithRetry('CA5', 'https://api.twilio.com/rec5', [30])
  assert.ok(waitedBeforeCall[0] >= 25, `ต้องรออย่างน้อยตาม delay ก่อนลองครั้งแรก ได้จริง ${waitedBeforeCall[0]}ms`)
  // คืนค่า stub เดิมกลับ กันกระทบเทสอื่นที่รันหลังจากนี้
  require.cache[googleSheetsPath].exports.sheetsService.saveRecordingUrl = async (callSid, recordingUrl) => {
    state.calls++
    if (state.throwOnAttempt && state.calls === state.throwOnAttempt) throw new Error('Quota exceeded')
    if (state.calls <= state.failNTimes) return false
    state.saved = { callSid, recordingUrl }
    return true
  }
})

test('saveRecordingWithRetry: หาแถวไม่เจอตลอดจน retry ครบ ก็เลิกลองโดยไม่ throw', async () => {
  state.failNTimes = 999
  await assert.doesNotReject(saveRecordingWithRetry('CA3', 'https://api.twilio.com/rec3', [1, 1]))
  assert.equal(state.calls, 2) // ลองครบตามจำนวน delay ที่ให้มา (ไม่มีความพยายามฟรีที่ t=0 อีกแล้ว)
  assert.equal(state.saved, null)
})

test('saveRecordingWithRetry: error ชั่วคราวระหว่างทาง (เช่น Sheets quota) ไม่ทำให้เลิก retry ทันที ยังลองรอบถัดไปต่อ', async () => {
  state.throwOnAttempt = 1 // ครั้งแรก throw แล้วครั้งที่ 2 ควรสำเร็จ
  await saveRecordingWithRetry('CA4', 'https://api.twilio.com/rec4', [1, 1])
  assert.equal(state.calls, 2)
  assert.deepEqual(state.saved, { callSid: 'CA4', recordingUrl: 'https://api.twilio.com/rec4' })
})
