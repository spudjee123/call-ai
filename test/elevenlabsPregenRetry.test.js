const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// Greeting pregen TTS retry (production incident 2026-09-03) — synthesizeSpeech() (ใช้เฉพาะตอน pregen ใน
// twilio.js) ไม่เคยมี retry มาก่อน ต่างจาก synthesizeSpeechStream() ที่มี retry-once อยู่แล้ว ทำให้ transient
// 429/5xx ระหว่าง pregen ทำให้ greeting ที่เตรียมไว้ล่วงหน้าใช้ไม่ได้ทันที ต้องไปสร้างใหม่สดๆ ตอนลูกค้ารับสาย
// (เกิดเป็นช่วงหน่วง/สะดุดตอนต้นสายจริงใน production) — stub axios ก่อน require elevenlabs.js ครั้งแรก
// (pattern เดียวกับ ttsContinuity.test.js) เพื่อคุม response/error ได้อิสระโดยไม่ยิง HTTP จริง
const state = { axiosPostImpl: null, postCallCount: 0 }

const axiosPath = require.resolve('axios')
require.cache[axiosPath] = {
  id: axiosPath, filename: axiosPath, loaded: true,
  exports: { post: (...args) => { state.postCallCount++; return state.axiosPostImpl(...args) } },
}

const { synthesizeSpeech } = require('../src/services/elevenlabs')

beforeEach(() => {
  state.axiosPostImpl = null
  state.postCallCount = 0
})

// PCM16 buffer เล็กที่สุดที่ downsample16to8()/pcm16BufferToMulaw() ประมวลผลได้โดยไม่ throw (4 bytes = 2 samples)
function fakeSuccessResponse() {
  return { data: Buffer.alloc(8) } // 4 samples @ 16-bit = พอสำหรับ downsample 2:1 ได้ผลลัพธ์จริง
}

function fakeAxiosError(status, detail) {
  const err = new Error(`Request failed with status code ${status}`)
  err.response = {
    status,
    statusText: status === 429 ? 'Too Many Requests' : 'Error',
    data: detail ? JSON.stringify({ detail }) : undefined,
  }
  return err
}

test('A1: attempt แรกสำเร็จ (200) → 1 request, ไม่ retry, คืน chunks ปกติ', async () => {
  state.axiosPostImpl = async () => fakeSuccessResponse()
  const chunks = await synthesizeSpeech('สวัสดีค่ะ', 'voice1')
  assert.equal(state.postCallCount, 1)
  assert.ok(Array.isArray(chunks))
})

test('A2: 429 ตอน attempt แรก แล้วสำเร็จตอน retry → 2 requests รวม, คืน chunks สำเร็จ', async () => {
  let calls = 0
  state.axiosPostImpl = async () => {
    calls++
    if (calls === 1) throw fakeAxiosError(429, { type: 'rate_limit_error', code: 'concurrent_limit_exceeded', request_id: 'req-1' })
    return fakeSuccessResponse()
  }
  const chunks = await synthesizeSpeech('สวัสดีค่ะ', 'voice1')
  assert.equal(state.postCallCount, 2, 'ต้อง retry 1 ครั้งหลัง 429 แล้วสำเร็จ')
  assert.ok(Array.isArray(chunks))
})

test('A3: 503 ตอน attempt แรก แล้วสำเร็จตอน retry → นับเป็น transient เหมือน 429', async () => {
  let calls = 0
  state.axiosPostImpl = async () => {
    calls++
    if (calls === 1) throw fakeAxiosError(503)
    return fakeSuccessResponse()
  }
  const chunks = await synthesizeSpeech('สวัสดีค่ะ', 'voice1')
  assert.equal(state.postCallCount, 2)
  assert.ok(Array.isArray(chunks))
})

test('A4: 401 → ไม่ retry เลย โยน error เดิมออกไปทันที', async () => {
  state.axiosPostImpl = async () => { throw fakeAxiosError(401) }
  await assert.rejects(
    () => synthesizeSpeech('สวัสดีค่ะ', 'voice1'),
    (err) => err.response?.status === 401
  )
  assert.equal(state.postCallCount, 1, '401 ไม่ใช่ transient error — ห้าม retry')
})

test('A5: 400 (permanent 4xx) → ไม่ retry เลย', async () => {
  state.axiosPostImpl = async () => { throw fakeAxiosError(400) }
  await assert.rejects(() => synthesizeSpeech('สวัสดีค่ะ', 'voice1'))
  assert.equal(state.postCallCount, 1)
})

test('A6: 429 ต่อเนื่อง 2 ครั้งติด → หยุดหลัง 2 attempts รวม โยน error เดิมออกไป (ไม่ retry ไม่จำกัด)', async () => {
  state.axiosPostImpl = async () => { throw fakeAxiosError(429, { type: 'rate_limit_error', code: 'concurrent_limit_exceeded', request_id: 'req-2' }) }
  await assert.rejects(
    () => synthesizeSpeech('สวัสดีค่ะ', 'voice1'),
    (err) => err.response?.status === 429
  )
  assert.equal(state.postCallCount, 2, 'ต้องลองสูงสุด 2 ครั้ง (attempt แรก + retry 1 ครั้ง) ไม่ใช่ retry ไม่จำกัด')
})

test('A6b: persistent 429 → structured log มี attempt/requestType/errorType/errorCode/requestId ถูกต้อง (observability)', async () => {
  state.axiosPostImpl = async () => { throw fakeAxiosError(429, { type: 'rate_limit_error', code: 'concurrent_limit_exceeded', request_id: 'req-3' }) }
  const originalError = console.error
  const logs = []
  console.error = (...args) => logs.push(args.join(' '))
  try {
    await assert.rejects(() => synthesizeSpeech('สวัสดีค่ะ', 'voice1'))
  } finally {
    console.error = originalError
  }
  const ttsErrorLog = logs.find(l => l.includes('[TTSProviderError]'))
  assert.ok(ttsErrorLog, 'ต้อง log [TTSProviderError] เมื่อ retry หมดแล้วยังล้มเหลว')
  const parsed = JSON.parse(ttsErrorLog.replace('[TTSProviderError] ', ''))
  assert.equal(parsed.requestType, 'pregen')
  assert.equal(parsed.attempt, 2)
  assert.equal(parsed.status, 429)
  assert.equal(parsed.errorType, 'rate_limit_error')
  assert.equal(parsed.errorCode, 'concurrent_limit_exceeded')
  assert.equal(parsed.requestId, 'req-3')
})
