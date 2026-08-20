const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// L1c2a — stub axios (สำหรับ elevenlabs.js) และ googleTTS.js ก่อน require tts.js ครั้งแรก (pattern เดียวกับที่
// ใช้ทั้ง repo มาตลอด) เพื่อตรวจ request body จริงที่จะถูกส่งไป ElevenLabs โดยไม่ยิง HTTP จริง และไม่ต้องพึ่ง
// Google Cloud credentials จริงในเครื่องที่รันเทส — นี่คือ hop เดียวที่ยังไม่มี test มาก่อน (chunkedTurn.test.js/
// _audioStreamHarness.js เดิม mock ที่ tts.js boundary เลย ข้าม elevenlabs.js/googleTTS.js ไปหมด ไม่เคยพิสูจน์
// ว่า previousText ถูก thread ผ่าน tts.js ไปถึง axios request body จริงหรือ wrapper ทำตกระหว่างทาง)
const state = { axiosPostImpl: null, isGoogleVoiceImpl: null, synthesizeSpeechThaiImpl: null }

const axiosPath = require.resolve('axios')
require.cache[axiosPath] = {
  id: axiosPath, filename: axiosPath, loaded: true,
  exports: { post: (...args) => state.axiosPostImpl(...args) },
}

const googleTTSPath = require.resolve('../src/services/googleTTS')
require.cache[googleTTSPath] = {
  id: googleTTSPath, filename: googleTTSPath, loaded: true,
  exports: {
    isGoogleVoice: (voiceId) => state.isGoogleVoiceImpl(voiceId),
    synthesizeSpeechThai: (...args) => state.synthesizeSpeechThaiImpl(...args),
  },
}

const { synthesizeSpeechStream } = require('../src/services/tts')

beforeEach(() => {
  state.axiosPostImpl = null
  state.isGoogleVoiceImpl = () => false // default: ElevenLabs path
  state.synthesizeSpeechThaiImpl = async () => { throw new Error('should not be called in ElevenLabs-path tests') }
})

// stream ว่างเปล่า (จบทันที) — เทสชุดนี้สนใจแค่ request body ที่ axios.post ถูกเรียกด้วย ไม่สนใจ audio pipeline
// (downsample/mulaw ถูก cover อยู่แล้วผ่าน mock ที่ tts.js boundary ในไฟล์อื่น)
function emptyStreamResponse() {
  return { data: (async function* () {})() }
}

async function drain(gen) {
  const out = []
  for await (const chunk of gen) out.push(chunk)
  return out
}

test('L1c2a: chunk แรก (ไม่มี previousText) → request body ไม่มี key previous_text เลย (ไม่ใช่ previous_text: undefined)', async () => {
  let capturedBody = null
  state.axiosPostImpl = async (url, body) => { capturedBody = body; return emptyStreamResponse() }

  await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null))

  assert.ok(capturedBody, 'axios.post ต้องถูกเรียก')
  assert.equal('previous_text' in capturedBody, false, 'ห้ามมี key previous_text เลยถ้าไม่ได้ส่ง previousText มา')
})

test('L1c2a: chunk ถัดไป (มี previousText) → request body มี previous_text ตรงกับข้อความที่ส่งมาเป๊ะ', async () => {
  let capturedBody = null
  state.axiosPostImpl = async (url, body) => { capturedBody = body; return emptyStreamResponse() }

  await drain(synthesizeSpeechStream('พอยต์นะคะ', 'voice1', null, 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000'))

  assert.equal(capturedBody.previous_text, 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000')
})

test('L1c2a: previousText เป็น undefined ชัดเจน → ไม่ serialize เป็น field ว่าง (เหมือนไม่ได้ส่งเลย)', async () => {
  let capturedBody = null
  state.axiosPostImpl = async (url, body) => { capturedBody = body; return emptyStreamResponse() }

  await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, undefined))

  assert.equal('previous_text' in capturedBody, false)
})

test('L1c2a: previousText เป็น null → ไม่ใส่ previous_text (ค่าเริ่มต้นของ adoptChunkedProducer ก่อน chunk แรกพูดจบ)', async () => {
  let capturedBody = null
  state.axiosPostImpl = async (url, body) => { capturedBody = body; return emptyStreamResponse() }

  await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, null))

  assert.equal('previous_text' in capturedBody, false)
})

test('L1c2a: previousText เป็น empty string → ไม่ถือว่ามีค่า ไม่ใส่ previous_text (falsy check ตั้งใจกว้าง)', async () => {
  let capturedBody = null
  state.axiosPostImpl = async (url, body) => { capturedBody = body; return emptyStreamResponse() }

  await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, ''))

  assert.equal('previous_text' in capturedBody, false)
})

test('L1c2a: url/text/model_id/voice_settings เดิมไม่เปลี่ยนเลยไม่ว่า previousText จะมีหรือไม่มี', async () => {
  let capturedUrl = null, capturedBody = null
  state.axiosPostImpl = async (url, body) => { capturedUrl = url; capturedBody = body; return emptyStreamResponse() }

  await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, 'ก่อนหน้า'))

  assert.match(capturedUrl, /\/text-to-speech\/voice1\/stream/)
  assert.equal(capturedBody.text, 'สวัสดีค่ะ')
  assert.equal(capturedBody.model_id, 'eleven_v3')
  assert.ok(capturedBody.voice_settings)
})

test('L1c2a: Google voice path ไม่ถูกกระทบเลย — ไม่เรียก axios.post ของ ElevenLabs แม้จะส่ง previousText มาด้วยก็ตาม', async () => {
  state.isGoogleVoiceImpl = () => true
  state.synthesizeSpeechThaiImpl = async () => [Buffer.from('fake-google-audio')]
  let axiosCalled = false
  state.axiosPostImpl = async () => { axiosCalled = true; return emptyStreamResponse() }

  const out = await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'th-TH-Neural2-C', null, 'ก่อนหน้า'))

  assert.equal(axiosCalled, false, 'Google voice path ต้องไม่แตะ ElevenLabs/axios เลย')
  assert.ok(out.length > 0, 'ยังต้องได้ audio จาก Google TTS ตามปกติ')
})

// ── L1c2a incident hotfix (production 2026-08-20) — [TTSProviderError] telemetry ──────────────────────────
// axios responseType:'stream' ทำให้ err.response.data ตอน error เป็น readable/async-iterable stream เหมือนกับ
// success response ไม่ใช่ parsed JSON — ทุก mock error response ด้านล่างจึงจำลอง data เป็น async generator

function withCapturedErrorLogs(fn) {
  const originalError = console.error
  const logs = []
  console.error = (...args) => { logs.push(args.join(' ')) }
  return fn().finally(() => { console.error = originalError }).then((result) => ({ result, logs }))
}

function streamOf(chunks) {
  return (async function* () {
    for (const c of chunks) yield Buffer.isBuffer(c) ? c : Buffer.from(c)
  })()
}

test('L1c2a incident hotfix: provider 400 → [TTSProviderError] log มี status/body metadata ครบ และ error object เดิมถูก rethrow แบบ reference เดิม', async () => {
  const originalErr = new Error('Request failed with status code 400')
  originalErr.response = {
    status: 400,
    statusText: 'Bad Request',
    data: streamOf([JSON.stringify({ detail: 'invalid previous_text' })]),
  }
  state.axiosPostImpl = async () => { throw originalErr }

  let caught = null
  const { logs } = await withCapturedErrorLogs(async () => {
    try {
      await drain(synthesizeSpeechStream('พอยต์นะคะ', 'voice1', null, 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000'))
    } catch (e) {
      caught = e
    }
  })

  assert.equal(caught, originalErr, 'ต้อง rethrow error object เดิมเป๊ะ (reference equality) ไม่สร้าง Error ใหม่ ไม่งั้น err.source/fallback classification ฝั่ง caller จะพัง')
  const providerErrorLogs = logs.filter(l => l.includes('[TTSProviderError]'))
  assert.equal(providerErrorLogs.length, 1)
  assert.match(providerErrorLogs[0], /"status":400/)
  assert.match(providerErrorLogs[0], /"statusText":"Bad Request"/)
  assert.match(providerErrorLogs[0], /"hasPreviousText":true/)
  assert.match(providerErrorLogs[0], /"responseData":"\{\\"detail/)
})

test('L1c2a incident hotfix: provider success (ไม่ error) → ไม่มี [TTSProviderError] log เลย และ audio stream ยังทำงานปกติ', async () => {
  state.axiosPostImpl = async () => ({ data: streamOf([Buffer.alloc(8)]) })

  const { result: out, logs } = await withCapturedErrorLogs(async () =>
    drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, 'ก่อนหน้า'))
  )

  assert.equal(logs.filter(l => l.includes('[TTSProviderError]')).length, 0)
  assert.ok(out.length >= 0)
})

test('L1c2a incident hotfix: barge-in ยกเลิก request กลางทาง (ERR_CANCELED) → ไม่ log [TTSProviderError] (กัน log noise ทุกครั้งที่ barge-in ปกติ)', async () => {
  const cancelErr = new Error('canceled')
  cancelErr.code = 'ERR_CANCELED'
  state.axiosPostImpl = async () => { throw cancelErr }

  let caught = null
  const { logs } = await withCapturedErrorLogs(async () => {
    try {
      await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, null))
    } catch (e) {
      caught = e
    }
  })

  assert.equal(caught, cancelErr)
  assert.equal(logs.filter(l => l.includes('[TTSProviderError]')).length, 0, 'barge-in ต้องไม่ทำให้เกิด log noise')
})

test('L1c2a incident hotfix: CanceledError (err.name) ก็ต้องไม่ log เหมือนกัน แม้ err.code จะไม่ใช่ ERR_CANCELED', async () => {
  const cancelErr = new Error('canceled')
  cancelErr.name = 'CanceledError'
  state.axiosPostImpl = async () => { throw cancelErr }

  const { logs } = await withCapturedErrorLogs(async () => {
    try { await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, null)) } catch { /* expected */ }
  })

  assert.equal(logs.filter(l => l.includes('[TTSProviderError]')).length, 0)
})

test('L1c2a incident hotfix: response body ยาวเกิน MAX_ERROR_BODY_BYTES (8192) → responseData ถูกตัด ไม่หลุด log เต็มก้อน', async () => {
  const bigChunk = 'x'.repeat(10000)
  const originalErr = new Error('Request failed with status code 400')
  originalErr.response = { status: 400, statusText: 'Bad Request', data: streamOf([bigChunk]) }
  state.axiosPostImpl = async () => { throw originalErr }

  const { logs } = await withCapturedErrorLogs(async () => {
    try { await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, 'ก่อนหน้า')) } catch { /* expected */ }
  })

  const line = logs.find(l => l.includes('[TTSProviderError]'))
  assert.ok(line)
  const parsed = JSON.parse(line.slice(line.indexOf('{')))
  assert.ok(parsed.responseData.length <= 8192, `responseData ต้องถูกตัดที่ 8192 bytes ไม่ใช่ ${parsed.responseData.length}`)
})

test('L1c2a incident hotfix: response.data อ่านไม่ได้ระหว่างทาง (stream throw) → responseData fallback เป็น "unavailable" ไม่ throw ซ้ำจน error log พังไปด้วย', async () => {
  const originalErr = new Error('Request failed with status code 400')
  originalErr.response = {
    status: 400,
    statusText: 'Bad Request',
    data: (async function* () { throw new Error('stream read failed') })(),
  }
  state.axiosPostImpl = async () => { throw originalErr }

  let caught = null
  const { logs } = await withCapturedErrorLogs(async () => {
    try { await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, 'ก่อนหน้า')) } catch (e) { caught = e }
  })

  assert.equal(caught, originalErr, 'อ่าน body ไม่ได้ต้องไม่กลบ error object เดิม')
  const line = logs.find(l => l.includes('[TTSProviderError]'))
  assert.ok(line, 'ต้องยัง log ได้แม้อ่าน body ไม่ได้')
  assert.match(line, /"responseData":"unavailable"/)
})

test('L1c2a incident hotfix: response.data stream ค้างกลางทาง (ไม่มี data/error/end event มาเพิ่มเลย ไม่ถึง byte cap ด้วย) → ต้อง bounded ด้วย wall-clock timeout ไม่ใช่แค่ byte cap ไม่งั้น throw err ตัวเดิมจะค้างไม่จำกัดเวลาไปด้วย', async () => {
  const originalErr = new Error('Request failed with status code 400')
  originalErr.response = {
    status: 400,
    statusText: 'Bad Request',
    // async iterator ที่ next() ไม่ resolve เลย จำลอง connection ค้างกลางทาง (ไม่ใช่ error ไม่ใช่ end แค่เงียบ)
    data: { [Symbol.asyncIterator]() { return { next: () => new Promise(() => {}) } } },
  }
  state.axiosPostImpl = async () => { throw originalErr }

  const startedAt = Date.now()
  let caught = null
  const { logs } = await withCapturedErrorLogs(async () => {
    try { await drain(synthesizeSpeechStream('สวัสดีค่ะ', 'voice1', null, 'ก่อนหน้า')) } catch (e) { caught = e }
  })
  const elapsedMs = Date.now() - startedAt

  assert.equal(caught, originalErr, 'ต้อง rethrow error เดิมแม้ตอน body อ่านไม่จบสักที')
  assert.ok(elapsedMs < 3000, `ต้อง bounded ด้วย timeout สั้นๆ ไม่ค้างไม่จำกัดเวลา (ใช้เวลาไปจริง ${elapsedMs}ms)`)
  const line = logs.find(l => l.includes('[TTSProviderError]'))
  assert.ok(line, 'ต้องยัง log ได้แม้ stream ค้าง (fallback ต้องทำงาน ไม่ใช่ log หายไปเลย)')
  assert.match(line, /"responseData":"unavailable"/)
})
