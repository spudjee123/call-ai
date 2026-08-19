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
