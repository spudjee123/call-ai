// C6c follow-up (STT listening, production discovery 2026-08-19) — unit tests สำหรับ googleSTT.js's
// proactive stream rotation (rotateForNextUtterance) ก่อนหน้านี้โมดูลนี้ไม่มีเทสเลย เพราะพึ่ง @google-cloud/speech
// จริงตอน module load — stub ทั้งแพ็กเกจผ่าน require.cache (pattern เดียวกับที่ใช้ทั้ง repo มาตลอด) แล้วปล่อยให้
// googleSTT.js ตัวจริงรันทับ fake stream ที่ควบคุมได้จากเทส พิสูจน์กลไก "เปิดฟัง utterance ถัดไปทันทีหลัง
// transcript ก่อนหน้า deliver ไม่ต้องรอ Twilio mark" ก่อนเชื่อมกับ audioStream.js
const { test } = require('node:test')
const assert = require('node:assert/strict')
const EventEmitter = require('events')

function makeFakeStream() {
  const stream = new EventEmitter()
  stream.ended = false
  stream.write = () => {}
  stream.end = () => { stream.ended = true }
  return stream
}

let createdStreams = []
const speechPath = require.resolve('@google-cloud/speech')
require.cache[speechPath] = {
  id: speechPath, filename: speechPath, loaded: true,
  exports: {
    SpeechClient: class {
      constructor() {}
      streamingRecognize() {
        const stream = makeFakeStream()
        createdStreams.push(stream)
        return stream
      }
    },
  },
}

const { transcribeStream } = require('../src/services/googleSTT')

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }
function emitInterim(stream, text) { stream.emit('data', { results: [{ isFinal: false, alternatives: [{ transcript: text }] }] }) }
function emitFinal(stream, text) { stream.emit('data', { results: [{ isFinal: true, alternatives: [{ transcript: text }] }] }) }

test('rotate ทันทีหลัง synthetic 900ms finalize deliver — ไม่ต้องรอ mark/reset() จากภายนอกอีกต่อไป', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  assert.equal(createdStreams.length, 1, 'ต้องสร้าง stream แรกทันทีตอนเริ่ม')

  const first = createdStreams[0]
  emitInterim(first, 'สวัสดี')
  assert.equal(createdStreams.length, 2, 'ต้อง prewarm stream ที่สองทันทีหลัง interim แรก (กลไกเดิม ไม่เปลี่ยน)')

  await delay(950) // เกิน INTERIM_FINALIZE_MS (900ms)

  assert.deepEqual(transcripts, ['สวัสดี'])
  assert.equal(createdStreams.length, 2, 'ไม่ควรสร้าง stream ที่สามเพิ่ม — ต้อง rotate ไปใช้ prewarm ตัวที่สองที่มีอยู่แล้วแทน')
  assert.equal(first.ended, true, 'stream แรก (เก่า) ต้องถูกปิดทันทีหลัง rotate ไม่ปล่อยค้าง')

  sttStream.end()
})

test('stream ที่เพิ่ง rotate มา (จาก prewarm) ต้องพร้อมฟัง interim ใหม่ได้ทันที ไม่ถูก mark closed ค้างจาก utterance ก่อนหน้า', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'สวัสดี')
  await delay(950) // rotate ไปยัง stream ที่สอง (prewarm) แล้ว

  const second = createdStreams[1]
  emitInterim(second, 'เดี๋ยวก่อนครับ') // นี่คือ interim ของ "utterance ถัดไป" ที่ควรฟังได้ระหว่าง AI พูดอยู่พอดี
  assert.equal(createdStreams.length, 3, 'stream ที่สอง (เพิ่ง rotate มา) ต้องรับ interim ได้ปกติ แล้ว prewarm ตัวที่สามต่อ — พิสูจน์ว่าไม่ได้ utteranceClosed=true ค้างมาจาก rotate (บั๊กที่ต้องระวังตามที่ออกแบบไว้)')

  await delay(950)
  assert.deepEqual(transcripts, ['สวัสดี', 'เดี๋ยวก่อนครับ'], 'utterance ที่สองต้อง deliver ได้ปกติเช่นกัน')

  sttStream.end()
})

test('rotate ทำงานจาก isFinal จริงจาก Google ด้วยเช่นกัน ไม่ใช่แค่ synthetic timer (ไม่มี interim นำมาก่อนเลย — ไม่มี prewarm ให้ใช้)', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitFinal(first, 'ตัวอย่าง final จริงจาก Google')
  assert.deepEqual(transcripts, ['ตัวอย่าง final จริงจาก Google'])
  assert.equal(createdStreams.length, 2, 'ไม่มี prewarm ให้ใช้ (ไม่เคยมี interim มาก่อน) ต้องสร้าง stream ใหม่ทันทีแทน')
  assert.equal(first.ended, true)

  sttStream.end()
})

test('late isFinal จาก stream เก่าหลัง rotate ไปแล้ว ต้องไม่ deliver ซ้ำ (กัน onTranscript ยิงสองครั้งสำหรับ utterance เดียว)', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'ทดสอบ')
  await delay(950) // synthetic finalize + rotate เกิดไปแล้ว — first ไม่ใช่ currentStream อีกต่อไป
  assert.equal(transcripts.length, 1)

  emitFinal(first, 'ทดสอบ') // Google ส่ง isFinal ตามหลังมาช้าๆ สำหรับ utterance เดิมที่ deliver ไปแล้ว
  await delay(10)
  assert.equal(transcripts.length, 1, 'ห้าม deliver ซ้ำจาก stream เก่าที่ rotate ทิ้งไปแล้ว')

  sttStream.end()
})
