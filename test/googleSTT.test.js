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

// ===== L1a — rollout-scoped STT endpoint experiment (interimFinalizeMs configurable) =====
// googleSTT.js เป็น STT ตัวเดียวที่ legacy และ chunked path ใช้ร่วมกัน ห้ามเปลี่ยน default 900ms ตรงๆ เพราะจะ
// กระทบ legacy production ทุกสายทันที — เทสชุดนี้พิสูจน์ว่า (1) ไม่ส่ง option เลย ยังคง 900ms เป๊ะ (กัน legacy ถูก
// กระทบ) (2) ส่ง option มา ใช้ค่านั้นจริง (3) mechanism เดิมทั้งหมด (cancel timer เมื่อ isFinal จริงมาก่อน, rotate
// หลัง deliver, cleanup ตอน end()) ยังทำงานถูกต้องไม่ว่า threshold จะเป็นค่าไหน

test('L1a: ไม่ส่ง option เลย → default ยัง 900ms เหมือนเดิมทุกประการ (legacy ต้องไม่ถูกกระทบ)', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'ทดสอบ default')
  await delay(700) // ยังไม่ถึง 900ms
  assert.equal(transcripts.length, 0, 'default ต้องยังไม่ deliver ที่ 700ms (ถ้า deliver แปลว่าเผลอใช้ threshold ต่ำกว่า 900 ไปแล้ว)')

  await delay(300) // รวม ~1000ms แล้ว เกิน 900ms
  assert.deepEqual(transcripts, ['ทดสอบ default'])

  sttStream.end()
})

test('L1a: interimFinalizeMs=600 (chunked experiment) → deliver เร็วขึ้นจริงตามค่าที่ส่งมา', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {}, { interimFinalizeMs: 600 })
  const first = createdStreams[0]

  emitInterim(first, 'ทดสอบ 600')
  await delay(400) // ยังไม่ถึง 600ms
  assert.equal(transcripts.length, 0, 'ยังไม่ควร deliver ก่อนถึง 600ms')

  await delay(300) // รวม ~700ms แล้ว เกิน 600ms
  assert.deepEqual(transcripts, ['ทดสอบ 600'])

  sttStream.end()
})

test('L1a: real Google isFinal มาก่อน synthetic timer (600ms) จะครบ → ต้อง cancel timer ถูก ไม่ deliver ซ้ำ', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {}, { interimFinalizeMs: 600 })
  const first = createdStreams[0]

  emitInterim(first, 'ทดสอบ')
  await delay(200) // ยังไม่ถึง 600ms
  emitFinal(first, 'ทดสอบ') // Google ส่ง isFinal จริงมาก่อน timer จะครบ

  await delay(700) // รอเลย 600ms ไปมากๆ เผื่อ timer เดิมไม่ได้ถูก cancel จะ deliver ซ้ำ
  assert.deepEqual(transcripts, ['ทดสอบ'], 'ต้อง deliver แค่ครั้งเดียวจาก isFinal จริง ไม่ใช่สองครั้ง (isFinal + synthetic timer ที่ควรถูก cancel ไปแล้ว)')

  sttStream.end()
})

test('L1a: rotation ยังเกิดหลัง transcript deliver แม้ threshold จะเปลี่ยนเป็น 600ms', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {}, { interimFinalizeMs: 600 })
  const first = createdStreams[0]

  emitInterim(first, 'ทดสอบ')
  await delay(650)

  assert.deepEqual(transcripts, ['ทดสอบ'])
  assert.equal(createdStreams.length, 2, 'ต้อง rotate ไปใช้ prewarm stream ที่สองทันที เหมือนกับ default 900ms')
  assert.equal(first.ended, true)

  sttStream.end()
})

// ===== Design A — interim regression protection (production incident 2026-08-20) =====
// production call จริงยืนยัน Google ส่ง interim ถอยหลังได้: "ok" → "ok ครับ" → "ok" แล้ว 900ms timer หยิบ "ok"
// (ค่าล่าสุด) ไปเป็น final ทั้งที่ "ok ครับ" คือคำตอบที่ถูกต้องจริง — ป้องกันเฉพาะ strict-prefix regression เท่านั้น
// (candidate ใหม่สั้นกว่าและเป็น prefix ของ candidate เดิมเป๊ะ หลัง normalize) ไม่ใช่ naive longest-wins

test('Design A: interim ถอยหลังแบบ strict-prefix ("ok"→"ok ครับ"→"ok") → deliver ค่าที่แข็งแรงกว่า "ok ครับ" ไม่ใช่ "ok"', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'ok')
  emitInterim(first, 'ok ครับ')
  emitInterim(first, 'ok') // regression — ต้องไม่ทับ "ok ครับ" ที่เก็บไว้

  await delay(950)
  assert.deepEqual(transcripts, ['ok ครับ'], 'ต้อง deliver candidate ที่แข็งแรงกว่า ไม่ใช่ regression ล่าสุด')

  sttStream.end()
})

test('Design A: interim เติบโตต่อเนื่อง (ไม่มี regression เลย) → deliver ค่าล่าสุดตามปกติ ไม่ถูกกระทบ', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'ok ครับ')
  emitInterim(first, 'ok ครับ ค่ะ')

  await delay(950)
  assert.deepEqual(transcripts, ['ok ครับ ค่ะ'], 'growth ปกติต้องไม่ถูกบล็อก')

  sttStream.end()
})

test('Design A: correction จริงที่ไม่ใช่ prefix relation (คำหลอน "สงคราม" → คำถูกต้อง "สนใจครับ") → ยอม overwrite ปกติ ไม่ใช่ regression', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'สงคราม')
  emitInterim(first, 'สนใจครับ')

  await delay(950)
  assert.deepEqual(transcripts, ['สนใจครับ'], 'correction ที่ไม่ใช่ prefix ต้องไม่ถูกปฏิเสธ (ไม่ใช่ naive longest-wins)')

  sttStream.end()
})

test('Design A: whitespace/case variation ระหว่าง candidate เดิมกับใหม่ ยังตรวจ regression ถูกต้อง (normalize เพื่อเปรียบเทียบเท่านั้น)', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'OK  ครับ') // เว้นวรรคซ้อน + ตัวพิมพ์ใหญ่
  emitInterim(first, 'ok') // regression (หลัง normalize: "ok ครับ" เทียบกับ "ok")

  await delay(950)
  assert.deepEqual(transcripts, ['OK  ครับ'], 'ค่าที่ deliver ต้องเป็นข้อความดิบเดิม ไม่ถูก normalize/lowercase ทิ้ง')

  sttStream.end()
})

test('Design A: real isFinal จาก Google มาแทนที่ระหว่างมี regression-protected state ค้างอยู่ → isFinal ชนะเสมอ', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'ok ครับ')
  emitInterim(first, 'ok') // regression protected — "ok ครับ" ยังถูกเก็บไว้
  emitFinal(first, 'ok ค่ะ ครับ') // isFinal จริงจาก Google ต้องชนะไม่ว่า protected state จะเป็นอะไร

  assert.deepEqual(transcripts, ['ok ค่ะ ครับ'])

  sttStream.end()
})

test('Design A: protected state ต้องไม่รั่วข้าม utterance หลัง rotate', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'ok ครับ')
  emitInterim(first, 'ok') // regression protected ภายใน utterance แรก
  await delay(950) // deliver "ok ครับ" แล้ว rotate ไป utterance ใหม่

  const second = createdStreams[1]
  emitInterim(second, 'ทดสอบ') // utterance ใหม่ต้องเริ่มจาก state สะอาด ไม่มี "ok ครับ" ค้างมาปน
  await delay(950)

  assert.deepEqual(transcripts, ['ok ครับ', 'ทดสอบ'])

  sttStream.end()
})

test('L1a: end() ระหว่าง timer ยังรอ (ก่อนครบ interimFinalizeMs) ต้อง cancel timer ไม่ deliver อะไรเลยหลัง end', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {}, { interimFinalizeMs: 600 })
  const first = createdStreams[0]

  emitInterim(first, 'ทดสอบ')
  await delay(200) // ยังไม่ถึง 600ms
  sttStream.end() // จำลอง stop/close event — ต้อง clearTimeout(interimTimer) ให้ถูก

  await delay(700) // รอเลยเวลาที่ timer เดิมควรจะครบไปมากๆ
  assert.equal(transcripts.length, 0, 'ห้าม deliver อะไรเลยหลังจากที่ end() ไปแล้ว แม้ timer เดิมจะยังไม่ครบตอนนั้นก็ตาม')
})
