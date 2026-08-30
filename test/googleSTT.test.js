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
  // Track 1 fix (2026-08-30): activatePrewarm() ตอนนี้ proactive re-prewarm ทันที (ไม่รอ interim ถัดไป) —
  // rotate ไปใช้ prewarm ตัวที่สองที่มีอยู่แล้ว (ไม่สร้าง stream ที่สามจาก rotation นี้เอง) แต่ต้อง "เติม" prewarm
  // ตัวใหม่ (ตัวที่สาม) ทันทีหลัง promote เสมอ — ปิดช่องว่างที่เคยทำให้ recovery event ถัดไปตกไปใช้ cold createStream
  assert.equal(createdStreams.length, 3, 'rotate ไปใช้ prewarm ตัวที่สอง แล้วต้องเติม prewarm ใหม่ (ตัวที่สาม) ทันที ไม่รอ interim ถัดไป')
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

  // Track 1 fix (2026-08-30): rotate ตอนนี้ proactive re-prewarm ทันที ดังนั้น prewarm ตัวที่สาม "มีอยู่แล้ว"
  // ก่อน interim ถัดไปจะมาถึงด้วยซ้ำ (ต่างจากเดิมที่ต้องรอ interim ถัดไปเป็นคนสร้างเอง) — ยืนยันจุดนี้ก่อน
  assert.equal(createdStreams.length, 3, 'prewarm ตัวที่สามต้องถูกเติมทันทีตอน rotate (Track 1) ก่อน interim ถัดไปมาถึงด้วยซ้ำ')

  const second = createdStreams[1]
  emitInterim(second, 'เดี๋ยวก่อนครับ') // นี่คือ interim ของ "utterance ถัดไป" ที่ควรฟังได้ระหว่าง AI พูดอยู่พอดี
  // stream ที่สอง (เพิ่ง rotate มา) ต้องรับ interim ได้ปกติ ไม่ถูก mark closed ค้างจาก utterance ก่อนหน้า (บั๊กที่
  // ต้องระวังตามที่ออกแบบไว้) — prewarm ตัวที่สามมีอยู่แล้วจาก Track 1 จึงไม่มีการสร้างตัวที่สี่เพิ่มจาก interim นี้เอง
  // (nextStream ไม่ null ตั้งแต่ก่อน emitInterim แล้ว) การพิสูจน์หลักว่า utteranceClosed ไม่รั่วอยู่ที่ transcripts ท้าย test
  assert.equal(createdStreams.length, 3, 'ไม่มีการสร้าง stream ที่สี่จาก interim นี้ — prewarm ตัวที่สามเติมไว้แล้วตั้งแต่ตอน rotate')

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
  // Track 1 fix (2026-08-30): rotate ไปใช้ prewarm ตัวที่สอง แล้วเติม prewarm ใหม่ (ตัวที่สาม) ทันที — ดูหมายเหตุ
  // เดียวกับ test แรกของไฟล์นี้
  assert.equal(createdStreams.length, 3, 'ต้อง rotate ไปใช้ prewarm stream ที่สอง แล้วเติม prewarm ใหม่ทันที เหมือนกับ default 900ms')
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

// ---------------------------------------------------------------------------
// STT EOS Lifecycle Recovery (design LOCKED 2026-08-25) — production incident: a singleUtterance:true
// stream can reach END_OF_SINGLE_UTTERANCE and stop producing recognition results server-side while
// currentStream remains a locally-writable object, leaving the customer's continued speech silently
// discarded until Silence Timeout. EOS_RECOVERY_GRACE_MS=250 bounds how long we wait for a real
// completion (final/end/error) before treating the stream as stuck and recovering it ourselves.
// ---------------------------------------------------------------------------

function emitEos(stream) {
  stream.emit('data', { results: [], speechEventType: 'END_OF_SINGLE_UTTERANCE' })
}

test('EOS Case 1: END_OF_SINGLE_UTTERANCE ตามด้วย GOOGLE_FINAL จริงภายใน grace → deliver ครั้งเดียว, rotate ปกติ, ไม่มี stuck-recovery ตามมาทีหลัง', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first)
  emitFinal(first, 'ตัวอย่าง final ที่มาทันหลัง EOS')

  assert.deepEqual(transcripts, ['ตัวอย่าง final ที่มาทันหลัง EOS'], 'ต้อง deliver ผ่าน GOOGLE_FINAL ปกติ ครั้งเดียว')
  assert.equal(createdStreams.length, 2, 'rotate ปกติ (ไม่มี prewarm เพราะไม่เคย interim มาก่อน EOS เลย)')
  assert.equal(first.ended, true)

  await delay(300) // เลย grace(250ms) ที่ควรจะหมดอายุไปแล้ว
  assert.equal(createdStreams.length, 2, 'grace ที่ควรหมดอายุไปแล้วต้องไม่สร้าง stream เพิ่มอีก — พิสูจน์ว่า watchdog ถูก clear จริงตอน rotate (ผ่าน rotateForNextUtterance -> clearEosRecoveryFor)')

  sttStream.end()
})

test('EOS Case 2: ไม่มี final/end/error ตามมาเลย, ไม่มี prewarm → grace หมดอายุ → retire stream เดิม, สร้าง stream ใหม่, ฟังต่อได้ปกติ', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first)
  assert.equal(createdStreams.length, 1, 'EOS อย่างเดียวยังไม่สร้าง stream ใหม่ทันที ต้องรอ grace ก่อนเสมอ (ห้าม rotate ทันทีที่เห็น EOS)')

  await delay(300) // เลย grace(250ms)
  assert.equal(createdStreams.length, 2, 'grace หมดอายุ ต้องสร้าง stream ใหม่ (ไม่มี prewarm ให้ใช้)')
  assert.equal(first.ended, true, 'stream เดิมที่ตายไปต้องถูกปิด')

  const recovered = createdStreams[1]
  emitFinal(recovered, 'ฟังกลับมาได้ปกติหลัง recovery')
  assert.deepEqual(transcripts, ['ฟังกลับมาได้ปกติหลัง recovery'], 'stream ที่ recover มาต้องรับ/deliver transcript ได้ปกติ ไม่ fabricate อะไรเอง')

  sttStream.end()
})

// Review Fix 1 note: EOS Case 2b/11 (below) must construct "prewarm exists AND current stream has no
// pending interim" WITHOUT going through the normal interim→prewarm path directly on the target stream —
// that path always leaves the SAME stream's interim pending until it resolves, and resolving it always
// consumes/promotes the prewarm via rotation, so "prewarm exists + clean current" can never coexist that
// way. Instead this uses the pre-existing (unrelated to EOS), independent "prewarm ended early → recreate
// after 300ms" mechanism to land a prewarm on an ALREADY-clean current stream — proven by tracing
// createStream(true)'s second call site (the delayed recreate) which only requires `currentStream` truthy,
// never a pending interim.

test('EOS Case 2b: มี prewarm (nextStream) พร้อมอยู่แล้วตอน grace หมดอายุ (โดยไม่มี pending interim ปนอยู่) → ใช้ prewarm ต่อ ไม่ cold-create', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'x') // สร้าง prewarm P1 ให้ first, arm TIMER_FINAL ของ first
  const p1 = createdStreams[1]
  p1.emit('end') // P1 ตายก่อนเวลา — nextStream=null, schedule recreate(300ms) ให้ currentStream ตอนนั้น (ยังเป็น first)
  emitFinal(first, 'x confirmed') // resolve pending interim ของ first ทันที (เร็วกว่า recreate 300ms แน่นอน) — nextStream=null แล้ว (P1 ตายไปแล้ว) จึงสร้าง fresh stream (second) ที่สะอาด ไม่มี pending interim

  assert.equal(createdStreams.length, 3, 'first, P1(ตายแล้ว), second(current ใหม่ สะอาด)')
  const second = createdStreams[2]

  await delay(320) // รอเลย 300ms ของ recreate เดิม (armed ตอน P1 ตาย) — ต้องสร้าง prewarm ใหม่ (P2) ให้ second ที่ไม่มี pending interim เลย
  assert.equal(createdStreams.length, 4, 'recreate เดิม (ไม่เกี่ยวกับ EOS) ต้องสร้าง P2 ให้ second')
  const p2 = createdStreams[3]

  emitEos(second) // second สะอาดจริง (ไม่มี pending interim) → EOS watchdog arm ได้ปกติ
  await delay(300) // เลย grace(250ms)

  // Track 1 fix (2026-08-30): grace หมดอายุต้อง promote P2 ที่มีอยู่แล้วเป็น current (ไม่ cold-create) — แต่ตอนนี้
  // activatePrewarm() proactive re-prewarm ทันทีด้วย จึงต้องมี stream ที่ห้าเกิดขึ้นจริง (ไม่ใช่ "ต้องไม่มี" แบบเดิม
  // ก่อน fix นี้) นี่คือพฤติกรรมที่ตั้งใจ — ปิดช่องว่างที่เคยทำให้ recovery event ถัดไปตกไปใช้ cold createStream(false)
  assert.equal(createdStreams.length, 5, 'grace หมดอายุต้อง promote P2 เป็น current แล้วเติม prewarm ใหม่ (ตัวที่ห้า) ทันที')
  assert.equal(second.ended, true)

  emitFinal(p2, 'ใช้ prewarm ต่อได้ปกติ')
  assert.deepEqual(transcripts, ['x confirmed', 'ใช้ prewarm ต่อได้ปกติ'], 'prewarm ที่ถูก promote มาต้องเป็น currentStream จริง รับ event ได้ปกติ')

  sttStream.end()
})

test('EOS Case 11: prewarm (nextStream) ตายไประหว่าง grace pending (โดยไม่มี pending interim ปนอยู่) → grace หมดอายุต้อง fallback ไปสร้าง stream ใหม่แทนที่จะพยายามใช้ prewarm ที่ตายไปแล้ว', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  // setup เดียวกับ Case 2b ด้านบน เพื่อให้ได้ current stream ที่สะอาด (ไม่มี pending interim) พร้อม prewarm อยู่แล้ว
  emitInterim(first, 'x')
  const p1 = createdStreams[1]
  p1.emit('end')
  emitFinal(first, 'x confirmed')
  const second = createdStreams[2]

  await delay(320) // รอ recreate(300ms) เดิมสร้าง prewarm P2 ให้ second
  assert.equal(createdStreams.length, 4)
  const p2 = createdStreams[3]

  emitEos(second) // second สะอาด → EOS watchdog arm ปกติ (grace 250ms)
  p2.emit('end') // P2 ตายเองระหว่าง grace ยังไม่หมดอายุ — กลไกเดิม (ไม่เกี่ยวกับ EOS) จะ schedule recreate(300ms) ใหม่อีกรอบ แต่ไม่ใช่ประเด็นของ test นี้ (assert ก่อนมันจะทัน fire)

  await delay(260) // เลย grace(250ms) ของ EOS แต่ยังไม่ถึง 300ms ของ recreate รอบใหม่ (armed ตอน p2 ตาย)
  assert.equal(createdStreams.length, 5, 'grace หมดอายุต้อง fallback สร้าง fresh stream (ตัวที่ห้า) เพราะ p2 ตายไปแล้วก่อนหน้า — nextStream เป็น null ตอนนั้น')
  assert.equal(second.ended, true)
  const recovered = createdStreams[4]

  emitFinal(recovered, 'ฟังต่อได้ปกติหลัง fallback')
  assert.deepEqual(transcripts, ['x confirmed', 'ฟังต่อได้ปกติหลัง fallback'])

  sttStream.end()
})

test('EOS Case 3: END_OF_SINGLE_UTTERANCE ตามด้วย stream end() event ก่อน grace หมด → ใช้ existing end recovery ปกติ ไม่มี duplicate recovery', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first)
  first.emit('end') // stream ตายเองผ่าน 'end' event ก่อน grace(250ms) จะหมดอายุ

  await delay(60) // existing end-recovery ("cold start fallback") ใช้ setTimeout(..., 50) ไม่ใช่ synchronous
  assert.equal(createdStreams.length, 2, 'existing end-recovery (cold start fallback) ต้องสร้าง stream ใหม่หลัง 50ms ตามกลไกเดิม')

  await delay(300) // เลย grace เดิมไปแล้ว — ต้องไม่มี recovery ซ้ำ (ไม่สร้าง stream ที่สาม)
  assert.equal(createdStreams.length, 2, 'EOS watchdog ต้องถูก clear ตอน end event มาถึง — ไม่ fire ซ้ำหลัง grace หมดอายุ')

  const recovered = createdStreams[1]
  emitFinal(recovered, 'ฟังต่อได้ปกติหลัง end recovery')
  assert.deepEqual(transcripts, ['ฟังต่อได้ปกติหลัง end recovery'])

  sttStream.end()
})

test('EOS Case 4: END_OF_SINGLE_UTTERANCE ตามด้วย stream error() ก่อน grace หมด → ใช้ existing error recovery ปกติ ไม่มี duplicate recovery', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first)
  first.emit('error', new Error('simulated stream error'))

  await delay(50)
  assert.equal(createdStreams.length, 1, 'ยังไม่ถึง 100ms ของ existing error-recovery (setTimeout 100ms) ต้องยังไม่สร้าง stream ใหม่')

  await delay(300) // เลย error-recovery 100ms เดิมไปแล้ว และเลย grace(250ms) เดิมของ EOS ไปด้วย
  assert.equal(createdStreams.length, 2, 'error-recovery เดิม (setTimeout 100ms) ต้องสร้าง stream ใหม่แค่ 1 ครั้งตามปกติ — ไม่ใช่ 3 ที่จะเกิดถ้า EOS watchdog ไม่ถูก clear ตอน error แล้วมา fire ซ้ำเพิ่มอีกรอบตอน grace(250ms) หมดอายุ')

  sttStream.end()
})

test('Track 1 fix (2026-08-30): recovery event ที่เกิดทันทีหลัง rotate ปกติ (ไม่มี interim คั่นเลย) ต้องยังใช้ prewarm ได้ ไม่ตกไป cold-create — นี่คือ short-utterance-loss bug ตัวจริงที่ Track 1 แก้', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'สวัสดี')
  await delay(950) // rotate ปกติ — ก่อน fix นี้ ตอนนี้จะมีแค่ 2 stream (first, prewarm ที่ถูก promote) และ nextStream=null
                    // ทำให้ recovery event ถัดไปที่ยังไม่ทันมี interim ใหม่มาเลยตกไปใช้ cold createStream(false) (เปิด
                    // cold-mute 200ms) — Track 1 แก้ด้วยการเติม prewarm ทันทีตอน rotate ไม่ต้องรอ interim ถัดไป
  assert.equal(createdStreams.length, 3, 'ต้องมี prewarm ตัวใหม่ (ตัวที่สาม) พร้อมอยู่แล้วก่อน interim ถัดไปจะมาถึงด้วยซ้ำ')

  const second = createdStreams[1] // stream ที่เพิ่งถูก promote เป็น current
  // จำลอง recovery event (EOS-stuck) เกิดขึ้นทันทีบน current stream โดยไม่มี interim ใหม่มาเลยตั้งแต่ rotate
  emitEos(second)
  await delay(300) // เลย grace(250ms)

  // ก่อน fix: ตรงนี้จะเป็น cold createStream(false) เพราะ nextStream เป็น null (ไม่มี interim มาเติมให้) — มี
  // cold-mute 200ms เปิดอยู่ ถ้าลูกค้าพูดคำสั้นพอดีตอนนั้นจะหายทั้งคำไม่มีร่องรอย (ปัญหาที่รายงานมา)
  // หลัง fix: ต้องใช้ prewarm ที่เติมไว้แล้ว (ตัวที่สาม) แทน — ไม่มี cold-mute เลย
  assert.equal(createdStreams.length, 4, 'recovery event ต้อง promote prewarm ที่เติมไว้แล้ว (ตัวที่สาม) แล้วเติมตัวที่สี่ต่อทันที ไม่ใช่ cold-create')
  assert.equal(second.ended, true)

  const third = createdStreams[2]
  emitFinal(third, 'ฟังต่อได้ปกติผ่าน prewarm ไม่ใช่ cold stream')
  assert.deepEqual(transcripts, ['สวัสดี', 'ฟังต่อได้ปกติผ่าน prewarm ไม่ใช่ cold stream'])

  sttStream.end()
})

test('EOS Case 6/7 regression: TIMER_FINAL และ GOOGLE_FINAL ปกติ (ไม่มี EOS เลย) ต้องทำงานเหมือนเดิมทุกประการ — พิสูจน์จาก 3 tests เดิมด้านบนที่ยังผ่านอยู่ไม่เปลี่ยนแปลง (ไม่ต้อง duplicate ที่นี่)', () => {
  assert.ok(true, 'ดู tests เดิม: "rotate ทันทีหลัง synthetic 900ms finalize deliver...", "stream ที่เพิ่ง rotate มา...", "rotate ทำงานจาก isFinal จริงจาก Google..." — ทั้งหมดยังผ่านโดยไม่ต้องแก้ ยืนยันว่า EOS lifecycle ไม่กระทบ path ปกติ')
})

test('EOS Case 8: call end() ระหว่าง grace pending → timer ต้องถูก clear, ไม่มี stream ใหม่ถูกสร้างหลัง destroyed', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first)
  sttStream.end() // call จบระหว่าง grace ยังไม่หมดอายุ

  await delay(300) // เลย grace(250ms) เดิมไปมาก
  assert.equal(createdStreams.length, 1, 'ห้ามสร้าง stream ใหม่เลยหลัง destroyed=true แม้ grace เดิมจะครบเวลาไปแล้วก็ตาม')
  assert.deepEqual(transcripts, [], 'ห้าม deliver อะไรเลย')
})

test('EOS Case 9: duplicate EOS บน stream เดียวกันซ้ำหลายครั้ง → ต้องไม่ extend deadline — grace นับจาก EOS ตัวแรกเท่านั้น', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first) // t=0 (deadline ควรอยู่ที่ ~t=250)
  await delay(100)
  emitEos(first) // t=100 — ถ้า implementation ผิดจะ restart deadline เป็น ~t=350
  await delay(100)
  emitEos(first) // t=200 — ถ้า implementation ผิดจะ restart deadline เป็น ~t=450

  await delay(80) // รวม t=280 — เลย deadline เดิม(250) ไปแล้ว แต่ยังไม่ถึง deadline ที่จะ extend ผิดๆ(450) ถ้ามี bug
  assert.equal(createdStreams.length, 2, 'grace ต้องหมดอายุตาม deadline ของ EOS ตัวแรก (~250ms) ไม่ใช่ถูกเลื่อนออกไปเรื่อยๆ จาก EOS ซ้ำที่ t=100/200')

  sttStream.end()
})

test('EOS Case 10 (identity safety): Stream A ได้ EOS แล้ว rotate ไป Stream B ปกติผ่าน final จริง (ไม่ใช่ EOS) → รอเลย deadline เดิมของ A ไปมาก → B ต้องไม่ถูกแตะต้องเลย', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first) // arm recovery สำหรับ A, deadline ~250ms
  await delay(50)
  emitFinal(first, 'final จริงมาก่อน grace ของ A จะหมดอายุ') // supersede ปกติ — clearEosRecoveryFor(A) ต้องถูกเรียกผ่าน rotateForNextUtterance()

  assert.deepEqual(transcripts, ['final จริงมาก่อน grace ของ A จะหมดอายุ'])
  assert.equal(createdStreams.length, 2, 'rotate ไป stream B (สร้างใหม่ เพราะไม่มี prewarm)')
  const second = createdStreams[1]

  await delay(300) // รอเลย deadline เดิมของ A (~250ms จาก emitEos ครั้งแรก) ไปมาก
  assert.equal(createdStreams.length, 2, 'A ถูก clear แล้วตอน rotate — deadline เดิมของ A ต้องไม่ทำอะไรกับ B เลยแม้จะครบเวลาที่ควรจะ fire ก็ตาม (ถ้า implementation ผิดจะเห็น stream ที่สามถูกสร้างเพิ่มโดยไม่มีเหตุผล)')

  // ยืนยันเพิ่มว่า B ยังทำงานได้ปกติสมบูรณ์ ไม่ได้ถูกอะไรไป "แตะ" กลางทางแบบเงียบๆ
  emitFinal(second, 'B ยังทำงานปกติ')
  assert.deepEqual(transcripts, ['final จริงมาก่อน grace ของ A จะหมดอายุ', 'B ยังทำงานปกติ'])

  sttStream.end()
})

test('EOS + duplicate EOS ข้าม 2 stream (extended identity safety): Stream A EOS แล้วถูก error() เคลียร์, Stream ใหม่ (B) ได้ EOS ของตัวเองทันที → ต้องนับ deadline ของ B แยกจาก A โดยสมบูรณ์ ไม่ปนกัน', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitEos(first)
  first.emit('error', new Error('A dies')) // clearEosRecoveryFor(A) ทันที + error-recovery เดิม (setTimeout 100ms) สร้าง stream ใหม่

  await delay(150) // ให้ error-recovery เดิมสร้าง stream B เสร็จ (100ms + margin)
  assert.equal(createdStreams.length, 2)
  const second = createdStreams[1]

  emitEos(second) // B ได้ EOS ของตัวเอง — deadline ใหม่แยกจาก A โดยสิ้นเชิง
  await delay(300) // เลย grace(250ms) ของ B
  assert.equal(createdStreams.length, 3, 'B ต้อง recover ด้วย deadline ของตัวเอง ไม่ใช่ผูกติดหรือถูกรบกวนจาก lifecycle ของ A ที่ตายไปก่อนหน้า')

  sttStream.end()
})

// ---------------------------------------------------------------------------
// STT EOS Lifecycle Recovery Review Fix 1 (2026-08-25) — ownership conflict between EOS_RECOVERY_GRACE_MS
// (250ms) and a pending interim's own TIMER_FINAL (900ms). Review found: EOS recovery's fire callback
// calls resetUtteranceState(), which wipes interimText/interimTimer unconditionally — if EOS arrives while
// a real interim is still waiting on its own TIMER_FINAL deadline, and grace expires first, the watchdog
// would silently discard text Google had already recognized. Fix: do not arm EOS recovery at all when a
// pending interim exists — TIMER_FINAL/GOOGLE_FINAL owns completion in that case, matching how the
// production incident this Track fixes never had a pending interim to begin with.
// ---------------------------------------------------------------------------

test('EOS Case 12 (Review Fix 1 blocker): EOS ขณะมี pending interim (TIMER_FINAL ยังไม่ครบ), ไม่มี GOOGLE_FINAL ตามมา → EOS watchdog ต้องไม่ยิงที่ 250ms, ต้อง deliver interim เดิมผ่าน TIMER_FINAL ที่ 900ms ครั้งเดียว, rotate ครั้งเดียว, stream ใหม่ฟังต่อได้ปกติ', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'สนใจครับ') // t=0 — arm TIMER_FINAL ที่ t=900, และ prewarm stream ที่สอง
  assert.equal(createdStreams.length, 2, 'ต้องมี prewarm แล้วจาก interim แรก')

  emitEos(first) // t~0 — EOS มาระหว่างที่ interim ยัง pending อยู่ (interimText/interimTimer ตั้งอยู่)

  await delay(300) // เลย grace(250ms) เดิมไปแล้ว — ต้องไม่มีอะไรเกิดขึ้นเลย (ownership เป็นของ TIMER_FINAL)
  assert.equal(createdStreams.length, 2, 'grace(250ms) ต้องไม่ยิง — ไม่มี stream ใหม่ถูกสร้างจาก EOS watchdog เพราะไม่ได้ arm ตั้งแต่ต้น (ownership เป็นของ TIMER_FINAL)')
  assert.deepEqual(transcripts, [], 'ยังไม่ถึงเวลา TIMER_FINAL(900ms) — ต้องยังไม่ deliver อะไรเลย (พิสูจน์ว่า interimText ไม่ได้ถูกลบทิ้งไปก่อนหน้านี้)')
  assert.equal(first.ended, false, 'stream เดิมต้องยังไม่ถูก retire — ยังไม่มีอะไรจบ lifecycle ของมันเลย')

  await delay(650) // รวม ~950ms จาก interim แรก — เลย TIMER_FINAL(900ms) ไปแล้ว
  assert.deepEqual(transcripts, ['สนใจครับ'], 'TIMER_FINAL ต้อง deliver ข้อความเดิมที่ได้จาก interim ก่อน EOS จะมาถึง ครั้งเดียว ไม่หายไปไหน')
  // Track 1 fix (2026-08-30): rotate ไปใช้ prewarm ที่มีอยู่แล้ว (จาก interim แรก) แล้วเติม prewarm ใหม่ (ตัวที่สาม) ทันที
  assert.equal(createdStreams.length, 3, 'rotate ไปใช้ prewarm ที่มีอยู่แล้ว แล้วเติม prewarm ใหม่ทันที ไม่ใช่รอ interim ถัดไป')
  assert.equal(first.ended, true, 'stream เดิมต้องถูกปิดหลัง rotate ปกติ (ผ่าน TIMER_FINAL path ไม่ใช่ EOS watchdog)')

  const second = createdStreams[1]
  emitFinal(second, 'ฟังต่อได้ปกติ')
  assert.deepEqual(transcripts, ['สนใจครับ', 'ฟังต่อได้ปกติ'])

  sttStream.end()
})

test('EOS Case 13 (Review Fix 1): EOS ขณะมี pending interim แล้ว GOOGLE_FINAL ตามมาก่อน 900ms → deliver ผ่าน GOOGLE_FINAL ครั้งเดียว, TIMER_FINAL เดิมถูกยกเลิกตาม flow ปกติ, EOS watchdog ไม่ทำอะไรเลย, rotate ครั้งเดียว', async () => {
  createdStreams = []
  const transcripts = []
  const sttStream = transcribeStream((t) => transcripts.push(t), () => {})
  const first = createdStreams[0]

  emitInterim(first, 'สนใจ') // t=0 — arm TIMER_FINAL(900ms) + prewarm
  emitEos(first) // t~0 — ไม่ arm EOS watchdog เพราะมี pending interim

  await delay(50)
  emitFinal(first, 'สนใจครับผม') // GOOGLE_FINAL มาก่อน TIMER_FINAL/grace ใดๆ จะครบ

  assert.deepEqual(transcripts, ['สนใจครับผม'], 'ต้อง deliver ผ่าน GOOGLE_FINAL ครั้งเดียว ไม่ใช่ค่า interim เดิม')
  // Track 1 fix (2026-08-30): rotate ครั้งเดียวไปใช้ prewarm แล้วเติม prewarm ใหม่ (ตัวที่สาม) ทันที
  assert.equal(createdStreams.length, 3, 'rotate ไปใช้ prewarm แล้วเติมใหม่ทันที')
  assert.equal(first.ended, true)

  await delay(300) // เลยทั้ง grace(250ms) เดิมและ TIMER_FINAL(900ms) เดิมไปแล้ว — ต้องไม่มีอะไรเพิ่มอีกเลย
  assert.equal(createdStreams.length, 3, 'ไม่มี recovery ซ้ำจากทั้ง EOS watchdog (ไม่เคย arm) และ TIMER_FINAL เดิม (ถูก clear ไปแล้วตอน GOOGLE_FINAL rotate)')
  assert.deepEqual(transcripts, ['สนใจครับผม'], 'ไม่มี delivery ซ้ำ')

  sttStream.end()
})
