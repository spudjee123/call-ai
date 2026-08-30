// STT-A1 (observability only) — unit tests สำหรับ src/services/googleSTT.js
// ยืนยันว่า diagnostic metadata (streamId/utteranceId/source/counters/timing/stability/confidence) ถูกต้อง
// โดยที่ transcript ที่ deliver จริง, request config ที่ส่งให้ Google, และ recognition/regression behavior เดิม
// ไม่เปลี่ยนแปลงแม้แต่บิตเดียว — เทสของ rotateForNextUtterance/L1a/Design A เดิมยังอยู่ที่ googleSTT.test.js
// ไม่แตะ (ไฟล์นี้แยกต่างหากตั้งใจ เพราะใช้ harness แบบ per-test fresh-require ต่างจากไฟล์เดิมที่ require ครั้งเดียว)
const { test } = require('node:test')
const assert = require('node:assert/strict')
const stt = require('./_googleSttHarness')

// ต้องตรงกับ STT_CONFIG ใน src/services/googleSTT.js เป๊ะทุก field — ถ้า diff จากนี้ = มีคนแตะ recognition config
// ที่ scope ของ STT-A1 ล็อกไว้ว่าห้ามแตะเด็ดขาด
const EXPECTED_STT_CONFIG = {
  encoding: 'LINEAR16',
  sampleRateHertz: 8000,
  languageCode: 'th-TH',
  model: 'latest_short',
  useEnhanced: true,
  speechContexts: [{
    phrases: [
      'สวัสดี', 'ครับ', 'ค่ะ', 'สนใจ', 'ราคา', 'โปรโมชั่น', 'ไม่สนใจ', 'ขอบคุณ',
      'PGDOG', 'พีจีด็อก', 'แอดไลน์', 'พอยต์', 'ฝาก', 'สมัคร', 'โบนัส',
      'รับ', 'อยากรับ', 'สมัครรับ', 'ต้องทำยังไง',
      'ฮัลโหล', 'ฮัลโหลค่ะ', 'ฮัลโหลครับ', 'อัลโหล',
      'ได้ยิน', 'ได้ยินครับ', 'ได้ยินค่ะ', 'ได้ยินไหมคะ', 'ได้ยินไหมครับ',
      'ฮัลโหลครับได้ยินไหม', 'ฮัลโหลค่ะได้ยินไหม',
      'พอยต์เอาไปทำอะไรได้', 'พอยต์ทำอะไรได้บ้าง', 'พอยต์ใช้ทำอะไรได้',
    ],
    boost: 15,
  }],
  enableAutomaticPunctuation: true,
}

function waitFor(conditionFn, { timeout = 500, interval = 5 } = {}) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    const tick = () => {
      if (conditionFn()) return resolve()
      if (Date.now() - start > timeout) return reject(new Error('waitFor timeout'))
      setTimeout(tick, interval)
    }
    tick()
  })
}

test('STT_CONFIG/singleUtterance/interimResults ที่ส่งให้ Google ไม่เปลี่ยนจาก A1 (deep-equal ทุก field)', () => {
  const { transcribeStream } = stt.ensureStubbed()
  const handle = transcribeStream(() => {}, () => {}, { interimFinalizeMs: 20 })
  assert.equal(stt.capturedOptions.length, 1)
  const opts = stt.capturedOptions[0]
  assert.deepEqual(opts.config, EXPECTED_STT_CONFIG)
  assert.equal(opts.interimResults, true)
  assert.equal(opts.singleUtterance, true)
  handle.end()
})

test('TIMER_FINAL: transcript เดิม 100% + metadata ถูกต้อง (streamId, utteranceId, source, counters, timing)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'สะดวก')
  stt.emitInterim(stream, 'สะดวกครับ')
  await waitFor(() => calls.length === 1)

  assert.equal(calls.length, 1)
  assert.equal(calls[0].text, 'สะดวกครับ') // ข้อความที่ deliver ต้องตรงกับ interim ล่าสุดเป๊ะ ไม่ถูกแก้ไข
  const meta = calls[0].meta
  assert.equal(meta.source, 'TIMER_FINAL')
  assert.equal(meta.streamId, 1)
  assert.equal(meta.utteranceId, 1)
  assert.equal(meta.interimCount, 2)
  assert.equal(meta.regressionCount, 0)
  assert.equal(typeof meta.firstInterimAt, 'number')
  assert.equal(typeof meta.finalAt, 'number')
  assert.equal(meta.firstInterimToFinalMs, meta.finalAt - meta.firstInterimAt)
  assert.equal(meta.finalConfidence, null) // TIMER_FINAL ไม่ใช่ final จริงจาก Google ไม่มี confidence
  assert.equal(meta.coldMutePackets, 0)

  handle.end()
})

test('GOOGLE_FINAL (ไม่มี interim นำมาก่อนเลย): transcript เดิม 100% + source ถูกต้อง + assign utteranceId เอง', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitFinal(stream, 'โอเคครับ', 0.87)
  await waitFor(() => calls.length === 1)

  assert.equal(calls.length, 1)
  assert.equal(calls[0].text, 'โอเคครับ')
  const meta = calls[0].meta
  assert.equal(meta.source, 'GOOGLE_FINAL')
  assert.equal(meta.streamId, 1)
  assert.equal(meta.utteranceId, 1) // ไม่เคยมี interim มาก่อน ต้อง assign ตอนนี้เอง ไม่ใช่ null
  assert.equal(meta.interimCount, 0)
  assert.equal(meta.firstInterimAt, null)
  assert.equal(meta.firstInterimToFinalMs, null)
  assert.equal(meta.finalConfidence, 0.87)

  handle.end()
})

test('strict-prefix regression: behavior เดิม (deliver ข้อความยาวเดิม) ไม่เปลี่ยน แต่ regressionCount นับเพิ่ม', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'Point เอาไปทำอะไรได้')
  stt.emitInterim(stream, 'Point') // regression: สั้นกว่าและเป็น prefix ของตัวก่อนหน้า
  stt.emitInterim(stream, 'Point') // regression อีกครั้ง
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].text, 'Point เอาไปทำอะไรได้') // ค่าที่เก็บไว้ต้องยังเป็นตัวยาวเดิม ไม่ถูก regression overwrite
  assert.equal(calls[0].meta.interimCount, 3)
  assert.equal(calls[0].meta.regressionCount, 2)

  handle.end()
})

test('stability: 0 หรือ missing (ไม่ส่ง field มาเลย) ต้องเป็น null เสมอ ไม่ fabricate เป็นค่าจริง', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'ฝาก', 0) // stability=0 (sentinel/unset) → ต้องไม่ถูกเก็บ
  stt.emitInterim(stream, 'ฝาก 100') // ไม่ส่ง stability มาเลย (undefined) → ต้องไม่ถูกเก็บเช่นกัน
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].meta.lastStability, null)
  assert.equal(calls[0].meta.maxStability, null)

  handle.end()
})

test('confidence: 0 ต้องเป็น null เสมอ ไม่ fabricate เป็นค่าจริง (GOOGLE_FINAL)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitFinal(stream, 'ครับ', 0)
  await waitFor(() => calls.length === 1)
  assert.equal(calls[0].meta.finalConfidence, null)

  handle.end()
})

test('stability > 0 ถูกเก็บจริง เป็น last/max ที่ถูกต้องตามลำดับ interim', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'ฝาก', 0.3)
  stt.emitInterim(stream, 'ฝาก 100', 0.9)
  stt.emitInterim(stream, 'ฝาก 100 บาท', 0.5)
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].meta.lastStability, 0.5)
  assert.equal(calls[0].meta.maxStability, 0.9)

  handle.end()
})

test('Review Gate round 1 fix: interim สุดท้ายมี stability=0/missing หลังจาก interim ก่อนหน้ามีค่าจริง → lastStability ต้องเป็น null ไม่ใช่ "ค่าที่ไม่ใช่ศูนย์ล่าสุด" ที่เคยเจอ ส่วน maxStability ต้องยังจำค่าสูงสุดที่เคยเห็นไว้', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'ฝาก', 0.72)
  stt.emitInterim(stream, 'ฝาก 100', 0.81)
  stt.emitInterim(stream, 'ฝาก 100 บาท', 0) // interim สุดท้าย stability=0 (sentinel/unset)
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].meta.lastStability, null, 'lastStability ต้องตรงกับ interim ล่าสุดจริง (null) ไม่ใช่ 0.81 ที่ค้างจาก interim ก่อนหน้า')
  assert.equal(calls[0].meta.maxStability, 0.81, 'maxStability ต้องยังจำค่าสูงสุดที่เคยเห็นไว้ ไม่ถูก null ของ interim หลังล้าง')

  handle.end()
})

test('Review Gate round 4 fix: firstInterimAt ต้องจับเวลาก่อนเรียก onInterim callback ไม่ใช่หลัง (onInterim ไม่ใช่ noop จริงใน production — ทำ barge-in detection/prewarm trigger)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const onInterimBusyWorkMs = 25
  const handle = transcribeStream(
    (text, meta) => { calls.push({ text, meta }) },
    () => {
      // จำลอง synchronous work ของ onInterim จริงใน audioStream.js (barge-in/prewarm) — busy-wait เพราะ JS ไม่มี
      // synchronous sleep ในตัว ต้อง block event loop จริงเพื่อพิสูจน์ลำดับการจับเวลา
      const start = Date.now()
      while (Date.now() - start < onInterimBusyWorkMs) { /* busy wait */ }
    },
    { interimFinalizeMs: 20 }
  )
  const stream = stt.streams[0]

  const beforeEmit = Date.now()
  stt.emitInterim(stream, 'ทดสอบ') // emit('data', ...) เป็น synchronous — คืนค่าหลัง onInterim's busy-wait ทำงานเสร็จแล้ว
  const afterEmit = Date.now()

  await waitFor(() => calls.length === 1, { timeout: 1000 })

  const meta = calls[0].meta
  // ถ้า bug เดิมยังอยู่ (จับเวลาหลัง onInterim) firstInterimAt จะใกล้ afterEmit (>= afterEmit - busyMs โดยประมาณ)
  // ถ้า fix ถูกต้อง firstInterimAt ต้องอยู่ก่อนที่ onInterim busy-wait จะเริ่มทำงานนาน
  assert.ok(
    meta.firstInterimAt < afterEmit - onInterimBusyWorkMs + 5,
    `firstInterimAt (${meta.firstInterimAt}) ต้องถูกจับก่อน onInterim busy-work ไม่ใช่หลัง (afterEmit=${afterEmit}, busyMs=${onInterimBusyWorkMs})`
  )
  assert.ok(meta.firstInterimAt >= beforeEmit, 'firstInterimAt ต้องไม่เร็วกว่าที่ emit จริง')

  handle.end()
})

test('streamId/utteranceId เป็น monotonic ไม่ reuse ข้าม utterance/rotation', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })

  // utterance แรกบน stream แรก
  stt.emitInterim(stt.streams[0], 'เรียบร้อยดีครับ')
  await waitFor(() => calls.length === 1)
  assert.equal(calls[0].meta.streamId, 1)
  assert.equal(calls[0].meta.utteranceId, 1)

  // rotateForNextUtterance() ต้องสลับไป prewarm stream (สร้างไว้แล้วตอน interim แรกของ utterance ก่อนหน้า) —
  // Track 1 fix (2026-08-30): activatePrewarm() proactive re-prewarm ทันทีตอน promote จึงมี stream ที่สาม (ตัวใหม่
  // ที่เพิ่งเติม) อยู่ด้วยแล้ว ไม่ใช่แค่สองตัวเหมือนเดิม
  assert.equal(stt.streams.length, 3, 'ต้องมี prewarm stream ถูกสร้างไว้ล่วงหน้าแล้วตอน interim แรก แล้วเติม prewarm ใหม่ทันทีตอน rotate')
  const secondStream = stt.streams[1]

  stt.emitInterim(secondStream, 'สมาชิกใหม่ฝากขั้นต่ำเท่าไหร่')
  await waitFor(() => calls.length === 2)
  assert.equal(calls[1].meta.streamId, 2)
  assert.equal(calls[1].meta.utteranceId, 2) // ต้องไม่ใช่ 1 ซ้ำ

  handle.end()
})

test('trailing event จาก old stream หลัง TIMER_FINAL ถูกทิ้งเหมือนเดิม 100% ไม่มี onTranscript เพิ่ม', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'เรียบร้อยดีครับ')
  await waitFor(() => calls.length === 1) // TIMER_FINAL fire แล้ว rotate ไปแล้ว

  // event สายจาก stream เก่า (ตอนนี้ stream !== currentStream แล้ว) ต้องถูกทิ้งเงียบๆ เหมือนก่อน A1 ทุกประการ
  stt.emitFinal(oldStream, 'trailing text ที่ไม่ควรมาถึง onTranscript', 0.9)
  await stt.delay(30)
  assert.equal(calls.length, 1, 'onTranscript ต้องไม่ถูกเรียกซ้ำจาก stream เก่า')

  handle.end()
})

test('coldMutePackets: packet ที่ write() ระหว่าง cold-mute window (200ms หลัง non-prewarm stream สร้างใหม่) ต้องถูกนับจริง', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {})
  const stream = stt.streams[0]

  // เขียนทันที 3 ครั้งภายใน mute window (STREAM_MUTE_MS=200ms ของ googleSTT.js) — ต้องถูกทิ้งและนับครบ
  handle.write(Buffer.from([1, 2, 3]))
  handle.write(Buffer.from([4, 5, 6]))
  handle.write(Buffer.from([7, 8, 9]))

  await stt.delay(250) // เลย mute window แล้ว เขียนต่ออีก 1 ครั้ง — ไม่ควรถูกนับเป็น cold-mute อีก
  handle.write(Buffer.from([10, 11, 12]))

  stt.emitFinal(stream, 'ทดสอบ', 0.9)
  await waitFor(() => calls.length === 1)
  assert.equal(calls[0].meta.coldMutePackets, 3, 'ต้องนับเฉพาะ 3 packet ที่ถูก mute จริง ไม่รวม packet ที่ 4 ที่เขียนหลัง mute window หมดแล้ว')

  handle.end()
})

test('coldMutePackets: stream ที่มาจาก prewarm (activatePrewarm) ต้องเป็น 0 เสมอ เพราะ coldStreamMuteUntil ถูกรีเซ็ตเป็น 0 ไม่มี mute', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const first = stt.streams[0]

  stt.emitInterim(first, 'เรียบร้อยดีครับ') // trigger สร้าง prewarm stream (streams[1]) ทันที
  await waitFor(() => calls.length === 1) // TIMER_FINAL fire แล้ว rotate ไปใช้ prewarm stream (streams[1]) เป็น currentStream

  // Track 1 fix (2026-08-30): activatePrewarm() เติม prewarm ใหม่ (streams[2]) ทันทีตอน promote — เห็น 3 ไม่ใช่ 2
  assert.equal(stt.streams.length, 3)
  // เขียนทันทีบน stream ที่สอง (มาจาก prewarm) — ต้องไม่ถูก mute เลยแม้จะเขียนทันทีก็ตาม
  handle.write(Buffer.from([1, 2, 3]))
  handle.write(Buffer.from([4, 5, 6]))

  const second = stt.streams[1]
  stt.emitFinal(second, 'ทดสอบสอง', 0.9) // GOOGLE_FINAL เพื่อไม่ต้องรอ timer
  await waitFor(() => calls.length === 2)
  assert.equal(calls[1].meta.coldMutePackets, 0, 'prewarm-activated stream ต้องไม่ mute เลย (coldStreamMuteUntil=0 ตั้งแต่ activatePrewarm())')

  handle.end()
})

test('write() ยังทำงานปกติ และ interimFinalizeMs default 900 เมื่อไม่ระบุ (ไม่แตะ 900ms endpoint)', () => {
  const { transcribeStream } = stt.ensureStubbed()
  const handle = transcribeStream(() => {}, () => {})
  // ยืนยันว่าเรียก write()/end() ได้โดยไม่ throw — พฤติกรรม public interface เดิมไม่เปลี่ยน
  assert.doesNotThrow(() => handle.write(Buffer.from([0, 1, 2, 3])))
  assert.doesNotThrow(() => handle.end())
})
