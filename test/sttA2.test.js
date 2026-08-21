// STT-A2 (diagnostic only, design revision 2026-08-21) — unit tests สำหรับ maxAlternatives request-shape gate
// และ acceptedAlternatives lifecycle ใน src/services/googleSTT.js
// ยืนยันว่า: A2 OFF ไม่แตะ request shape เดิมแม้แต่บิตเดียว, A2 ON ส่ง maxAlternatives:3 จริง, transcript ที่
// deliver เข้า conversation ยังคงมาจาก alternatives[0] เท่านั้นไม่ว่า A2 จะ ON/OFF, และ TIMER_FINAL ไม่มีทาง
// ปนกับ alternatives ของ interim ที่ถูก regression-reject ไปแล้ว
const { test } = require('node:test')
const assert = require('node:assert/strict')
const stt = require('./_googleSttHarness')

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

test('A2 OFF (maxAlternatives ไม่ระบุ): request config deep-equal STT_CONFIG เดิมเป๊ะ ไม่มี maxAlternatives key เลย', () => {
  const { transcribeStream } = stt.ensureStubbed()
  const handle = transcribeStream(() => {}, () => {}, { interimFinalizeMs: 20 })
  const opts = stt.capturedOptions[0]
  assert.deepEqual(opts.config, EXPECTED_STT_CONFIG)
  assert.equal('maxAlternatives' in opts.config, false)
  handle.end()
})

test('A2 OFF (maxAlternatives: null ส่งมาตรงๆ): request config ยังเหมือนเดิมเป๊ะ', () => {
  const { transcribeStream } = stt.ensureStubbed()
  const handle = transcribeStream(() => {}, () => {}, { interimFinalizeMs: 20, maxAlternatives: null })
  const opts = stt.capturedOptions[0]
  assert.deepEqual(opts.config, EXPECTED_STT_CONFIG)
  handle.end()
})

test('A2 OFF (maxAlternatives: 1): ยังไม่ควร spread key เข้า config (1 alternative คือ default อยู่แล้ว)', () => {
  const { transcribeStream } = stt.ensureStubbed()
  const handle = transcribeStream(() => {}, () => {}, { interimFinalizeMs: 20, maxAlternatives: 1 })
  const opts = stt.capturedOptions[0]
  assert.deepEqual(opts.config, EXPECTED_STT_CONFIG)
  assert.equal('maxAlternatives' in opts.config, false)
  handle.end()
})

test('A2 ON (maxAlternatives: 3): request config มี maxAlternatives:3 จริง ส่วนอื่นเหมือน STT_CONFIG เดิมทุกประการ', () => {
  const { transcribeStream } = stt.ensureStubbed()
  const handle = transcribeStream(() => {}, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  const opts = stt.capturedOptions[0]
  assert.equal(opts.config.maxAlternatives, 3)
  const { maxAlternatives, ...rest } = opts.config
  assert.deepEqual(rest, EXPECTED_STT_CONFIG)
  handle.end()
})

test('MANDATORY: accepted interim A → regression B rejected → TIMER_FINAL delivers A, diagnostics carry A\'s alternatives, never B\'s', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'พอยต์เอาไปทำอะไร', undefined, [
    { transcript: 'พอยต์ใช้ทำอะไร' }, { transcript: 'พอยต์ทำอะไรได้บ้าง' },
  ]) // A — accepted (not a regression, interimText was empty before this)
  stt.emitInterim(stream, 'พอยต์', undefined, [
    { transcript: 'จุด' }, { transcript: 'พอ' },
  ]) // B — strict-prefix regression of A, must be rejected
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].text, 'พอยต์เอาไปทำอะไร') // delivered text = A, not B
  assert.equal(calls[0].meta.source, 'TIMER_FINAL')
  assert.equal(calls[0].meta.alternatives[0].text, 'พอยต์เอาไปทำอะไร')
  assert.equal(calls[0].meta.alternatives[0].selected, true)
  assert.equal(calls[0].meta.alternatives.length, 3)
  const allTexts = calls[0].meta.alternatives.map(a => a.text)
  assert.ok(!allTexts.includes('จุด') && !allTexts.includes('พอ') && !allTexts.includes('พอยต์'), 'B\'s alternatives ต้องไม่ปนมาเลย')
  assert.ok(allTexts.includes('พอยต์ใช้ทำอะไร') && allTexts.includes('พอยต์ทำอะไรได้บ้าง'), 'A\'s alternatives ต้องอยู่ครบ')

  handle.end()
})

test('A2 OFF: sttMeta.alternatives ต้องเป็น null เสมอ ทั้ง TIMER_FINAL และ GOOGLE_FINAL', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'ทดสอบ', undefined, [{ transcript: 'อื่น' }]) // ส่ง alternatives มาด้วยแม้ A2 OFF (จำลอง Google ส่งมาเผื่อ)
  await waitFor(() => calls.length === 1)
  assert.equal(calls[0].meta.alternatives, null)

  handle.end()
})

test('GOOGLE_FINAL: alternatives มาจาก final result นั้นตรงๆ ไม่ผ่าน acceptedAlternatives ของ interim ก่อนหน้า', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'ทดสอบก่อน', undefined, [{ transcript: 'ทดสอบก่อนนะ' }]) // interim ที่ไม่เกี่ยวกับ final ที่จะมาถึง
  stt.emitFinal(stream, 'โอเคครับ', 0.9, [{ transcript: 'โอเคค่ะ', confidence: 0.5 }, { transcript: 'โอเค' }])
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].text, 'โอเคครับ')
  assert.equal(calls[0].meta.source, 'GOOGLE_FINAL')
  assert.equal(calls[0].meta.alternatives[0].text, 'โอเคครับ')
  assert.equal(calls[0].meta.alternatives.length, 3)
  const allTexts = calls[0].meta.alternatives.map(a => a.text)
  assert.ok(!allTexts.includes('ทดสอบก่อน') && !allTexts.includes('ทดสอบก่อนนะ'), 'ต้องไม่ปนกับ interim ก่อนหน้าเลย')

  handle.end()
})

test('Google คืน alternative น้อยกว่าที่ขอ (แค่ 1 รายการ แม้ maxAlternatives=3) — ไม่ crash, array มีแค่ 1 entry', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  const stream = stt.streams[0]

  stt.emitInterim(stream, 'ทดสอบ') // ไม่ส่ง otherAlternatives เลย — Google คืนมาแค่ alt0
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].meta.alternatives.length, 1)
  assert.equal(calls[0].meta.alternatives[0].text, 'ทดสอบ')
  assert.equal(calls[0].meta.alternatives[0].selected, true)

  handle.end()
})

test('alternatives confidence: >0 เก็บจริง, 0/missing → null (ไม่ fabricate)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  const stream = stt.streams[0]

  stt.emitFinal(stream, 'ทดสอบ', 0.9, [{ transcript: 'อีกทาง', confidence: 0 }, { transcript: 'อีกทางสอง' }])
  await waitFor(() => calls.length === 1)

  assert.equal(calls[0].meta.alternatives[0].confidence, 0.9)
  assert.equal(calls[0].meta.alternatives[1].confidence, null) // confidence=0 → null
  assert.equal(calls[0].meta.alternatives[2].confidence, null) // ไม่ส่ง confidence มาเลย → null

  handle.end()
})

test('STT_CONFIG/singleUtterance/interimResults ไม่เปลี่ยนไม่ว่า A2 ON หรือ OFF (deep-equal ทั้งสองกรณี)', () => {
  const { transcribeStream: t1 } = stt.ensureStubbed()
  const h1 = t1(() => {}, () => {}, { interimFinalizeMs: 20 })
  assert.equal(stt.capturedOptions[0].interimResults, true)
  assert.equal(stt.capturedOptions[0].singleUtterance, true)
  h1.end()

  const { transcribeStream: t2 } = stt.ensureStubbed()
  const h2 = t2(() => {}, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  assert.equal(stt.capturedOptions[0].interimResults, true)
  assert.equal(stt.capturedOptions[0].singleUtterance, true)
  h2.end()
})

test('trailing event จาก old stream หลัง TIMER_FINAL ยังถูกทิ้งเหมือนเดิม แม้ A2 ON (ไม่กระทบ old-stream guard เดิม)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, { interimFinalizeMs: 20, maxAlternatives: 3 })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'เรียบร้อยดีครับ')
  await waitFor(() => calls.length === 1)

  stt.emitFinal(oldStream, 'trailing ที่ไม่ควรมาถึง', 0.9, [{ transcript: 'อีกตัว' }])
  await stt.delay(30)
  assert.equal(calls.length, 1, 'onTranscript ต้องไม่ถูกเรียกซ้ำจาก stream เก่า')

  handle.end()
})
