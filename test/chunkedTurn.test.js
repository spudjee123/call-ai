const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// stub askClaudeStreamChunked/synthesizeSpeechStream ก่อน require chunkedTurn.js
// (เหมือน pattern ใน webhook.test.js — คุมพฤติกรรมทั้งสองได้อิสระต่อเทส ไม่ยิง API จริง)
const state = { claudeImpl: null, ttsImpl: null }

const claudePath = require.resolve('../src/services/claude')
require.cache[claudePath] = {
  id: claudePath, filename: claudePath, loaded: true,
  exports: {
    askClaudeStreamChunked: (session, signal, onControl) => state.claudeImpl(session, signal, onControl),
  },
}

const ttsPath = require.resolve('../src/services/tts')
require.cache[ttsPath] = {
  id: ttsPath, filename: ttsPath, loaded: true,
  exports: {
    synthesizeSpeechStream: (text, voiceId, signal, previousText) => state.ttsImpl(text, voiceId, signal, previousText),
  },
}

const { runChunkedTurn, speakFixedText, createChunkedProducer, adoptChunkedProducer } = require('../src/websocket/chunkedTurn')
const { createTurnMetrics } = require('../src/utils/turnMetrics')
const { createTurnState, markAudioCommitted } = require('../src/utils/turnState')
const { createCallState, bumpGeneration } = require('../src/utils/generationGuard')

beforeEach(() => {
  state.claudeImpl = null
  state.ttsImpl = null
})

function fakeClaude(deltas, { control } = {}) {
  return async function* (session, signal, onControl) {
    for (const d of deltas) yield d
    if (control) onControl?.(control)
  }
}

function makeSocket() {
  return { sent: [], readyState: 1, OPEN: 1, send(msg) { this.sent.push(JSON.parse(msg)) } }
}

// callState/generationId แยกจาก turnMetrics/turnState โดยตั้งใจ — callState คือ mutable "current generation"
// pointer ระดับทั้งสาย (เหมือนของจริงใน audioStream.js) ส่วน generationId คือค่าที่ freeze ไว้ตอนเริ่มเทิร์นนี้
function makeMetricsAndState(overrides = {}) {
  const callState = createCallState()
  const generationId = bumpGeneration(callState)
  const turnMetrics = createTurnMetrics({ callSid: 'CA1', generationId, path: 'chunked', rolloutBucket: 0, rolloutPercent: 100, ...overrides })
  const turnState = createTurnState(generationId)
  return { turnMetrics, turnState, callState, generationId }
}

test('1) text delta หลายก้อน → speech chunk ถูกเรียงลำดับถูกต้องตอนส่งเข้า TTS', async () => {
  state.claudeImpl = fakeClaude(['Hello wor', 'ld. Nice to meet you.'])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, ['Hello world.', 'Nice to meet you.'])
})

test('2) unicode (surrogate pair) แตกกลาง delta คนละก้อน → buffer ต่อกันได้ข้อความสมบูรณ์', async () => {
  const deltas = ['Hi \uD83D', '\uDE00 bye there.']
  state.claudeImpl = fakeClaude(deltas)
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(ttsCalls[0], 'Hi 😀 bye there.')
})

test('3) หลาย speech chunks (3 ประโยค) → TTS ถูกเรียกตามลำดับเดียวกับที่ปรากฏใน stream', async () => {
  state.claudeImpl = fakeClaude(['One thing. ', 'Two thing. ', 'Three thing.'])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, ['One thing.', 'Two thing.', 'Three thing.'])
})

test('4) Claude stream จบไม่มี punctuation ปิดท้าย → remainder ที่เหลือถูก flush ไปพูดด้วย', async () => {
  state.claudeImpl = fakeClaude(['just talking along without any ending mark'])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, ['just talking along without any ending mark'])
})

test('5) empty delta ปนมาระหว่างทาง → ไม่ทำให้เกิดการเรียก TTS ว่างเปล่า', async () => {
  state.claudeImpl = fakeClaude(['', 'Hello there friend.', ''])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, ['Hello there friend.'])
  assert.ok(!ttsCalls.some(t => t === ''))
})

test('6) first non-empty delta → t3 ถูก set (ผ่าน markOnce ที่ผูกไว้กับจุดรับ delta จริง)', async () => {
  state.claudeImpl = fakeClaude(['', 'First real delta. ', 'Second delta.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(typeof turnMetrics.t3, 'number')
})

test('7) first speech chunk ที่ chunker emit → t4 ถูก set', async () => {
  state.claudeImpl = fakeClaude(['Hello world. More text after.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(typeof turnMetrics.t4, 'number')
})

test('8-10) t5/t6/t7 set ตั้งแต่ครั้งแรกและไม่ขยับตาม chunk/audio ก้อนถัดๆ ไป, AUDIO_COMMITTED ติดหลังส่งจริง', async () => {
  state.claudeImpl = fakeClaude(['One. ', 'Two. ', 'Three.'])
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const snapshots = []
  state.ttsImpl = async function* () {
    yield Buffer.from('a')
    snapshots.push({ t6: turnMetrics.t6, t7: turnMetrics.t7, audioCommitted: turnState.audioCommitted })
    yield Buffer.from('b')
    snapshots.push({ t6: turnMetrics.t6, t7: turnMetrics.t7, audioCommitted: turnState.audioCommitted })
  }
  const socket = makeSocket()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })

  assert.equal(typeof turnMetrics.t5, 'number')
  assert.equal(typeof turnMetrics.t6, 'number')
  assert.equal(typeof turnMetrics.t7, 'number')
  assert.equal(turnState.audioCommitted, true)

  const first = snapshots[0]
  assert.ok(first.t6 != null && first.t7 != null && first.audioCommitted === true, 'ควรติดตั้งแต่ audio chunk แรกของ TTS call แรก')
  for (const s of snapshots) {
    assert.equal(s.t6, first.t6, 't6 ต้องไม่ขยับตาม chunk ถัดไป')
    assert.equal(s.t7, first.t7, 't7 ต้องไม่ขยับตาม chunk ถัดไป')
  }
})

test('11) end_call ผ่าน onControl เท่านั้น — ไม่มี text ไหนถูกส่งเข้า TTS เลย', async () => {
  state.claudeImpl = async function* (session, signal, onControl) {
    onControl?.({ type: 'end_call' })
  }
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const controlEvents = []
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, onControl: (c) => controlEvents.push(c) })
  assert.deepEqual(controlEvents, [{ type: 'end_call' }])
  assert.deepEqual(ttsCalls, [])
  assert.equal(turnMetrics.t3, null)
  assert.equal(turnMetrics.t5, null)
})

test('end_call หลังพูดข้อความสุดท้าย → คำพูดสุดท้ายต้องพูดจบก่อน runChunkedTurn resolve (ไม่ตัดกลางทาง)', async () => {
  // จำลอง "ขอบคุณค่ะ" ตามด้วย end_call tool — ต้องพูดให้จบก่อนค่อยถือว่าเทิร์นนี้จบ
  state.claudeImpl = async function* (session, signal, onControl) {
    yield 'ขอบคุณค่ะ แล้วพบกันใหม่ค่ะ.'
    onControl?.({ type: 'end_call' })
  }
  const ttsCalls = []
  const controlEvents = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, onControl: (c) => controlEvents.push(c) })
  // สองประโยคสมบูรณ์ → ถูก chunker ตัดเป็น 2 speech chunk ตามธรรมชาติ (ไม่ใช่พูดรวดเดียวทั้งก้อน) — นี่คือพฤติกรรมที่ถูกต้อง
  assert.deepEqual(ttsCalls, ['ขอบคุณค่ะ', 'แล้วพบกันใหม่ค่ะ.'])
  assert.equal(result.totalSent, 2)
  assert.deepEqual(controlEvents, [{ type: 'end_call' }])
  // ณ จุดที่ runChunkedTurn resolve แล้ว แปลว่า queue ถูก drain หมดแล้วจริง — caller (audioStream.js)
  // ค่อย apply end-call hangup policy ทีหลังจากตรงนี้ได้อย่างปลอดภัย ไม่มีทางตัดเสียงกลางประโยคได้
})

test('12) abort กลางทาง (barge-in) → หยุดทันที ไม่ throw และไม่พูดอะไรเพิ่มเลย แม้มี chunk ที่เพิ่ง enqueue ไปก่อน abort เศษเสี้ยววินาที', async () => {
  const controller = new AbortController()
  state.claudeImpl = async function* () {
    yield 'Hello world. ' // ถูก chunk และ enqueue ไปแล้วก่อน abort บรรทัดถัดไป
    controller.abort()
    yield 'Should never reach here.'
  }
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: controller.signal, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  // "หยุดทันที" ต้องหมายถึงหยุดจริง — แม้ chunk แรกจะถูก enqueue ไปแล้วก่อน abort เสี้ยววินาที ก็ต้องไม่ถูกพูดออกไป
  // (ไม่งั้นจะเป็น "เสียงผี" ที่หลุดออกไปหลังลูกค้าขัดจังหวะแล้ว ผิดหลักการเดียวกับที่ B.5 ทั้งชุดถูกออกแบบมาป้องกัน)
  assert.deepEqual(ttsCalls, [])
  assert.equal(result.totalSent, 0)
})

test('13) Claude error (ไม่ใช่ abort) → propagate ออกจาก runChunkedTurn ให้ caller จัดการเอง พร้อม tag source=CLAUDE (C4c)', async () => {
  state.claudeImpl = async function* () { throw new Error('Claude boom') }
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await assert.rejects(
    () => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId }),
    (err) => { assert.match(err.message, /Claude boom/); assert.equal(err.source, 'CLAUDE'); return true }
  )
})

test('14) TTS error (ไม่ใช่ abort) → propagate ออกจาก runChunkedTurn ให้ caller จัดการเอง พร้อม tag source=TTS (C4c)', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { throw new Error('TTS boom') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await assert.rejects(
    () => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId }),
    (err) => { assert.match(err.message, /TTS boom/); assert.equal(err.source, 'TTS'); return true }
  )
})

test('15) socket ไม่ OPEN ตอน TTS คืน audio → ไม่ markAudioCommitted และไม่ส่งอะไรออก socket เลย', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  socket.readyState = 0 // ไม่ใช่ OPEN(1)
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(turnState.audioCommitted, false)
  assert.equal(result.totalSent, 0)
  assert.deepEqual(socket.sent, [])
})

test('result.fullText คือ raw concatenation ของทุก delta ทั้งเทิร์น (ไม่ trim/ไม่แทรกช่องว่างเอง) เผื่อ caller เก็บ history', async () => {
  const deltas = ['Hi \uD83D', '\uDE00 there. ', 'More text.']
  state.claudeImpl = fakeClaude(deltas)
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(result.fullText, deltas.join(''))
})

test('onFirstAudioSent callback ถูกเรียกครั้งเดียวตอน audio ก้อนแรกถูกส่งจริง', async () => {
  state.claudeImpl = fakeClaude(['One. ', 'Two.'])
  state.ttsImpl = async function* () { yield Buffer.from('a'); yield Buffer.from('b') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  let calls = 0
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, onFirstAudioSent: () => { calls++ } })
  assert.equal(calls, 1)
})

test('onFirstDelta ถูกเรียกครั้งเดียวตอน delta ที่ไม่ว่างก้อนแรกมาถึง (C4b — hook ให้ watchdog ภายนอกเคลียร์ timer)', async () => {
  state.claudeImpl = fakeClaude(['', 'First real. ', 'Second delta.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  let calls = 0
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, onFirstDelta: () => { calls++ } })
  assert.equal(calls, 1)
})

test('onFirstChunk ถูกเรียกครั้งเดียวตอน speech chunk แรกพร้อม ไม่ว่าจะเจอ boundary กลางลูปหรือจาก final flush (C4b — hook ให้ Watchdog B เคลียร์ timer)', async () => {
  state.claudeImpl = fakeClaude(['Hello world. ', 'Second sentence.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  let calls = 0
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, onFirstChunk: () => { calls++ } })
  assert.equal(calls, 1)
})

test('onFirstChunk ถูกเรียกจาก final-flush path ด้วย (ไม่มี boundary เจอเลยระหว่างทาง มีแต่ก้อนสุดท้ายตอน stream จบ)', async () => {
  state.claudeImpl = fakeClaude(['no punctuation at all in this text'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  let calls = 0
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, onFirstChunk: () => { calls++ } })
  assert.equal(calls, 1)
})

test('C4b Watchdog C: onFirstTtsRequest/onFirstTtsAudio ถูกเรียกครั้งเดียวต่อทั้งเทิร์น แม้มีหลาย speech chunk เข้า TTS ต่อกัน', async () => {
  // 3 ประโยค = 3 รอบ TTS request/audio ในเทิร์นเดียว — ฮุคของ Watchdog C ต้องยิงแค่ครั้งแรกของทั้งเทิร์นเท่านั้น
  // (ไม่ใช่ต่อ chunk) ไม่งั้น chunk #2/#3 จะไป rearm watchdog หลัง AUDIO_COMMITTED ไปแล้วอย่างผิดๆ
  state.claudeImpl = fakeClaude(['One. ', 'Two. ', 'Three.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  let requestCalls = 0
  let audioCalls = 0
  await runChunkedTurn({
    session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId,
    onFirstTtsRequest: () => { requestCalls++ },
    onFirstTtsAudio: () => { audioCalls++ },
  })
  assert.equal(requestCalls, 1, 'onFirstTtsRequest ต้องยิงแค่ครั้งเดียวทั้งเทิร์น ไม่ใช่ต่อ speech chunk')
  assert.equal(audioCalls, 1, 'onFirstTtsAudio ต้องยิงแค่ครั้งเดียวทั้งเทิร์น ไม่ใช่ต่อ speech chunk')
})

test('C4b Watchdog C: onFirstTtsRequest ยิงก่อน synthesizeSpeechStream ถูกเรียกจริงเสมอ (arm ก่อน request ไม่ใช่หลัง)', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  const order = []
  state.ttsImpl = async function* () { order.push('tts-called'); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({
    session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId,
    onFirstTtsRequest: () => order.push('armed'),
  })
  assert.deepEqual(order, ['armed', 'tts-called'])
})

// ---------------------------------------------------------------------------
// Checkpoint C3b — isCurrentGeneration() guards ที่ 5 boundary
// ---------------------------------------------------------------------------

test('C3b-1) Gen เก่าถูก invalidate ระหว่างรอ Claude delta ถัดไป → delta ที่มาหลัง bump ไม่ถูกประมวลผล และ defense-in-depth ที่ consumer กัน chunk ที่ enqueue ไปก่อนหน้าไม่ให้หลุดออก TTS', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  state.claudeImpl = async function* () {
    yield 'First. ' // ยัง current ตอนนี้ — chunker เจอ boundary และ enqueue ได้จริง (t3/t4 ติดถูกต้อง)
    bumpGeneration(callState) // จำลอง turn ใหม่เริ่มไปแล้วระหว่าง Gen เดิมยังไม่ทันจบ
    yield 'Should never be processed — gen เดิม stale แล้ว'
  }
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  // t3/t4 ติดค่าจริง เพราะ delta แรกถูกประมวลผลตอนยัง current อยู่ (guard ที่ boundary 1/2 ปล่อยผ่านถูกต้องแล้ว ณ ตอนนั้น)
  assert.equal(typeof turnMetrics.t3, 'number')
  assert.equal(typeof turnMetrics.t4, 'number')
  // แต่ chunk ที่ enqueue ไปแล้วนั้นต้องไม่มีวันถูกพูดออกไป — boundary 3 (ก่อน TTS request) ที่ consumer จับ staleness
  // ได้ทันเวลาก่อนเริ่มพูดจริง เป็น defense-in-depth ชั้นถัดไปที่ทำงานถูกต้อง แม้ producer เองไม่มีทางรู้ล่วงหน้า
  assert.deepEqual(ttsCalls, [])
  assert.equal(result.totalSent, 0)
})

test('C3b-2) chunker เจอ boundary พอดีตอน gen เพิ่งถูก invalidate → ไม่ enqueue ไม่ markOnce(t4)', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  state.claudeImpl = async function* () {
    yield 'partial tex' // ยังไม่ครบ boundary
    bumpGeneration(callState) // invalidate ก่อนตัวอักษรที่จะทำให้ครบ boundary มาถึง
    yield 't. done.'
  }
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, [])
  assert.equal(turnMetrics.t4, null)
  assert.equal(result.totalSent, 0)
})

test('C3b-3) chunk ถูก enqueue ไว้ก่อน gen ถูก invalidate (ระหว่างรอคิว) → ไม่เข้า TTS request เลย ไม่ markOnce(t5)', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  state.claudeImpl = fakeClaude(['Hello world. ']) // enqueue สำเร็จตั้งแต่ก่อน consumer เริ่มทำงานด้วยซ้ำ
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  bumpGeneration(callState) // invalidate generation ของเทิร์นนี้ไปแล้วตั้งแต่ก่อนเรียก runChunkedTurn เลย (จำลอง race ที่ generation ใหม่มาเร็วมาก)
  const socket = makeSocket()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, [])
  assert.equal(turnMetrics.t5, null)
  assert.equal(result.totalSent, 0)
})

test('C3b-4/5) ElevenLabs audio กลับมาหลัง gen ถูก invalidate ระหว่าง TTS กำลังสตรีม → ไม่ markOnce(t6/t7), ไม่ AUDIO_COMMITTED, ไม่ socket.send', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () {
    yield Buffer.from('a') // audio ก้อนแรกผ่านได้ตามปกติ (gen ยัง current)
    bumpGeneration(callState) // barge-in เกิดขึ้นกลาง TTS stream ของ chunk เดียวกัน
    yield Buffer.from('b') // ก้อนนี้ต้องถูกกันไว้ ไม่ส่งออก ไม่ mark อะไรเพิ่ม
  }
  const socket = makeSocket()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(result.totalSent, 1) // มีแค่ก้อนแรกก่อน bump เท่านั้นที่ผ่าน guard
  assert.equal(socket.sent.length, 1)
})

// ---------------------------------------------------------------------------
// Checkpoint C3c-2 — speakFixedText() ใช้สำหรับ follow-up question ตอน blocked end_call
// ---------------------------------------------------------------------------

test('C3c-1) end_call blocked ก่อนมี audio ใดๆ → follow-up พูดได้ปกติ ผ่าน GENERATING → TTS_PENDING → AUDIO_COMMITTED', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  assert.equal(turnState.phase, 'GENERATING')
  const socket = makeSocket()
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const result = await speakFixedText({ text: 'มีอะไรสอบถามเพิ่มเติมไหมคะ', signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, startingSentCount: 0 })
  assert.equal(result.sentCount, 1)
  assert.equal(turnState.phase, 'AUDIO_COMMITTED')
  assert.equal(turnState.audioCommitted, true)
  assert.equal(socket.sent.length, 1)
})

test('C3c-2) end_call blocked หลังมี audio ของข้อความหลักไปแล้ว (AUDIO_COMMITTED) → follow-up ต่อได้ แต่ state ต้องไม่ regress กลับ TTS_PENDING', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  markAudioCommitted(turnState) // จำลองว่าข้อความหลักของ Claude ถูกพูด/commit ไปแล้วก่อนหน้านี้ในเทิร์นเดียวกัน
  assert.equal(turnState.phase, 'AUDIO_COMMITTED')

  const socket = makeSocket()
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const result = await speakFixedText({ text: 'มีอะไรสอบถามเพิ่มเติมไหมคะ', signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, startingSentCount: 3 })
  assert.equal(result.sentCount, 1)
  assert.equal(turnState.phase, 'AUDIO_COMMITTED', 'ห้าม regress กลับไป TTS_PENDING เด็ดขาด')
  assert.equal(socket.sent.length, 1)
})

test('C3c-3) generation stale ไปแล้วก่อนเรียก speakFixedText เลย → ไม่เริ่ม TTS ใดๆ ทั้งสิ้น', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  bumpGeneration(callState) // stale ไปแล้วตั้งแต่ก่อนเรียก
  const socket = makeSocket()
  let ttsCalled = false
  state.ttsImpl = async function* () { ttsCalled = true; yield Buffer.from('a') }
  const result = await speakFixedText({ text: 'มีอะไรสอบถามเพิ่มเติมไหมคะ', signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, startingSentCount: 0 })
  assert.equal(result.sentCount, 0)
  assert.equal(ttsCalled, false)
  assert.equal(turnMetrics.t5, null)
  assert.deepEqual(socket.sent, [])
})

test('C3c-4) generation stale กลาง TTS ของ follow-up → audio ที่เหลือไม่ถึง Twilio', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const socket = makeSocket()
  state.ttsImpl = async function* () {
    yield Buffer.from('a') // ก้อนแรกผ่านได้ตามปกติ
    bumpGeneration(callState) // barge-in เกิดกลาง TTS ของ follow-up นี้เอง
    yield Buffer.from('b') // ต้องถูกกันไว้
  }
  const result = await speakFixedText({ text: 'มีอะไรสอบถามเพิ่มเติมไหมคะ', signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, startingSentCount: 0 })
  assert.equal(result.sentCount, 1)
  assert.equal(socket.sent.length, 1)
})

test('C3c-5) startingSentCount > 0 (follow-up ต่อจากข้อความหลักที่พูดไปแล้ว) → ไม่นับเป็น "first audio chunk" ซ้ำอีกรอบ', async () => {
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const socket = makeSocket()
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  let onFirstCalls = 0
  await speakFixedText({ text: 'follow up', signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId, startingSentCount: 5, onFirstAudioSent: () => { onFirstCalls++ } })
  assert.equal(onFirstCalls, 0, 'ไม่ใช่ audio chunk แรกของทั้งเทิร์น (มี 5 ก้อนก่อนหน้าจากข้อความหลักแล้ว) ไม่ควรเรียก onFirstAudioSent ซ้ำ')
})

// ---------------------------------------------------------------------------
// L1b — createChunkedProducer / adoptChunkedProducer (primitives ที่ runChunkedTurn ถูก decompose ออกมา)
// ---------------------------------------------------------------------------

test('L1b) createChunkedProducer: emitControl buffer end_call เข้า controlEvent แต่ไม่ forward เอง (quarantine ก่อนมี attachForwarder)', async () => {
  state.claudeImpl = async function* (session, signal, onControl) { onControl?.({ type: 'end_call' }) }
  const producer = createChunkedProducer({ session: {}, signal: null })
  await producer.done
  assert.deepEqual(producer.controlEvent, { type: 'end_call' })
})

test('L1b) attachForwarder: replay controlEvent ที่ buffer ไว้ก่อนหน้าทันทีตอน attach ถ้ายัง valid', async () => {
  state.claudeImpl = async function* (session, signal, onControl) { onControl?.({ type: 'end_call' }) }
  const producer = createChunkedProducer({ session: {}, signal: null })
  await producer.done
  const forwarded = []
  producer.attachForwarder(ev => forwarded.push(ev))
  assert.deepEqual(forwarded, [{ type: 'end_call' }])
})

test('L1b) attachForwarder: ห้าม replay ถ้า getIsValid() เป็น false ตอน attach แม้ controlEvent จะถูก buffer ไว้ตอนยัง valid ก็ตาม (control-buffered-while-valid-then-stale-before-adopt)', async () => {
  let valid = true
  state.claudeImpl = async function* (session, signal, onControl) { onControl?.({ type: 'end_call' }) } // ยิงตอน valid=true
  const producer = createChunkedProducer({ session: {}, signal: null, getIsValid: () => valid })
  await producer.done
  assert.deepEqual(producer.controlEvent, { type: 'end_call' }, 'ยังต้อง buffer ไว้เหมือนเดิม ไม่แตะ controlEvent เอง')
  valid = false // generation กลายเป็น stale ก่อน adoption
  const forwarded = []
  producer.attachForwarder(ev => forwarded.push(ev))
  assert.deepEqual(forwarded, [], 'ต้องไม่ replay end_call ที่ stale ออกไปเด็ดขาด')
})

test('L1b) attachForwarder: ห้าม attach เลยด้วยซ้ำถ้า signal.aborted ตอนเรียก (controlForwarder ต้องยังเป็น null)', async () => {
  const controller = new AbortController()
  controller.abort()
  state.claudeImpl = async function* () {}
  const producer = createChunkedProducer({ session: {}, signal: controller.signal })
  await producer.done
  let called = 0
  producer.attachForwarder(() => { called++ })
  // ยิง emitControl ไม่ได้อยู่แล้ว (guard ที่ emitControl เอง) แต่ยืนยันผ่าน controlEvent ว่าไม่มีอะไรถูก forward
  assert.equal(called, 0)
})

test('L1b) emitControl: ถูก guard ด้วย signal.aborted — end_call ที่มาหลัง abort ไม่ถูก buffer เลย (late control after abort)', async () => {
  const controller = new AbortController()
  state.claudeImpl = async function* (session, signal, onControl) {
    controller.abort()
    onControl?.({ type: 'end_call' }) // จำลอง late event ที่มาหลัง abort ไปแล้วเศษเสี้ยววินาที
  }
  const producer = createChunkedProducer({ session: {}, signal: controller.signal })
  await producer.done
  assert.equal(producer.controlEvent, null)
})

test('L1b) emitControl: ถูก guard ด้วย getIsValid() ด้วย (ไม่ใช่แค่ signal) — generation stale ระหว่างทางก็บล็อกเหมือนกัน', async () => {
  let valid = true
  state.claudeImpl = async function* (session, signal, onControl) {
    valid = false
    onControl?.({ type: 'end_call' })
  }
  const producer = createChunkedProducer({ session: {}, signal: null, getIsValid: () => valid })
  await producer.done
  assert.equal(producer.controlEvent, null)
})

test('L1b) onFirstDelta/onFirstChunk: replay ทันทีถ้าเหตุการณ์เกิดไปแล้วก่อน subscribe (ใช้ตอน adopt speculation ที่มี progress อยู่แล้ว)', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  const producer = createChunkedProducer({ session: {}, signal: null })
  await producer.done
  let deltaCalls = 0, chunkCalls = 0
  producer.onFirstDelta(() => { deltaCalls++ })
  producer.onFirstChunk(() => { chunkCalls++ })
  assert.equal(deltaCalls, 1)
  assert.equal(chunkCalls, 1)
})

test('L1b) waitForFirstProgress: resolve ทันทีถ้าไม่มี delta เลยแต่ producer จบแล้ว (empty response case)', async () => {
  state.claudeImpl = async function* () {} // จบทันทีไม่มี delta เลย
  const producer = createChunkedProducer({ session: {}, signal: null })
  await producer.done
  let resolved = false
  producer.waitForFirstProgress().then(() => { resolved = true })
  await Promise.resolve()
  assert.equal(resolved, true, 'producerDone=true ต้องทำให้ waitForFirstProgress resolve ได้แม้ไม่เคยมี delta เลย')
})

test('L1b) waitForFirstProgress: ไม่ resolve จนกว่าจะมี delta แรกจริง (ไม่ใช่ chunk) — ตื่นเร็วกว่า waitForWork() เสมอ', async () => {
  let releaseDelta
  const gate = new Promise(resolve => { releaseDelta = resolve })
  state.claudeImpl = async function* () {
    await gate
    yield 'delta arrived but no chunk boundary yet'
  }
  const producer = createChunkedProducer({ session: {}, signal: null })
  let progressResolved = false
  producer.waitForFirstProgress().then(() => { progressResolved = true })
  await new Promise(r => setTimeout(r, 10))
  assert.equal(progressResolved, false, 'ยังไม่มี delta เลย ต้องไม่ resolve')
  releaseDelta()
  await new Promise(r => setTimeout(r, 10))
  assert.equal(progressResolved, true, 'delta แรกมาแล้วต้อง resolve ทันที แม้จะยังไม่มี chunk boundary ก็ตาม')
})

test('L1b) adoptChunkedProducer: attach ให้ producer ที่ buffer chunk ไว้แล้วก่อน adopt → consumer drain queue เดิมได้ปกติ (BUFFERED_HIT/READY_HIT scenario)', async () => {
  state.claudeImpl = fakeClaude(['Ready before adopt. '])
  const producer = createChunkedProducer({ session: {}, signal: null })
  await producer.done // producer จบไปแล้วทั้งก้อนก่อน adopt เลย (READY_HIT)
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const result = await adoptChunkedProducer({ producer, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.deepEqual(ttsCalls, ['Ready before adopt.'])
  assert.equal(result.totalSent, 1)
})

// ---------------------------------------------------------------------------
// L1c2a — previous_text continuity + per-chunk telemetry
// ---------------------------------------------------------------------------

test('L1c2a) chunk แรกของเทิร์น → previousText เป็น null (ไม่มี predecessor จริง)', async () => {
  state.claudeImpl = fakeClaude(['Hello. ', 'World.'])
  const previousTexts = []
  state.ttsImpl = async function* (text, voiceId, signal, previousText) { previousTexts.push(previousText); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(previousTexts[0], null)
})

test('L1c2a) chunk ที่สอง → previousText ตรงกับข้อความ chunk แรกที่เพิ่งพูดจบไปเป๊ะ', async () => {
  state.claudeImpl = fakeClaude(['Hello. ', 'World.'])
  const previousTexts = []
  state.ttsImpl = async function* (text, voiceId, signal, previousText) { previousTexts.push(previousText); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(previousTexts[1], 'Hello.')
})

test('L1c2a) chunk ที่ไม่เคยถูกพูดจริง (sentCount=0 เพราะ socket ปิดกลางทาง) → ไม่กลายเป็น previousText ของ chunk ถัดไป', async () => {
  state.claudeImpl = fakeClaude(['First. ', 'Second.'])
  let call = 0
  const previousTexts = []
  const socket = makeSocket()
  state.ttsImpl = async function* (text, voiceId, signal, previousText) {
    call++
    previousTexts.push(previousText)
    if (call === 1) { socket.readyState = 0; yield Buffer.from('a'); return } // chunk แรก "ได้ audio" จาก provider แต่ไม่เคยถูกส่งจริง (socket ปิดพอดี)
    socket.readyState = 1
    yield Buffer.from('b')
  }
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(previousTexts[1], null, 'chunk แรกไม่เคยถูกส่งจริง (socket ปิด) ต้องไม่กลายเป็น predecessor ของ chunk ที่สอง (ยังเป็นค่าเริ่มต้น null เหมือนไม่มี predecessor)')
})

test('L1c2a) onChunkTelemetry (ผ่าน [ChunkMetrics] log): requestStartedAt <= firstAudioAt <= requestDoneAt เรียงลำดับถูกต้อง', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { await new Promise(r => setTimeout(r, 5)); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); }
  try {
    await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  } finally {
    console.log = originalLog
  }
  const line = logs.find(l => l.includes('[ChunkMetrics]'))
  assert.ok(line, 'ต้องมี [ChunkMetrics] log')
  const record = JSON.parse(line.slice(line.indexOf('{')))
  assert.equal(record.chunkIndex, 0)
  assert.ok(record.ttfbMs >= 0, 'ttfbMs ต้อง >= 0')
  assert.ok(record.requestDurationMs >= record.ttfbMs, 'requestDurationMs ต้อง >= ttfbMs')
  assert.equal(record.providerGapMs, null, 'chunk แรกไม่มี predecessor ต้อง providerGapMs=null')
  assert.equal(record.sendGapMs, null)
  assert.equal(record.sentCount, 1)
})

test('L1c2a) [ChunkMetrics] ยิงแม้ TTS error ก่อนเสียงแรก (finally เสมอ) — timestamp ที่ไม่เกิดขึ้นจริงเป็น null ไม่ fabricate', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { throw new Error('TTS boom') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')) }
  try {
    await assert.rejects(() => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId }))
  } finally {
    console.log = originalLog
  }
  const line = logs.find(l => l.includes('[ChunkMetrics]'))
  assert.ok(line, '[ChunkMetrics] ต้องยิงแม้ error ก่อนเสียงแรก')
  const record = JSON.parse(line.slice(line.indexOf('{')))
  assert.equal(record.ttfbMs, null, 'ไม่มี audio มาถึงเลยก่อน error — ttfbMs ต้องเป็น null ไม่ใช่ 0 ปลอมๆ')
  assert.equal(record.sentCount, 0)
})

test('L1c2a) providerGapMs/sendGapMs มีค่าจริงตั้งแต่ chunk ที่สองเป็นต้นไป', async () => {
  state.claudeImpl = fakeClaude(['One thing. ', 'Two thing.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')) }
  try {
    await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  } finally {
    console.log = originalLog
  }
  const records = logs.filter(l => l.includes('[ChunkMetrics]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
  assert.equal(records.length, 2)
  assert.equal(records[0].providerGapMs, null)
  assert.equal(records[0].sendGapMs, null)
  assert.equal(typeof records[1].providerGapMs, 'number')
  assert.equal(typeof records[1].sendGapMs, 'number')
})

test('stale callback ต้องไม่มีสิทธิ์แตะ metrics/state ของ generation อื่นเลย แม้จะเป็นแค่การ mark ไม่ใช่ side effect ที่มองเห็นได้จากภายนอก', async () => {
  // จำลอง Gen 12 (เทิร์นนี้) audio กลับมาหลัง Gen 13 เริ่มไปแล้ว — ยืนยันว่า t6/t7/audioCommitted ของ Gen 12
  // ต้องค้างที่ null/false ตลอดไป ไม่ใช่แค่ไม่ส่ง socket.send() แต่ยังไม่ถูก mark เป็นค่าอะไรเลยด้วย
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () {
    bumpGeneration(callState) // ก่อน audio ก้อนแรกจะมาถึงด้วยซ้ำ — เทิร์นถัดไปเริ่มไปแล้วจริงๆ
    yield Buffer.from('a')
  }
  const socket = makeSocket()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId })
  assert.equal(turnMetrics.t6, null)
  assert.equal(turnMetrics.t7, null)
  assert.equal(turnState.audioCommitted, false)
  assert.deepEqual(socket.sent, [])
})
