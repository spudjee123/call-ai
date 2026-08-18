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
    synthesizeSpeechStream: (text, voiceId, signal) => state.ttsImpl(text, voiceId, signal),
  },
}

const { runChunkedTurn } = require('../src/websocket/chunkedTurn')
const { createTurnMetrics } = require('../src/utils/turnMetrics')
const { createTurnState } = require('../src/utils/turnState')

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

function makeMetricsAndState(overrides = {}) {
  const turnMetrics = createTurnMetrics({ callSid: 'CA1', generationId: 1, path: 'chunked', rolloutBucket: 0, rolloutPercent: 100, ...overrides })
  const turnState = createTurnState(1)
  return { turnMetrics, turnState }
}

test('1) text delta หลายก้อน → speech chunk ถูกเรียงลำดับถูกต้องตอนส่งเข้า TTS', async () => {
  state.claudeImpl = fakeClaude(['Hello wor', 'ld. Nice to meet you.'])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.deepEqual(ttsCalls, ['Hello world.', 'Nice to meet you.'])
})

test('2) unicode (surrogate pair) แตกกลาง delta คนละก้อน → buffer ต่อกันได้ข้อความสมบูรณ์', async () => {
  const deltas = ['Hi \uD83D', '\uDE00 bye there.']
  state.claudeImpl = fakeClaude(deltas)
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.equal(ttsCalls[0], 'Hi 😀 bye there.')
})

test('3) หลาย speech chunks (3 ประโยค) → TTS ถูกเรียกตามลำดับเดียวกับที่ปรากฏใน stream', async () => {
  state.claudeImpl = fakeClaude(['One thing. ', 'Two thing. ', 'Three thing.'])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.deepEqual(ttsCalls, ['One thing.', 'Two thing.', 'Three thing.'])
})

test('4) Claude stream จบไม่มี punctuation ปิดท้าย → remainder ที่เหลือถูก flush ไปพูดด้วย', async () => {
  state.claudeImpl = fakeClaude(['just talking along without any ending mark'])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.deepEqual(ttsCalls, ['just talking along without any ending mark'])
})

test('5) empty delta ปนมาระหว่างทาง → ไม่ทำให้เกิดการเรียก TTS ว่างเปล่า', async () => {
  state.claudeImpl = fakeClaude(['', 'Hello there friend.', ''])
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.deepEqual(ttsCalls, ['Hello there friend.'])
  assert.ok(!ttsCalls.some(t => t === ''))
})

test('6) first non-empty delta → t3 ถูก set (ผ่าน markOnce ที่ผูกไว้กับจุดรับ delta จริง)', async () => {
  state.claudeImpl = fakeClaude(['', 'First real delta. ', 'Second delta.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.equal(typeof turnMetrics.t3, 'number')
})

test('7) first speech chunk ที่ chunker emit → t4 ถูก set', async () => {
  state.claudeImpl = fakeClaude(['Hello world. More text after.'])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.equal(typeof turnMetrics.t4, 'number')
})

test('8-10) t5/t6/t7 set ตั้งแต่ครั้งแรกและไม่ขยับตาม chunk/audio ก้อนถัดๆ ไป, AUDIO_COMMITTED ติดหลังส่งจริง', async () => {
  state.claudeImpl = fakeClaude(['One. ', 'Two. ', 'Three.'])
  const { turnMetrics, turnState } = makeMetricsAndState()
  const snapshots = []
  state.ttsImpl = async function* () {
    yield Buffer.from('a')
    snapshots.push({ t6: turnMetrics.t6, t7: turnMetrics.t7, audioCommitted: turnState.audioCommitted })
    yield Buffer.from('b')
    snapshots.push({ t6: turnMetrics.t6, t7: turnMetrics.t7, audioCommitted: turnState.audioCommitted })
  }
  const socket = makeSocket()
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })

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
  const { turnMetrics, turnState } = makeMetricsAndState()
  const controlEvents = []
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, onControl: (c) => controlEvents.push(c) })
  assert.deepEqual(controlEvents, [{ type: 'end_call' }])
  assert.deepEqual(ttsCalls, [])
  assert.equal(turnMetrics.t3, null)
  assert.equal(turnMetrics.t5, null)
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
  const { turnMetrics, turnState } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: controller.signal, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  // "หยุดทันที" ต้องหมายถึงหยุดจริง — แม้ chunk แรกจะถูก enqueue ไปแล้วก่อน abort เสี้ยววินาที ก็ต้องไม่ถูกพูดออกไป
  // (ไม่งั้นจะเป็น "เสียงผี" ที่หลุดออกไปหลังลูกค้าขัดจังหวะแล้ว ผิดหลักการเดียวกับที่ B.5 ทั้งชุดถูกออกแบบมาป้องกัน)
  assert.deepEqual(ttsCalls, [])
  assert.equal(result.totalSent, 0)
})

test('13) Claude error (ไม่ใช่ abort) → propagate ออกจาก runChunkedTurn ให้ caller จัดการเอง', async () => {
  state.claudeImpl = async function* () { throw new Error('Claude boom') }
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await assert.rejects(
    () => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState }),
    /Claude boom/
  )
})

test('14) TTS error (ไม่ใช่ abort) → propagate ออกจาก runChunkedTurn ให้ caller จัดการเอง', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { throw new Error('TTS boom') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  await assert.rejects(
    () => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState }),
    /TTS boom/
  )
})

test('15) socket ไม่ OPEN ตอน TTS คืน audio → ไม่ markAudioCommitted และไม่ส่งอะไรออก socket เลย', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  socket.readyState = 0 // ไม่ใช่ OPEN(1)
  const { turnMetrics, turnState } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.equal(turnState.audioCommitted, false)
  assert.equal(result.totalSent, 0)
  assert.deepEqual(socket.sent, [])
})

test('result.fullText คือ raw concatenation ของทุก delta ทั้งเทิร์น (ไม่ trim/ไม่แทรกช่องว่างเอง) เผื่อ caller เก็บ history', async () => {
  const deltas = ['Hi \uD83D', '\uDE00 there. ', 'More text.']
  state.claudeImpl = fakeClaude(deltas)
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  const result = await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState })
  assert.equal(result.fullText, deltas.join(''))
})

test('onFirstAudioSent callback ถูกเรียกครั้งเดียวตอน audio ก้อนแรกถูกส่งจริง', async () => {
  state.claudeImpl = fakeClaude(['One. ', 'Two.'])
  state.ttsImpl = async function* () { yield Buffer.from('a'); yield Buffer.from('b') }
  const socket = makeSocket()
  const { turnMetrics, turnState } = makeMetricsAndState()
  let calls = 0
  await runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, onFirstAudioSent: () => { calls++ } })
  assert.equal(calls, 1)
})
