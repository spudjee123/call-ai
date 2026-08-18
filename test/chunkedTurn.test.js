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

test('13) Claude error (ไม่ใช่ abort) → propagate ออกจาก runChunkedTurn ให้ caller จัดการเอง', async () => {
  state.claudeImpl = async function* () { throw new Error('Claude boom') }
  state.ttsImpl = async function* () { yield Buffer.from('a') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await assert.rejects(
    () => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId }),
    /Claude boom/
  )
})

test('14) TTS error (ไม่ใช่ abort) → propagate ออกจาก runChunkedTurn ให้ caller จัดการเอง', async () => {
  state.claudeImpl = fakeClaude(['Hello world. '])
  state.ttsImpl = async function* () { throw new Error('TTS boom') }
  const socket = makeSocket()
  const { turnMetrics, turnState, callState, generationId } = makeMetricsAndState()
  await assert.rejects(
    () => runChunkedTurn({ session: {}, signal: null, socket, streamSid: 'SS1', voiceId: 'v1', turnMetrics, turnState, callState, generationId }),
    /TTS boom/
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
