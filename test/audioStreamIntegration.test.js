// Checkpoint C6a — integration tests ที่ขับเคลื่อน registerWebSocket() จริงผ่าน _audioStreamHarness.js
// ปิด known gap ที่สะสมมาตั้งแต่ C0: wiring-level contract ที่ unit test ของ utilities อย่างเดียวพิสูจน์ไม่ได้
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const callSessions = require('../src/utils/callSessions')
const harness = require('./_audioStreamHarness')

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

function makeSession(overrides = {}) {
  return {
    name: 'ทดสอบ',
    campaign: { voice_id: 'voice1', script: 'ระบบทดสอบ' },
    messages: [],
    ...overrides,
  }
}

let callSidCounter = 0
function nextCallSid() { callSidCounter++; return `CA_TEST_${callSidCounter}` }

harness.ensureStubbed() // ต้อง stub + require audioStream.js ก่อนเทสแรกจะแตะ state ได้

beforeEach(() => {
  const state = harness.getState()
  state.claudeStreamImpl = async function* () { yield 'default legacy response.' }
  state.claudeStreamChunkedImpl = async function* () {}
  state.ttsImpl = async function* () { yield Buffer.from('audio') }
  state.rolloutPercent = 0
})

test('smoke: connect + start + final transcript ที่ rollout 0% → ได้ media event ส่งออกจริงจาก legacy path', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 0

  let chunkedCalled = false
  state.claudeStreamChunkedImpl = async function* () { chunkedCalled = true }
  state.claudeStreamImpl = async function* () { yield 'ยินดีต้อนรับค่ะ.' }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('สวัสดีค่ะ')

  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'ต้องมี media event ถูกส่งจริงจาก legacy TTS path')
  assert.equal(chunkedCalled, false, 'rollout 0% ต้องไม่เรียก askClaudeStreamChunked เลย')

  const markEvents = socket.sent.filter(e => e.event === 'mark')
  assert.ok(markEvents.some(e => e.mark?.name === 'ai_done'), 'ต้องมี mark ai_done ปิดท้ายเทิร์นด้วย')

  harness.disconnect(socket)
})

test('1) rollout 0% → legacy path only: askClaudeStreamChunked ไม่ถูกเรียกแม้แต่ครั้งเดียวตลอดเทิร์น', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 0
  let chunkedCalls = 0
  let legacyCalls = 0
  state.claudeStreamChunkedImpl = async function* () { chunkedCalls++ }
  state.claudeStreamImpl = async function* () { legacyCalls++; yield 'ตอบจาก legacy.' }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('ทดสอบ')

  assert.equal(chunkedCalls, 0)
  assert.equal(legacyCalls, 1)
  harness.disconnect(socket)
})

test('2) rollout 100% → chunked path only: askClaudeStream (legacy) ไม่ถูกเรียกแม้แต่ครั้งเดียวตลอดเทิร์น', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 100
  let chunkedCalls = 0
  let legacyCalls = 0
  state.claudeStreamChunkedImpl = async function* () { chunkedCalls++; yield 'ตอบจาก chunked path.' }
  state.claudeStreamImpl = async function* () { legacyCalls++; yield 'ไม่ควรถูกเรียก.' }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('ทดสอบ')

  assert.equal(chunkedCalls, 1)
  assert.equal(legacyCalls, 0)
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'ต้องมี media event ถูกส่งจริงจาก chunked TTS path')
  harness.disconnect(socket)
})

test('C6c follow-up: t2 (Claude request sent) ต้องถูก mark สำหรับ chunked turn ด้วย ไม่ใช่ null เหมือนที่เจอจาก production trace จริง', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 100
  state.claudeStreamChunkedImpl = async function* () { yield 'ตอบจาก chunked path.' }

  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }
  let metrics
  try {
    const socket = harness.connect({ callSid })
    harness.sendStart(socket)
    await harness.sendFinalTranscript('ทดสอบ')
    harness.disconnect(socket)
    const metricsLine = logs.find(l => l.includes('[Metrics]'))
    metrics = JSON.parse(metricsLine.slice(metricsLine.indexOf('{')))
  } finally {
    console.log = originalLog
  }

  assert.equal(metrics.path, 'chunked')
  assert.equal(typeof metrics.t2, 'number', 't2 ต้องมีค่าจริง ไม่ใช่ null — เดิมเป็นช่องโหว่ที่ทำให้ claudeTTFT/requestToAudio วัดไม่ได้เลยสำหรับ chunked turn')
  assert.ok(metrics.t1 <= metrics.t2, 't2 ต้องเกิดหลัง t1 เสมอ (ไม่ใช่ก่อนหน้า)')
})

test('7) rollout ถูก freeze ตอนเริ่มสาย — เปลี่ยนค่าใน config กลางสาย ไม่กระทบเทิร์นถัดไปของสายเดิม', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 0 // ตอนเริ่มสาย = 0%
  let chunkedCalls = 0
  let legacyCalls = 0
  state.claudeStreamChunkedImpl = async function* () { chunkedCalls++ }
  state.claudeStreamImpl = async function* () { legacyCalls++; yield 'legacy turn 1.' }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('เทิร์นแรก')
  assert.equal(legacyCalls, 1)
  assert.equal(chunkedCalls, 0)

  // จำลอง Sheets ถูกแก้เป็น 100% "ระหว่าง" สายนี้ยังไม่วางสาย — เหมือน background poll ของ rolloutConfig อัปเดต cache ใหม่
  state.rolloutPercent = 100
  state.claudeStreamImpl = async function* () { legacyCalls++; yield 'legacy turn 2.' }
  await harness.sendFinalTranscript('เทิร์นสอง')

  assert.equal(legacyCalls, 2, 'เทิร์นที่สองของสายเดิมต้องยังใช้ legacy ต่อ ไม่สลับไป chunked แม้ config เปลี่ยนไปแล้วก็ตาม')
  assert.equal(chunkedCalls, 0)
  harness.disconnect(socket)
})

test('8) stop → callState.ended ทำให้ chunked attempt ที่ยังค้างอยู่ไม่มีสิทธิ์ส่งเสียงเพิ่มหลัง stop', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 100

  let resumeGenerator
  const paused = new Promise(resolve => { resumeGenerator = resolve })
  state.claudeStreamChunkedImpl = async function* () {
    yield 'First chunk before pause. '
    await paused // ค้างตรงนี้จนกว่าเทสจะปล่อยต่อ — จำลอง delta ที่มาช้า
    yield 'Should never be spoken after stop.'
  }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)

  const turnPromise = harness.sendFinalTranscript('ทดสอบ')
  await delay(30) // ให้ turn เริ่มประมวลผลจนถึงจุด pause ในตัว generator

  const sentBeforeStop = socket.sent.filter(e => e.event === 'media').length
  assert.ok(sentBeforeStop > 0, 'ก่อน stop ควรมีเสียงถูกส่งไปแล้วอย่างน้อยจากก้อนแรก (พิสูจน์ว่า turn เริ่มประมวลผลจริง ไม่ใช่ยังไม่เริ่ม)')

  harness.sendStop(socket) // เรียก endCall(callState) ภายใน
  resumeGenerator() // ปล่อยให้ generator ทำงานต่อ (จำลอง delta ที่มาช้าหลัง stop)
  await turnPromise

  const sentAfterStop = socket.sent.filter(e => e.event === 'media').length
  assert.equal(sentAfterStop, sentBeforeStop, 'ไม่ควรมี media event เพิ่มขึ้นเลยหลังจาก stop ถูกเรียกไปแล้ว')
})

test('3) barge-in หลัง turn จบ (isSpeaking ยังรอ mark, sttProcessing คืนเป็น false แล้ว) → ส่ง clear event แล้วประมวลผลเทิร์นใหม่ได้ปกติ', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 100
  state.claudeStreamChunkedImpl = async function* () { yield 'First response.' }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  // จบสมบูรณ์ — sttProcessing กลับเป็น false ใน finally แต่ isSpeaking ยัง true (ไม่มี 'mark' event จริงส่งกลับมาในเทส)
  await harness.sendFinalTranscript('ประโยคแรก')

  let secondTurnCalls = 0
  state.claudeStreamChunkedImpl = async function* () { secondTurnCalls++; yield 'Second response.' }
  await harness.sendFinalTranscript('ขอโทษนะคะ หนูพูดแทรกเข้ามา') // ยาวพอไม่ถูกมองเป็น echo/fragment สั้น

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'barge-in ต้องส่ง clear event ไปที่ Twilio ก่อนเริ่มเทิร์นใหม่')
  assert.equal(secondTurnCalls, 1, 'เทิร์นใหม่หลัง barge-in ต้องประมวลผลได้ปกติ ไม่ค้าง')

  harness.disconnect(socket)
})

test('5) AUDIO_COMMITTED แล้ว error เกิดที่ TTS chunk ถัดไป → claimFallback ปฏิเสธ ไม่มี fallback เกิดขึ้นเลย', async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 100
  state.claudeStreamChunkedImpl = async function* () { yield 'First sentence. Second sentence.' }
  let legacyCalls = 0
  state.claudeStreamImpl = async function* () { legacyCalls++; yield 'ไม่ควรถูกเรียก.' }

  let ttsCallCount = 0
  state.ttsImpl = async function* () {
    ttsCallCount++
    if (ttsCallCount === 1) { yield Buffer.from('a'); return } // chunk แรกสำเร็จ → commit เสียงไปแล้ว
    throw new Error('TTS boom on chunk 2') // chunk สองพังหลัง commit ไปแล้ว
  }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('ทดสอบ')

  assert.equal(legacyCalls, 0, 'ห้าม fallback หลัง AUDIO_COMMITTED แล้ว แม้ chunk ถัดไปจะพังก็ตาม')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'chunk แรกต้องยังถูกส่งออกไปจริงก่อนที่ chunk สองจะพัง')
  harness.disconnect(socket)
})

test('4) Claude first-delta timeout จริง (ไม่มี delta มาเลย) → fallback ไปใช้ legacy Claude/TTS แล้วพูดได้จริง', { timeout: 10000 }, async () => {
  const callSid = nextCallSid()
  callSessions.set(callSid, makeSession())
  const state = harness.getState()
  state.rolloutPercent = 100
  // ไม่ yield อะไรเลย ไม่ resolve เลย — จำลอง Claude ไม่ตอบอะไรกลับมาจนกว่า CLAUDE_FIRST_DELTA_TIMEOUT_MS (3000ms) จะหมด
  state.claudeStreamChunkedImpl = async function* () { await new Promise(() => {}) }
  let legacyCalls = 0
  state.claudeStreamImpl = async function* () { legacyCalls++; yield 'คำตอบจาก fallback.' }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('ทดสอบ') // จะใช้เวลาจริงประมาณ 3 วินาทีกว่า watchdog จะ timeout แล้ว fallback

  assert.equal(legacyCalls, 1, 'ต้อง fallback ไปเรียก legacy askClaudeStream หลัง watchdog timeout')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'fallback ต้องพูดออกไปจริงผ่าน legacy TTS')
  harness.disconnect(socket)
})

test('6a) end_call ที่ shouldBlockEndCall=true (ลูกค้าสนใจ, AI ยังไม่ถามเพิ่มเติม) → follow-up ถูกพูด ไม่ hangup', async () => {
  const callSid = nextCallSid()
  const session = makeSession()
  callSessions.set(callSid, session)
  const state = harness.getState()
  state.rolloutPercent = 100
  state.claudeStreamChunkedImpl = async function* (s, signal, onControl) {
    yield 'ยินดีค่ะ.'
    onControl?.({ type: 'end_call' })
  }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('สนใจค่ะ') // มี "สนใจ" + ไม่มี negation + AI ไม่พูด "เพิ่มเติม" → shouldBlockEndCall=true

  assert.equal(session.hangupReason, undefined, 'ไม่ควรถูกตั้งค่า hangupReason เลยถ้า end_call ถูก block')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length >= 2, 'ควรมีอย่างน้อย 2 รอบเสียง (คำตอบหลัก + follow-up question)')
  harness.disconnect(socket)
})

test('6b) end_call ที่ shouldBlockEndCall=false (ไม่มีสัญญาณสนใจ) → hangup จริง (hangupReason ถูกตั้งค่าทันที)', async () => {
  const callSid = nextCallSid()
  const session = makeSession()
  callSessions.set(callSid, session)
  const state = harness.getState()
  state.rolloutPercent = 100
  state.claudeStreamChunkedImpl = async function* (s, signal, onControl) {
    yield 'ขอบคุณค่ะ.'
    onControl?.({ type: 'end_call' })
  }

  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await harness.sendFinalTranscript('ไม่สนใจค่ะ') // ไม่มี hasInterest → shouldBlockEndCall คืน false ทันที ไม่บล็อก

  assert.equal(session.hangupReason, 'ai_ended', 'end_call ที่ไม่ถูก block ต้องตั้ง hangupReason ทันที (ก่อนถึง socket.close() timer จริง)')
  harness.disconnect(socket)
})
