// Checkpoint C6a — integration tests ที่ขับเคลื่อน registerWebSocket() จริงผ่าน _audioStreamHarness.js
// ปิด known gap ที่สะสมมาตั้งแต่ C0: wiring-level contract ที่ unit test ของ utilities อย่างเดียวพิสูจน์ไม่ได้
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const callSessions = require('../src/utils/callSessions')
const harness = require('./_audioStreamHarness')

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

// C4c follow-up (commit-aware fallback watchdog) — จับ [Metrics] log บรรทัดเดียวของเทิร์นนั้น แล้ว parse เป็น
// object ตรงๆ ใช้แทนการ parse ข้อความ console.error ของ [Fallback] เพราะ fallbackOutcome อยู่ใน turnMetrics
// อยู่แล้ว (ดู audioStream.js บรรทัดที่ push [Metrics] log ในส่วนท้ายที่ legacy/chunked ทั้งสอง branch มาบรรจบกัน)
async function captureMetrics(fn) {
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }
  try {
    await fn()
  } finally {
    console.log = originalLog
  }
  const metricsLine = logs.find(l => l.includes('[Metrics]'))
  return metricsLine ? JSON.parse(metricsLine.slice(metricsLine.indexOf('{'))) : null
}

// greeting เล่นเสมอ 300ms หลัง 'start' (setTimeout(playGreeting, 300) ใน audioStream.js) ไม่มี flag ปิดได้จาก
// ข้างนอก — เทสที่ต้อง assert จำนวน media event ของเทิร์นทดสอบแบบเป๊ะๆ (ไม่ใช่แค่ > 0) ต้องรอให้ greeting จบไปก่อน
// แล้วเคลียร์ log ทิ้ง ไม่งั้น greeting's เสียงจะปนเข้ามานับรวมด้วยโดยไม่ตั้งใจ (เจอจริงตอนเขียนเทสชุด fallback watchdog นี้)
//
// ต้องรอผ่าน "unlock" ของ greeting ด้วย ไม่ใช่แค่ตอนมันส่ง chunk เสร็จ — greeting ตั้ง isSpeaking=true และปลดล็อก
// อีกที (fallback-unlock timer) หลังจากนั้นอีก playbackMs = sent*20+1500ms (1 chunk = 1520ms) ถ้าเทสส่ง final
// transcript ก่อนปลดล็อก จะโดน "Short fragment during AI speech — ignoring echo" กลืนทิ้งเงียบๆ (transcript สั้น)
// หรือถูกมองเป็น barge-in ของ greeting โดยไม่ตั้งใจ (transcript ยาว) — ไม่ใช่เทิร์นแรกปกติที่เทสต้องการ
//
// rolloutPercent ต้องตั้ง "ก่อน" sendStart เสมอ เพราะ rollout freeze ครั้งเดียวตอน 'start' (C0 design, ดู
// test 7 ที่พิสูจน์ invariant นี้ไว้แล้ว) — ตั้งทีหลังจะไม่มีผลกับสายนี้อีกเลยตลอดสาย
async function connectPastGreeting(callSid, { rolloutPercent = 100, sessionOverrides = {} } = {}) {
  const state = harness.getState()
  state.rolloutPercent = rolloutPercent
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')], ...sessionOverrides })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(2000) // 300ms (greeting timer) + 1520ms (unlock ของ greeting เอง) + margin
  socket.sent.length = 0
  return { socket, session, state }
}

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

// ===== C4c follow-up — commit-aware fallback watchdog (production incident 2026-08-19) =====
// จำลอง timeline เดียวกับสาย production จริงที่เจอบั๊ก (barge-in retry call, CAae2f6baed62fed9d4dcc7ff23f199725):
// chunked พัง → fallback เริ่ม → ElevenLabs ช้า/ค้างระหว่างทาง → terminal watchdog เดิม abort กลางประโยคทั้งที่
// ลูกค้าได้ยินไปแล้วบางส่วน (totalSent/fullText หายไปเป็น 0/'') ทดสอบชุดนี้ (9-14) ครอบ DoD A-F ที่ล็อกไว้ก่อน patch

test('9) fallback pre-commit timeout (ไม่มีเสียงคอมมิตเลย) → FALLBACK_TIMEOUT, totalSent=0, ไม่มี media event ส่งออกเลย [DoD A]', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') } // ล้มทันที → เข้า fallback ไม่ต้องรอ watchdog 3 วง
  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก fallback ที่จะไม่ได้พูดเลย.' }
  state.ttsImpl = async function* () { await new Promise(() => {}) } // ไม่ yield อะไรเลย ไม่ resolve เลย — ElevenLabs ไม่ตอบกลับมาก่อน commit

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ')) // ใช้เวลาจริงประมาณ 8 วินาทีกว่า precommit watchdog จะ timeout

  assert.equal(metrics.fallbackOutcome, 'FALLBACK_TIMEOUT')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 0, 'ไม่ควรมี media event ใดๆ เลยถ้าไม่เคย commit')
  harness.disconnect(socket)
})

test('10) fallback commit เสียงก้อนแรกใกล้ precommit deadline เดิมมาก (~7.9s) → ห้ามถูก abort ที่ 8.0s [DoD F: stale-timer boundary]', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') }
  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก fallback หลังคอมมิตใกล้ deadline.' }
  state.ttsImpl = async function* () {
    await delay(7900) // คอมมิตก้อนแรกใกล้ FALLBACK_PRECOMMIT_TIMEOUT_MS (8000ms) มากที่สุดเท่าที่ยังปลอดภัยสำหรับเทส
    yield Buffer.from('chunk1')
  }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.fallbackOutcome, 'SPOKEN', 'precommit timer เก่าต้องไม่ยิง abort หลัง commit ไปแล้ว แม้จะใกล้ deadline เดิมมากแค่ไหนก็ตาม')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0)
  harness.disconnect(socket)
})

test('11) fallback regression: commit ที่ 5.1s แล้วพูดต่อผ่าน 8.0s เดิมไปได้ปกติ (จำลอง production incident 2026-08-19 เป๊ะ) [DoD B]', { timeout: 20000 }, async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') } // แทนที่ TTS_FIRST_AUDIO_TIMEOUT watchdog ตัวจริงของโปรดักชัน — error ก็เข้า fallback เหมือนกัน ทำเทสเร็วขึ้น
  state.claudeStreamImpl = async function* () { yield 'คำตอบยาวที่พูดคร่อมเวลา 8 วินาทีเดิม.' }
  state.ttsImpl = async function* () {
    await delay(5100)
    yield Buffer.from('chunk1') // คอมมิต — precommit timer ถูกแทนที่ด้วย idle timer จากตรงนี้
    await delay(3500) // เวลารวมผ่านไป ~8.6s แล้ว (เกิน deadline เดิมของ precommit timer ที่ 8.0s ไปแล้ว)
    yield Buffer.from('chunk2')
  }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.fallbackOutcome, 'SPOKEN')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 2, 'ต้องได้ครบทั้ง 2 ก้อน ไม่ถูกตัดกลางคันที่ 8.0s เหมือนบั๊กเดิมที่เจอจาก production')
  harness.disconnect(socket)
})

test('12) fallback commit แล้ว stall เกิน FALLBACK_IDLE_TIMEOUT_MS (6s) → FALLBACK_PARTIAL_TIMEOUT, history เป็น marker ไม่ใช่ข้อความเต็ม, endCallRequested ถูกบังคับ false [DoD C]', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid)
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') }
  state.claudeStreamImpl = async function* () { yield 'คำตอบยาว [END_CALL]' } // มี end_call intent ปนมาด้วย ต้องถูกเพิกเฉยเมื่อเป็น partial
  state.ttsImpl = async function* () {
    yield Buffer.from('chunk1') // คอมมิตทันที
    await new Promise(() => {}) // ค้างตลอดไป — ไม่มี progress เพิ่มอีกเลย
  }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.fallbackOutcome, 'FALLBACK_PARTIAL_TIMEOUT')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 1, 'ต้องมี chunk แรกที่คอมมิตไปแล้วจริง ไม่ใช่ 0 เหมือนบั๊กเดิม')
  assert.equal(session.hangupReason, undefined, 'ห้ามวางสายจาก end_call intent ของคำตอบที่ไม่สมบูรณ์')
  const lastAssistantMsg = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.ok(lastAssistantMsg.content.includes('ขัดจังหวะ'), 'history ต้องเป็น marker บอกว่าคำตอบไม่สมบูรณ์ ไม่ใช่ spokenText เต็มก้อน')
  harness.disconnect(socket)
})

test('13) fallback provider error หลัง commit → FALLBACK_PARTIAL_ERROR ไม่ใช่ FALLBACK_ERROR ธรรมดา (ไม่รายงานเหมือน "ไม่มีเสียงออก") [DoD D]', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') }
  state.claudeStreamImpl = async function* () { yield 'คำตอบที่จะพังกลางคัน.' }
  state.ttsImpl = async function* () {
    yield Buffer.from('chunk1') // คอมมิตก่อน
    throw new Error('ElevenLabs stream boom หลัง commit')
  }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.fallbackOutcome, 'FALLBACK_PARTIAL_ERROR')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 1)
  harness.disconnect(socket)
})

// หมายเหตุสำคัญ (เจอระหว่างเขียนเทสนี้ ไม่ใช่สมมติฐานเดิม): onTranscript ทั้งระบบ (audioStream.js) เช็ค
// `if (sttProcessing) return` ก่อนถึง `if (isSpeaking) { bargeIn() }` เสมอ (ไม่ว่า path ไหน) — แปลว่า barge-in
// ผ่าน final-transcript callback เป็นไปได้จริงเฉพาะตอน turn เดิม "ประมวลผลเสร็จสมบูรณ์แล้ว" (sttProcessing กลับ
// เป็น false ใน finally) แต่ isSpeaking ยังค้าง true อยู่ (รอ mark/playback-unlock) เท่านั้น — ไม่ใช่ระหว่างที่
// generator ยังค้างส่ง delta/chunk อยู่จริง (ตรงกับที่ Test 3 เดิมทำอยู่แล้ว และตรงกับที่ production Call 2 เจอ
// จริงสองรอบ: interrupt ลงจังหวะหลัง AI พูดจบเทิร์นแล้วเท่านั้น) เป็นข้อจำกัดของกลไก barge-in เดิมทั้งระบบ ไม่ใช่
// อะไรที่ patch นี้เปลี่ยน จึงทดสอบ DoD-E ตามรูปแบบที่ระบบรองรับจริงแทน (เหมือน Test 3 แต่ผ่าน fallback path)
test('14) barge-in ทันทีหลัง fallback commit+จบเทิร์นแล้ว (isSpeaking ยังรอ mark เหมือน Test 3) → generation ใหม่ประมวลผลได้ปกติ ไม่มี ghost audio หลุดเพิ่ม [DoD E]', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') }
  state.claudeStreamImpl = async function* () { yield 'คำตอบแรกจาก fallback.' }
  state.ttsImpl = async function* () { yield Buffer.from('chunk1') } // คอมมิตแล้วจบเทิร์นตามปกติ — sttProcessing reset จริงใน finally เหมือน production

  await harness.sendFinalTranscript('ทดสอบ') // fallback commit + จบสมบูรณ์

  const sentBeforeBargeIn = socket.sent.filter(e => e.event === 'media').length
  assert.equal(sentBeforeBargeIn, 1, 'chunk แรกของ fallback ต้องถูกคอมมิตไปแล้วก่อน barge-in')

  let secondTurnCalls = 0
  state.claudeStreamChunkedImpl = async function* () { secondTurnCalls++; yield 'ตอบเทิร์นใหม่หลัง barge-in.' }
  await harness.sendFinalTranscript('ขอโทษนะคะ หนูพูดแทรกเข้ามา') // ยาวพอไม่ถูกมองเป็น echo/fragment สั้น

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'barge-in ต้องส่ง clear event ไปที่ Twilio')
  assert.equal(secondTurnCalls, 1, 'เทิร์นใหม่หลัง barge-in ต้องประมวลผลได้ปกติ ไม่ค้าง แม้เทิร์นก่อนหน้ามาจาก fallback path')

  const mediaAfter = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaAfter.length, sentBeforeBargeIn + 1, 'เสียงที่เพิ่มขึ้นหลัง barge-in ต้องมาจากเทิร์นใหม่เท่านั้น 1 ก้อน ไม่มีอะไรจาก fallback เก่าหลุดเพิ่มมาอีก')

  harness.disconnect(socket)
})

// ===== C6c follow-up — barge-in gate fix: แยก interrupt control ออกจาก transcript processing single-flight
// gate (production discovery 2026-08-19) =====
// เดิม onTranscript เช็ค `if (sttProcessing) return` ก่อนถึง `if (isSpeaking) bargeIn()` เสมอ — แปลว่าลูกค้าพูด
// แทรกตอน AI ยัง generate/พูดอยู่จริง (sttProcessing=true) จะถูกทิ้งเงียบๆ ก่อนแม้แต่จะพยายาม barge-in เลย แก้โดย
// แยกสองเรื่องออกจากกัน: bargeIn() ยิงได้ทันทีไม่ว่า sttProcessing จะเป็นอะไร ส่วน sttProcessing ยังคุม
// single-flight ของ Claude/TTS pipeline เหมือนเดิม — ถ้า barge-in เกิดตอนเทิร์นเดิมยัง sttProcessing=true อยู่จริง
// transcript จะถูกเก็บไว้ (pendingTranscript, ช่องเดียว latest-wins) แล้วให้เทิร์นเดิมสั่ง process ต่อเองหลังปล่อย
// sttProcessing จริงใน processTranscript() (ดู audioStream.js)
//
// DoD D (sttProcessing=true แต่ isSpeaking=false → behavior เดิมยังคงเดิม) ไม่มีเทสแยกเพราะ state นี้ไม่ reachable
// จริงในระบบ — isSpeaking/sttProcessing ถูกตั้ง true คู่กันเสมอที่จุดเริ่ม processTranscript() แบบ synchronous
// (ไม่มี await คั่นระหว่างสองบรรทัดนี้ จึงไม่มีช่องให้ transcript อื่นแทรกเข้ามาเห็น state นี้ได้) เมื่อ state ไม่มีทาง
// เกิดจริง โค้ด branch ที่จะรันก็คือ busy-drop เดิม (byte-identical กับก่อน patch) — พิสูจน์ด้วยการอ่านโค้ดแทนเทส

test('15) DoD A: final transcript ระหว่าง isSpeaking=true+sttProcessing=true → bargeIn ทันที ไม่ทิ้ง transcript ประมวลผลเป็นเทิร์นใหม่หลัง turn เดิมปล่อย sttProcessing', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 }) // legacy path — เรียบง่ายสุดสำหรับพิสูจน์กลไก gate เอง ไม่ต้องพึ่ง chunked/fallback

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () {
    yield 'กำลังตอบคำถามแรก'
    await oldTurnGate // ค้างตรงนี้ — จำลอง AI ยัง generate อยู่จริง (sttProcessing=true) ตอนลูกค้าพูดแทรก
    yield ' ต่อจนจบประโยค' // ต้องไม่ถูกใช้เลยหลัง barge-in (signal.aborted ต้องกันไว้)
  }

  const oldTurnPromise = harness.sendFinalTranscript('คำถามแรกครับ')
  await delay(30) // ให้เทิร์นแรกเข้าสู่ isSpeaking=true, sttProcessing=true และเริ่ม await Claude ค้างอยู่แล้ว

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบเรื่องถอนเงิน' }

  // ลูกค้าพูดแทรกระหว่างเทิร์นแรกยังไม่ปล่อย sttProcessing เลย
  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'bargeIn ต้องยิงทันทีแม้เทิร์นเดิมยัง sttProcessing=true อยู่ — ต้องเห็น clear event ทันที')
  assert.equal(newTurnCalls, 0, 'ยังไม่ควรเริ่มเทิร์นใหม่ทันที — เทิร์นเดิมยังไม่ปล่อย sttProcessing')

  resumeOldTurn() // ปล่อยให้เทิร์นเดิมทำงานต่อ — จะพบว่า signal ตัวเองถูก abort ไปแล้วจาก bargeIn()
  await oldTurnPromise

  assert.equal(newTurnCalls, 1, 'transcript ที่พูดแทรกไว้ต้องถูก process เป็นเทิร์นใหม่ทันทีหลังเทิร์นเดิมปล่อย sttProcessing')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน', 'transcript ที่พูดแทรกต้องไม่หาย ต้องถูกบันทึกเข้า session จริง')

  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'เทิร์นใหม่ต้องพูดออกมาจริง')

  harness.disconnect(socket)
})

test('16) DoD C: final transcript หลายอันมาระหว่าง turn เดิมยัง processing → ไม่สร้างหลาย Claude turns ใช้ transcript ล่าสุดเสมอ (latest-wins ช่องเดียว)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield 'กำลังตอบ'; await oldTurnGate }

  const oldTurnPromise = harness.sendFinalTranscript('คำถามแรก')
  await delay(30)

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบ' }

  await harness.sendFinalTranscript('แทรกแรก')
  await harness.sendFinalTranscript('แทรกสอง')
  await harness.sendFinalTranscript('แทรกสามล่าสุด')

  resumeOldTurn()
  await oldTurnPromise

  assert.equal(newTurnCalls, 1, 'ห้ามเกิดหลาย Claude turn จากการพูดแทรกหลายครั้งระหว่างรอเทิร์นเดิมปล่อย sttProcessing')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'แทรกสามล่าสุด', 'ต้องใช้ transcript ล่าสุด (latest-wins) ไม่ใช่อันแรกที่แทรกเข้ามา')

  harness.disconnect(socket)
})

test('17) DoD B+F: barge-in ระหว่าง fallback ที่ audioCommitted=true → generation เก่าถูก invalidate ทันที ไม่มี ghost audio ไม่มี recovery ซ้ำ (fallbackOutcome=STALE) generation ใหม่ตอบได้หลัง turn เดิมปล่อย sttProcessing', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid) // rolloutPercent=100 default — chunked+fallback path
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') } // ล้มทันที → เข้า fallback
  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก fallback ที่กำลังพูดอยู่' }

  let resumeStall
  const stallGate = new Promise(resolve => { resumeStall = resolve })
  state.ttsImpl = async function* () {
    yield Buffer.from('chunk1') // คอมมิตทันที — audioCommitted=true, sttProcessing ยังเป็น true (ยัง await อยู่)
    await stallGate // ค้างตรงนี้ — จำลองช่วงที่ fallback ยังไม่จบงานตัวเอง (เหมือน production incident) barge-in จะเกิดตอนนี้
    yield Buffer.from('GHOST_AUDIO_old_gen_chunk2') // ต้องไม่ถูกส่งออกไปเลยหลัง barge-in (generation guard ต้องกันไว้)
  }

  const oldTurnPromise = captureMetrics(() => harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยครับ'))
  await delay(30) // ให้ chunk แรกส่งออกไปจริง (commit) ก่อน

  const sentBeforeBargeIn = socket.sent.filter(e => e.event === 'media').length
  assert.ok(sentBeforeBargeIn > 0, 'ต้องมีเสียง fallback คอมมิตไปแล้วก่อน barge-in')

  let newTurnCalls = 0
  state.claudeStreamChunkedImpl = async function* () { newTurnCalls++; yield 'ตอบคำถามใหม่หลัง barge-in' }
  // สำคัญ: reset ttsImpl ด้วย ไม่งั้นเทิร์นใหม่ (chunked success คราวนี้) จะไปเรียก stub เดิมที่ยังค้าง GHOST_AUDIO
  // อยู่ต่อ (state.ttsImpl เป็น stub เดียวใช้ร่วมกันทั้ง chunked path หลักและ fallback path) ทำให้เทสเข้าใจผิดว่า
  // เสียงที่มาจากเทิร์นใหม่ (ถูกต้องแล้ว) เป็น ghost audio ของเทิร์นเก่า
  state.ttsImpl = async function* () { yield Buffer.from('clean-new-turn-chunk') }

  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'barge-in ต้องยิงทันทีแม้ fallback เดิมยัง sttProcessing=true อยู่')
  assert.equal(newTurnCalls, 0, 'ยังไม่ควรเริ่มเทิร์นใหม่ทันที — fallback เดิมยังไม่ปล่อย sttProcessing')

  resumeStall() // ปล่อยให้ fallback เดิมทำงานต่อ — จะพบว่า generation ตัวเองเก่าไปแล้วจาก barge-in
  const metrics = await oldTurnPromise

  assert.equal(metrics.fallbackOutcome, 'STALE', 'fallback เดิมต้องรู้ตัวว่า generation เก่าแล้วผ่าน isCurrentGeneration ไม่พยายาม recovery ซ้ำ')

  const leaked = socket.sent.filter(e => e.event === 'media')
    .some(e => Buffer.from(e.media.payload, 'base64').toString().includes('GHOST_AUDIO'))
  assert.equal(leaked, false, 'ห้ามมี audio ของ generation เก่า (chunk2 ที่ resume หลัง barge-in) หลุดออกไปเด็ดขาด (ไม่มี ghost audio)')

  assert.equal(newTurnCalls, 1, 'transcript ที่พูดแทรกไว้ต้องถูก process เป็นเทิร์นใหม่หลัง fallback เก่าปล่อย sttProcessing จริง')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')

  harness.disconnect(socket)
})

// ===== C6c follow-up — STT listening: interim ระหว่าง isSpeaking=true เป็น interrupt-control signal =====
// googleSTT.js ตอนนี้ rotate stream ให้ฟังต่อเนื่องทันทีหลัง utterance ก่อนหน้า deliver ไม่รอ Twilio mark อีกแล้ว
// (พิสูจน์แยกใน test/googleSTT.test.js) — เทสชุดนี้พิสูจน์ฝั่ง audioStream.js: onInterim ต้อง trigger bargeIn()
// ทันทีตอน isSpeaking=true (เร็วกว่ารอ final ที่มี debounce 900ms ในตัว) แต่ final ของประโยคเดียวกันที่ตามมาต้อง
// ไม่หาย ต้องไหลผ่าน bargeInPendingFinal → pendingTranscript/latest-wins เดียวกับที่พิสูจน์ไปแล้วในชุด 15-17

test('18) DoD B+C+E+F (STT listening): interim ระหว่าง isSpeaking=true trigger bargeIn ทันที final ที่ตามมาไม่หาย ประมวลผลเป็นเทิร์นใหม่พอดี 1 ครั้ง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 }) // legacy path — เรียบง่ายสุดสำหรับพิสูจน์กลไก gate เอง

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield 'กำลังตอบคำถามแรก'; await oldTurnGate }

  const oldTurnPromise = harness.sendFinalTranscript('คำถามแรกครับ')
  await delay(30) // เทิร์นแรกเข้าสู่ isSpeaking=true, sttProcessing=true และเริ่ม await Claude ค้างอยู่แล้ว

  // ลูกค้าเริ่มพูดแทรก — STT ส่ง interim มาก่อน final เสมอ (ยาวพอ ไม่ใช่ fragment สั้น)
  harness.sendInterim('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.equal(clearEvents.length, 1, 'interim ต้อง trigger bargeIn ทันทีครั้งเดียว ไม่ต้องรอ final')

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบเรื่องถอนเงิน' }

  // final ของประโยคเดียวกันมาถึงทีหลัง (STT debounce 900ms ตามจริง แต่เทสส่งตรงๆ ได้เลย)
  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')
  assert.equal(newTurnCalls, 0, 'final ที่มาระหว่างเทิร์นเดิมยัง sttProcessing=true ต้องถูก queue ไว้ก่อน ไม่ใช่ประมวลผลทันที')

  resumeOldTurn()
  await oldTurnPromise

  assert.equal(newTurnCalls, 1, 'ต้องมีเทิร์นใหม่พอดี 1 ครั้งจาก final ที่ถูก queue ไว้ (bargeInPendingFinal)')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน', 'ข้อความที่พูดแทรกต้องไม่หาย')

  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'เทิร์นใหม่ต้องพูดออกมาจริง')

  harness.disconnect(socket)
})

test('19) DoD G: interim สั้น (fragment/echo) ระหว่าง isSpeaking=true ต้องไม่ trigger bargeIn เลย', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield 'กำลังตอบ'; await oldTurnGate }

  const oldTurnPromise = harness.sendFinalTranscript('คำถามแรก')
  await delay(30)

  harness.sendInterim('เอ่อ') // สั้น < 2 คำ, < 8 ตัวอักษร — echo/noise ตาม filter เดียวกับ final-transcript path

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.equal(clearEvents.length, 0, 'interim สั้นต้องไม่ trigger bargeIn เลย')

  resumeOldTurn()
  await oldTurnPromise
  harness.disconnect(socket)
})

test('20) DoD D: interim trigger bargeIn ระหว่าง fallback ที่ audioCommitted=true → ไม่มี ghost audio เทิร์นใหม่ตอบได้ปกติ', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid) // rolloutPercent=100 default — chunked+fallback
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') }
  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก fallback ที่กำลังพูดอยู่' }

  let resumeStall
  const stallGate = new Promise(resolve => { resumeStall = resolve })
  state.ttsImpl = async function* () {
    yield Buffer.from('chunk1') // คอมมิตทันที
    await stallGate
    yield Buffer.from('GHOST_AUDIO_old_gen_chunk2') // ต้องไม่ถูกส่งออกไปเลยหลัง barge-in
  }

  const oldTurnPromise = captureMetrics(() => harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยครับ'))
  await delay(30)

  const sentBeforeBargeIn = socket.sent.filter(e => e.event === 'media').length
  assert.ok(sentBeforeBargeIn > 0, 'ต้องมีเสียง fallback คอมมิตไปแล้วก่อน barge-in')

  harness.sendInterim('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน') // trigger bargeIn ผ่าน interim แทนที่จะรอ final

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'interim ต้อง trigger bargeIn ได้แม้ fallback เดิมยัง sttProcessing=true อยู่')

  let newTurnCalls = 0
  state.claudeStreamChunkedImpl = async function* () { newTurnCalls++; yield 'ตอบคำถามใหม่หลัง barge-in' }
  state.ttsImpl = async function* () { yield Buffer.from('clean-new-turn-chunk') } // reset กัน stub เดิมที่ยังมี GHOST_AUDIO ค้างอยู่ปนกับเทิร์นใหม่

  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')
  assert.equal(newTurnCalls, 0, 'final ต้องถูก queue ไว้ก่อน — fallback เดิมยังไม่ปล่อย sttProcessing')

  resumeStall()
  const metrics = await oldTurnPromise

  assert.equal(metrics.fallbackOutcome, 'STALE', 'fallback เดิมต้องรู้ตัวว่า generation เก่าแล้ว ไม่พยายาม recovery ซ้ำ')
  const leaked = socket.sent.filter(e => e.event === 'media')
    .some(e => Buffer.from(e.media.payload, 'base64').toString().includes('GHOST_AUDIO'))
  assert.equal(leaked, false, 'ห้ามมี audio ของ generation เก่าหลุดออกไปเด็ดขาด (ไม่มี ghost audio)')
  assert.equal(newTurnCalls, 1, 'ต้องมีเทิร์นใหม่พอดี 1 ครั้งจาก final ที่ถูก queue ไว้')

  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน')

  harness.disconnect(socket)
})

// ===== L1a — rollout-scoped STT endpoint experiment: audioStream.js ต้องเลือก interimFinalizeMs ถูก branch =====
// mechanism ของ googleSTT.js เองพิสูจน์แยกไว้แล้วใน test/googleSTT.test.js — เทสชุดนี้พิสูจน์แค่ฝั่ง wiring:
// audioStream.js ต้องส่งค่าที่ "ตรงกับ rollout ที่ freeze แล้วของสายนั้น" เข้า transcribeStream() จริง ไม่ใช่ค่าคงที่

test('21) L1a: legacy (rollout=0%) ต้องส่ง interimFinalizeMs=900 (ค่า default เดิม) เข้า transcribeStream()', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })
  assert.equal(state.lastSttOptions?.interimFinalizeMs, 900, 'legacy ต้องไม่ถูกกระทบจาก experiment เลย ต้องได้ 900ms เดิมเป๊ะ')
  harness.disconnect(socket)
})

test('22) L1a: chunked (rollout=100%) ต้องส่ง interimFinalizeMs=600 (ค่าทดลอง) เข้า transcribeStream()', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid) // rolloutPercent=100 default
  assert.equal(state.lastSttOptions?.interimFinalizeMs, 600, 'chunked ต้องได้ค่าทดลอง 600ms ตาม L1a')
  harness.disconnect(socket)
})
