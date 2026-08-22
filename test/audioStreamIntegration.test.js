// Checkpoint C6a — integration tests ที่ขับเคลื่อน registerWebSocket() จริงผ่าน _audioStreamHarness.js
// ปิด known gap ที่สะสมมาตั้งแต่ C0: wiring-level contract ที่ unit test ของ utilities อย่างเดียวพิสูจน์ไม่ได้
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const callSessions = require('../src/utils/callSessions')
const harness = require('./_audioStreamHarness')

// A2: LEGACY_FRESH_CLAUDE_TIMEOUT_MS production default คือ 6000ms — ต้องตั้ง override ก่อน harness ที่ require
// audioStream.js ตัวจริงครั้งแรก (module-level const อ่านค่าตอน load ครั้งเดียว) ไม่งั้นเทสที่ต้องการให้ watchdog
// นี้ timeout จริงจะต้องรอ 6 วินาทีทุกครั้ง ทำให้ suite ช้าขึ้นโดยไม่จำเป็น — audioStream.js เองบังคับด้วย
// NODE_ENV==='test' ก่อนยอมรับ override ตัวนี้เลย (กัน env ตัวนี้หลงค้างใน production แล้ว legacy ทั้งหมด
// timeout ที่ 80ms โดยไม่มีใครตั้งใจ) จึงต้องตั้ง NODE_ENV ที่นี่ด้วย ไม่ใช่แค่ค่า override เฉยๆ
process.env.NODE_ENV = 'test'
process.env.LEGACY_CLAUDE_TIMEOUT_MS_OVERRIDE = '80'

// ต้องตรงกับ LEGACY_RECOVERY_PHRASE ใน audioStream.js เป๊ะ (ไม่ได้ export ออกมา เป็น internal constant ของโมดูล)
const LEGACY_RECOVERY_PHRASE = 'ขอโทษค่ะ เมื่อกี้ตอบช้าไปนิดนึง รบกวนพูดอีกครั้งได้ไหมคะ'

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
  // L2a production exposure gate (design revision 2026-08-20) — reset ทุกเทสเหมือน rolloutPercent ข้างบน กัน
  // ค่าที่เทสก่อนหน้าตั้งไว้หลุดข้ามมาปนเทสถัดไปโดยไม่ตั้งใจ default fail-closed เหมือน production cold start
  state.legacyObservedConfig = { percent: 0, campaignId: null }
  // L2b — reset เช่นกัน + คืน claudeConditionalImpl เป็น default (delegate ไป claudeStreamImpl)
  state.legacyEarlyTtsConfig = { percent: 0, campaignId: null }
  state.claudeConditionalImpl = null
  // STT-A2 — reset เช่นกัน default fail-closed
  state.sttA2Config = { percent: 0, campaignId: null }
  // A2.1 Shadow — reset เช่นกัน default fail-closed, independent จาก sttA2Config เอง
  state.sttA2ShadowConfig = { percent: 0, campaignId: null }
})

// L2a exposure gate tests — campaign id คงที่ใช้ร่วมกันเพื่อจำลอง "dedicated test campaign" ตามแผน production จริง
const L2A_CAMPAIGN_ID = 'CAMPAIGN_L2A_TEST'
function l2aCampaign(overrides = {}) {
  return { voice_id: 'voice1', script: 'ระบบทดสอบ', id: L2A_CAMPAIGN_ID, ...overrides }
}

// L2b exposure gate tests — คนละ campaign id จาก L2A ตั้งใจ (พิสูจน์ independence)
const L2B_CAMPAIGN_ID = 'CAMPAIGN_L2B_TEST'
function l2bCampaign(overrides = {}) {
  return { voice_id: 'voice1', script: 'ระบบทดสอบ', id: L2B_CAMPAIGN_ID, ...overrides }
}

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

  // Design B: mark name เปลี่ยนจาก bare "ai_done" เป็น owned "ai_done:<pipelineId>" — ยืนยัน kind ถูกต้องด้วย prefix match
  const markEvents = socket.sent.filter(e => e.event === 'mark')
  assert.ok(markEvents.some(e => /^ai_done:\d+$/.test(e.mark?.name)), 'ต้องมี owned mark ai_done:<id> ปิดท้ายเทิร์นด้วย')

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

test('22) L1a: chunked (rollout=100%) ต้องส่ง interimFinalizeMs=900 เข้า transcribeStream() (600ms ถูก REJECT จาก production evidence แล้ว — กลับมาใช้ 900 เหมือน legacy)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid) // rolloutPercent=100 default
  assert.equal(state.lastSttOptions?.interimFinalizeMs, 900, 'chunked ต้องได้ 900ms เหมือน legacy หลัง 600ms ถูก reject — mechanism/wiring ยังอยู่ครบ เผื่อทดลองค่าอื่นทีหลัง')
  harness.disconnect(socket)
})

// ===== Commit A — legacy prewarm bounded grace (production incident 2026-08-19) =====
// เดิม `aiText = await myPrewarm` ไม่มี deadline เลย — prewarm ที่ยัง pending ตอน final มาถึงสามารถ block final
// path ได้ไม่มีขอบเขต (production trace จริง: Claude ตอบช้า ~11s บน prewarm request ที่เริ่มไว้ก่อน final)
// เทสชุดนี้พิสูจน์ทุก state ตามที่ล็อกไว้ก่อนแก้: READY/PENDING-within-grace/PENDING-timeout/null-result/
// unusable/barge-in-during-grace/late-resolution-after-timeout/teardown-during-grace

test('23) Commit A: prewarm READY ก่อน final มาถึง → hit ทันที ไม่มี fresh call ซ้ำ', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  state.claudeStreamImpl = async function* () { callCount++; yield 'คำตอบจาก prewarm' }

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await delay(30) // ให้ prewarm resolve จริงก่อน final มาถึง (READY แล้ว)
  await harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่')

  assert.equal(callCount, 1, 'ต้องเรียก Claude แค่ครั้งเดียว (prewarm) ไม่มี fresh call ซ้ำ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก prewarm')
  harness.disconnect(socket)
})

test('24) Commit A: prewarm ยัง pending ตอน final มาถึง แต่ resolve ภายใน grace (150ms) → hit ไม่มี fresh call', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  state.claudeStreamImpl = async function* () {
    callCount++
    await delay(80) // ช้ากว่าจะ resolve ตอน final มาถึง แต่ยังเร็วกว่า grace 150ms
    yield 'คำตอบจาก prewarm ช้าหน่อย'
  }

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่') // ส่งทันที ไม่รอ prewarm

  assert.equal(callCount, 1, 'ต้องเรียก Claude แค่ครั้งเดียว (prewarm resolve ทันภายใน grace)')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก prewarm ช้าหน่อย')
  harness.disconnect(socket)
})

test('25) Commit A: prewarm ยัง pending เกิน grace (150ms) → abort prewarm request จริง แล้ว fresh call พอดี 1 ครั้ง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  let firstCallSignal = null
  state.claudeStreamImpl = async function* (s, isGreeting, signal) {
    callCount++
    if (callCount === 1) {
      firstCallSignal = signal
      await new Promise(() => {}) // ค้างตลอดไป — จำลอง Claude ช้าผิดปกติแบบที่เจอจริงใน production (~11s)
    } else {
      yield 'คำตอบจาก fresh call'
    }
  }

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await delay(10) // ให้ prewarm เริ่มจริงก่อน
  await harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่')

  assert.equal(callCount, 2, 'ต้องมี fresh call เกิดขึ้นจริงอีกครั้งหลัง grace หมด (นอกเหนือจาก prewarm ที่ยังค้างอยู่)')
  assert.equal(firstCallSignal.aborted, true, 'prewarm request เดิมที่ยังค้างอยู่ต้องถูก abort จริงที่ signal level ไม่ใช่แค่ทิ้ง reference เฉยๆ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh call')
  harness.disconnect(socket)
})

test('26) Commit A: prewarm resolve เป็น null (Claude ตอบว่างเปล่า) → ถือเป็น miss fresh call เกิดขึ้น 1 ครั้ง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  state.claudeStreamImpl = async function* () {
    callCount++
    if (callCount === 1) return // ไม่ yield อะไรเลย → prewarm resolve เป็น null
    yield 'คำตอบจาก fresh call หลัง prewarm null'
  }

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await delay(10)
  await harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่')

  assert.equal(callCount, 2, 'prewarm resolve เป็น null ต้องถือเป็น miss ไปเรียก fresh call ต่อ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh call หลัง prewarm null')
  harness.disconnect(socket)
})

test('27) Commit A: prewarm unusable (interim ไม่ตรงกับ final) → ไม่มี grace wait เลย ไปเรียก fresh call ทันที', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  state.claudeStreamImpl = async function* () {
    callCount++
    if (callCount === 1) { await new Promise(() => {}) } // prewarm ค้างตลอดไป (ไม่ควรถูกใช้เลย)
    else yield 'คำตอบจาก fresh call'
  }

  harness.sendInterim('สวัสดีครับ') // interim ไม่เกี่ยวกับ final ด้านล่างเลย
  await delay(10)
  const startedAt = Date.now()
  await harness.sendFinalTranscript('ไม่มีอะไรตรงกับ interim เลยครับ') // ไม่ match isPrewarmUsable
  assert.ok(Date.now() - startedAt < 100, 'ไม่ควรมี grace wait เลยเพราะ interim ไม่ match final ตั้งแต่ต้น')

  assert.equal(callCount, 2, 'ต้องข้าม prewarm ไปเรียก fresh call ทันที ไม่ต้องรอ grace เลย')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh call')
  harness.disconnect(socket)
})

test('28) Commit A: barge-in ระหว่าง grace wait → เทิร์นจบแบบไม่มีเสียง ไม่เรียก fresh call ของตัวเอง, transcript ที่พูดแทรกไม่หาย', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  state.claudeStreamImpl = async function* (s, isGreeting, signal) {
    callCount++
    if (callCount === 1) {
      // จำลอง SDK จริงที่ signal.abort() ทำให้ request ยุติเร็ว (ไม่ yield อะไรเลย) แทนที่จะค้างตลอดไปแบบไม่สนใจ signal
      await new Promise((resolve) => { signal.addEventListener('abort', () => resolve(), { once: true }) })
      return
    }
    yield 'ตอบเรื่องที่พูดแทรก'
  }

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await delay(10)
  const turnPromise = harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่') // เข้า grace wait (150ms) — เทิร์นนี้ isSpeaking=true แล้ว

  await delay(50) // อยู่ในช่วง grace wait แน่นอน (< 150ms)
  harness.sendInterim('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน') // ยาวพอไม่ใช่ echo — trigger bargeIn() เพราะ isSpeaking=true อยู่

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'barge-in ต้อง trigger ทันทีแม้เทิร์นเดิมกำลังรอ prewarm grace อยู่')

  await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน') // final ของ interrupt — ต้องถูก queue ไว้ (sttProcessing ยัง true เพราะเทิร์นเดิมยัง await grace ไม่จบ)

  await turnPromise // ปล่อยให้เทิร์นเดิม (ที่ถูก barge-in) จบ แล้ว drain transcript ที่ queue ไว้ต่อ

  assert.equal(callCount, 2, 'callCount=1 คือ prewarm เดิมที่ถูก abort (ไม่นับเป็น fresh call ใหม่) callCount=2 คือเทิร์นใหม่หลัง barge-in เท่านั้น')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'ตอบเรื่องที่พูดแทรก')
  const lastUser = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUser.content, 'เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน', 'transcript ที่พูดแทรกต้องไม่หาย')

  harness.disconnect(socket)
})

test('29) Commit A: prewarm ที่ resolve ช้าหลัง grace timeout ไปแล้ว ต้องไม่ถูกใช้ย้อนหลัง (ไม่มี double commit)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeLatePrewarm
  const lateGate = new Promise(resolve => { resumeLatePrewarm = resolve })
  let callCount = 0
  state.claudeStreamImpl = async function* () {
    callCount++
    if (callCount === 1) { await lateGate; yield 'คำตอบ prewarm ที่มาสายเกินไปแล้ว' }
    else { yield 'คำตอบจาก fresh call' }
  }

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await delay(10)
  await harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่') // grace หมด (150ms) แล้วไปเรียก fresh call จนจบสมบูรณ์ก่อน

  assert.equal(callCount, 2)
  const messagesBeforeLate = session.messages.length
  const lastAssistantBefore = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistantBefore.content, 'คำตอบจาก fresh call')

  resumeLatePrewarm() // ปล่อย prewarm เก่าให้ resolve ตอนนี้ (สายเกินไปแล้ว)
  await delay(50) // ให้เวลามันทำงานถ้าจะทำอะไรผิดพลาด

  assert.equal(session.messages.length, messagesBeforeLate, 'prewarm ที่มาสายต้องไม่ commit อะไรเพิ่มเข้า history เลย')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'ต้องมีเสียงจาก fresh call ที่ถูกต้องเท่านั้น')

  harness.disconnect(socket)
})

test('30) Commit A: disconnect ระหว่าง grace wait ต้องไม่ throw/ค้าง', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* () { await new Promise(() => {}) } // ค้างตลอดไป

  harness.sendInterim('สวัสดีครับขอสอบถามโปรโมชั่น')
  await delay(10)
  const turnPromise = harness.sendFinalTranscript('สวัสดีครับขอสอบถามโปรโมชั่นสมาชิกใหม่')
  await delay(30) // อยู่ในช่วง grace wait

  harness.disconnect(socket) // ปิดสายระหว่างที่ยังรอ grace อยู่

  await turnPromise // ต้องไม่ throw หรือค้างตลอดไป (grace หมดตามธรรมชาติที่ 150ms แล้วจบ turn อย่างปลอดภัย)

  assert.ok(true, 'ไม่ throw/ค้าง')
})

// ===== Commit A2 — legacy fresh-call watchdog + canned recovery phrase =====
// LEGACY_FRESH_CLAUDE_TIMEOUT_MS ถูก override เป็น 80ms ที่หัวไฟล์ (ผ่าน env var ก่อน harness require audioStream.js
// ครั้งแรก) — production ยังคง 6000ms เดิม จุดสำคัญที่สุดที่เทสชุดนี้ต้องพิสูจน์ (ตามที่ล็อกไว้ก่อนแก้): barge-in ที่
// ทำให้ askClaudeStream() throw AbortError ต้องถูกจัดเป็น outcome 'aborted' ไม่ใช่ 'error' ไม่งั้น recovery phrase
// จะพูดทับเสียงลูกค้าที่กำลังพูดแทรกอยู่จริง

test('31) Commit A2: fresh call ปกติ (เร็วกว่า timeout มาก) → behavior เดิมทุกประการ ไม่มี recovery phrase', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })
  state.claudeStreamImpl = async function* () { yield 'คำตอบปกติ' }

  await harness.sendFinalTranscript('สวัสดีค่ะ')

  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบปกติ')
  harness.disconnect(socket)
})

test('32) Commit A2: fresh call เกิน LEGACY_FRESH_CLAUDE_TIMEOUT_MS (ค้างตลอดไป ไม่สนใจ signal เลย — จำลอง incident เดิมเป๊ะ) → พูด recovery phrase, abort request เดิมจริง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let capturedSignal = null
  state.claudeStreamImpl = async function* (s, isGreeting, signal) {
    capturedSignal = signal
    await new Promise(() => {}) // ค้างตลอดไป ไม่สนใจ signal เลย
  }

  await harness.sendFinalTranscript('สวัสดีค่ะขอสอบถามโปรโมชั่น')

  assert.equal(capturedSignal.aborted, true, 'ต้อง abort request เดิมจริงตอน timeout ไม่ใช่แค่ทิ้ง reference')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, LEGACY_RECOVERY_PHRASE)
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'ต้องมีเสียง recovery phrase ออกไปจริง')
  harness.disconnect(socket)
})

test('32b) Commit A2: fresh Claude สำเร็จ (success) แต่ไม่ yield ข้อความเลย (blank/empty result) → ต้องพูด recovery phrase เช่นกัน ไม่ใช่เงียบ', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let callCount = 0
  // จำลอง askClaudeStream ของจริงที่ yield เฉพาะ text.length >= 3 — ที่นี่ไม่ yield อะไรเลย (สำเร็จแต่ไม่มีข้อความ)
  state.claudeStreamImpl = async function* () { callCount++ }

  await harness.sendFinalTranscript('สวัสดีค่ะขอสอบถามโปรโมชั่น')

  assert.equal(callCount, 1, 'ต้องเรียก Claude แค่ครั้งเดียว ไม่ retry ซ้ำ')
  const assistantMessages = session.messages.filter(m => m.role === 'assistant')
  assert.equal(assistantMessages.length, 1, 'ห้ามมี blank assistant message ใน history เลย มีแค่ recovery phrase เดียว')
  assert.equal(assistantMessages[0].content, LEGACY_RECOVERY_PHRASE)
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'ต้องมีเสียง recovery phrase ออกไปจริง ไม่ใช่ความเงียบ')
  harness.disconnect(socket)
})

test('33) Commit A2: genuine Claude error (throw ทันที ไม่เกี่ยวกับ abort เลย) → พูด recovery phrase เดียวกัน', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* () { throw new Error('Claude API error จริง ไม่เกี่ยวกับ abort เลย') }

  await harness.sendFinalTranscript('สวัสดีค่ะขอสอบถามโปรโมชั่น')

  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, LEGACY_RECOVERY_PHRASE)
  harness.disconnect(socket)
})

test('34) Commit A2 (critical): barge-in ทำให้ askClaudeStream throw AbortError → ต้องจัดเป็น aborted ไม่ใช่ error ห้ามพูด recovery phrase ทับลูกค้า', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* (s, isGreeting, signal) {
    await new Promise((resolve, reject) => {
      // จำลอง SDK จริงที่ throw ทันทีเมื่อ signal ถูก abort (ไม่ใช่แค่ hang เฉยๆ แบบเทส 32)
      signal.addEventListener('abort', () => {
        const err = new Error('The user aborted a request.')
        err.name = 'AbortError'
        reject(err)
      }, { once: true })
    })
  }

  const turnPromise = harness.sendFinalTranscript('สวัสดีค่ะขอสอบถามโปรโมชั่น')
  await delay(20) // อยู่ในช่วง 80ms timeout window แน่นอน — เทิร์นนี้ isSpeaking=true แล้ว

  let secondTurnCalls = 0
  state.claudeStreamImpl = async function* () { secondTurnCalls++; yield 'ตอบเรื่องที่พูดแทรก' }
  harness.sendInterim('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน') // ยาวพอ trigger bargeIn()

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'barge-in ต้องยิงจริง')

  await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน')
  await turnPromise

  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.notEqual(lastAssistant.content, LEGACY_RECOVERY_PHRASE, 'ห้ามพูด recovery phrase ทับ barge-in — ต้อง classify เป็น aborted ไม่ใช่ error')
  assert.equal(lastAssistant.content, 'ตอบเรื่องที่พูดแทรก')
  assert.equal(secondTurnCalls, 1, 'ต้องมีเทิร์นใหม่พอดี 1 ครั้งจาก final ที่ถูก queue ไว้')
  harness.disconnect(socket)
})

test('35) Commit A2: barge-in ระหว่างพูด recovery phrase เอง (หลังส่งเสียงไปแล้วบางส่วน) → ห้าม commit recovery phrase เต็มลง history ต้องใช้ partial marker', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeRecoveryTts
  const recoveryGate = new Promise(resolve => { resumeRecoveryTts = resolve })
  state.ttsImpl = async function* () {
    yield Buffer.from('chunk1') // ก้อนแรกของ recovery phrase ออกไปแล้วจริง
    await recoveryGate // ค้างตรงนี้ — barge-in จะเกิดตอนนี้
    yield Buffer.from('GHOST_recovery_chunk2') // ต้องไม่ถูกส่งออกไปเลยหลัง barge-in
  }
  state.claudeStreamImpl = async function* () { await new Promise(() => {}) } // fresh call ค้างตลอดไป → timeout → recovery

  const turnPromise = harness.sendFinalTranscript('สวัสดีค่ะขอสอบถามโปรโมชั่น')
  await delay(120) // ให้ timeout (80ms) ยิงแล้วเริ่มพูด recovery phrase, commit chunk แรกไปแล้วแน่นอน

  const sentBeforeBargeIn = socket.sent.filter(e => e.event === 'media').length
  assert.ok(sentBeforeBargeIn > 0, 'recovery phrase ต้องเริ่มพูดไปแล้วบางส่วนก่อน barge-in')

  let secondTurnCalls = 0
  state.claudeStreamImpl = async function* () { secondTurnCalls++; yield 'ตอบเรื่องใหม่' }
  state.ttsImpl = async function* () { yield Buffer.from('clean-new-turn-chunk') } // reset กัน stub เดิมที่ยังมี GHOST_recovery ค้างอยู่ปนกับเทิร์นใหม่
  harness.sendInterim('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน')

  resumeRecoveryTts() // ปล่อยให้ recovery phrase's TTS พยายามพูดต่อ (ต้องถูก generation guard กันไว้)

  await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน')
  await turnPromise

  const assistantMessages = session.messages.filter(m => m.role === 'assistant')
  const oldTurnCommit = assistantMessages.at(-2) // เทิร์นเก่า (recovery ที่ถูกขัดจังหวะ) ต้องมาก่อนเทิร์นใหม่ใน history
  assert.ok(oldTurnCommit, 'เทิร์นเก่าต้อง commit อะไรบางอย่างเข้า history (partial marker) ไม่ใช่ข้ามไปเฉยๆ')
  assert.notEqual(oldTurnCommit.content, LEGACY_RECOVERY_PHRASE, 'ห้าม commit recovery phrase เต็มเพราะถูกขัดจังหวะกลางคัน')
  assert.match(oldTurnCommit.content, /ขัดจังหวะ/, 'ต้องใช้ partial marker เดียวกับที่ fallback partial ใช้')

  const lastAssistant = assistantMessages.at(-1)
  assert.equal(lastAssistant.content, 'ตอบเรื่องใหม่', 'เทิร์นใหม่หลัง barge-in ต้องตอบถูกต้องปกติ')

  const leaked = socket.sent.filter(e => e.event === 'media').some(e => Buffer.from(e.media.payload, 'base64').toString().includes('GHOST_recovery'))
  assert.equal(leaked, false, 'ห้ามมี audio ของ recovery เก่าหลุดออกไปหลัง barge-in')

  harness.disconnect(socket)
})

// ===== L1b — chunked speculative prewarm (design locked 2026-08-19, 4 review rounds) =====
// รวม 3 corrections สุดท้ายจากรอบ implementation-authorize เข้าไปในทุกเทสที่เกี่ยวข้องอยู่แล้ว:
//   #1 bridgeAbort ผูกกับ childSignal ของแต่ละ watchdog attempt (ไม่ใช่ outer signal เฉยๆ)
//   #2 producer's getIsValid() upgrade เป็น generation guard จริงหลัง adopt (ไม่ใช่แค่ AbortSignal)
//   #3 prewarmAgeAtFinalMs snapshot ก่อน grace ใดๆ (ดู test 51)
test('36) L1b: BUFFERED_HIT — safe chunk พร้อมอยู่แล้วก่อน final (producer ยังสตรีมต่อ) → adopt ทันที Claude ถูกเรียกครั้งเดียว', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  let releaseRest
  const restGate = new Promise(resolve => { releaseRest = resolve })
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    yield 'พร้อมพูดได้เลยค่ะ. ' // ตัด boundary ทันที (strong '.') → enqueue จริงก่อน final
    await restGate // producer ยังไม่จบตอน final มาถึง
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)
  const turnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(20)
  releaseRest()
  await turnPromise

  assert.equal(callCount, 1, 'ต้องเรียก askClaudeStreamChunked แค่ครั้งเดียว (speculation ถูก adopt ไม่ใช่ fresh call ซ้ำ)')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0)
  harness.disconnect(socket)
})

test('37) L1b: READY_HIT — speculation จบเต็มก้อนก่อน final มาถึงเลย → adopt, TTS เริ่มจาก buffered chunk ทันที', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    yield 'พร้อมพูดได้เลยค่ะ.'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // ให้ speculation จบสมบูรณ์ (producerDone=true) ก่อน final
  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  assert.equal(callCount, 1)
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0)
  harness.disconnect(socket)
})

test('38) L1b: DELTA_ONLY_HIT — มี delta แต่ยังไม่มี chunk ตอน final มาถึง → adopt ทันที ไม่รอ pre-adopt wait ยาวก่อน (design correction #2)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  let releaseChunk
  const chunkGate = new Promise(resolve => { releaseChunk = resolve })
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    yield 'เอ่อ เดี๋ยวก่อนนะ' // delta มา ไม่มี boundary ให้ตัดเลย (< SOFT_TIMEOUT_MS 300ms, < FALLBACK_MIN_LENGTH 25)
    await chunkGate
    yield ' พร้อมแล้วค่ะ.'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // delta แรกมาแล้วจริง แต่ยังไม่มี chunk
  const turnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(20) // ให้ adoption commit เกิดขึ้นจริง (ควรเกิดทันที ไม่รอ 2000ms แบบดีไซน์เก่าที่ถูก reject)

  const startedWaiting = Date.now()
  releaseChunk() // ปล่อยให้ chunk พร้อมหลัง adopt แล้ว — Watchdog B ตัวจริงของเทิร์นเป็นคนคุมต่อ ไม่ใช่ pre-adopt wait
  await turnPromise
  const elapsed = Date.now() - startedWaiting
  assert.ok(elapsed < 500, `ต้อง adopt ทันทีแล้วรอ Watchdog B จริงแทน ไม่ใช่ pre-adopt wait ยาว — ใช้เวลาอีก ${elapsed}ms หลัง release`)

  assert.equal(callCount, 1)
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0)
  harness.disconnect(socket)
})

test('39) L1b: DELTA_ONLY_HIT adopted แต่ chunk ไม่มาเลยเกิน CHUNK_READY_TIMEOUT_MS → fallback ไป legacy จริง (Watchdog B ตัวจริงของเทิร์นยังทำงาน ไม่หายไปหลัง adopt)', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  state.claudeStreamChunkedImpl = async function* () {
    yield 'เอ่อ เดี๋ยวก่อนนะ' // delta มา ไม่มี boundary เลย แล้วค้างตลอดไป (ไม่เคยมี chunk)
    await new Promise(() => {})
  }
  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก legacy fallback' }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)
  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก legacy fallback')
  harness.disconnect(socket)
})

test('40) L1b: zero-progress grace ตื่นทันทีที่ delta แรกมาถึง ไม่ใช่รอครบ 150ms เต็ม (Correction #1, design รอบ 4)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    await delay(50) // ช้ากว่า final แต่เร็วกว่า grace เต็ม (150ms) มาก
    yield 'มาแล้วค่ะ พร้อมตอบ.'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  const startedAt = Date.now()
  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ') // final มาทันทีก่อน speculation มี progress เลย
  const elapsed = Date.now() - startedAt

  assert.equal(callCount, 1, 'ต้องเรียกแค่ครั้งเดียว (speculation ถูก adopt หลัง grace ตื่นจาก delta แรก)')
  assert.ok(elapsed < 120, `grace ต้องตื่นทันทีที่ delta มา (~50ms) ไม่ใช่รอครบ 150ms เต็ม — ใช้เวลาไป ${elapsed}ms`)
  harness.disconnect(socket)
})

test('41) L1b: zero-progress grace timeout (150ms) เต็ม ไม่มี delta มาเลย → DROP speculation + fresh chunked เกิดขึ้นพอดี 1 ครั้ง (รวม speculative เป็น 2 ครั้ง)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  let firstSignal = null
  state.claudeStreamChunkedImpl = async function* (s, signal) {
    callCount++
    if (callCount === 1) {
      firstSignal = signal
      await new Promise(() => {}) // ค้างตลอดไป ไม่เคย yield delta เลย
    } else {
      yield 'คำตอบจาก fresh chunked call'
    }
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(10)
  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  assert.equal(callCount, 2, 'ต้องมี fresh chunked call เกิดขึ้นจริงอีกครั้งหลัง grace timeout')
  assert.equal(firstSignal.aborted, true, 'speculative producer เดิมต้องถูก abort จริงที่ signal level ไม่ใช่แค่ทิ้ง reference เฉยๆ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh chunked call')
  harness.disconnect(socket)
})

test('42) L1b: MISMATCH_FRESH — interim ไม่ตรงกับ final เลย → ไม่มี grace wait เลย ไปเรียก fresh chunked ทันที', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    if (callCount === 1) { await new Promise(() => {}) } // speculation ค้างตลอดไป (ไม่ควรถูกใช้เลย)
    else yield 'คำตอบจาก fresh chunked'
  }

  harness.sendInterim('สวัสดีครับ')
  await delay(10)
  const startedAt = Date.now()
  await harness.sendFinalTranscript('ไม่มีอะไรตรงกับ interim เลยครับ')
  assert.ok(Date.now() - startedAt < 100, 'ไม่ควรมี grace wait เลยเพราะ mismatch ตั้งแต่ต้น')

  assert.equal(callCount, 2)
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh chunked')
  harness.disconnect(socket)
})

test('43) L1b: EMPTY_FRESH — speculation จบแบบว่างเปล่า (ไม่มี delta ไม่มี control เลย) → DROP fresh chunked เกิดขึ้น 1 ครั้ง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    if (callCount === 1) return // จบทันทีไม่ yield อะไรเลย ไม่เรียก end_call ด้วย
    yield 'คำตอบจาก fresh chunked หลัง speculation ว่างเปล่า'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(20) // ให้ speculation จบไปแล้วจริง (producerDone=true) ก่อน final
  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  assert.equal(callCount, 2, 'response ว่างเปล่าต้องถือเป็น DROP ไปเรียก fresh call ต่อ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh chunked หลัง speculation ว่างเปล่า')
  harness.disconnect(socket)
})

test('44) L1b: ERROR_FRESH — speculative Claude error กลางทาง (มี partial chunk buffer อยู่ก่อน error) → DROP ทั้งก้อนรวม partial ด้วย ไม่ splice กับ fresh', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('audio') }
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    if (callCount === 1) {
      yield 'ข้อความ speculative ที่ไม่ควรถูกพูดเลย.' // ได้ chunk มาก่อน error
      throw new Error('Claude boom mid-speculation')
    }
    yield 'คำตอบจาก fresh chunked หลัง error'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(20) // ให้ error เกิดขึ้นจริงก่อน final (producerError ถูก set)
  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  assert.equal(callCount, 2, 'ERROR ต้อง DROP ทั้งหมด ไปเรียก fresh call ใหม่')
  assert.ok(!ttsCalls.some(t => t.includes('speculative')), 'partial speculative text ที่ error ต้องไม่ถูกส่งเข้า TTS เลย')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก fresh chunked หลัง error')
  harness.disconnect(socket)
})

test('45) L1b: CONTROL_ONLY_HIT — speculative response เป็น end_call ล้วนๆ ไม่มี text เลย → adopt, endCallRequested=true, ไม่มี TTS เลย', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('audio') }
  state.claudeStreamChunkedImpl = async function* (s, signal, onControl) {
    callCount++
    onControl?.({ type: 'end_call' })
  }

  harness.sendInterim('ไม่สะดวกคุยแล้วครับ วางสายเลย')
  await delay(20) // ให้ speculation จบแล้ว (producerDone=true, controlEvent buffered) ก่อน final
  await harness.sendFinalTranscript('ไม่สะดวกคุยแล้วครับ วางสายเลย')

  assert.equal(callCount, 1, 'ต้องเรียกแค่ครั้งเดียว (adopt control-only response)')
  assert.deepEqual(ttsCalls, [], 'ไม่มี text ให้พูดเลย ไม่ควรมี TTS call ใดๆ')
  assert.equal(session.hangupReason, 'ai_ended', 'end_call ที่ adopt มาต้องถูก policy เดิม (hangup) ปฏิบัติเหมือน fresh chunked end_call ทุกประการ')
  harness.disconnect(socket)
})

test('46) L1b: disconnect ระหว่าง speculation ยังไม่ถูก adopt (ก่อน final มาถึง) → producer ถูก abort จริง ไม่ throw/ค้าง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let specSignal = null
  state.claudeStreamChunkedImpl = async function* (s, signal) {
    specSignal = signal
    await new Promise(() => {})
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(20)
  assert.ok(specSignal && !specSignal.aborted)

  assert.doesNotThrow(() => harness.disconnect(socket))
  await delay(10)
  assert.equal(specSignal.aborted, true, 'speculation ต้องถูก abort จริงตอนสายจบ')
})

test('47) L1b: barge-in หลัง adoption ต้อง abort speculative Claude producer ด้วย ไม่ใช่แค่หยุด TTS (Correction #1 — bridge ผ่าน childSignal ไม่ใช่ outer signal)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let specSignal = null
  state.claudeStreamChunkedImpl = async function* (s, signal) {
    specSignal = signal
    yield 'เอ่อ เดี๋ยวก่อนนะ' // DELTA_ONLY_HIT — adopt ทันทีตอน final โดยไม่ต้องรอ chunk
    await new Promise(() => {}) // ยังไม่จบ ยัง "มีชีวิต" อยู่ตอน adopt แล้วก็ตอน barge-in
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)
  harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ') // ไม่ await — isSpeaking=true ตั้งแต่ต้นเทิร์นแล้ว แม้ยังไม่มี chunk ให้พูดจริงก็ตาม
  await delay(50) // ให้ adopt เกิดจริง (DELTA_ONLY_HIT ไม่ต้องรอ)

  assert.ok(specSignal, 'speculation ต้องเริ่มจริง')
  assert.equal(specSignal.aborted, false, 'ยังไม่ถูก abort ก่อน barge-in')

  harness.sendInterim('เดี๋ยวก่อนครับขอถามเรื่องอื่น') // barge-in ระหว่าง speculation ที่ adopt แล้วยังมีชีวิตอยู่

  await delay(20)
  assert.equal(specSignal.aborted, true, 'speculative Claude producer ต้องถูก abort จริงผ่าน bridge หลัง barge-in ไม่ใช่แค่หยุดส่งเสียงเฉยๆ')

  harness.disconnect(socket)
})

test('48) L1b: interim ใหม่ที่ยาวขึ้นมากพอหลังผ่าน throttle cooldown (retrigger) → speculation เก่าถูก abort สะอาด ตัวใหม่เป็นตัวที่ adopt', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  let firstSignal = null
  state.claudeStreamChunkedImpl = async function* (s, signal) {
    callCount++
    if (callCount === 1) {
      firstSignal = signal
      await new Promise((resolve) => { signal.addEventListener('abort', () => resolve(), { once: true }) })
      return
    }
    yield 'คำตอบจาก speculation ที่สอง'
  }

  harness.sendInterim('ขอสอบถาม')
  await delay(20)
  await delay(700) // ผ่าน throttle cooldown (700ms) ก่อน retrigger ได้จริง
  harness.sendInterim('ขอสอบถามโปรโมชั่นสมาชิกใหม่ตอนนี้เลยค่ะ')
  await delay(20)

  assert.equal(firstSignal.aborted, true, 'speculation แรกต้องถูก abort ตอน retrigger')

  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นสมาชิกใหม่ตอนนี้เลยค่ะ')

  assert.equal(callCount, 2, 'ต้องมี speculation ตัวที่สองเริ่มจริงหลัง retrigger')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'คำตอบจาก speculation ที่สอง')
  harness.disconnect(socket)
})

test('49) L1b: interim ใหม่ที่มาเร็วเกินไป (ยังไม่ผ่าน throttle cooldown) → ไม่ retrigger speculation เดิมยังทำงานต่อ', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* () {
    callCount++
    yield 'คำตอบจาก speculation แรก.'
  }

  harness.sendInterim('ขอสอบถาม')
  await delay(20)
  harness.sendInterim('ขอสอบถามโปรโมชั่นสมาชิกใหม่ตอนนี้เลยค่ะ') // ยาวขึ้นมากพอ แต่มาเร็วเกินไป (ยังไม่ผ่าน throttle 700ms)
  await delay(20)

  assert.equal(callCount, 1, 'ต้องไม่ retrigger เพราะยังอยู่ในช่วง throttle cooldown')
  harness.disconnect(socket)
})

test('50) L1b: barge-in ระหว่าง zero-progress grace wait → ABORTED ไม่พยายามเริ่ม fresh chunked เลย (Correction #4a)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* (s, signal) {
    callCount++
    if (callCount === 1) {
      await new Promise((resolve) => { signal.addEventListener('abort', () => resolve(), { once: true }) })
      return
    }
    yield 'ตอบเรื่องที่พูดแทรก'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(10)
  const turnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ') // ยังไม่มี progress เลย เข้า zero-progress grace (150ms)

  await delay(50) // อยู่ในช่วง grace wait แน่นอน (< 150ms)
  harness.sendInterim('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน') // trigger bargeIn() เพราะ isSpeaking=true อยู่แล้วตั้งแต่ต้นเทิร์น

  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.ok(clearEvents.length > 0, 'barge-in ต้อง trigger ทันทีแม้เทิร์นเดิมกำลังรอ zero-progress grace อยู่')

  await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน') // final ของ interrupt — ถูก queue ไว้ (sttProcessing ยัง true)

  await turnPromise

  assert.equal(callCount, 2, 'callCount=1 คือ speculation เดิมที่ถูก abort (ไม่นับเป็น fresh call) callCount=2 คือเทิร์นใหม่หลัง barge-in เท่านั้น — ต้องไม่มี fresh chunked call แทรกระหว่างนั้น')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content, 'ตอบเรื่องที่พูดแทรก')
  harness.disconnect(socket)
})

test('51) L1b: prewarmAgeAtFinalMs ต้อง snapshot ก่อน grace ไม่ใช่หลัง grace (Correction #3), prewarmOutcome=GRACE_HIT ถูกบันทึกถูกต้อง, canonical t3 ถูก mark หลัง adopt', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  state.claudeStreamChunkedImpl = async function* () {
    await delay(60) // ยังไม่มี progress ตอน final มาถึง ต้องรอ grace ก่อน
    yield 'มาแล้วค่ะ พร้อมตอบ.'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // speculation อายุ ~30ms ตอน final มาถึง (ยังไม่มี delta เลย)
  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ'))

  assert.equal(metrics.prewarmOutcome, 'GRACE_HIT')
  assert.ok(metrics.prewarmAgeAtFinalMs >= 15 && metrics.prewarmAgeAtFinalMs < 100, `prewarmAgeAtFinalMs ควร ~30ms (วัดตอน final มาถึงจริง ไม่ใช่บวก grace เข้าไปด้วย) ได้ ${metrics.prewarmAgeAtFinalMs}`)
  assert.equal(typeof metrics.t3, 'number', 'canonical t3 ต้องถูก mark หลัง adopt สำเร็จ')
  harness.disconnect(socket)
})

test('52) L1b: MISMATCH_FRESH ยังต้อง record prewarm telemetry เต็มชุดก่อน abort (ไม่ทิ้งแค่ prewarmOutcome เฉยๆ — blocker จาก commit-gate review)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let callCount = 0
  state.claudeStreamChunkedImpl = async function* (s, signal) {
    callCount++
    if (callCount === 1) {
      yield 'พร้อมพูดได้เลยค่ะ. ' // ได้ delta+chunk จริงก่อนที่ mismatch จะถูกตรวจพบตอน final
      // ค้างจนกว่าจะถูก abort จริงตอน mismatch (ไม่ใช่ค้างตลอดไปแบบไม่มีเงื่อนไข — ไม่งั้นถ้า mock ตัวนี้ถูกเรียก
      // ซ้ำเป็น fresh call รอบสองหลัง mismatch มันจะค้างตลอดไปจนโดน MAX_CALL_DURATION_MS (300s) เหมือนบั๊กที่เจอจริง
      // ระหว่างรัน full suite รอบนี้ — เทสก่อนหน้าเคยพลาดจุดนี้เพราะ mock ไม่แยก call แรก/ที่สอง)
      await new Promise((resolve) => { signal.addEventListener('abort', () => resolve(), { once: true }) })
      return
    }
    yield 'คำตอบจาก fresh chunked หลัง mismatch.'
  }

  harness.sendInterim('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // ให้ chunk ถูก enqueue จริงก่อน final (มี speculative work ที่ "เสียไป" จริง ไม่ใช่ handle เปล่า)
  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ไม่มีอะไรตรงกับ interim เลยครับ'))

  assert.equal(callCount, 2, 'ต้องมี fresh chunked call เกิดขึ้นจริงหลัง mismatch (ไม่ใช่ speculation ตัวเดียวที่ค้าง)')
  assert.equal(metrics.prewarmOutcome, 'MISMATCH_FRESH')
  assert.ok(metrics.prewarmStartedAt != null, 'prewarmStartedAt ต้องถูก record แม้ mismatch ไม่ใช่ปล่อย null')
  assert.ok(metrics.prewarmAgeAtFinalMs >= 0, 'prewarmAgeAtFinalMs ต้องมีค่าจริง')
  assert.equal(metrics.prewarmBufferedChunks, 1, 'ต้องสะท้อนจำนวน chunk ที่ speculation เสียไปจริงตอน miss')
  assert.equal(typeof metrics.prewarmFirstDeltaMs, 'number', 'delta เกิดขึ้นจริงก่อน mismatch ต้องถูกบันทึกด้วย ไม่ใช่ null')
  assert.equal(typeof metrics.prewarmFirstChunkMs, 'number', 'chunk เกิดขึ้นจริงก่อน mismatch ต้องถูกบันทึกด้วย ไม่ใช่ null')
  harness.disconnect(socket)
})

// ===== L1c1 — protected numeric boundary (production defect 2026-08-19) =====
test('53) L1c1 integration: delta ที่ทำให้ candidate ลงท้ายตัวเลขมาถึงตอน elapsedMs ข้าม SOFT_TIMEOUT_MS แล้ว → ไม่ flush แยกจากหน่วยนับที่ตามมา และไม่ทำให้ CHUNK_READY_TIMEOUT (2000ms) fire ก่อนเวลา', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  const ttsCalls = []
  state.ttsImpl = async function* (text) { ttsCalls.push(text); yield Buffer.from('audio') }
  state.claudeStreamChunkedImpl = async function* () {
    yield 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท '
    await delay(350) // ผลักดัน elapsedMs ให้ข้าม SOFT_TIMEOUT_MS(300ms) ก่อน delta ถัดไปจะมาถึง — จำลองจังหวะจริงที่เจอใน production
    yield 'รับ 2,000 ' // ตอนนี้ elapsedMs ~350ms, buffer ลงท้ายตัวเลข — ก่อนแก้ L1c1 จะโดน flush แยกทันทีตรงนี้
    await delay(100) // หน่วยนับตามมาไม่นาน (ยังอยู่ในงบ HARD_MAX_MS=800ms ของ numeric protection)
    yield 'พอยต์นะคะ'
  }

  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  assert.ok(!ttsCalls.some(t => /2,000$/.test(t.trim())), 'ห้ามมี chunk ที่ลงท้าย "2,000" เดี่ยวๆ หลุดเข้า TTS แยกจากหน่วยนับ (defect เดิม)')
  assert.ok(ttsCalls.some(t => t.includes('2,000') && t.includes('พอยต์')), 'ตัวเลขกับหน่วยนับต้องถูกพูดรวมเป็น chunk เดียวกัน')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'ต้องมีเสียงออกจริง ไม่ใช่ fallback เพราะ CHUNK_READY_TIMEOUT fire ผิดจังหวะ')
  harness.disconnect(socket)
})

test('54) L1c1 follow-up (commit-gate review): buffer ที่ถูก numeric protection กันอยู่ แต่ Claude เงียบเกิน HARD_MAX_MS โดยไม่มี delta ใหม่มาปลุกเลย → ต้องถูก flush เองผ่าน expiry timer ก่อน stream จะจบด้วยซ้ำ ไม่ใช่รอ delta ถัดไปหรือ final flush', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 100 })

  let turnStartedAt = null
  let ttsCalledAt = null
  state.ttsImpl = async function* (text) {
    if (ttsCalledAt === null) ttsCalledAt = Date.now()
    yield Buffer.from('audio')
  }
  state.claudeStreamChunkedImpl = async function* () {
    turnStartedAt = Date.now()
    yield 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 ' // numeric-protected candidate — ไม่มี delta ใหม่มาปลุกหลังจากนี้เลย
    await delay(900) // Claude เงียบเกิน HARD_MAX_MS (800ms) แต่ยังไม่จบ stream จริงจนกว่าจะครบ 900ms — จำลอง stall จริง
  }

  await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

  assert.ok(ttsCalledAt !== null, 'ต้องมี TTS ถูกเรียกจริง (chunk ถูก flush)')
  const ttsDelayMs = ttsCalledAt - turnStartedAt
  assert.ok(ttsDelayMs < 900, `TTS ต้องถูกเรียกจาก expiry timer (~800ms) ก่อน stream จะจบเองที่ 900ms — ใช้เวลาไป ${ttsDelayMs}ms (ถ้าไม่มี expiry timer จะรอจนถึง final flush ที่ ~900ms แทน)`)
  assert.ok(ttsDelayMs >= 750, `expiry timer ต้อง honor HARD_MAX_MS (800ms) ไม่ตัดเร็วเกินไปก่อนครบเวลา — ใช้เวลาไป ${ttsDelayMs}ms`)

  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant.content.trim(), 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000') // fullTextAccum เป็น raw concatenation ของ delta ไม่ trim เอง (พฤติกรรมเดิม ไม่เกี่ยวกับ L1c1)

  harness.disconnect(socket)
})

// ===== Design B — short-ack lifecycle (production incident 2026-08-20, design rounds 1-6) =====
// คำรับคำสั้นที่ลูกค้าพูดจริง (ครับ/ค่ะ/โอเค/ok ฯลฯ) เคยหายเงียบจากทั้ง echo-during-speech filter และ post-mark
// echo filter — เทสชุดนี้พิสูจน์ owner-scoped lifecycle เต็มวง: STT candidate → classification → speaking owner
// (activePipelineId ร่วมกันทั้ง normal turn/greeting/silence) → owned mark/no-audio completion → exactly-once dispatch

test('Design B: Tier2 ack ("ครับ") ระหว่าง AI กำลัง generate อยู่จริง → ไม่ bargeIn ทันที ไม่มี clear event ไม่เริ่มเทิร์นใหม่ทันที', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await oldTurnGate }

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // isSpeaking=true ตั้งแต่ต้น processTranscript() แล้ว แม้ Claude ยังไม่ตอบเสร็จเลยด้วยซ้ำ

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบรับทราบ' }

  await harness.sendFinalTranscript('ครับ')

  assert.equal(newTurnCalls, 0, 'Tier2 ack ต้องไม่ trigger เทิร์นใหม่ทันที')
  assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0, 'ต้องไม่มี clear event — ไม่ตัดเสียง AI กลางประโยคจาก particle เดี่ยว')

  resumeOldTurn()
  await oldTurnPromise
  harness.disconnect(socket)
})

test('Design B: Tier2 ack ที่ deferred ไว้ระหว่าง AI พูดจริง (มี audio ส่งแล้ว) → deliver เป็นเทิร์นใหม่หลัง owned mark กลับมาตรง owner เท่านั้น', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeTts
  const ttsGate = new Promise(resolve => { resumeTts = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังพูดอยู่' }
  state.ttsImpl = async function* () { yield Buffer.from('chunk1'); await ttsGate }

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // isSpeaking=true, มี audio ส่งไปแล้วจริง (totalSent>0) แต่ turn ยังไม่จบ (TTS ค้างอยู่)

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบรับทราบ' }
  await harness.sendFinalTranscript('ครับ') // Tier2 capture

  resumeTts()
  await oldTurnPromise

  const markSent = socket.sent.filter(e => e.event === 'mark').at(-1)
  assert.match(markSent.mark.name, /^ai_done:\d+$/, 'ต้องเป็น owned mark')
  assert.equal(newTurnCalls, 0, 'ยังไม่ควร deliver จนกว่า mark จะกลับมาจริง (ไม่ใช่แค่ตอน turn จบ)')

  socket.emit('message', JSON.stringify({ event: 'mark', mark: markSent.mark })) // จำลอง Twilio echo mark กลับมา
  await delay(20)

  assert.equal(newTurnCalls, 1, 'หลัง owned mark กลับมาตรง owner ต้อง deliver ack เป็นเทิร์นใหม่')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'ครับ', 'ข้อความที่ deliver ต้องเป็น raw text เดิมเป๊ะ')

  harness.disconnect(socket)
})

test('Design B: Tier1 ack ("โอเคครับ") ระหว่าง AI พูดอยู่ → bargeIn ทันทีเหมือน transcript ยาวทั่วไป (ไม่ defer)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await oldTurnGate }

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบเรื่องใหม่' }

  await harness.sendFinalTranscript('โอเคครับ')

  assert.ok(socket.sent.filter(e => e.event === 'clear').length > 0, 'Tier1 ต้อง bargeIn ทันที (มี clear event)')
  assert.equal(newTurnCalls, 0, 'เทิร์นเดิมยังไม่ปล่อย sttProcessing — เทิร์นใหม่ยังไม่เริ่มจนกว่าจะปล่อย')

  resumeOldTurn()
  await oldTurnPromise
  assert.equal(newTurnCalls, 1)
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'โอเคครับ')

  harness.disconnect(socket)
})

test('Design B (round 6): mark ที่ owner ไม่ตรง (stale) มาถึงระหว่าง pendingEndCall=true → ต้องไม่ schedule ปิดสาย ไม่ unlock isSpeaking เลย', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* () { yield 'ขอบคุณค่ะ [END_CALL]' }
  await harness.sendFinalTranscript('ไม่สนใจค่ะ') // จบด้วย pendingEndCall=true จริง, ai_done:<N> ถูกส่งออกไป

  const realMark = socket.sent.filter(e => e.event === 'mark').at(-1)
  const realOwnerId = Number(realMark.mark.name.split(':')[1])
  const staleMarkName = `ai_done:${realOwnerId - 1}` // owner เก่ากว่า (เช่น ของ greeting) จงใจไม่ตรง activePipelineId ปัจจุบัน

  socket.emit('message', JSON.stringify({ event: 'mark', mark: { name: staleMarkName } }))
  await delay(1100) // เกิน setTimeout(close, 1000) ที่ pendingEndCall branch เดิมจะตั้งถ้า mark ผ่าน guard ไปได้

  assert.equal(socket.closed, undefined, 'stale mark ต้องไม่ schedule ปิดสายเลย แม้ pendingEndCall จะเป็น true อยู่ก่อนแล้วก็ตาม')

  harness.disconnect(socket)
})

test('Design B (round 6): unowned bare mark (ไม่มี ":ownerId" เลย) ระหว่าง AI กำลังพูดอยู่จริง → ไม่ unlock isSpeaking (ต่างจาก policy เดิมที่ยัง unlock)', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeTts
  const ttsGate = new Promise(resolve => { resumeTts = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก' }
  state.ttsImpl = async function* () { yield Buffer.from('chunk1'); await ttsGate }

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // isSpeaking=true, audio กำลังส่งอยู่จริง

  socket.emit('message', JSON.stringify({ event: 'mark', mark: { name: 'ai_done' } })) // legacy/unowned bare mark
  await delay(20)

  // ยืนยันว่ายัง isSpeaking=true อยู่ทางอ้อม: interim สั้น ("ครับ") ที่ส่งตอนนี้ต้องยังเข้า Tier2-defer path
  // (ต้องผ่าน `if (isSpeaking)` เท่านั้นถึงจะไปถึงจุด classify/defer — ถ้า unowned mark unlock ไปแล้วจริง
  // transcript นี้จะตกไปที่ branch อื่นแทน ไม่ใช่ defer)
  await harness.sendFinalTranscript('ครับ')
  const clearEvents = socket.sent.filter(e => e.event === 'clear')
  assert.equal(clearEvents.length, 0, 'ยังไม่ควรมี clear event ใดๆ (Tier2 defer ไม่ bargeIn อยู่แล้ว แต่ค่านี้ยืนยันว่าไม่ได้ตกไป branch อื่นที่ไม่ใช่ isSpeaking branch)')

  resumeTts()
  await oldTurnPromise
  harness.disconnect(socket)
})

test('Design B (round 6): unowned bare mark ระหว่าง pendingEndCall=true → ต้องไม่ schedule ปิดสาย', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* () { yield 'ขอบคุณค่ะ [END_CALL]' }
  await harness.sendFinalTranscript('ไม่สนใจค่ะ') // pendingEndCall=true

  socket.emit('message', JSON.stringify({ event: 'mark', mark: { name: 'ai_done' } })) // ไม่มี owner เลย
  await delay(1100)

  assert.equal(socket.closed, undefined, 'unowned mark ต้องไม่ schedule ปิดสายเลยเช่นกัน')

  harness.disconnect(socket)
})

test('Design B (round 5-6): รูปแบบ owned mark ที่ผิด (ai_done:, ai_done:0, ai_done:-1, ai_done:1.5, ai_done:abc, unknown:12, เลขเกิน MAX_SAFE_INTEGER) → ทุกอันถูกปฏิเสธ ไม่มี side effect ใดๆ เลย', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeTts
  const ttsGate = new Promise(resolve => { resumeTts = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก' }
  state.ttsImpl = async function* () { yield Buffer.from('chunk1'); await ttsGate }

  harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // isSpeaking=true ค้างอยู่ (TTS gate ยังไม่ปล่อย)

  // round 6 correction: regex ^[1-9]\d*$ เดิมยอมให้เลขเกิน Number.MAX_SAFE_INTEGER ผ่านได้ (ตัวเลขยาวถูกต้องตาม
  // pattern) แต่ Number(...) สูญเสีย precision จริง — ต้องเช็ค Number.isSafeInteger() ซ้ำหลังแปลงเสมอ
  const invalidNames = ['ai_done:', 'ai_done:0', 'ai_done:-1', 'ai_done:1.5', 'ai_done:abc', 'unknown:12', 'ai_done:9007199254740992']
  for (const name of invalidNames) {
    socket.emit('message', JSON.stringify({ event: 'mark', mark: { name } }))
  }
  await delay(20)

  // ถ้า mark ใดหลุดผ่าน guard ไปได้จะ unlock isSpeaking แล้ว startSilenceTimer() — ตรวจทางอ้อมผ่าน Tier2 defer
  // ยังทำงานปกติ (แปลว่ายัง isSpeaking=true อยู่จริง ไม่มี mark ไหนหลุดผ่านไป unlock ได้เลย)
  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'x' }
  await harness.sendFinalTranscript('ครับ')
  assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0, 'ต้องยัง isSpeaking=true อยู่ — ไม่มี mark รูปแบบผิดหลุดผ่าน guard ไป unlock ได้เลย')

  resumeTts()
  await delay(30)
  harness.disconnect(socket)
})

test('Design B (round 6): parseMarkName boundary — 9007199254740991 (MAX_SAFE_INTEGER พอดี) parse เป็น ownerId ตัวเลขจริง, 9007199254740992 (เกิน 1) ถูกปฏิเสธเป็น ownerId=null เหมือน mark รูปแบบผิดอื่น', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }
  try {
    socket.emit('message', JSON.stringify({ event: 'mark', mark: { name: 'ai_done:9007199254740991' } }))
    socket.emit('message', JSON.stringify({ event: 'mark', mark: { name: 'ai_done:9007199254740992' } }))
    await delay(20)
  } finally {
    console.log = originalLog
  }

  assert.ok(logs.some(l => l.includes('ownerId=9007199254740991')), 'เลขที่เท่ากับ MAX_SAFE_INTEGER พอดีต้อง parse เป็นตัวเลขจริง ไม่ใช่ null (แค่บังเอิญไม่ตรง activePipelineId ปัจจุบันเลยไม่ trigger side effect)')
  const rejectedLogs = logs.filter(l => l.includes('kind=ai_done') && l.includes('ownerId=null'))
  assert.ok(rejectedLogs.length > 0, 'เลขที่เกิน MAX_SAFE_INTEGER ต้องถูกปฏิเสธเป็น ownerId=null เหมือน mark รูปแบบผิดอื่นๆ ไม่ใช่แปลงเป็นตัวเลขที่ precision เพี้ยนแล้วปล่อยผ่าน')

  harness.disconnect(socket)
})

// หมายเหตุจากรอบก่อน — ทดสอบ dispatcher rejection ด้วยการทำให้ Claude throw พบว่า legacy path มี recovery-phrase
// safety net ของตัวเอง (LEGACY_RECOVERY_PHRASE) ที่จับ Claude error ไว้ก่อนถึง tryDeliverPendingShortAck's catch
// เสมอ (processTranscriptDispatch resolve สำเร็จด้วยการพูด recovery phrase แทน ไม่ reject) เทสนี้จึงต้องทำให้
// dispatch failure เกิด "นอก" safety net นั้นจริงๆ — currentSession.messages.push() เป็นบรรทัดแรกสุดของ
// processTranscript() อยู่นอก try/catch ของ Claude/TTS ทั้งหมด (ยืนยันด้วย node -e ว่า push บน frozen array
// throw TypeError เสมอไม่ว่า caller จะ strict mode หรือไม่) — แต่ freeze ทั้ง array แบบเปลือยๆ พังใส่ main turn
// เองด้วย (main turn เองก็ push 2 ครั้ง: user message ตอนต้น + assistant message ตอนท้าย) รอบแรกที่เขียนเทสนี้
// freeze ก่อน resumeClaude() เร็วเกินไป ดันไปพัง main turn's เอง assistant-push แทน (บั๊กในเทสเอง ไม่ใช่ production
// code — เจอจาก stack trace ที่ throw จาก onTranscript โดยตรง ไม่ใช่จาก tryDeliverPendingShortAck) — แก้ด้วยการ
// นับจำนวน push แทนการ freeze เปลือย: ปล่อยให้ 2 push แรกของ main turn (user + assistant) สำเร็จตามปกติ แล้วให้
// push ที่ 3 (ของ ack's own processTranscript() ซึ่งเป็นบรรทัดแรกสุดของมันพอดี) throw แทน — ไม่ใช่ production-only
// test hook ใหม่ (เป็นการ mock dependency มาตรฐานเดียวกับที่ state.claudeStreamImpl/ttsImpl ทำอยู่แล้วทั้งไฟล์)
test('Design B: tryDeliverPendingShortAck — dispatcher reject จากจุดที่หลุด internal Claude/TTS recovery safety net จริง (session.messages เขียนไม่ได้) → catch/log ไม่ throw ไม่ retry ไม่มี duplicate turn', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let pushCount = 0
  const originalPush = session.messages.push.bind(session.messages)
  session.messages.push = (...args) => {
    pushCount++
    if (pushCount === 3) throw new Error('simulated session.messages write failure') // เฉพาะ push ที่ 3 (ack's own user-message push) — push ที่ 4 เป็นต้นไป (turn ใหม่หลัง recover) ต้องสำเร็จปกติ
    return originalPush(...args)
  }

  let resumeClaude
  const claudeGate = new Promise(resolve => { resumeClaude = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await claudeGate }
  state.ttsImpl = async function* () {} // 0 chunks → no-audio hand-off ทันทีตอน turn หลักจบ

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ') // push #1 (user) สำเร็จ
  await delay(30)

  await harness.sendFinalTranscript('ครับ') // Tier2 capture — ไม่แตะ session.messages เลย (แค่ตั้ง pendingShortAck)

  let claudeCalledForAck = false
  state.claudeStreamImpl = async function* () { claudeCalledForAck = true; yield 'ไม่ควรถูกเรียกถึงตรงนี้เลย' }

  const originalError = console.error
  const errorLogs = []
  console.error = (...args) => { errorLogs.push(args.join(' ')) }
  try {
    resumeClaude() // main turn จบ → push #2 (assistant) สำเร็จ → TTS 0 chunks → no-audio hand-off เรียก dispatcher → ack's push #3 throw
    await oldTurnPromise
    await delay(50)
  } finally {
    console.error = originalError
  }

  assert.equal(claudeCalledForAck, false, 'dispatch ต้อง throw ก่อนถึง Claude เลยด้วยซ้ำ (บรรทัดแรกสุดของ processTranscript())')
  assert.ok(errorLogs.some(l => l.includes('[ShortAck] Dispatch failed')), 'ต้อง catch/log ผ่าน tryDeliverPendingShortAck จริง ไม่ throw ออกไปเป็น unhandled rejection (main turn เองก็ต้องไม่ crash — oldTurnPromise ต้อง resolve ปกติ ไม่ reject)')

  await delay(50) // เผื่อ retry อัตโนมัติที่ไม่ควรมี
  assert.equal(claudeCalledForAck, false, 'ต้องไม่ retry เองอัตโนมัติ — ack ถูก claim (pendingShortAck=null) ไปแล้วตั้งแต่ก่อน dispatch ครั้งแรก')
  assert.equal(pushCount, 3, 'ต้องมี push พยายามแค่ 3 ครั้งเป๊ะ (user+assistant ของ main turn, user ของ ack ที่ fail) ไม่ใช่ 4+ ที่แปลว่ามี retry หรือ duplicate turn')

  // Round-7 correction (code review finding P1) — dispatch failure ที่เกิดก่อนถึง finally หลักของ processTranscript()
  // เอง (เช่น exception ตอน setup ก่อนเข้า try/catch ของ Claude/TTS) จะทำให้ sttProcessing/isSpeaking ค้าง true
  // ตลอดไปถ้า catch ของ tryDeliverPendingShortAck ไม่ reset state เอง — พิสูจน์ว่าสาย "ฟื้น" ได้จริงหลัง failure
  // ไม่ใช่แค่ไม่ throw/ไม่ retry เฉยๆ: turn ใหม่ปกติหลังจากนี้ต้องไม่ถูกทิ้งเป็น busy และต้องเรียก Claude ได้จริง
  let claudeCalledForRecoveryTurn = false
  state.claudeStreamImpl = async function* () { claudeCalledForRecoveryTurn = true; yield 'ตอบเทิร์นใหม่หลัง recover' }
  state.ttsImpl = async function* () { yield Buffer.from('chunk') }

  const originalLog = console.log
  const dropLogs = []
  console.log = (...args) => { dropLogs.push(args.join(' ')); originalLog(...args) }
  try {
    await harness.sendFinalTranscript('เทิร์นใหม่หลัง dispatch failure ต้องทำงานได้ปกติ')
  } finally {
    console.log = originalLog
  }

  assert.equal(dropLogs.some(l => l.includes('Transcript dropped (busy')), false, 'สายต้องไม่ค้างเป็น busy ตลอดกาลหลัง dispatch failure')
  assert.equal(claudeCalledForRecoveryTurn, true, 'turn ใหม่หลัง failure ต้องเรียก Claude ได้ปกติ — สายต้อง recover ไม่ใช่ค้างฟังไม่ได้อีกเลย')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'เทิร์นใหม่หลัง dispatch failure ต้องทำงานได้ปกติ')

  harness.disconnect(socket)
})

test('Design B: no-audio completion — เทิร์นปกติที่ TTS คืน 0 chunks + Tier2 ack ที่ capture ไว้ก่อนหน้า → deliver ทันที ไม่รอ mark ที่ไม่มีวันมา', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeClaude
  const claudeGate = new Promise(resolve => { resumeClaude = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await claudeGate }
  state.ttsImpl = async function* () {} // 0 chunks เสมอ — totalSent จะเป็น 0

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30) // isSpeaking=true, Claude ยังไม่ตอบเสร็จ

  let newTurnCalls = 0
  const dispatchedTexts = []
  await harness.sendFinalTranscript('ครับ') // Tier2 capture ระหว่าง Claude ยัง generate อยู่ (pipeline เดียวกับ turn หลัก)

  resumeClaude() // ปล่อยให้ turn หลักตอบจบ → TTS คืน 0 chunks → totalSent=0 → no-audio hand-off ต้องทำงานทันที
  await oldTurnPromise

  const markEvents = socket.sent.filter(e => e.event === 'mark')
  assert.equal(markEvents.length, 0, 'totalSent=0 ต้องไม่มี mark ส่งออกเลย')

  const userMsgs = session.messages.filter(m => m.role === 'user').map(m => m.content)
  assert.ok(userMsgs.includes('ครับ'), 'ack ต้องถูก deliver แล้วผ่าน no-audio hand-off โดยไม่ต้องรอ mark ที่ไม่มีวันมา')

  harness.disconnect(socket)
})

test('Design B: no-audio completion — greeting fallback (askClaude) ที่ TTS คืน 0 chunks + Tier2 ack ระหว่าง greeting → deliver ทันที', async () => {
  const callSid = nextCallSid()
  const state = harness.getState()
  state.rolloutPercent = 0
  let resumeGreeting
  const greetingGate = new Promise(resolve => { resumeGreeting = resolve })
  state.askClaudeImpl = async () => { await greetingGate; return 'สวัสดีค่ะ' }
  state.ttsImpl = async function* () {} // greeting fallback ก็คืน 0 chunks เช่นกัน

  const session = makeSession() // ไม่มี greetingChunks เลย → บังคับเข้า fallback-generate path (askClaude)
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(320) // ผ่าน setTimeout(playGreeting, 300) ให้ playGreeting เริ่มทำงานแล้ว เข้าสู่ isSpeaking=true, กำลังรอ askClaude (ค้างที่ greetingGate)

  await harness.sendFinalTranscript('ครับ') // Tier2 ระหว่าง greeting กำลัง generate อยู่ (isSpeaking=true จาก playGreeting)

  resumeGreeting() // ปล่อยให้ askClaude คืนค่า → speakAndWait → TTS 0 chunks → sent===0 → no-audio hand-off
  await delay(50)

  const userMsgs = session.messages.filter(m => m.role === 'user').map(m => m.content)
  assert.ok(userMsgs.includes('ครับ'), 'ack ระหว่าง greeting fallback ต้อง deliver ได้โดยไม่ต้องรอ mark')

  harness.disconnect(socket)
})

test('Design B (round 6): silence prompt เป็น speaking owner ของตัวเอง (bump activePipelineId แยกจาก turn ก่อนหน้า) และ deliver Tier2 ack ผ่าน owned silence_done mark ได้ถูกต้อง', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* () { yield 'คำตอบสั้น' }
  await harness.sendFinalTranscript('สวัสดีค่ะ')
  const prevMark = socket.sent.filter(e => e.event === 'mark').at(-1)
  const prevOwner = Number(prevMark.mark.name.split(':')[1])
  socket.emit('message', JSON.stringify({ event: 'mark', mark: prevMark.mark })) // unlock ให้ silence timer เริ่มนับจริง
  await delay(20)
  socket.sent.length = 0

  let resumeSilenceTts
  const silenceGate = new Promise(resolve => { resumeSilenceTts = resolve })
  state.ttsImpl = async function* () { yield Buffer.from('silence-audio'); await silenceGate }

  await delay(8050) // รอ silenceTimer (8000ms) จริง trigger handleSilence()

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบรับทราบ' }
  await harness.sendFinalTranscript('ครับ') // Tier2 ระหว่าง silence prompt กำลังพูด ("ได้ยินอยู่ไหมคะ")
  assert.equal(newTurnCalls, 0, 'ไม่ควร bargeIn ทันทีระหว่าง silence prompt กำลังพูด')

  resumeSilenceTts()
  await delay(50)

  const silenceMark = socket.sent.filter(e => e.event === 'mark').at(-1)
  assert.match(silenceMark.mark.name, /^silence_done:\d+$/, 'ต้องเป็น owned silence_done mark')
  const silenceOwner = Number(silenceMark.mark.name.split(':')[1])
  assert.ok(silenceOwner > prevOwner, 'handleSilence() ต้อง bump activePipelineId เป็นของตัวเอง ไม่ใช้ owner เดิมของ turn ก่อนหน้า (round 6 fix หลัก)')

  socket.emit('message', JSON.stringify({ event: 'mark', mark: silenceMark.mark }))
  await delay(20)

  assert.equal(newTurnCalls, 1, 'หลัง silence_done mark กลับมาตรง owner ต้อง deliver ack')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'ครับ')

  harness.disconnect(socket)
})

test('Design B: mark เก่าจาก pipeline ที่ถูกทิ้งไปแล้ว (ผ่าน fallback-unlock timer ไม่ใช่ bargeIn) มาถึงช้า → ต้องไม่ consume ack ของ pipeline นั้นเข้า pipeline ใหม่ที่ไม่เกี่ยวข้อง', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeClaude
  const claudeGate = new Promise(resolve => { resumeClaude = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await claudeGate }
  state.ttsImpl = async function* () { yield Buffer.from('chunk1') } // 1 chunk → playbackMs = 20+1500 = 1520ms

  const turnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)
  await harness.sendFinalTranscript('ครับ') // Tier2 capture ผูกกับ pipeline นี้

  resumeClaude()
  await turnPromise // turn จบ ส่ง mark ออกไปแล้ว (ยังไม่ echo กลับ) sttProcessing ปล่อยแล้ว

  const pendingOldMark = socket.sent.filter(e => e.event === 'mark').at(-1)

  await delay(1550) // เกิน playbackMs (1520ms) จริง → fallback-unlock timer flip isSpeaking=false เอง โดยไม่ผ่าน mark/bargeIn เลย

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบเทิร์นใหม่' }
  await harness.sendFinalTranscript('เทิร์นใหม่ปกติที่ไม่เกี่ยวกับ ack เลย') // isSpeaking=false อยู่แล้ว → ตรง processTranscript() ทันที ไม่ผ่าน bargeIn()
  assert.equal(newTurnCalls, 1, 'pipeline ใหม่ต้องเริ่มได้ปกติ')

  socket.emit('message', JSON.stringify({ event: 'mark', mark: pendingOldMark.mark })) // mark เก่ามาถึงตอนนี้ (activePipelineId ขยับไปแล้ว)
  await delay(20)

  assert.equal(newTurnCalls, 1, 'mark เก่าต้องไม่ trigger เทิร์นเพิ่มอีก (ownerId ไม่ตรง activePipelineId ปัจจุบันแล้ว)')
  const userMsgs = session.messages.filter(m => m.role === 'user').map(m => m.content)
  assert.ok(!userMsgs.includes('ครับ'), 'ack "ครับ" ของ pipeline ที่ถูกทิ้งไปแล้วต้องไม่ถูก deliver เข้า session เลย')

  harness.disconnect(socket)
})

test('Design B: bargeIn() คือ central supersession — Tier2 ack ที่ pending อยู่ถูกทิ้งทันทีเมื่อมี turn ที่แข็งแรงกว่าเข้ามาแทน', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await oldTurnGate }

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)

  await harness.sendFinalTranscript('ครับ') // Tier2 capture ก่อน

  let newTurnCalls = 0
  const dispatchedTexts = []
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบเรื่องถอนเงิน' }
  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน') // ยาวพอ trigger bargeIn() จริง — ต้อง supersede ack ที่ pending อยู่

  assert.ok(socket.sent.filter(e => e.event === 'clear').length > 0, 'ต้อง bargeIn จริงจาก transcript ที่แข็งแรงกว่า')

  resumeOldTurn()
  await oldTurnPromise
  await delay(20)

  assert.equal(newTurnCalls, 1, 'ต้องมีแค่เทิร์นเดียวจาก transcript ที่แข็งแรงกว่า ไม่ใช่สองเทิร์น')
  const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
  assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน', 'ack "ครับ" ที่ pending อยู่ก่อนหน้าต้องถูกทิ้งไป ไม่ใช่ deliver เป็น turn แยก')
  const userMsgs = session.messages.filter(m => m.role === 'user').map(m => m.content)
  assert.equal(userMsgs.filter(t => t === 'ครับ').length, 0, 'ack "ครับ" ต้องไม่ถูก deliver เลยไม่ว่าจุดไหน')

  harness.disconnect(socket)
})

test('Design B: pendingShortAck ที่ค้างอยู่ถูกเคลียร์เมื่อสายจบ (stop/close) → ไม่มี delivery ใดๆ เกิดขึ้นหลังสายจบ ไม่ crash', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeTts
  const ttsGate = new Promise(resolve => { resumeTts = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก' }
  state.ttsImpl = async function* () { yield Buffer.from('chunk1'); await ttsGate }

  harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)
  await harness.sendFinalTranscript('ครับ') // Tier2 capture ค้างไว้

  harness.disconnect(socket) // ปิดสายทันทีโดยที่ ack ยังค้างอยู่ (ยังไม่มี mark กลับมาเลย)
  resumeTts() // ปล่อย TTS ที่ค้างอยู่ทีหลัง (จำลอง timing จริงที่ cleanup อาจมาก่อน async work เดิมจะ settle)
  await delay(30)

  // ไม่ throw / ไม่ crash คือเงื่อนไขหลักของเทสนี้ (assert ผ่านแค่ถึงตรงนี้ก็พิสูจน์แล้ว) — ยืนยันเพิ่มว่าไม่มี ack หลุดเข้า session
  const userMsgs = session.messages.filter(m => m.role === 'user').map(m => m.content)
  assert.ok(!userMsgs.includes('ครับ'), 'ack ต้องไม่ถูก deliver เลยหลังสายจบไปแล้ว')
})

// หมายเหตุสำคัญที่เจอตอนรัน: legacy path มี recovery-phrase safety net ของตัวเองอยู่แล้วสำหรับ Claude error
// (LEGACY_RECOVERY_PHRASE, ดู audioStream.js:1044-1057) — Claude throw ระหว่าง fresh call จึงไม่ใช่ "unhandled
// dispatch failure" ที่ tryDeliverPendingShortAck's catch จะเห็นเลย (processTranscriptDispatch resolve สำเร็จ
// ด้วยการพูด recovery phrase แทน ไม่ reject) เทสนี้จึงพิสูจน์สิ่งที่เกิดขึ้นจริง: ack dispatch ที่เจอ Claude error
// ยังต้อง resilient เหมือน turn ปกติทุกประการ (recovery phrase, exactly-once, ไม่ crash) ไม่ใช่พิสูจน์ catch
// block ของ tryDeliverPendingShortAck โดยตรง (branch นั้นเป็น defense-in-depth สำหรับความล้มเหลวที่หลุดจาก
// safety net ชั้นในสุดจริงๆ เท่านั้น เช่น exception ก่อนเข้า try/catch ของ processTranscript เอง)
test('Design B: dispatch turn ใหม่จาก short-ack ที่เจอ Claude error ระหว่างทาง → ใช้ recovery-phrase safety net เดิมของ legacy ได้ปกติ ไม่ crash ไม่ retry ซ้ำ', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  let resumeClaude
  const claudeGate = new Promise(resolve => { resumeClaude = resolve })
  state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await claudeGate }
  // main turn ต้อง 0 chunks (trigger no-audio hand-off) แต่ recovery-phrase ของ ack's own turn ต้องมีอย่างน้อย
  // 1 chunk ไม่งั้น speakFixedText().sentCount===0 จะทำให้ fullText ไม่ถูก commit เข้า session.messages เลยตาม
  // design เดิม ("ห้ามพูดจบครบเพียงเพราะประโยคสั้น" — sentCount>0 เท่านั้นถึง commit) — ใช้ call-count แยกสอง
  // behavior แทนการ reassign state.ttsImpl กลางทาง (reassign ก่อน resumeClaude() จะไปกระทบ main turn's TTS call
  // ที่ยังไม่เกิดขึ้นจริงด้วย เพราะ harness อ่าน state.ttsImpl แบบ dynamic ต่อ call ไม่ capture ตอนเริ่ม — บั๊กที่
  // เจอจริงตอนรันเทสนี้ครั้งแรก)
  let ttsCallCount = 0
  state.ttsImpl = async function* () {
    ttsCallCount++
    if (ttsCallCount === 1) return // main turn: 0 chunks
    yield Buffer.from('recovery-audio') // ack's recovery-phrase turn: มีเสียงจริง
  }

  const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
  await delay(30)
  await harness.sendFinalTranscript('ครับ') // Tier2 capture

  let dispatchCalls = 0
  state.claudeStreamImpl = async function* () {
    dispatchCalls++
    throw new Error('boom — จำลอง Claude ล้มระหว่าง dispatch turn ใหม่จาก ack')
  }

  resumeClaude() // turn หลักจบ → TTS 0 chunks → no-audio hand-off เรียก dispatcher ที่ Claude จะ throw
  await oldTurnPromise
  await delay(150) // LEGACY_CLAUDE_TIMEOUT_MS_OVERRIDE=80ms ในไฟล์นี้ — ให้เวลาพอสำหรับ watchdog/recovery พูดจบ

  assert.equal(dispatchCalls, 1, 'ต้องพยายาม dispatch แค่ครั้งเดียว ไม่ retry ซ้ำ')
  const userMsgs = session.messages.filter(m => m.role === 'user').map(m => m.content)
  assert.equal(userMsgs.filter(t => t === 'ครับ').length, 1, 'ack ต้องถูก push เข้า session แค่ครั้งเดียว ไม่ซ้ำ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, LEGACY_RECOVERY_PHRASE, 'ต้อง fallback ไปใช้ recovery phrase เดิมของ legacy ได้ปกติ ไม่ crash ไม่หลุดออกไปเป็น unhandled rejection')

  harness.disconnect(socket)
})

test('Design B: classifier — "OK"/"OK." classify Tier2, "ok ครับ"/"okครับ"/"OK ครับ." classify Tier1 (whitespace/punctuation-insensitive) โดย raw text ที่ deliver ไม่ถูกแก้เลย', async () => {
  const cases = [
    { text: 'OK', expectTier1: false },
    { text: 'OK.', expectTier1: false },
    { text: 'ok ครับ', expectTier1: true },
    { text: 'okครับ', expectTier1: true },
    { text: 'OK ครับ.', expectTier1: true },
  ]

  for (const { text, expectTier1 } of cases) {
    const callSid = nextCallSid()
    const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

    let resumeOldTurn
    const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลัก'; await oldTurnGate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    let newTurnCalls = 0
    state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบ' }
    await harness.sendFinalTranscript(text)

    const bargedInImmediately = socket.sent.filter(e => e.event === 'clear').length > 0
    assert.equal(bargedInImmediately, expectTier1, `"${text}" ควร ${expectTier1 ? 'Tier1 (bargeIn ทันที)' : 'Tier2 (defer)'} — ได้ clear=${bargedInImmediately}`)

    resumeOldTurn()
    await oldTurnPromise
    await delay(20)

    if (expectTier1) {
      const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
      assert.equal(lastUserMsg.content, text, `raw text ที่ deliver ต้องเป็น "${text}" เป๊ะ ไม่ถูก normalize/lowercase ทิ้ง`)
    }

    harness.disconnect(socket)
  }
})

test('Design B: Context 2 (post-mark <500ms) — ack ที่รู้จัก (ทั้ง Tier1/Tier2) ผ่าน echo filter ได้ ไม่ถูก suppress เหมือน fragment ทั่วไป', async () => {
  const callSid = nextCallSid()
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0 })

  state.claudeStreamImpl = async function* () { yield 'คำตอบสั้น' }
  await harness.sendFinalTranscript('สวัสดีค่ะ')
  const mark = socket.sent.filter(e => e.event === 'mark').at(-1)
  socket.emit('message', JSON.stringify({ event: 'mark', mark: mark.mark }))
  await delay(10) // unlock แล้ว lastMarkTime ถูกตั้งใหม่ — อยู่ในหน้าต่าง <500ms แน่นอน

  let newTurnCalls = 0
  state.claudeStreamImpl = async function* () { newTurnCalls++; yield 'ตอบรับทราบ' }
  await harness.sendFinalTranscript('ครับ') // สั้นกว่า threshold เดิม (wc<3, len<10) แต่เป็น whitelist → ต้องผ่าน

  assert.equal(newTurnCalls, 1, 'ack ที่รู้จักภายใน 500ms หลัง mark ต้องผ่าน echo filter ได้ ไม่ถูก suppress')

  harness.disconnect(socket)
})

// ===== L2a — legacy Claude instrumentation (design locked 2026-08-20, review rounds 1-3) =====
// พิสูจน์แค่ wiring ระดับ audioStream.js↔turnMetrics เท่านั้น — behavior จริงของ askClaudeObservedFullResponse()
// เอง (milestone timing, numeric-protection timer mirror, abort/error propagation) มีเทสละเอียดแยกอยู่แล้วใน
// test/claude.test.js ไม่ต้องพิสูจน์ซ้ำที่นี่

test('L2a: fresh legacy call ปกติ → turnMetrics มี legacyClaude* fields ครบตามที่ควรมี (requestAt/firstDeltaAt/fullAt ไม่ null, outcome=COMPLETED, derived metrics คำนวณได้จริง) และ canonical t2/t3/t4 เดิมไม่ถูกกระทบ', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID } // OBSERVED — ต้องเปิดกลุ่มไว้ก่อน test นี้จึงจะเห็น telemetry
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  state.claudeStreamImpl = async function* () { yield 'สวัสดีค่ะ มีโปรโมชั่นให้ค่ะ' }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('มีโปรโมชั่นอะไรบ้างคะ'))

  assert.ok(metrics.legacyClaudeRequestAt != null, 'requestAt ต้องถูกเขียนจริง')
  assert.ok(metrics.legacyClaudeFirstDeltaAt != null, 'firstDeltaAt ต้องถูกเขียนจริง')
  assert.ok(metrics.legacyClaudeFullAt != null, 'fullAt ต้องถูกเขียนจริง (turn จบปกติ)')
  assert.equal(metrics.legacyClaudeOutcome, 'COMPLETED')
  assert.ok(metrics.legacyClaudeTTFTMs != null && metrics.legacyClaudeTTFTMs >= 0, 'derived TTFT ต้องคำนวณได้จริงจาก field ใหม่')
  assert.ok(metrics.legacyFullCompletionMs != null && metrics.legacyFullCompletionMs >= 0)
  // harness stub เริ่มต้น (ไม่ได้ set claudeObservedImpl ตรงๆ) ไม่ยิง firstSafeAt สังเคราะห์ให้ — behavior จริงของ
  // firstSafeAt ถูกพิสูจน์แยกใน test/claude.test.js แล้ว ที่นี่แค่ยืนยันว่า field มีอยู่ (เป็น null ตามความจริงของ stub)
  assert.equal(metrics.legacyClaudeFirstSafeAt, null)
  // round-3 correction 3 (ห้ามปน legacyClaude* กับ canonical t2/t3/t4) — ยืนยันว่า t3/t4 ยังเป็น null ตามเดิมเป๊ะ
  // สำหรับ legacy (comment เดิมใน turnMetrics.js) ไม่ได้ถูก L2a populate ทับโดยไม่ตั้งใจ
  assert.equal(metrics.t3, null)
  assert.equal(metrics.t4, null)

  harness.disconnect(socket)
})

test('L2a integration (required test 17): fresh legacy Claude ค้างเกิน LEGACY_CLAUDE_TIMEOUT → turnMetrics.legacyClaudeOutcome = TIMEOUT, recovery phrase พูดตามพฤติกรรมเดิมทุกประการ', { timeout: 15000 }, async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  state.claudeStreamImpl = async function* () { await new Promise(() => {}) } // ค้างตลอดไป ไม่เคย resolve — บังคับให้ LEGACY_CLAUDE_TIMEOUT (80ms override ของไฟล์นี้) ทำงานจริง

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ'))

  assert.equal(metrics.legacyClaudeOutcome, 'TIMEOUT')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, LEGACY_RECOVERY_PHRASE, 'recovery phrase ต้องพูดเหมือน askClaudeStream() เดิมทุกประการ')

  harness.disconnect(socket)
})

test('L2a integration (required test 18): fresh legacy Claude error กลางทาง (ไม่ใช่ abort) → turnMetrics.legacyClaudeOutcome = ERROR, recovery phrase พูดตามพฤติกรรมเดิมทุกประการ', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  state.claudeStreamImpl = async function* () { throw new Error('boom — จำลอง Claude API ล้มจริงกลางทาง') }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ'))

  assert.equal(metrics.legacyClaudeOutcome, 'ERROR')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, LEGACY_RECOVERY_PHRASE)

  harness.disconnect(socket)
})

test('L2a integration (required test 19): fresh legacy Claude สำเร็จแต่ไม่มีข้อความให้พูดเลย (ว่างเปล่า) → turnMetrics.legacyClaudeOutcome = EMPTY, recovery phrase พูดตามพฤติกรรมเดิมทุกประการ', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  state.claudeStreamImpl = async function* () {} // ไม่ yield อะไรเลย

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ'))

  assert.equal(metrics.legacyClaudeOutcome, 'EMPTY')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, LEGACY_RECOVERY_PHRASE)

  harness.disconnect(socket)
})

test('L2a integration (required test 15): barge-in ระหว่าง fresh legacy Claude call กำลังรอ → turnMetrics.legacyClaudeOutcome = ABORTED สำหรับเทิร์นเดิม ไม่มีการพูด recovery phrase ทับ', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () {
    yield 'กำลังตอบคำถามแรก'
    await oldTurnGate
  }

  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }

  const oldTurnPromise = harness.sendFinalTranscript('คำถามแรกครับ')
  await delay(30) // ให้เทิร์นแรกเข้าสู่ fresh-call แล้วจริง (isSpeaking=true, sttProcessing=true, ผ่าน askClaudeObservedFullResponse ไปแล้ว)

  state.claudeStreamImpl = async function* () { yield 'ตอบเรื่องถอนเงิน' } // สำหรับเทิร์นใหม่หลัง barge-in
  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องถอนเงินก่อน') // ยาวพอ trigger bargeIn จริง

  resumeOldTurn()
  await oldTurnPromise
  await delay(20)
  console.log = originalLog

  const oldMetricsLine = logs.find(l => l.includes('[Metrics]') && l.includes('"generationId":1,'))
  assert.ok(oldMetricsLine, 'ต้องเจอ [Metrics] log ของเทิร์นแรก (generationId=1)')
  const oldMetrics = JSON.parse(oldMetricsLine.slice(oldMetricsLine.indexOf('{')))
  assert.equal(oldMetrics.legacyClaudeOutcome, 'ABORTED')
  assert.ok(oldMetrics.legacyClaudeFirstDeltaAt != null, 'firstDeltaAt ที่มาถึงก่อน barge-in ต้องยังถูกเก็บไว้')
  assert.equal(oldMetrics.legacyClaudeFullAt, null, 'fullAt ต้องเป็น null เพราะเทิร์นถูก abort ก่อนจบจริง (ไม่ใช่ full completion)')

  harness.disconnect(socket)
})

test('L2a (required test 28/29): prewarm HIT (grace success) → ไม่มี fresh legacyClaude* timestamp ใดๆ ถูก fabricate เลย แม้ legacyObserved=true ก็ตาม เพราะ fresh-call branch ไม่เคยทำงานจริง (prewarm ยังใช้ askClaudeStream() เดิมเสมอ ไม่ขึ้นกับ gate นี้)', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID } // ตั้งใจเปิด OBSERVED ไว้ — พิสูจน์ว่า prewarm HIT ยังบายพาสได้แม้ gate เปิดอยู่
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก prewarm' } // resolve เร็วมาก ไม่มี await ค้าง — prewarm เองก็ใช้ askClaudeStream() เดิมไม่เปลี่ยน

  harness.sendInterim('อยากทราบโปรโมชั่นสมาชิกใหม่') // trigger startPrewarm() ด้วย interim ยาวพอ
  await delay(30) // ให้ prewarm resolve จริงก่อน final มาถึง (เร็วกว่า PREWARM_GRACE_MS=150ms มาก)

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('อยากทราบโปรโมชั่นสมาชิกใหม่ครับ')) // isPrewarmUsable ต้อง match กับ interim (final.includes(interim))

  assert.equal(metrics.legacyClaudeRequestAt, null, 'prewarm HIT ต้องไม่เคยเข้า fresh-call branch เลย — ห้าม fabricate requestAt')
  assert.equal(metrics.legacyClaudeFirstDeltaAt, null)
  assert.equal(metrics.legacyClaudeFullAt, null)
  assert.equal(metrics.legacyClaudeOutcome, null, 'ต้องเป็น null เพราะไม่เคยมี fresh attempt เกิดขึ้นจริง')

  harness.disconnect(socket)
})

// ===== L2a production exposure gate (design revision 2026-08-20, round 4 corrections) =====
// bucket ที่ใช้ในเทสด้านล่างคำนวณไว้ล่วงหน้าจริงจาก getLegacyObservedBucket() (verified — ดู test/rolloutBucket.test.js
// fixture เดียวกัน): CA_L2A_Q1=44, CA_L2A_NQ1=50, CA_L2A_WRONGCAMP=15, CA_L2A_MISSCAMP=9, CA_L2A_CHUNK=45, CA_L2A_MID=69, CA_L2A_KILL=43

test('L2a exposure (required test 18/26/27): matching campaign + qualifying bucket → OBSERVED, telemetry populates, [Metrics] มี frozen assignment metadata ครบ', async () => {
  const callSid = 'CA_L2A_Q1' // bucket=44
  harness.getState().legacyObservedConfig = { percent: 50, campaignId: L2A_CAMPAIGN_ID } // 44 < 50 → qualifies
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  let observedCalls = 0, streamCalls = 0
  state.claudeObservedImpl = async function* (session, signal, onMilestone) {
    observedCalls++
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('firstDeltaAt', Date.now())
    onMilestone?.('fullAt', Date.now())
    yield 'observed response'
  }
  state.claudeStreamImpl = async function* () { streamCalls++; yield 'ไม่ควรถูกเรียก' }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(observedCalls, 1, 'legacyObserved=true ต้องเรียก askClaudeObservedFullResponse (required test 24)')
  assert.equal(streamCalls, 0, 'ต้องไม่เรียก askClaudeStream เลยตอน OBSERVED')
  assert.equal(metrics.legacyObserved, true)
  assert.equal(metrics.legacyObservedBucket, 44)
  assert.equal(metrics.legacyObservedPercentAtStart, 50)
  assert.equal(metrics.legacyObservedCampaignMatched, true)
  assert.ok(metrics.legacyClaudeRequestAt != null && metrics.legacyClaudeFullAt != null, 'OBSERVED ต้อง populate telemetry จริง')
  assert.equal(metrics.legacyClaudeOutcome, 'COMPLETED')

  harness.disconnect(socket)
})

test('L2a exposure (required test 19/23/25): matching campaign แต่ bucket ไม่ผ่านเกณฑ์ → CONTROL, ใช้ askClaudeStream, legacyClaude* fields ทั้งหมดเป็น null', async () => {
  const callSid = 'CA_L2A_NQ1' // bucket=50
  harness.getState().legacyObservedConfig = { percent: 50, campaignId: L2A_CAMPAIGN_ID } // 50 < 50 = false → ไม่ qualify
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  let observedCalls = 0, streamCalls = 0
  state.claudeObservedImpl = async function* () { observedCalls++; yield 'ไม่ควรถูกเรียก' }
  state.claudeStreamImpl = async function* () { streamCalls++; yield 'CONTROL response' }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(streamCalls, 1, 'CONTROL ต้องเรียก askClaudeStream (required test 23)')
  assert.equal(observedCalls, 0, 'ต้องไม่เรียก askClaudeObservedFullResponse เลยตอน CONTROL')
  assert.equal(metrics.legacyObserved, false)
  assert.equal(metrics.legacyObservedCampaignMatched, true, 'campaign match ได้ แต่ bucket ไม่ผ่านเกณฑ์ยังคง CONTROL')
  assert.equal(metrics.legacyClaudeRequestAt, null)
  assert.equal(metrics.legacyClaudeFirstDeltaAt, null)
  assert.equal(metrics.legacyClaudeFullAt, null)
  assert.equal(metrics.legacyClaudeOutcome, null, 'required test 25 — CONTROL ต้อง null ทุก legacyClaude* field ไม่ใช่แค่ timestamp')

  harness.disconnect(socket)
})

test('L2a exposure (required test 20): bucket ผ่านเกณฑ์แต่ campaign ไม่ตรง → CONTROL เสมอ', async () => {
  const callSid = 'CA_L2A_WRONGCAMP' // bucket=15
  harness.getState().legacyObservedConfig = { percent: 50, campaignId: L2A_CAMPAIGN_ID } // 15 < 50 qualifies bucket-wise
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign({ id: 'SOME_OTHER_CAMPAIGN' }) } })

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyObservedCampaignMatched, false)
  assert.equal(metrics.legacyObserved, false, 'campaign ไม่ตรง ต้อง CONTROL แม้ bucket จะผ่านเกณฑ์ก็ตาม')
  assert.equal(metrics.legacyClaudeOutcome, null)

  harness.disconnect(socket)
})

test('L2a exposure (required test 21): session.campaign ไม่มี id เลย (missing) → CONTROL — "ไม่มี campaign_id" ต้องไม่แปลว่า "ทุก campaign"', async () => {
  const callSid = 'CA_L2A_MISSCAMP' // bucket=9
  harness.getState().legacyObservedConfig = { percent: 50, campaignId: L2A_CAMPAIGN_ID } // 9 < 50 qualifies bucket-wise
  // sessionOverrides.campaign ไม่มี field id เลย (จำลอง session ที่ campaign ไม่ผูก id ใดๆ)
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: { voice_id: 'voice1', script: 'ระบบทดสอบ' } } })

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyObservedCampaignMatched, false)
  assert.equal(metrics.legacyObserved, false)
  assert.equal(metrics.legacyClaudeOutcome, null)

  harness.disconnect(socket)
})

test('L2a exposure (required test 22): chunked=true (rollout_percent สูง) → legacyObserved=false เสมอ ไม่ว่า observed config จะเป็นอะไร', async () => {
  const callSid = 'CA_L2A_CHUNK'
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID } // เปิด observed 100% ตั้งใจ — พิสูจน์ว่า chunked ยัง override เป็น false เสมอ
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 100, sessionOverrides: { campaign: l2aCampaign() } })

  state.claudeStreamChunkedImpl = async function* () { yield 'ตอบจาก chunked path' }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.path, 'chunked')
  assert.equal(metrics.legacyObserved, false, 'chunked=true ต้อง override legacyObserved เป็น false เสมอ (required test 22)')
  assert.equal(metrics.legacyClaudeOutcome, null)

  harness.disconnect(socket)
})

test('L2a exposure (required test 31): config เปลี่ยนกลางสาย ไม่กระทบ decision ที่ freeze ไปแล้วตอน WS start (sticky ต่อสายเหมือน rollout)', async () => {
  const callSid = 'CA_L2A_MID' // bucket=69
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID } // 69 < 100 qualifies ตอนเริ่มสาย
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  const metricsA = await captureMetrics(() => harness.sendFinalTranscript('เทิร์นแรก'))
  assert.equal(metricsA.legacyObserved, true, 'เทิร์นแรกต้อง OBSERVED ตาม config ตอน start')

  // เปลี่ยน config "กลางสาย" (จำลอง admin แก้ Sheets แล้ว background refresh อัปเดตแล้ว) — ห้ามกระทบสายนี้ที่ freeze ไปแล้ว
  harness.getState().legacyObservedConfig = { percent: 0, campaignId: null }
  await delay(20)

  const metricsB = await captureMetrics(() => harness.sendFinalTranscript('เทิร์นที่สอง'))
  assert.equal(metricsB.legacyObserved, true, 'เทิร์นที่สองในสายเดียวกันต้องยัง OBSERVED เหมือนเดิม — decision freeze ตอน start ครั้งเดียว ไม่ re-evaluate ทุกเทิร์น')
  assert.equal(metricsB.legacyObservedBucket, 69)
  assert.equal(metricsB.legacyObservedPercentAtStart, 100, 'percentAtStart ต้องเป็นค่า ณ ตอน start ไม่ใช่ค่าที่เปลี่ยนไปกลางสาย')

  harness.disconnect(socket)
})

test('L2a exposure (required test 32): kill switch — สายใหม่หลัง config refresh กลับเป็น 0 ต้องได้ CONTROL (ไม่กระทบสายเก่าที่ freeze OBSERVED ไปแล้ว)', async () => {
  const oldCallSid = 'CA_L2A_KILL' // bucket=43
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID }
  const { socket: oldSocket } = await connectPastGreeting(oldCallSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })
  const oldMetrics = await captureMetrics(() => harness.sendFinalTranscript('สายเก่า'))
  assert.equal(oldMetrics.legacyObserved, true, 'สายเก่าต้อง OBSERVED ตาม config ตอนที่มันเริ่ม')

  // kill switch: แก้ Sheets เป็น 0 แล้ว refresh สำเร็จแล้ว (จำลองด้วยการ set state ตรงๆ — harness ไม่มี polling delay จริง)
  harness.getState().legacyObservedConfig = { percent: 0, campaignId: null }

  const newCallSid = nextCallSid()
  const { socket: newSocket, state: newState } = await connectPastGreeting(newCallSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })
  const newMetrics = await captureMetrics(() => harness.sendFinalTranscript('สายใหม่หลัง kill switch'))
  assert.equal(newMetrics.legacyObserved, false, 'สายใหม่ที่เริ่มหลัง kill switch ต้องได้ CONTROL ทันที')
  assert.equal(newMetrics.legacyClaudeOutcome, null)

  // สายเก่าที่ปิดไปแล้วไม่เกี่ยว — ยืนยันแค่ค่าที่ capture ไว้ตอนสายเก่ายังเปิดอยู่ (oldMetrics) ไม่ถูกเขียนทับย้อนหลัง
  assert.equal(oldMetrics.legacyObserved, true)

  harness.disconnect(oldSocket)
  harness.disconnect(newSocket)
})

test('L2a exposure (required test 30): runLegacyFallback (chunked path fallback-to-legacy) ไม่ถูกกระทบจาก exposure gate เลย — ยังใช้ askClaudeStream เสมอไม่ว่า legacyObservedConfig จะเป็นอะไร', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID } // เปิด observed 100% ตั้งใจ — runLegacyFallback ต้องไม่แตะเลย
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 100, sessionOverrides: { campaign: l2aCampaign() } }) // chunked path

  let observedCalls = 0, streamCalls = 0
  state.claudeStreamChunkedImpl = async function* () { throw new Error('chunked boom') } // ล้มทันที → เข้า runLegacyFallback
  state.claudeObservedImpl = async function* () { observedCalls++; yield 'ไม่ควรถูกเรียกเลย' }
  state.claudeStreamImpl = async function* () { streamCalls++; yield 'คำตอบจาก fallback' }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(streamCalls, 1, 'runLegacyFallback ต้องเรียก askClaudeStream เหมือนเดิมทุกประการ')
  assert.equal(observedCalls, 0, 'runLegacyFallback ต้องไม่ถูก gate เปลี่ยนไปเรียก askClaudeObservedFullResponse เลย (นอกขอบเขตของ L2a gate ทั้งหมด)')
  assert.equal(metrics.legacyClaudeOutcome, null, 'fallback ไม่ใช่ fresh-call branch ที่ gate แตะ — legacyClaude* ต้องยัง null')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length > 0, 'fallback ต้องพูดออกมาได้จริง')

  harness.disconnect(socket)
})

// ===== L2b — conditional legacy early TTS wiring (design locked 2026-08-21, review round 3) =====
// พิสูจน์ 4 gate criteria ที่ผู้ใช้ระบุไว้เป็นพิเศษ: (1) Claude timeout หลัง audio commit ต้องไม่ kill/replay,
// (2) tail TTS fail หลัง commit ต้องไม่ restart, (3) history เขียน finalText ครั้งเดียวจาก milestone ไม่ใช่จาก
// การต่อ chunk, (4) chunked/legacyObserved/legacyEarlyTts ต้องไม่มีทาง active พร้อมกัน — รวมถึง END_CALL
// guard-and-reset (mandatory refinement 3) และ signal separation (mandatory refinement 1)

test('L2b precedence (required criterion 4): chunked=true → legacyEarlyTts=false เสมอ ไม่ว่า earlyTts config จะ match แค่ไหน', async () => {
  const callSid = 'CA_L2B_PRECEDENCE_1'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 100, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeStreamChunkedImpl = async function* () { yield 'ตอบจาก chunked' }

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.path, 'chunked')
  assert.equal(metrics.legacyEarlyTts, false)
  assert.equal(metrics.legacyEarlyTtsOutcome, null)
  // Track L — chunked path ไม่เคยเรียก askClaudeConditionalStream เลย ต้องไม่มี l2b* field ไหนถูก populate
  assert.equal(metrics.l2bSystemPromptCharCount, null)
  assert.equal(metrics.l2bPriorHistoryCharCount, null)
  assert.equal(metrics.l2bRequestMessageCount, null)
  assert.equal(metrics.l2bCurrentUserCharCount, null)
  assert.equal(metrics.l2bApproxInputTextCharCount, null)
  assert.equal(metrics.l2bResponseCharCount, null)
  // Track M — เช่นกัน ไม่มี l2bChunk* field ไหนถูก populate สำหรับ chunked path
  assert.equal(metrics.l2bChunkReason, null)
  assert.equal(metrics.l2bChunkCharCount, null)
  assert.equal(metrics.l2bChunkDeltaCount, null)
  assert.equal(metrics.l2bChunkFirstCandidateElapsedMs, null)
  assert.equal(metrics.l2bChunkNumericProtectionBlocked, null)
  assert.equal(metrics.l2bChunkPreSafeDeltaGapMs, null)
  harness.disconnect(socket)
})

test('L2b precedence (required criterion 4): legacyObserved=true → legacyEarlyTts=false เสมอ แม้ earlyTts campaign+bucket จะผ่านเกณฑ์ของตัวเองก็ตาม', async () => {
  const callSid = 'CA_L2B_PRECEDENCE_2'
  harness.getState().legacyObservedConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID }
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2A_CAMPAIGN_ID } // campaign เดียวกับ observed ตั้งใจ — ให้ earlyTts เอง "ผ่านเกณฑ์" ถ้าดูตัวมันเองเฉยๆ
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2aCampaign() } })

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyObserved, true)
  assert.equal(metrics.legacyEarlyTtsCampaignMatched, true, 'earlyTts campaign match เองต้องผ่าน (พิสูจน์ว่า precedence เป็นตัวบังคับจริง ไม่ใช่แค่บังเอิญ campaign ไม่ตรง)')
  assert.equal(metrics.legacyEarlyTts, false, 'legacyObserved ต้อง win แม้ legacyEarlyTts เองจะ qualify')
  assert.equal(metrics.legacyEarlyTtsOutcome, null)
  // Track L — legacyObserved path ก็ไม่เคยเรียก askClaudeConditionalStream เช่นกัน ต้องไม่มี l2b* field ไหนถูก populate
  assert.equal(metrics.l2bSystemPromptCharCount, null)
  assert.equal(metrics.l2bPriorHistoryCharCount, null)
  assert.equal(metrics.l2bRequestMessageCount, null)
  assert.equal(metrics.l2bCurrentUserCharCount, null)
  assert.equal(metrics.l2bApproxInputTextCharCount, null)
  assert.equal(metrics.l2bResponseCharCount, null)
  // Track M — เช่นกัน
  assert.equal(metrics.l2bChunkReason, null)
  assert.equal(metrics.l2bChunkCharCount, null)
  assert.equal(metrics.l2bChunkDeltaCount, null)
  assert.equal(metrics.l2bChunkFirstCandidateElapsedMs, null)
  assert.equal(metrics.l2bChunkNumericProtectionBlocked, null)
  assert.equal(metrics.l2bChunkPreSafeDeltaGapMs, null)
  harness.disconnect(socket)
})

test('L2b precedence (required criterion 4): chunked=false, legacyObserved=false, campaign match + bucket ผ่านเกณฑ์ → legacyEarlyTts=true', async () => {
  const callSid = 'CA_L2B_PRECEDENCE_3' // bucket=43
  harness.getState().legacyEarlyTtsConfig = { percent: 50, campaignId: L2B_CAMPAIGN_ID } // 43<50 qualifies
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyObserved, false)
  assert.equal(metrics.legacyEarlyTts, true)
  assert.equal(metrics.legacyEarlyTtsOutcome, 'COMPLETED')
  harness.disconnect(socket)
})

// ===== Track L — Claude request/response size diagnostics wiring (design locked 2026-08-22, R3) =====
// Track L's actual computation (inputStats/responseCharCount) is proven against real claude.js in
// test/claudeConditional.test.js — this section proves only the consumer-side wiring: onEarlyTtsMilestone
// ใน audioStream.js ต้อง map ค่าที่ milestone ส่งมาเข้า turnMetrics.l2b* ให้ตรงเป๊ะ และ malformed payload ต้อง
// fail-safe เป็น null โดยไม่กระทบ [Metrics] log ส่วนที่เหลือ

test('Track L wiring: onMilestone("inputStats", {...}) และ ("responseCharCount", N) ต้อง map เข้า turnMetrics.l2b* ตรงเป๊ะใน [Metrics] log', async () => {
  const callSid = 'CA_L2B_TRACKL_WIRING'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('inputStats', {
      systemPromptCharCount: 321,
      priorHistoryCharCount: 45,
      requestMessageCount: 7,
      currentUserCharCount: 12,
      approxInputTextCharCount: 378,
    })
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'คำตอบทดสอบ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'คำตอบทดสอบ')
    onMilestone?.('responseCharCount', 'คำตอบทดสอบ'.length)
    onMilestone?.('endCallRequested', false)
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.l2bSystemPromptCharCount, 321)
  assert.equal(metrics.l2bPriorHistoryCharCount, 45)
  assert.equal(metrics.l2bRequestMessageCount, 7)
  assert.equal(metrics.l2bCurrentUserCharCount, 12)
  assert.equal(metrics.l2bApproxInputTextCharCount, 378)
  assert.equal(metrics.l2bResponseCharCount, 'คำตอบทดสอบ'.length)
  harness.disconnect(socket)
})

test('Track L wiring: inputStats(null) จาก producer (เช่น computation ฝั่ง claude.js throw) → ทุก l2b* field เป็น null ไม่กระทบ [Metrics] log ส่วนอื่น', async () => {
  const callSid = 'CA_L2B_TRACKL_NULL_INPUTSTATS'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('inputStats', null)
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'คำตอบทดสอบ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'คำตอบทดสอบ')
    onMilestone?.('endCallRequested', false)
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.l2bSystemPromptCharCount, null)
  assert.equal(metrics.l2bPriorHistoryCharCount, null)
  assert.equal(metrics.l2bRequestMessageCount, null)
  assert.equal(metrics.l2bCurrentUserCharCount, null)
  assert.equal(metrics.l2bApproxInputTextCharCount, null)
  assert.equal(metrics.l2bResponseCharCount, null, 'ไม่มี responseCharCount milestone ยิงเลยในเทสนี้ ต้องเหลือ default null')
  assert.equal(metrics.legacyEarlyTtsOutcome, 'COMPLETED', 'inputStats(null) ต้องไม่กระทบ path การันตี turn อื่นๆ เลย')
  harness.disconnect(socket)
})

// ===== Track M — chunk boundary telemetry wiring (design locked 2026-08-22, R3) =====
// Track M's actual computation (reason/candidate/gap/deltaCount) is proven against real claude.js in
// test/claudeConditional.test.js — this section proves only the consumer-side wiring: onEarlyTtsMilestone
// ใน audioStream.js ต้อง map ค่าที่ chunkReasonStats ส่งมาเข้า turnMetrics.l2bChunk* ให้ตรงเป๊ะ และ malformed
// payload ต้อง fail-safe เป็น null โดยไม่กระทบ [Metrics] log ส่วนที่เหลือ

test('Track M wiring: onMilestone("chunkReasonStats", {...}) ต้อง map เข้า turnMetrics.l2bChunk* ตรงเป๊ะใน [Metrics] log', async () => {
  const callSid = 'CA_L2B_TRACKM_WIRING'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('firstDeltaAt', Date.now())
    onMilestone?.('chunkReasonStats', {
      reason: 'NATURAL_BOUNDARY_HARD_MAX',
      charCount: 42,
      deltaCount: 3,
      firstCandidateElapsedMs: 350,
      numericProtectionBlocked: true,
      preSafeDeltaGapMs: 480,
    })
    onMilestone?.('firstSafeAt', Date.now())
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'คำตอบทดสอบ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'คำตอบทดสอบ')
    onMilestone?.('endCallRequested', false)
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.l2bChunkReason, 'NATURAL_BOUNDARY_HARD_MAX')
  assert.equal(metrics.l2bChunkCharCount, 42)
  assert.equal(metrics.l2bChunkDeltaCount, 3)
  assert.equal(metrics.l2bChunkFirstCandidateElapsedMs, 350)
  assert.equal(metrics.l2bChunkNumericProtectionBlocked, true)
  assert.equal(metrics.l2bChunkPreSafeDeltaGapMs, 480)
  harness.disconnect(socket)
})

test('Track M wiring — Review Fix 1: chunkReasonStats ที่เป็น object จริงแต่ field ผิดรูปแบบ (reason ไม่อยู่ใน enum, charCount เป็น string, deltaCount null, firstCandidateElapsedMs=NaN, numericProtectionBlocked เป็น string, preSafeDeltaGapMs ติดลบ) → ทุก l2bChunk* field ต้องเป็น null ทั้ง 6 ไม่ partial และไม่กระทบ turn จริง', async () => {
  const callSid = 'CA_L2B_TRACKM_MALFORMED'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('firstDeltaAt', Date.now())
    onMilestone?.('chunkReasonStats', {
      reason: 'BAD_REASON',
      charCount: 'abc',
      deltaCount: null,
      firstCandidateElapsedMs: NaN,
      numericProtectionBlocked: 'yes',
      preSafeDeltaGapMs: -10,
    })
    onMilestone?.('firstSafeAt', Date.now())
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'คำตอบทดสอบ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'คำตอบทดสอบ')
    onMilestone?.('endCallRequested', false)
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.l2bChunkReason, null, 'reason ไม่อยู่ใน CHUNK_REASON enum ต้องไม่ผ่าน')
  assert.equal(metrics.l2bChunkCharCount, null)
  assert.equal(metrics.l2bChunkDeltaCount, null)
  assert.equal(metrics.l2bChunkFirstCandidateElapsedMs, null)
  assert.equal(metrics.l2bChunkNumericProtectionBlocked, null)
  assert.equal(metrics.l2bChunkPreSafeDeltaGapMs, null)
  assert.equal(metrics.legacyEarlyTtsOutcome, 'COMPLETED', 'malformed payload ต้องไม่กระทบ turn จริงเลย')
  harness.disconnect(socket)
})

test('Track M wiring: ไม่มี chunkReasonStats milestone ยิงเลย (chunkDelay=null เคส) → ทุก l2bChunk* field เหลือ default null ไม่กระทบ [Metrics] log ส่วนอื่น', async () => {
  const callSid = 'CA_L2B_TRACKM_NULL'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'คำตอบทดสอบ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'คำตอบทดสอบ')
    onMilestone?.('endCallRequested', false)
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.l2bChunkReason, null)
  assert.equal(metrics.l2bChunkCharCount, null)
  assert.equal(metrics.l2bChunkDeltaCount, null)
  assert.equal(metrics.l2bChunkFirstCandidateElapsedMs, null)
  assert.equal(metrics.l2bChunkNumericProtectionBlocked, null)
  assert.equal(metrics.l2bChunkPreSafeDeltaGapMs, null)
  assert.equal(metrics.legacyEarlyTtsOutcome, 'COMPLETED', 'ไม่มี chunkReasonStats ต้องไม่กระทบ path การันตี turn อื่นๆ เลย')
  harness.disconnect(socket)
})

test('L2b (required, mandatory refinement 1 — signal separation): Claude tail timeout หลัง audio commit แล้ว → ไม่พูด recovery ทับ ไม่ fabricate history ไม่มี media event เพิ่มหลัง commit', { timeout: 15000 }, async () => {
  const callSid = 'CA_L2B_TIMEOUT_POSTCOMMIT'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('firstDeltaAt', Date.now())
    onMilestone?.('firstSafeAt', Date.now())
    onMilestone?.('mode', 'CHUNKED')
    yield 'เริ่มพูดไปแล้วค่ะ' // chunk แรก — ต้องถูกพูด/commit จริง
    await new Promise(() => {}) // ค้างตลอดไป — จำลอง Claude tail ไม่ตอบต่อ ให้ watchdog (80ms override) timeout จริง
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsOutcome, 'TIMEOUT_POSTCOMMIT')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 1, 'ต้องมีแค่ chunk แรกที่ commit ไปแล้ว ห้ามมี recovery phrase มาต่อท้าย')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.notEqual(lastAssistant?.content, LEGACY_RECOVERY_PHRASE, 'ห้ามพูด recovery phrase ทับเสียงที่ commit ไปแล้ว')
  harness.disconnect(socket)
})

// BLOCKER D (design review round 4) — runAttemptWithWatchdog() ไม่รอ attemptPromise (loser) settle ก่อน return
// เมื่อ watchdog ชนะ race — speakFixedText() ผูกกับ outer signal (ตั้งใจ, ไม่ผูก childSignal) จึงยังทำงานต่อได้
// เบื้องหลังหลัง runAttemptWithWatchdog คืนค่าไปแล้ว สองเทสนี้เจาะเคสที่ watchdog fire ขณะ speakFixedText() ยัง
// in-flight อยู่จริง (ttsImpl หน่วง 300ms > watchdog override 80ms) พิสูจน์ว่า outcome-branching รอ loser จริงก่อน
// ตัดสินใจ ไม่ race กับ turnState.audioCommitted

test('L2b (BLOCKER D fix, race case 1): watchdog fires ขณะ speakFixedText() ยัง in-flight (loser) แล้ว loser สำเร็จ commit จริง → ต้องรอ loser ก่อน แล้วได้ TIMEOUT_POSTCOMMIT ไม่ race เป็น PRECOMMIT ผิดๆ', { timeout: 15000 }, async () => {
  const callSid = 'CA_L2B_RACE_INFLIGHT_SUCCESS'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })

  state.ttsImpl = async function* () {
    await delay(300) // ช้ากว่า watchdog override (80ms) มาก — speakFixedText() ยังรอ ElevenLabs อยู่ตอน watchdog fire แน่นอน
    yield Buffer.from('audio-chunk')
  }
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'CHUNKED')
    yield 'เริ่มพูดช้าๆ'
    await new Promise(() => {}) // ไม่เคย fullAt/delta ต่อ — Claude ยังไม่จบ ระหว่างที่ speakFixedText กำลังรอ TTS ของ chunk แรกอยู่
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsOutcome, 'TIMEOUT_POSTCOMMIT',
    'ต้องรอ loser (speakFixedText ที่กำลัง commit อยู่) จบก่อนตัดสิน ไม่อ่าน audioCommitted=false ก่อนเวลาแล้ว classify PRECOMMIT ผิด')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 1, 'ต้องมีแค่ chunk เดียวจาก loser ที่ commit สำเร็จ ไม่มี recovery phrase มาแทรกซ้อน/ซ้ำ')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.notEqual(lastAssistant?.content, LEGACY_RECOVERY_PHRASE, 'ต้องไม่พูด recovery ทับเสียงที่ loser กำลังจะ commit')
  harness.disconnect(socket)
})

test('L2b (BLOCKER D fix, race case 2): watchdog fires ขณะ speakFixedText() ยัง in-flight (loser) แล้ว loser พังจริง (ไม่เคย commit) → ยัง TIMEOUT_PRECOMMIT ถูกต้อง พูด recovery ได้ครั้งเดียว ไม่ race/ซ้ำ', { timeout: 15000 }, async () => {
  const callSid = 'CA_L2B_RACE_INFLIGHT_FAIL'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })

  let ttsCallCount = 0
  state.ttsImpl = async function* () {
    ttsCallCount++
    if (ttsCallCount === 1) {
      await delay(300) // ช้ากว่า watchdog override (80ms) — ยังไม่ทันจบตอน watchdog fire — เฉพาะ loser (chunk แรก) เท่านั้นที่พัง
      throw new Error('ElevenLabs ล้มขณะ loser ยังรอ')
    }
    yield Buffer.from('recovery-audio') // recovery phrase (เรียกทีหลัง คนละ speakFixedText call) ต้องสำเร็จปกติ
  }
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'CHUNKED')
    yield 'พยายามพูดแต่จะพัง'
    await new Promise(() => {})
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsOutcome, 'TIMEOUT_PRECOMMIT', 'loser TTS พังโดยไม่เคย commit จริง (audioCommitted ยัง false) ต้องยัง PRECOMMIT ถูกต้อง')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, LEGACY_RECOVERY_PHRASE, 'precommit ต้องพูด recovery ได้ตามปกติ')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 1, 'ต้องมีแค่ recovery phrase เดียว ไม่ race/ซ้อนกับความพยายามเดิมที่พังไปแล้ว')
  harness.disconnect(socket)
})

test('L2b (required criterion 1): Claude timeout ก่อน commit เสียงเลย (precommit) → ยังพูด recovery phrase ได้ตามปกติ (เหมือน legacy เดิม)', { timeout: 15000 }, async () => {
  const callSid = 'CA_L2B_TIMEOUT_PRECOMMIT'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = () => (async function* () { await new Promise(() => {}) })() // ไม่เคย yield อะไรเลย ไม่มี delta ใดๆ มาถึง

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsOutcome, 'TIMEOUT_PRECOMMIT')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, LEGACY_RECOVERY_PHRASE, 'precommit timeout ต้องพูด recovery phrase ปกติ (ไม่มีอะไรให้ preserve)')
  harness.disconnect(socket)
})

test('L2b (required criterion 2, mandatory refinement 1): tail TTS error หลัง commit แล้ว → ไม่ restart จากต้น ไม่ resend chunk แรกซ้ำ ไม่ fabricate history', async () => {
  const callSid = 'CA_L2B_TAIL_TTS_FAIL'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  let ttsCallCount = 0
  state.ttsImpl = async function* () {
    ttsCallCount++
    if (ttsCallCount === 1) { yield Buffer.from('audio-chunk-1'); return }
    throw new Error('ElevenLabs tail boom')
  }
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'CHUNKED')
    yield 'ก้อนแรกพูดสำเร็จ'
    yield 'ก้อนสองจะพัง'
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsOutcome, 'ERROR_POSTCOMMIT')
  assert.equal(ttsCallCount, 2, 'ต้องพยายามพูด chunk สองจริง ไม่ retry/restart จาก chunk แรกใหม่')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 1, 'ต้องมีแค่ chunk แรกที่ส่งสำเร็จ ไม่มีการ resend/restart')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.notEqual(lastAssistant?.content, LEGACY_RECOVERY_PHRASE)
  harness.disconnect(socket)
})

test('L2b (required criterion 3, mandatory refinement 2): history เขียนจาก finalText milestone เท่านั้น ไม่ใช่การต่อ chunk ที่พูดไป', async () => {
  const callSid = 'CA_L2B_HISTORY'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'CHUNKED')
    yield 'พูดไปก้อนหนึ่ง' // สิ่งที่ลูกค้าได้ยินจริง
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'ข้อความ canonical จริงที่ไม่ตรงกับ chunk ที่พูดไปเป๊ะ') // ตั้งใจให้ต่างกัน พิสูจน์ source
    onMilestone?.('endCallRequested', false)
  })()

  await harness.sendFinalTranscript('ทดสอบ')

  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, 'ข้อความ canonical จริงที่ไม่ตรงกับ chunk ที่พูดไปเป๊ะ',
    'history ต้องมาจาก finalText milestone ตรงๆ ไม่ใช่จาก chunk ที่ยิง TTS ออกไป')
  harness.disconnect(socket)
})

// BLOCKER F (design review round 4) — representative [Metrics] examples: CHUNKED success ที่มี >=2 speech
// segment จริง (เทสอื่นๆ ก่อนหน้ามีแค่ SINGLE_SHOT success หรือ CHUNKED ที่ commit แค่ 1 chunk ก่อนพัง/timeout)
test('L2b telemetry (BLOCKER F): CHUNKED success ที่มี 2 segment จริง → t5/t6/t7 first-only ตลอดทั้งเทิร์น, segment count ถูกต้อง, legacyEarlyTts* ครบ', async () => {
  const callSid = 'CA_L2B_CHUNKED_MULTI_SEGMENT'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('firstDeltaAt', Date.now())
    onMilestone?.('firstSafeAt', Date.now())
    onMilestone?.('mode', 'CHUNKED')
    yield 'ประโยคแรกที่พูดก่อน.'
    yield 'ประโยคที่สองที่พูดต่อ.'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'ประโยคแรกที่พูดก่อน. ประโยคที่สองที่พูดต่อ.')
    onMilestone?.('endCallRequested', false)
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsMode, 'CHUNKED')
  assert.equal(metrics.legacyEarlyTtsOutcome, 'COMPLETED')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.equal(mediaEvents.length, 2, 'ต้องมี 2 segment จริง (2 chunk = 2 synthesizeSpeechStream request แยกกัน ผ่าน stub ที่ yield 1 audio chunk ต่อ 1 เรียก)')
  // t5/t6/t7 (canonical) ต้อง first-only ตลอดทั้งเทิร์น — markOnce() เดิมของ synthesizeAndSend รับประกันสิ่งนี้อยู่
  // แล้วโดยไม่ต้องเขียนโค้ดเพิ่มฝั่ง L2b เลย (ใช้ speakFixedText/synthesizeAndSend ตัวเดียวกับ chunked path)
  assert.ok(metrics.t5 != null && metrics.t6 != null && metrics.t7 != null)
  console.log('[BLOCKER F example] CHUNKED success (2 segments):', JSON.stringify(metrics))
  harness.disconnect(socket)
})

test('L2b (required, mandatory refinement 3): endCallRequested + shouldBlockEndCall=true → follow-up ถูกพูด, endCallRequested reset เป็น false, ไม่ hangup', async () => {
  const callSid = 'CA_L2B_ENDCALL_BLOCK'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'ขอบคุณค่ะ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'ขอบคุณค่ะ') // ไม่มีคำว่า "เพิ่มเติม" — shouldBlockEndCall ต้อง block
    onMilestone?.('endCallRequested', true)
  })()

  await harness.sendFinalTranscript('สนใจครับ') // มี "สนใจ" ไม่มี negation → hasInterest=true, hasNegation=false

  assert.equal(session.hangupReason, undefined, 'ต้องไม่ hangup เพราะ guard block ไปแล้ว (endCallRequested ต้องถูก reset)')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  // BLOCKER E (design review round 4) — exact match ไม่ใช่แค่ .includes(): history ต้องสะท้อน "Claude finalText +
  // follow-up" เป๊ะๆ เหมือนที่ลูกค้าได้ยินจริง (canonicalFinalText='ขอบคุณค่ะ' + ' ' + followUp) เพื่อให้เทิร์นถัดไปที่
  // Claude เห็น history นี้ รู้ว่า AI เพิ่งถามคำถามเพิ่มเติมไปแล้วจริง ไม่ใช่แค่ "มี follow-up คำบางคำปนอยู่ที่ไหนก็ได้"
  assert.equal(lastAssistant?.content, 'ขอบคุณค่ะ มีอะไรสอบถามเพิ่มเติมไหมคะ',
    'history ต้องเป็น canonicalFinalText + follow-up เป๊ะ ไม่ใช่แค่มีคำว่า follow-up ปนอยู่')
  assert.ok(!lastAssistant?.content.includes('END_CALL'), 'marker ต้องไม่หลุดเข้า history เด็ดขาด')
  const mediaEvents = socket.sent.filter(e => e.event === 'media')
  assert.ok(mediaEvents.length >= 2, 'ต้องพูดทั้งคำตอบเดิม + follow-up (คนละ TTS request กัน)')
  harness.disconnect(socket)
})

test('L2b: endCallRequested + shouldBlockEndCall=false → hangup เกิดจริง (ไม่ถูก block)', async () => {
  const callSid = 'CA_L2B_ENDCALL_ALLOW'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'SINGLE_SHOT')
    yield 'ขอบคุณที่สนใจนะคะ'
    onMilestone?.('fullAt', Date.now())
    onMilestone?.('finalText', 'ขอบคุณที่สนใจนะคะ')
    onMilestone?.('endCallRequested', true)
  })()

  await harness.sendFinalTranscript('ไม่สนใจค่ะ') // negation → hasNegation=true → shouldBlockEndCall=false

  assert.equal(session.hangupReason, 'ai_ended')
  harness.disconnect(socket)
})

test('L2b (required): prewarm HIT → บายพาส askClaudeConditionalStream ทั้งหมด ไม่มี legacyEarlyTts telemetry ใดๆ ถูก fabricate', async () => {
  const callSid = 'CA_L2B_PREWARM_HIT'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })
  let conditionalCalls = 0
  state.claudeConditionalImpl = () => { conditionalCalls++; return (async function* () { yield 'ไม่ควรถูกเรียก' })() }
  state.claudeStreamImpl = async function* () { yield 'คำตอบจาก prewarm' }

  harness.sendInterim('อยากทราบโปรโมชั่นสมาชิกใหม่')
  await delay(30)
  const metrics = await captureMetrics(() => harness.sendFinalTranscript('อยากทราบโปรโมชั่นสมาชิกใหม่ครับ'))

  assert.equal(conditionalCalls, 0, 'prewarm HIT ต้องบายพาส L2b ทั้งหมด ไม่เรียก askClaudeConditionalStream เลย')
  assert.equal(metrics.legacyEarlyTtsOutcome, null)
  assert.equal(metrics.legacyEarlyTtsRequestAt, null)
  harness.disconnect(socket)
})

test('L2b (design review round 4): fullAt disarms watchdog — TTS หลัง Claude จบแล้วช้ากว่า watchdog window รวม ต้องไม่ถูกนับเป็น Claude timeout', { timeout: 15000 }, async () => {
  const callSid = 'CA_L2B_FULLAT_DISARM'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })

  // Claude "จบ" (fullAt) ทันทีตั้งแต่ก่อน TTS เริ่มด้วยซ้ำ แต่ TTS ของ chunk เดียวใช้เวลานานกว่า watchdog override
  // (80ms) มาก — ถ้า fullAt ไม่ disarm watchdog จริง จะ fire ผิดๆ กลายเป็น timeout ทั้งที่ Claude ตอบสำเร็จไปแล้ว
  state.ttsImpl = async function* () {
    await delay(200)
    yield Buffer.from('audio-chunk')
  }
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    onMilestone?.('mode', 'SINGLE_SHOT')
    onMilestone?.('fullAt', Date.now()) // Claude จบก่อน TTS จะเริ่มด้วยซ้ำ — ต้อง disarm watchdog ตรงนี้
    onMilestone?.('finalText', 'คำตอบสำเร็จเร็วมาก')
    onMilestone?.('endCallRequested', false)
    yield 'คำตอบสำเร็จเร็วมาก'
  })()

  const metrics = await captureMetrics(() => harness.sendFinalTranscript('ทดสอบ'))

  assert.equal(metrics.legacyEarlyTtsOutcome, 'COMPLETED',
    'Claude จบไปแล้วจริงตั้งแต่ก่อน TTS เริ่ม แม้ TTS จะช้ากว่า watchdog window ก็ไม่ควรถูกนับเป็น Claude timeout')
  assert.ok(metrics.legacyEarlyTtsFullAt != null)
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.equal(lastAssistant?.content, 'คำตอบสำเร็จเร็วมาก')
  harness.disconnect(socket)
})

test('L2b: barge-in ก่อน Claude commit เสียงใดๆ เลย → outcome=ABORTED ไม่พูด recovery ไม่ fabricate history', async () => {
  const callSid = 'CA_L2B_ABORTED'
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, session, state } = await connectPastGreeting(callSid, { rolloutPercent: 0, sessionOverrides: { campaign: l2bCampaign() } })

  let resumeOldTurn
  const oldTurnGate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
    onMilestone?.('requestAt', Date.now())
    await oldTurnGate
    yield 'ไม่ควรมาถึง'
  })()

  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }

  const oldTurnPromise = harness.sendFinalTranscript('คำถามแรกครับ')
  await delay(30) // ให้เทิร์นแรกเข้าสู่ fresh-call (L2b) แล้วจริง

  state.claudeStreamImpl = async function* () { yield 'ตอบเรื่องใหม่' } // เทิร์นใหม่หลัง barge-in ไม่ผ่าน L2b (legacyEarlyTtsConfig ยังเปิดอยู่ แต่ config เดิม frozen ต่อสายแล้ว ไม่กระทบ — ยังเป็น legacyEarlyTts เดิม แต่ใช้ default claudeConditionalImpl)
  state.claudeConditionalImpl = null
  await harness.sendFinalTranscript('เดี๋ยวก่อนครับ ขอถามเรื่องอื่นก่อน') // ยาวพอ trigger bargeIn จริง

  resumeOldTurn()
  await oldTurnPromise
  await delay(20)
  console.log = originalLog

  const oldMetricsLine = logs.find(l => l.includes('[Metrics]') && l.includes('"generationId":1,'))
  assert.ok(oldMetricsLine, 'ต้องเจอ [Metrics] log ของเทิร์นแรก (generationId=1)')
  const oldMetrics = JSON.parse(oldMetricsLine.slice(oldMetricsLine.indexOf('{')))
  assert.equal(oldMetrics.legacyEarlyTtsOutcome, 'ABORTED')
  const lastAssistant = session.messages.filter(m => m.role === 'assistant').at(-1)
  assert.notEqual(lastAssistant?.content, LEGACY_RECOVERY_PHRASE)
  harness.disconnect(socket)
})
