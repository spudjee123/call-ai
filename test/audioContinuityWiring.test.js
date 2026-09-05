// Track A (AudioContinuity Wiring Fix, production audit 2026-09-05) — production audit found recordFrameSent()
// was only ever wired into audioStream.js's own hand-rolled CONTROL/L2a inline TTS loops (lines that predate
// chunkedTurn.js entirely), never into speakFixedText()/synthesizeAndSend()/adoptChunkedProducer()/
// runChunkedTurn() in chunkedTurn.js — which is what L2b (legacyEarlyTts, ~100% of real production traffic
// per the audit) and the chunked path actually send audio through. Result: [AudioContinuity] misclassified
// nearly all real turns as NO_AUDIO/PRECOMMIT_BARGE even when audio was genuinely sent and played, making
// candidateToFirstAudioMs/firstAudioToClearMs/outboundP95FrameGapMs/outboundMaxFrameGapMs permanently null.
//
// This file proves the fix end-to-end through the real WS handler (not just chunkedTurn.js's own unit tests,
// see chunkedTurn.test.js for those) — instrumentation-only, zero customer-facing behavior change: every
// assertion here that touches socket.sent/session.messages must match pre-fix behavior exactly; the only
// thing allowed to differ is the [AudioContinuity] log content itself.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const callSessions = require('../src/utils/callSessions')
const harness = require('./_audioStreamHarness')

process.env.NODE_ENV = 'test'

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

function makeSession(overrides = {}) {
  return {
    name: 'ทดสอบ',
    campaign: { voice_id: 'voice1', script: 'ระบบทดสอบ' },
    messages: [],
    ...overrides,
  }
}

const L2B_CAMPAIGN_ID = 'CAMPAIGN_AC_WIRING_L2B_TEST'
function l2bCampaign(overrides = {}) {
  return { voice_id: 'voice1', script: 'ระบบทดสอบ', id: L2B_CAMPAIGN_ID, ...overrides }
}

let callSidCounter = 0
function nextCallSid() { callSidCounter++; return `CA_ACWIRING_${callSidCounter}` }

async function connectPastGreeting(callSid, { sessionOverrides = {} } = {}) {
  const state = harness.getState()
  state.rolloutPercent = 0
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')], ...sessionOverrides })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(2000) // 300ms (greeting timer) + 1520ms (unlock ของ greeting เอง) + margin
  socket.sent.length = 0
  return { socket, session, state }
}

// จับ [AudioContinuity] JSON line จากช่วงที่ fn() รัน
async function captureAudioContinuity(fn) {
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }
  try {
    await fn()
  } finally {
    console.log = originalLog
  }
  const lines = logs.filter(l => l.includes('[AudioContinuity]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
  return lines
}

harness.ensureStubbed()

beforeEach(() => {
  const state = harness.getState()
  state.claudeStreamImpl = async function* () { yield 'default response.' }
  state.claudeStreamChunkedImpl = async function* () {}
  state.ttsImpl = async function* () { yield Buffer.from('audio') }
  state.rolloutPercent = 0
  state.legacyObservedConfig = { percent: 0, campaignId: null }
  state.legacyEarlyTtsConfig = { percent: 0, campaignId: null }
  state.sttA2Config = { percent: 0, campaignId: null }
  state.sttA2ShadowConfig = { percent: 0, campaignId: null }
  state.claudeConditionalImpl = null
})

test('L2b (legacyEarlyTts) primary speech path: เสียงที่ส่งจริงต้องถูกนับ (framesSent>0, providerFirstAudioAt ตั้งค่าจริง) — ก่อนแก้เคยเป็น NO_AUDIO ผิดๆ', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { sessionOverrides: { campaign: l2bCampaign() } })
  try {
    state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
      onMilestone?.('requestAt', Date.now())
      onMilestone?.('firstDeltaAt', Date.now())
      onMilestone?.('firstSafeAt', Date.now())
      onMilestone?.('fullAt', Date.now())
      onMilestone?.('mode', 'CHUNKED')
      onMilestone?.('finalText', 'ยอดเงินของคุณคือหนึ่งพันบาทค่ะ')
      onMilestone?.('endCallRequested', false)
      yield 'ยอดเงินของคุณคือหนึ่งพันบาทค่ะ'
    })()

    const lines = await captureAudioContinuity(async () => {
      await harness.sendFinalTranscript('เช็คยอดหน่อยครับ')
    })

    assert.equal(lines.length, 1)
    assert.equal(lines[0].outcome, 'COMPLETED_NO_BARGE', 'มีเสียงจริง ไม่ถูก barge — ต้องไม่ใช่ NO_AUDIO อีกต่อไป')
    assert.ok(lines[0].framesSent >= 1, 'framesSent ต้องนับเฟรมจริงที่ส่งผ่าน speakFixedText() ของ L2b')
    assert.ok(lines[0].providerFirstAudioAt != null)
    assert.ok(lines[0].twilioFirstMediaAt != null)
    assert.ok(lines[0].bytesSent > 0)
    assert.ok(socket.sent.some(e => e.event === 'media'), 'sanity: audio จริงถูกส่งไป Twilio')
  } finally {
    harness.disconnect(socket)
  }
})

test('L2b หลาย speech chunk ต่อเทิร์นเดียว (หลาย speakFixedText call) → framesSent รวมถูกต้อง ไม่ double-count', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { sessionOverrides: { campaign: l2bCampaign() } })
  try {
    state.ttsImpl = async function* () { yield Buffer.from('ab'); yield Buffer.from('cd') } // 2 byte-frame ต่อ TTS call
    state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
      onMilestone?.('requestAt', Date.now())
      onMilestone?.('firstDeltaAt', Date.now())
      onMilestone?.('firstSafeAt', Date.now())
      onMilestone?.('fullAt', Date.now())
      onMilestone?.('mode', 'CHUNKED')
      onMilestone?.('finalText', 'ประโยคแรก ประโยคที่สอง')
      onMilestone?.('endCallRequested', false)
      yield 'ประโยคแรก ' // speakFixedText call #1
      yield 'ประโยคที่สอง' // speakFixedText call #2
    })()

    const lines = await captureAudioContinuity(async () => {
      await harness.sendFinalTranscript('เล่าให้ฟังหน่อย')
    })

    assert.equal(lines.length, 1)
    const mediaSent = socket.sent.filter(e => e.event === 'media').length
    assert.equal(lines[0].framesSent, mediaSent, 'framesSent ต้องตรงกับจำนวน media event ที่ส่งจริงเป๊ะ ไม่มากไม่น้อย')
    assert.equal(lines[0].framesSent, 4, '2 speech chunks x 2 byte-frame ต่อ chunk = 4 เฟรมจริง')
  } finally {
    harness.disconnect(socket)
  }
})

test('L2b: barge-in ที่เกิดหลังเสียงเริ่มเล่นจริงแล้ว (POST_AUDIO_BARGE) — ก่อนแก้เคยถูกจัดผิดเป็น PRECOMMIT_BARGE เสมอเพราะ telemetry บอด (H.1 fix proof)', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state, session } = await connectPastGreeting(callSid, { sessionOverrides: { campaign: l2bCampaign() } })
  try {
    // chunk แรกพูดจริงแล้วค้างตลอดไป (ไม่จบเทิร์นเอง) ให้เรามี "audio กำลังเล่นอยู่" ก่อนสั่ง barge-in ตรงๆ
    state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
      onMilestone?.('requestAt', Date.now())
      onMilestone?.('firstDeltaAt', Date.now())
      onMilestone?.('firstSafeAt', Date.now())
      onMilestone?.('mode', 'CHUNKED')
      yield 'ยอดฝากของคุณคือหนึ่งพันบาท'
      await new Promise(() => {}) // ค้างตลอดไป
    })()

    const firstTurnPromise = harness.sendFinalTranscript('เช็คยอดหน่อยครับ')
    await delay(30) // ให้ chunk แรกถูกพูดออกไปจริงก่อน (activeSpokenRef/providerFirstAudioAt populated)

    state.claudeConditionalImpl = null
    state.claudeStreamImpl = async function* () { yield 'ตอบต่อ' }

    const lines = await captureAudioContinuity(async () => {
      await harness.sendFinalTranscript('เดี๋ยวก่อนครับ') // final ตรงๆ ระหว่าง isSpeaking=true → bargeIn() ทันที
      await delay(100)
      await firstTurnPromise // ต้อง await ให้เทิร์นแรก (ที่ถูก barge) ไหลไปถึง shared tail จริงก่อน — [AudioContinuity] ของมัน log ที่นั่น (pattern เดียวกับ R1.1-3 ใน audioStreamIntegration.test.js)
    })

    assert.ok(lines.length >= 1, 'ต้องมี [AudioContinuity] อย่างน้อย 1 บรรทัดจากเทิร์นที่ถูก barge')
    const bargedLine = lines.find(l => l.clearSentAt != null)
    assert.ok(bargedLine, 'ต้องมี record ที่ clearSentAt ถูกตั้งค่า (แปลว่าถูก barge จริง)')
    assert.equal(bargedLine.outcome, 'POST_AUDIO_BARGE', 'มีเสียงเล่นอยู่ก่อน barge จริง — ต้องไม่ใช่ PRECOMMIT_BARGE (บั๊กเดิมก่อนแก้ Track A)')
    assert.ok(bargedLine.framesSent >= 1)
    assert.ok(bargedLine.providerFirstAudioAt != null)
    // FINAL-triggered barge (ไม่ใช่ INTERIM_CONFIRM) ไม่มี candidateFirstAt เลย (bargeIn() ส่งแค่ candidateConfirmAt/
    // candidateConfirmText สำหรับ trigger นี้ — ดู audioStream.js's bargeIn() call site) → candidateBeforeFirstAudio
    // ต้องเป็น null (คำนวณไม่ได้) ไม่ใช่ false — นี่คือ semantics ที่ถูกต้องอยู่แล้ว ไม่ใช่สิ่งที่ Track A ต้องแก้
    assert.equal(bargedLine.candidateBeforeFirstAudio, null)
  } finally {
    harness.disconnect(socket)
  }
})

test('L2b: barge-in ที่เกิดก่อนมีเสียงส่งเลยสักเฟรม (Claude ยังคิดอยู่) → ยังคง PRECOMMIT_BARGE เหมือนเดิม (ไม่ over-claim ว่ามีเสียง)', async () => {
  const callSid = nextCallSid()
  harness.getState().legacyEarlyTtsConfig = { percent: 100, campaignId: L2B_CAMPAIGN_ID }
  const { socket, state } = await connectPastGreeting(callSid, { sessionOverrides: { campaign: l2bCampaign() } })
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeConditionalImpl = (sess, signal, onMilestone) => (async function* () {
      onMilestone?.('requestAt', Date.now())
      await gate // ไม่มี delta ใดๆ เลยจนกว่าจะถูกปล่อย — จำลอง Claude กำลังคิดอยู่ตอนโดน barge
    })()

    const firstTurnPromise = harness.sendFinalTranscript('เช็คยอดหน่อยครับ')
    await delay(20)

    state.claudeConditionalImpl = null
    state.claudeStreamImpl = async function* () { yield 'ตอบต่อ' }

    const lines = await captureAudioContinuity(async () => {
      await harness.sendFinalTranscript('เดี๋ยวก่อนครับ')
      await delay(100)
      await firstTurnPromise // ต้อง await ให้เทิร์นแรกไหลไปถึง shared tail จริงก่อน (pattern เดียวกับ R1.1-3)
    })

    resumeOldTurn() // ปลด gate ที่เหลือทิ้ง (เทิร์นแรก settle ผ่าน abort-race ของ runAttemptWithWatchdog ไปแล้วจริง ไม่ต้องรอ gate นี้)

    const bargedLine = lines.find(l => l.clearSentAt != null)
    assert.ok(bargedLine, 'ต้องมี record ที่ถูก barge')
    assert.equal(bargedLine.outcome, 'PRECOMMIT_BARGE')
    assert.equal(bargedLine.framesSent, 0, 'ไม่มีเสียงส่งเลยจริงๆ ก่อนโดน barge')
    assert.equal(bargedLine.providerFirstAudioAt, null)
  } finally {
    harness.disconnect(socket)
  }
})

test('CONTROL/L2a recovery phrase (Claude ตอบว่างเปล่า) — ก่อนแก้ recovery phrase ผ่าน speakFixedText() ไม่เคยถูกนับใน AudioContinuity เลย', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid) // default state — CONTROL path (ไม่มี legacyEarlyTts/llmProvider)
  try {
    state.claudeStreamImpl = async function* () { yield '' } // ตอบว่างเปล่า → shouldSpeakRecovery=true → speakFixedText(LEGACY_RECOVERY_PHRASE)

    const lines = await captureAudioContinuity(async () => {
      await harness.sendFinalTranscript('มีโปรอะไรบ้างครับ')
    })

    assert.equal(lines.length, 1)
    assert.equal(lines[0].outcome, 'COMPLETED_NO_BARGE', 'recovery phrase ถูกพูดออกไปจริงและไม่ถูก barge')
    assert.ok(lines[0].framesSent >= 1, 'recovery phrase ที่พูดผ่าน speakFixedText() ต้องถูกนับเฟรมด้วยหลังแก้ Track A')
  } finally {
    harness.disconnect(socket)
  }
})
