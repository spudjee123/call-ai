// BV2-T (Guarded Barge-in V2, telemetry-only, Design Freeze 2026-09-03) — ยืนยันว่า [BARGE_DIAG] emit event
// ตรงกับ branch เดิมเป๊ะ (dumb emitter, ไม่ derive เอง) และไม่มีทางกระทบ behavior เดิมแม้แต่นิดเดียว: ทุก assertion
// ที่เช็ค socket.sent/session.messages ในไฟล์นี้ต้องได้ผลเหมือนก่อน BV2-T ทุกประการ — ผลต่างเดียวที่ยอมรับได้คือ
// log line ใหม่ ไม่ใช่ผลลัพธ์ของสาย
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

let callSidCounter = 0
function nextCallSid() { callSidCounter++; return `CA_BARGEDIAG_${callSidCounter}` }

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

// จับ [BARGE_DIAG] JSON line จากช่วงที่ fn() รัน
async function captureBargeDiag(fn) {
  const originalLog = console.log
  const originalError = console.error
  const logs = []
  const errors = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }
  console.error = (...args) => { errors.push(args.join(' ')); originalError(...args) }
  try {
    await fn()
  } finally {
    console.log = originalLog
    console.error = originalError
  }
  const diagLines = logs.filter(l => l.includes('[BARGE_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
  return { diagLines, logs, errors }
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

// เข้า isSpeaking=true โดย turn เดิมยัง "ค้าง" อยู่ (gate) — ให้ interim ระหว่างนี้เดิน candidate lifecycle ได้จริง
function makeGatedTurn(state, text) {
  let resumeOldTurn
  const gate = new Promise(resolve => { resumeOldTurn = resolve })
  state.claudeStreamImpl = async function* () { yield text; await gate }
  return { resumeOldTurn, gate }
}

test('CANDIDATE_OPENED: interim แรกระหว่าง isSpeaking=true ต้อง emit CANDIDATE_OPENED พร้อม provider/candidateText/ackTier=null (คำทั่วไป ไม่ใช่ ack)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterim('ระบบทดสอบ')
      await delay(5)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].event, 'CANDIDATE_OPENED')
    assert.equal(diagLines[0].callSid, callSid)
    assert.equal(diagLines[0].provider, 'google')
    assert.equal(diagLines[0].candidateText, 'ระบบทดสอบ')
    assert.equal(diagLines[0].ackTier, null)
    assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0, 'สัญญาณเดียวห้าม barge-in จริง (2-signal ยังไม่ครบ)')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('CANDIDATE_OPENED: candidate ที่เปิดจากคำ ack (เช่น "ครับ") ต้อง label ackTier=TIER2 แต่ยังเปิด candidate ตามปกติ (label เท่านั้น ไม่ gate — B1 lock)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterim('ครับ')
      await delay(5)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].event, 'CANDIDATE_OPENED')
    assert.equal(diagLines[0].ackTier, 'TIER2')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('B1 note: "ไม่" ต้อง label ackTier=TIER2 เหมือนเดิม — classifyAck()/TIER2_ACKS เป็น whitelist เก่าของ Design B (final-path defer) คนละกลไกกับ NORMAL/BACKCHANNEL/STRONG_INTERRUPT ที่ B1 ล็อกไว้สำหรับ BV2-B ในอนาคต — BV2-T ห้ามแก้ classifyAck() จึงต้องสะท้อนค่าที่มีอยู่จริงเป๊ะ', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterim('ไม่')
      await delay(5)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].event, 'CANDIDATE_OPENED')
    assert.equal(diagLines[0].candidateText, 'ไม่')
    assert.equal(diagLines[0].ackTier, 'TIER2', 'classifyAck() ต้องไม่ถูกแก้โดย BV2-T — "ไม่" อยู่ใน TIER2_ACKS มาก่อนแล้วจาก Design B')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('REGRESSION: interim ที่สั้นลงจาก candidate เดิม (STT shrink) ต้อง emit REGRESSION และ candidate เดิมยังอยู่ ไม่ reset', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    harness.sendInterim('หนึ่งพันหกร้อยเก้าสิบเจ็ด ค่ะ')
    await delay(5)

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterim('หนึ่งพันหกร้อยเก้าสิบเจ็ด') // สั้นลง (ตัด "ค่ะ" ทิ้ง)
      await delay(5)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].event, 'REGRESSION')
    assert.equal(diagLines[0].candidateText, 'หนึ่งพันหกร้อยเก้าสิบเจ็ดค่ะ')
    assert.equal(diagLines[0].regressedText, 'หนึ่งพันหกร้อยเก้าสิบเจ็ด')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('CANDIDATE_RESET: interim ที่ไม่ต่อเนื่องกับ candidate เดิม (คนละประโยค) ต้อง emit CANDIDATE_RESET แล้วเปิด candidate ใหม่', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    harness.sendInterim('คิดถึง')
    await delay(5)

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterim('ระบบ') // ไม่ใช่ extension ของ "คิดถึง"
      await delay(5)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].event, 'CANDIDATE_RESET')
    assert.equal(diagLines[0].oldCandidateText, 'คิดถึง')
    assert.equal(diagLines[0].newCandidateText, 'ระบบ')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('ECHO_SUPPRESSED: interim ที่เป็นหางของสิ่ง AI กำลังพูดอยู่จริง ต้อง emit ECHO_SUPPRESSED และห้ามเปิด candidate/bargeIn เลย (behavior เดิมไม่เปลี่ยน)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    // เทิร์นจบแบบปกติ (ไม่ gate) — ข้อความเต็มถูกส่งเข้า TTS จริงแล้ว activeSpokenRef จึงมีข้อมูลจริง — isSpeaking
    // ยังเป็น true เพราะไม่ได้ echo mark กลับ (pattern เดียวกับ sttDiagAudioStream.test.js's ACTIVE_PLAYBACK_ECHO test)
    state.claudeStreamImpl = async function* () { yield 'ตอนนี้คุณลูกค้าสะดวกคุยไหมคะ' }
    await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterim('คุยไหมคะ') // หางของสิ่ง AI กำลังพูดอยู่จริง
      await delay(5)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].event, 'ECHO_SUPPRESSED')
    assert.equal(diagLines[0].interimText, 'คุยไหมคะ')
    assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0, 'ห้าม bargeIn() เลยเพราะเป็น echo — behavior เดิม')
  } finally {
    harness.disconnect(socket)
  }
})

test('CONFIRMED (INTERIM_CONFIRM): 2-signal ครบ ต้อง emit CANDIDATE_OPENED แล้วตามด้วย CONFIRMED จาก bargeIn() พร้อม bargeTrigger/candidate fields/ackTier ครบ และต้อง bargeIn() จริง (ส่ง clear)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    const { diagLines } = await captureBargeDiag(async () => {
      harness.sendInterimConfirmed('เดี๋ยวก่อนครับ')
      await delay(5)
    })

    assert.equal(diagLines.length, 2)
    assert.equal(diagLines[0].event, 'CANDIDATE_OPENED')
    assert.equal(diagLines[1].event, 'CONFIRMED')
    const confirmed = diagLines[1]
    assert.equal(confirmed.bargeTrigger, 'INTERIM_CONFIRM')
    assert.equal(confirmed.candidateFirstText, 'เดี๋ยวก่อนครับ')
    assert.equal(confirmed.candidateConfirmText, 'เดี๋ยวก่อนครับ')
    assert.ok(typeof confirmed.candidateFirstAt === 'number')
    assert.ok(typeof confirmed.candidateConfirmAt === 'number')
    assert.equal(confirmed.ackTier, null, '"เดี๋ยวก่อนครับ" ไม่ใช่ TIER1/TIER2 whole-phrase ack')
    assert.ok(socket.sent.some(e => e.event === 'clear'), 'ต้อง bargeIn() จริง — behavior เดิมไม่เปลี่ยน')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('CONFIRMED (FINAL_TIER1): Tier1 ack ("โอเคครับ") ระหว่าง isSpeaking=true ต้อง bargeIn() ทันทีจาก final path พร้อม emit CONFIRMED bargeTrigger=FINAL_TIER1 ackTier=TIER1', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'ตอนนี้คุณลูกค้าสะดวกคุยไหมคะ' }
    await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

    const { diagLines } = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('โอเคครับ')
    })

    const confirmed = diagLines.find(d => d.event === 'CONFIRMED')
    assert.ok(confirmed, 'ต้อง emit CONFIRMED เมื่อ bargeIn() ถูกเรียกจาก final path จริง')
    assert.equal(confirmed.bargeTrigger, 'FINAL_TIER1')
    assert.equal(confirmed.ackTier, 'TIER1')
    assert.equal(confirmed.candidateFirstAt, null, 'final-triggered ไม่มี interim candidate ก่อนหน้า')
    assert.ok(socket.sent.some(e => e.event === 'clear'))
  } finally {
    harness.disconnect(socket)
  }
})

test('Design B ไม่เปลี่ยน: Tier2 ack ("ครับ") เดี่ยวๆ บน final path ระหว่าง isSpeaking=true ต้อง defer เหมือนเดิม (ไม่ bargeIn) → ไม่มี CONFIRMED emit เลย', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'ตอนนี้คุณลูกค้าสะดวกคุยไหมคะ' }
    await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')

    const { diagLines } = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ครับ')
    })

    assert.equal(diagLines.length, 0, 'Tier2 ack ต้อง defer ก่อนถึง bargeIn() เลย — ต้องไม่มี [BARGE_DIAG] event ใดๆ')
    assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0, 'behavior เดิม: ห้าม bargeIn() จาก Tier2 เดี่ยว')
  } finally {
    harness.disconnect(socket)
  }
})

test('[BARGE_DIAG] emit failure ต้องไม่กระทบ call flow เลย (diagnostic failure must never affect call flow)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { resumeOldTurn } = makeGatedTurn(state, 'คำตอบหลักที่กำลังตอบอยู่')
    harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30)

    const originalLog = console.log
    const originalError = console.error
    const errors = []
    console.log = (...args) => { if (args[0]?.includes?.('[BARGE_DIAG]')) throw new Error('boom') ; originalLog(...args) }
    console.error = (...args) => { errors.push(args.join(' ')); originalError(...args) }
    try {
      harness.sendInterim('ระบบทดสอบ')
      await delay(5)
    } finally {
      console.log = originalLog
      console.error = originalError
    }

    assert.ok(errors.some(e => e.includes('[BARGE_DIAG] emit failed')), 'ต้อง catch แล้ว log error แทน ไม่ throw ออกไปกระทบ onInterim')

    resumeOldTurn()
  } finally {
    harness.disconnect(socket)
  }
})

test('providerMeta (BV2-T additive field on [STT_DIAG]): sttMeta ที่มี providerMeta ต้องถูกใส่เข้า [STT_DIAG] log, ไม่มีก็ไม่มี key นี้เลย', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    const originalLog = console.log
    const logs = []
    console.log = (...args) => { logs.push(args.join(' ')) }
    try {
      await harness.sendFinalTranscript('มี providerMeta', {
        streamId: 1, utteranceId: 1, source: 'DEEPGRAM_FINAL',
        interimCount: 0, regressionCount: 0, firstInterimAt: null, lastInterimAt: null,
        finalAt: 100, firstInterimToFinalMs: null, lastStability: null, maxStability: null,
        finalConfidence: null, coldMutePackets: 0,
        providerMeta: { speechFinal: true, requestId: 'req-abc' },
      })
    } finally {
      console.log = originalLog
    }
    const line1 = logs.filter(l => l.includes('[STT_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
    assert.equal(line1.length, 1)
    assert.deepEqual(line1[0].providerMeta, { speechFinal: true, requestId: 'req-abc' })
  } finally {
    harness.disconnect(socket)
  }

  const callSid2 = nextCallSid()
  const { socket: socket2 } = await connectPastGreeting(callSid2)
  try {
    const originalLog = console.log
    const logs = []
    console.log = (...args) => { logs.push(args.join(' ')) }
    try {
      await harness.sendFinalTranscript('ไม่มี providerMeta', {
        streamId: 1, utteranceId: 1, source: 'TIMER_FINAL',
        interimCount: 0, regressionCount: 0, firstInterimAt: null, lastInterimAt: null,
        finalAt: 100, firstInterimToFinalMs: null, lastStability: null, maxStability: null,
        finalConfidence: null, coldMutePackets: 0,
      })
    } finally {
      console.log = originalLog
    }
    const line2 = logs.filter(l => l.includes('[STT_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
    assert.equal(line2.length, 1)
    assert.equal('providerMeta' in line2[0], false, 'Google-style sttMeta (ไม่มี providerMeta) ต้องไม่มี key นี้เลย ไม่ใช่ null')
  } finally {
    harness.disconnect(socket2)
  }
})
