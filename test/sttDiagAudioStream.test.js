// STT-A1 (observability only) — ยืนยัน disposition/reason mapping ที่ audioStream.js emit [STT_DIAG] จริง ตรงกับ
// filter/branch เดิมเป๊ะ (ไม่ derive เอง), และ "diagnostic failure ต้องไม่กระทบ call flow" ตามที่ล็อกไว้
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
function nextCallSid() { callSidCounter++; return `CA_STTDIAG_${callSidCounter}` }

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

// จับ [STT_DIAG] JSON line จากช่วงที่ fn() รัน
async function captureSttDiag(fn) {
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
  const diagLines = logs.filter(l => l.includes('[STT_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
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

const FAKE_META = {
  streamId: 3, utteranceId: 7, source: 'TIMER_FINAL',
  interimCount: 5, regressionCount: 1,
  firstInterimAt: 100, lastInterimAt: 400, finalAt: 900, firstInterimToFinalMs: 800,
  lastStability: null, maxStability: null, finalConfidence: null, coldMutePackets: 0,
}

test('DELIVERED: transcript ที่ผ่านทุก filter ต้อง emit [STT_DIAG] disposition=DELIVERED reason=null พร้อม metadata ที่ส่งเข้ามาครบ', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะ สนใจโปรโมชั่น', FAKE_META)
    })

    assert.equal(diagLines.length, 1)
    const d = diagLines[0]
    assert.equal(d.callSid, callSid)
    assert.equal(d.disposition, 'DELIVERED')
    assert.equal(d.reason, null)
    assert.equal(d.text, 'สวัสดีค่ะ สนใจโปรโมชั่น')
    assert.equal(d.streamId, 3)
    assert.equal(d.utteranceId, 7)
    assert.equal(d.source, 'TIMER_FINAL')
    assert.equal(d.interimCount, 5)
    assert.equal(d.regressionCount, 1)
    assert.equal(d.firstInterimToFinalMs, 800)
  } finally {
    harness.disconnect(socket)
  }
})

test('sttMeta ไม่ถูกส่งมา (undefined) → ไม่ emit [STT_DIAG] เลย แต่ conversation ยังทำงานปกติ (backward-compatible)', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะ') // ไม่ส่ง sttMeta เลย เหมือน caller เก่า
    })

    assert.equal(diagLines.length, 0)
    assert.ok(socket.sent.some(e => e.event === 'media'), 'turn ต้อง process ต่อปกติแม้ไม่มี sttMeta')
  } finally {
    harness.disconnect(socket)
  }
})

test('DROPPED/BARGE_IN_COOLDOWN: transcript ที่มาระหว่าง cooldown 400ms หลัง bargeIn() (final-triggered, ไม่ใช่ interim-triggered) ต้อง emit DROPPED', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true

    // ตัวนี้ trigger bargeIn() ผ่าน final-transcript path (ไม่ใช่ interim) → bargeInCooldown=true, bargeInPendingFinal ยังเป็น false
    await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน')

    const { diagLines } = await captureSttDiag(async () => {
      // ตัวนี้มาระหว่าง cooldown (400ms) และ bargeInPendingFinal=false → ต้องโดน DROPPED/BARGE_IN_COOLDOWN
      await harness.sendFinalTranscript('ข้อความที่มาซ้ำระหว่าง cooldown', FAKE_META)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DROPPED')
    assert.equal(diagLines[0].reason, 'BARGE_IN_COOLDOWN')

    resumeOldTurn()
    await oldTurnPromise
  } finally {
    harness.disconnect(socket)
  }
})

test('DEFERRED/TIER2_ACK: Tier2 ack ("ครับ") ระหว่าง isSpeaking=true ต้อง emit DEFERRED ไม่ใช่ DROPPED', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('ครับ', FAKE_META) // Tier2 ack เดี่ยว
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DEFERRED')
    assert.equal(diagLines[0].reason, 'TIER2_ACK')
    assert.equal(diagLines[0].text, 'ครับ')

    resumeOldTurn()
    await oldTurnPromise
  } finally {
    harness.disconnect(socket)
  }
})

test('DROPPED/SHORT_FRAGMENT_ECHO: fragment สั้นที่ไม่ใช่ ack ระหว่าง isSpeaking=true ต้อง emit DROPPED', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('เอ่อ', FAKE_META) // 1 คำ, สั้นกว่า 8 ตัวอักษร, ไม่ใช่ ack ที่รู้จัก
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DROPPED')
    assert.equal(diagLines[0].reason, 'SHORT_FRAGMENT_ECHO')

    resumeOldTurn()
    await oldTurnPromise
  } finally {
    harness.disconnect(socket)
  }
})

test('DROPPED/POST_MARK_ECHO: fragment สั้นภายใน 500ms หลัง mark echo กลับมา (ไม่ใช่ ack ที่รู้จัก) ต้อง emit DROPPED', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    await harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ') // เทิร์นปกติจบเร็ว (ttsImpl default ไม่ gate)
    const markSent = socket.sent.filter(e => e.event === 'mark').at(-1)
    assert.ok(markSent, 'ต้องมี mark ถูกส่งหลังเทิร์นจบ')
    socket.emit('message', JSON.stringify({ event: 'mark', mark: markSent.mark })) // จำลอง Twilio echo mark กลับมา (ตั้ง lastMarkTime)
    await delay(5)

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('เอ่อ', FAKE_META) // สั้น ไม่ใช่ ack, ภายใน 500ms หลัง mark
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DROPPED')
    assert.equal(diagLines[0].reason, 'POST_MARK_ECHO')
  } finally {
    harness.disconnect(socket)
  }
})

test('lifecycle guard (pendingEndCall) ไม่ emit [STT_DIAG] เลย — ตาม scope ที่ล็อกไว้ว่า A1 ไม่ยุ่งกับ call-shutdown semantics', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'จบการสนทนานะคะ [END_CALL]' }
    await harness.sendFinalTranscript('ไม่มีอะไรแล้วค่ะ') // ทำให้ pendingEndCall=true

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('ข้อความหลัง end call ถูก request แล้ว', FAKE_META)
    })

    assert.equal(diagLines.length, 0, 'pendingEndCall guard ต้องไม่มี [STT_DIAG] ตามที่ล็อก scope ไว้')
  } finally {
    harness.disconnect(socket)
  }
})

// Review Gate round 2 amendment (approved): 5 exit points ที่ก่อนหน้านี้ไม่มี [STT_DIAG] เลย — 3 จุดใช้
// DEFERRED/PENDING_TRANSCRIPT (pendingTranscript queue variants), 2 จุดใช้ DROPPED/BUSY (sttProcessing ยัง true)
// ทดสอบครบเฉพาะ 3 จุด PENDING_TRANSCRIPT ที่ construct ผ่าน integration harness ได้จริง
//
// Review Gate round 3 (accepted, ไม่ต้องไล่ต่อ): อีก 2 จุด DROPPED/BUSY (200ms-grace retry และ non-barge-in
// overlap) ยืนยันด้วย code review + probe เชิงประจักษ์หลายรอบว่า reachability ยัง unproven ภายใต้ invariant
// ปัจจุบัน (bargeInCooldown 400ms > 200ms grace window, และ isSpeaking=false ระหว่าง sttProcessing=true ปกติ
// จะเจอ pendingTranscript ตั้งไว้แล้วเข้า latest-wins branch ก่อนเสมอ) — ตกลงรับเป็น defensive-path telemetry
// ที่ code-reviewed แต่ไม่มี integration test คลุม โดยเจตนา ไม่สร้าง test-only hook เพื่อบังคับ coverage
// (ดู comment คู่กันที่ audioStream.js ตรง emitSttDiag(sttMeta, 'DROPPED', 'BUSY', ...) ทั้งสองจุด)

test('DEFERRED/PENDING_TRANSCRIPT (1/3 — old turn still processing): transcript ที่ trigger bargeIn ผ่าน final-path ขณะเทิร์นเดิมยัง sttProcessing=true ต้อง emit DEFERRED และ queue จริงตาม behavior เดิม', async () => {
  const callSid = nextCallSid()
  const { socket, state, session } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true, sttProcessing=true (gated)

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน', FAKE_META)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DEFERRED')
    assert.equal(diagLines[0].reason, 'PENDING_TRANSCRIPT')
    assert.equal(diagLines[0].text, 'เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน')

    // behavior เดิมต้องไม่เปลี่ยน: transcript ที่ queue ไว้ต้องถูก drain/process จริงหลัง turn เดิมปล่อย sttProcessing
    resumeOldTurn()
    await oldTurnPromise
    await delay(50)
    const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
    assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน', 'transcript ที่ deferred ไว้ต้องถูก process จริงหลัง drain ไม่ใช่หายไปเฉยๆ')
  } finally {
    harness.disconnect(socket)
  }
})

test('DEFERRED/PENDING_TRANSCRIPT (2/3 — final หลัง interim-triggered barge-in ขณะยัง busy): ต้อง emit DEFERRED และ queue จริงตาม behavior เดิม', async () => {
  const callSid = nextCallSid()
  const { socket, state, session } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true, sttProcessing=true (gated)

    harness.sendInterim('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน') // trigger bargeIn ผ่าน onInterim → bargeInPendingFinal=true, isSpeaking=false
    await delay(5)

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน', FAKE_META) // final ตัวจริงของ interim เดียวกัน
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DEFERRED')
    assert.equal(diagLines[0].reason, 'PENDING_TRANSCRIPT')

    resumeOldTurn()
    await oldTurnPromise
    await delay(50)
    const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
    assert.equal(lastUserMsg.content, 'เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน')
  } finally {
    harness.disconnect(socket)
  }
})

test('DEFERRED/PENDING_TRANSCRIPT (3/3 — additional transcript ระหว่างคิวมีของค้างอยู่): ต้อง emit DEFERRED และ latest-wins replace จริงตาม behavior เดิม (ไม่ใช่ FIFO)', async () => {
  const callSid = nextCallSid()
  const { socket, state, session } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบหลักที่กำลังตอบอยู่'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามโปรโมชั่นหน่อยค่ะ')
    await delay(30) // isSpeaking=true, sttProcessing=true (gated)

    await harness.sendFinalTranscript('เดี๋ยวก่อนครับขอถามเรื่องอื่นก่อน', FAKE_META) // queue #1 (path 1/3)
    await delay(450) // รอ bargeInCooldown (400ms) หมดก่อน ไม่งั้นตัวถัดไปโดน BARGE_IN_COOLDOWN แทนที่จะถึง branch นี้

    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('ประโยคที่สามที่ควรมาแทนที่คิวเดิม', FAKE_META)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DEFERRED')
    assert.equal(diagLines[0].reason, 'PENDING_TRANSCRIPT')
    assert.equal(diagLines[0].text, 'ประโยคที่สามที่ควรมาแทนที่คิวเดิม')

    resumeOldTurn()
    await oldTurnPromise
    await delay(50)
    const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
    assert.equal(lastUserMsg.content, 'ประโยคที่สามที่ควรมาแทนที่คิวเดิม', 'latest-wins ต้องยังทำงานเหมือนเดิม — คิวที่สองแทนที่คิวแรกไปแล้ว ไม่ใช่ FIFO')
  } finally {
    harness.disconnect(socket)
  }
})

// STT-A2 (design revision 2026-08-21): emitSttDiag ต้อง serialize sttMeta.alternatives เข้า [STT_DIAG] เฉพาะ
// ตอนมีข้อมูลจริง (A2 ON) — non-A2 calls (FAKE_META ด้านบนไม่มี key นี้เลย) ต้องไม่มี key alternatives โผล่มา
// ในทุก test ก่อนหน้านี้อยู่แล้วโดยปริยาย — เทสนี้ยืนยันฝั่งตรงข้าม: พอมีข้อมูลจริงต้อง serialize ออกมาถูกต้อง
test('STT-A2: [STT_DIAG] serialize sttMeta.alternatives เข้าไปจริงเมื่อมีข้อมูล (A2 ON)', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    const metaWithAlternatives = {
      ...FAKE_META,
      alternatives: [
        { index: 0, text: 'พอยต์เอาไปทำอะไร', confidence: null, selected: true },
        { index: 1, text: 'พอยต์ใช้ทำอะไร', confidence: null, selected: false },
      ],
    }
    const { diagLines } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('พอยต์เอาไปทำอะไร', metaWithAlternatives)
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].disposition, 'DELIVERED')
    assert.deepEqual(diagLines[0].alternatives, metaWithAlternatives.alternatives)
  } finally {
    harness.disconnect(socket)
  }
})

test('STT-A2: [STT_DIAG] ไม่มี key "alternatives" เลยเมื่อ sttMeta.alternatives เป็น null (A2 OFF)', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    const { diagLines, logs } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะ สนใจโปรโมชั่น', { ...FAKE_META, alternatives: null })
    })

    assert.equal(diagLines.length, 1)
    assert.equal('alternatives' in diagLines[0], false)
    const diagLine = logs.find(l => l.includes('[STT_DIAG]'))
    assert.ok(!diagLine.includes('"alternatives"'), 'key alternatives ต้องไม่ปรากฏใน raw JSON เลยตอน A2 OFF')
  } finally {
    harness.disconnect(socket)
  }
})

test('diagnostic failure ต้องไม่กระทบ call flow: field ที่ JSON.stringify ไม่ได้ (BigInt) → conversation ยังดำเนินต่อปกติ', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    // BigInt ทำให้ JSON.stringify throw แน่นอน — ต่างจาก circular reference ตรงที่ emitSttDiag ประกอบ object
    // ใหม่จาก field ที่ whitelist ไว้เท่านั้น (ไม่ spread sttMeta ทั้งก้อน) จึงต้องฉีดผ่าน field ที่ถูกอ่านจริง
    const badMeta = { ...FAKE_META, firstInterimAt: 10n }

    const { diagLines, errors } = await captureSttDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะ สนใจโปรโมชั่น', badMeta)
    })

    assert.equal(diagLines.length, 0, 'ไม่มี [STT_DIAG] line ที่ serialize สำเร็จ')
    assert.ok(errors.some(e => e.includes('[STT_DIAG] emit failed')), 'ต้อง log error แบบ non-fatal แทนที่จะ throw ออกไป')
    assert.ok(socket.sent.some(e => e.event === 'media'), 'turn ต้อง process ต่อและส่งเสียงจริงแม้ diagnostic พัง')
  } finally {
    harness.disconnect(socket)
  }
})

// A2.1 Shadow Google Final Diagnostics (design revision 2026-08-21, Design Gate v2 PASS) — wiring-level
// tests: activation formula frozen at 'start' correctly reaches transcribeStream()'s options (enableShadow),
// and [STT_SHADOW_DIAG] is a genuinely separate log line from [STT_DIAG] with the same diagnostic-failure
// isolation guarantee. Bucket values below are pre-computed real fixtures from getSttA2Bucket()/
// getSttA2ShadowBucket() (verified — see test/rolloutBucket.test.js) for the exact callSid strings used.
const A21_SHADOW_CAMPAIGN_ID = 'CAMPAIGN_A21_SHADOW_TEST'
function a21ShadowCampaign(overrides = {}) {
  return { voice_id: 'voice1', script: 'ระบบทดสอบ', id: A21_SHADOW_CAMPAIGN_ID, ...overrides }
}

// จับ [STT_SHADOW_DIAG] JSON line จากช่วงที่ fn() รัน — คนละ prefix จาก [STT_DIAG] เจตนา (design requirement:
// shadow events must be clearly separated from live-stream events ทั้งใน code และใน log)
async function captureSttShadowDiag(fn) {
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
  const diagLines = logs.filter(l => l.includes('[STT_SHADOW_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
  return { diagLines, logs, errors }
}

test('A2.1 Shadow wiring: A2 ON + matching campaign + qualifying shadow bucket → enableShadow=true reaches transcribeStream() options', async () => {
  const callSid = 'CA_A21_Q1' // a2Bucket=51, shadowBucket=51
  const state = harness.getState()
  state.rolloutPercent = 0
  state.sttA2Config = { percent: 100, campaignId: A21_SHADOW_CAMPAIGN_ID }
  state.sttA2ShadowConfig = { percent: 100, campaignId: A21_SHADOW_CAMPAIGN_ID }
  const session = makeSession({ campaign: a21ShadowCampaign(), greetingChunks: [Buffer.from('pregenerated-greeting')] })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(10)

  assert.equal(state.lastSttOptions?.maxAlternatives, 3, 'A2 ต้อง ON ด้วย (precondition ของ shadow)')
  assert.equal(state.lastSttOptions?.enableShadow, true)
  assert.equal(typeof state.lastSttOptions?.onShadowDiagnostic, 'function')

  harness.disconnect(socket)
})

test('A2.1 Shadow wiring: A2 ON แต่ shadow bucket ของตัวเองไม่ผ่านเกณฑ์ → enableShadow=false แม้ A2 เอง ON อยู่ (พิสูจน์ independent gate)', async () => {
  const callSid = 'CA_A21_NQ1' // a2Bucket=21 (qualifies for percent=100), shadowBucket=76 (ไม่ผ่านเกณฑ์ percent=50)
  const state = harness.getState()
  state.rolloutPercent = 0
  state.sttA2Config = { percent: 100, campaignId: A21_SHADOW_CAMPAIGN_ID }
  state.sttA2ShadowConfig = { percent: 50, campaignId: A21_SHADOW_CAMPAIGN_ID }
  const session = makeSession({ campaign: a21ShadowCampaign(), greetingChunks: [Buffer.from('pregenerated-greeting')] })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(10)

  assert.equal(state.lastSttOptions?.maxAlternatives, 3, 'A2 เองยัง ON')
  assert.equal(state.lastSttOptions?.enableShadow, false, 'shadow bucket ไม่ผ่านเกณฑ์ของตัวเอง ต้อง OFF แม้ A2 ON')

  harness.disconnect(socket)
})

test('A2.1 Shadow wiring: A2 OFF (percent=0) แต่ shadow config เองตั้งไว้ครบ (percent=100 + campaign matched) → enableShadow ต้องยังเป็น false เสมอ (sttA2===true เป็นเงื่อนไขบังคับ)', async () => {
  const callSid = 'CA_A21_A2OFF'
  const state = harness.getState()
  state.rolloutPercent = 0
  state.sttA2Config = { percent: 0, campaignId: null } // A2 OFF (default fail-closed)
  state.sttA2ShadowConfig = { percent: 100, campaignId: A21_SHADOW_CAMPAIGN_ID } // shadow's own gate ครบทุกอย่าง
  const session = makeSession({ campaign: a21ShadowCampaign(), greetingChunks: [Buffer.from('pregenerated-greeting')] })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(10)

  assert.equal(state.lastSttOptions?.maxAlternatives, null, 'A2 ต้อง OFF ตาม config')
  assert.equal(state.lastSttOptions?.enableShadow, false, 'A2 OFF ต้องทำให้ A2.1 OFF เสมอ ไม่ว่า shadow config ของตัวเองจะเป็นอะไร')

  harness.disconnect(socket)
})

test('[STT_SHADOW_DIAG]: onShadowDiagnostic ที่ audioStream.js ส่งเข้า transcribeStream() เขียน log แยก prefix จาก [STT_DIAG] พร้อม inject callSid ให้ครบ', async () => {
  const callSid = 'CA_A21_LOGLINE'
  const state = harness.getState()
  state.rolloutPercent = 0
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')] })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(10)

  const shadowPayload = {
    streamId: 1, utteranceId: 1, timerFinalText: 'ทดสอบ', timerFinalAt: 1000,
    shadowFinalAt: 1200, shadowFinalDelayMs: 200,
    shadowAlternatives: [{ index: 0, text: 'ทดสอบครับ', confidence: null, selected: true }],
    shadowOutcome: 'FINAL',
  }
  const { diagLines } = await captureSttShadowDiag(async () => {
    state.lastSttOptions.onShadowDiagnostic(shadowPayload)
  })

  assert.equal(diagLines.length, 1)
  assert.equal(diagLines[0].callSid, callSid)
  assert.equal(diagLines[0].shadowOutcome, 'FINAL')
  assert.equal(diagLines[0].timerFinalText, 'ทดสอบ')
  assert.deepEqual(diagLines[0].shadowAlternatives, shadowPayload.shadowAlternatives)

  harness.disconnect(socket)
})

test('[STT_SHADOW_DIAG] emit failure: field ที่ JSON.stringify ไม่ได้ (BigInt) → log error แบบ non-fatal เท่านั้น ไม่ throw ออกไปกระทบอะไรเลย', async () => {
  const callSid = 'CA_A21_LOGFAIL'
  const state = harness.getState()
  state.rolloutPercent = 0
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')] })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(10)

  const badPayload = { shadowOutcome: 'FINAL', timerFinalAt: 10n } // BigInt ทำให้ JSON.stringify throw แน่นอน
  const { diagLines, errors } = await captureSttShadowDiag(async () => {
    assert.doesNotThrow(() => state.lastSttOptions.onShadowDiagnostic(badPayload))
  })

  assert.equal(diagLines.length, 0)
  assert.ok(errors.some(e => e.includes('[STT_SHADOW_DIAG] emit failed')), 'ต้อง log error แบบ non-fatal แทนที่จะ throw ออกไป')

  harness.disconnect(socket)
})
