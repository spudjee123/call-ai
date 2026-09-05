// Track B — Opening Hello Guard (design locked 2026-09-05, production audit finding: "ฮัลโหล" caused 12.8% of
// all barge-in events, 64/499, during the opening greeting — a Thai customer answering the phone naturally,
// not a real interruption). Rollout-gated (own Sheet keys/bucket namespace, default OFF, campaign/percent
// controllable exactly like legacyEarlyTts/sttA2) — see rolloutConfig.js's classifyOpeningHelloGuardConfig().
//
// Scope discipline: this guard is NARROW — only a greeting-only utterance ("ฮัลโหล"/"ฮัลโหลครับ"/"ฮัลโหลค่ะ",
// exact match after normalizeForClassification) during the OPENING GREETING itself is suppressed. Anything
// else (meaningful speech, the same word outside the opening, the guard disabled) must behave EXACTLY like
// production did before this track — every assertion here that isn't specifically about suppression proves
// that baseline is unchanged.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const callSessions = require('../src/utils/callSessions')
const harness = require('./_audioStreamHarness')

process.env.NODE_ENV = 'test'

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

const OPENING_HELLO_CAMPAIGN_ID = 'CAMPAIGN_OPENING_HELLO_TEST'
function makeSession(overrides = {}) {
  return {
    name: 'ทดสอบ',
    campaign: { voice_id: 'voice1', script: 'ระบบทดสอบ', id: OPENING_HELLO_CAMPAIGN_ID },
    messages: [],
    ...overrides,
  }
}

let callSidCounter = 0
function nextCallSid() { callSidCounter++; return `CA_OPENHELLO_${callSidCounter}` }

// ต่างจาก connectPastGreeting() ของเทสไฟล์อื่น (รอ 2000ms จน greeting เล่นจบไปแล้ว) — เทสนี้ต้องจับจังหวะที่ greeting
// "กำลังเล่นอยู่จริง" (isSpeaking=true, pipelineId=1) ดังนั้นรอแค่พอให้ playGreeting() (setTimeout 300ms) เริ่มทำงาน
// แล้วส่ง media/mark ออกไปแล้ว แต่ยังไม่ถึง fallback-unlock timer (300ms+1520ms=1820ms สำหรับ 1 pregenerated chunk)
async function connectDuringGreeting(callSid, { sessionOverrides = {} } = {}) {
  const state = harness.getState()
  state.rolloutPercent = 0
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')], ...sessionOverrides })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(500) // หลัง playGreeting() (300ms) เริ่มและส่งเสียงไปแล้ว แต่ยังไม่ถึง fallback-unlock (1820ms)
  socket.sent.length = 0
  return { socket, session, state }
}

// ปล่อยให้ greeting เล่นจบไปตามธรรมชาติ (fallback-unlock timer) แล้วค่อยคืน control — ใช้กับเคสที่ต้องการทดสอบ
// "หลัง opening จบแล้ว" โดยเฉพาะ (Case 6)
async function connectPastGreeting(callSid, { sessionOverrides = {} } = {}) {
  const state = harness.getState()
  state.rolloutPercent = 0
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')], ...sessionOverrides })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(2000)
  socket.sent.length = 0
  return { socket, session, state }
}

// จับ [BARGE_DIAG] JSON line จากช่วงที่ fn() รัน
async function captureBargeDiag(fn) {
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')); originalLog(...args) }
  try {
    await fn()
  } finally {
    console.log = originalLog
  }
  return logs.filter(l => l.includes('[BARGE_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
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
  state.openingHelloGuardConfig = { percent: 100, campaignId: OPENING_HELLO_CAMPAIGN_ID } // ON by default in this file — Case 7 overrides to OFF explicitly
  state.claudeConditionalImpl = null
})

test('Case 1: opening active + interim "ฮัลโหล" แล้วยืนยันด้วย "ฮัลโหลครับ" (2-signal) → suppressed, ไม่เรียก bargeIn(), greeting เล่นต่อ', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectDuringGreeting(callSid)
  try {
    const lines = await captureBargeDiag(async () => {
      harness.sendInterim('ฮัลโหล')
      harness.sendInterim('ฮัลโหลครับ')
      await delay(10)
    })

    assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0, 'ห้าม bargeIn() — greeting ต้องเล่นต่อ')
    const suppressed = lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO')
    assert.ok(suppressed, 'ต้อง emit SUPPRESS_OPENING_HELLO')
    assert.equal(suppressed.bargeTrigger, 'INTERIM_CONFIRM')
    assert.equal(suppressed.openingGreetingActive, true)
    assert.equal(suppressed.openingHelloGuardEnabled, true)
    assert.equal(suppressed.openingHelloMatched, true)
    assert.equal(suppressed.openingHelloAction, 'SUPPRESS_OPENING_HELLO')
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 2: opening active + final "ฮัลโหลค่ะ" ตรงๆ → ไม่ false-barge, greeting เล่นต่อ', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectDuringGreeting(callSid)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหลค่ะ')
    })

    assert.equal(socket.sent.filter(e => e.event === 'clear').length, 0)
    const suppressed = lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO')
    assert.ok(suppressed)
    assert.equal(suppressed.bargeTrigger, 'FINAL')
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 3: opening active + final "ฮัลโหล โทรมาจากไหนครับ" (มีเนื้อหาจริง) → REAL barge-in, AI หยุดทันที', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectDuringGreeting(callSid)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหล โทรมาจากไหนครับ')
    })

    assert.ok(socket.sent.some(e => e.event === 'clear'), 'ต้อง bargeIn() จริง — มีเนื้อหาความหมายเกินกว่าแค่ทักทาย')
    assert.equal(lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined, 'ต้องไม่ถูก suppress')
    const confirmed = lines.find(l => l.event === 'CONFIRMED')
    assert.ok(confirmed)
    assert.equal(confirmed.bargeTrigger, 'FINAL')
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 4: opening active + final "ไม่สะดวกค่ะ" (คำปฏิเสธ) → REAL barge-in เสมอ ต้องไม่ถูกจัดเป็น greeting-only acknowledgement', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectDuringGreeting(callSid)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ไม่สะดวกค่ะ')
    })

    assert.ok(socket.sent.some(e => e.event === 'clear'), 'ต้อง bargeIn() จริง')
    assert.equal(lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined)
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 5: opening active + final "ฮัลโหล ไม่สะดวกค่ะ" (ทักทายแล้วตามด้วยเจตนาจริง) → REAL barge-in', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectDuringGreeting(callSid)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหล ไม่สะดวกค่ะ')
    })

    assert.ok(socket.sent.some(e => e.event === 'clear'), 'ต้อง bargeIn() จริง เพราะมีเจตนาจริงตามหลัง "ฮัลโหล"')
    assert.equal(lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined)
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 6: opening จบไปแล้วตามธรรมชาติ + final "ฮัลโหล" → ใช้ behavior ปกติ (ไม่ suppress อีกต่อไป — guard ไม่รั่วไปเทิร์นถัดไป)', async () => {
  const callSid = nextCallSid()
  const { socket, session } = await connectPastGreeting(callSid) // greeting จบแล้วจริง (isSpeaking=false, fallback-unlock ทำงานไปแล้ว)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหล') // ตอนนี้เป็นแค่ transcript ปกติของเทิร์นใหม่ ไม่ใช่ระหว่าง greeting แล้ว
    })

    assert.equal(lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined, 'guard ต้องไม่ทำงานอีกต่อไปหลัง opening จบแล้ว')
    // "ฮัลโหล" ไม่ใช่ ack ที่รู้จัก (ไม่ใช่ TIER1/TIER2) และ isSpeaking=false อยู่แล้ว → ไหลเข้า processTranscript() ปกติเป็นเทิร์นใหม่
    const lastUserMsg = session.messages.filter(m => m.role === 'user').at(-1)
    assert.equal(lastUserMsg?.content, 'ฮัลโหล', 'ต้องถูกส่งเข้า Claude เป็นเทิร์นปกติ ไม่ใช่ถูกทิ้งเงียบๆ')
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 7: feature flag OFF (percent=0) + opening active + "ฮัลโหลครับ" → behavior ตรงกับ production เดิมก่อน Track B เป๊ะ (bargeIn จริง) — พิสูจน์ kill-switch', async () => {
  const callSid = nextCallSid()
  harness.getState().openingHelloGuardConfig = { percent: 0, campaignId: null } // OFF
  const { socket } = await connectDuringGreeting(callSid)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหลครับ')
    })

    assert.ok(socket.sent.some(e => e.event === 'clear'), 'flag OFF → ต้อง bargeIn() เหมือน production เดิมก่อนมี Track B')
    assert.equal(lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined)
    const confirmed = lines.find(l => l.event === 'CONFIRMED')
    assert.ok(confirmed)
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 8: flag ON แต่ตอนนี้ไม่ใช่ opening greeting (isSpeaking=false ตอนนั้น) → ไม่มี suppression พิเศษใดๆ', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid) // greeting จบแล้ว, flag ยังเป็น ON จาก beforeEach
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหลครับ') // ไม่ใช่ระหว่าง opening แล้ว
    })

    assert.equal(lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined, 'flag ON เฉยๆ ไม่พอ ต้อง isOpeningGreetingPlaying() ด้วย')
  } finally {
    harness.disconnect(socket)
  }
})

test('Case 9: interim/final symmetry — "ฮัลโหลครับ" ถูก suppress เหมือนกันไม่ว่าจะมาจาก path ไหน', async () => {
  const callSidInterim = nextCallSid()
  const { socket: socketInterim } = await connectDuringGreeting(callSidInterim)
  let interimSuppressed
  try {
    const lines = await captureBargeDiag(async () => {
      harness.sendInterim('ฮัลโหลครับ')
      harness.sendInterim('ฮัลโหลครับ')
      await delay(10)
    })
    interimSuppressed = lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO')
    assert.ok(interimSuppressed, 'interim path ต้อง suppress')
    assert.equal(socketInterim.sent.filter(e => e.event === 'clear').length, 0)
  } finally {
    harness.disconnect(socketInterim)
  }

  const callSidFinal = nextCallSid()
  const { socket: socketFinal } = await connectDuringGreeting(callSidFinal)
  try {
    const lines = await captureBargeDiag(async () => {
      await harness.sendFinalTranscript('ฮัลโหลครับ')
    })
    const finalSuppressed = lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO')
    assert.ok(finalSuppressed, 'final path ต้อง suppress เหมือนกัน')
    assert.equal(socketFinal.sent.filter(e => e.event === 'clear').length, 0)
  } finally {
    harness.disconnect(socketFinal)
  }
})

test('Case 10: คำสั้นที่มีเจตนาจริง ("ใคร"/"อะไร"/"ไม่"/"ไม่ค่ะ") ระหว่าง opening → ต้องไม่ถูกจัดเป็น greeting-only, คงพฤติกรรม barge-in ปกติ', async () => {
  for (const text of ['ใคร', 'อะไร', 'ไม่', 'ไม่ค่ะ']) {
    const callSid = nextCallSid()
    const { socket } = await connectDuringGreeting(callSid)
    try {
      const lines = await captureBargeDiag(async () => {
        await harness.sendFinalTranscript(text)
      })
      assert.equal(
        lines.find(l => l.event === 'SUPPRESS_OPENING_HELLO'), undefined,
        `"${text}" ต้องไม่ถูก suppress เป็น greeting-only`
      )
      // หมายเหตุ: "ไม่"/"ไม่ค่ะ" อยู่ใน TIER2_ACKS (Design B, เก่ากว่านี้) จึงจะ DEFER (ไม่ bargeIn ทันที) ไม่ใช่เพราะ
      // opening-hello guard — ตรวจแค่ว่าไม่ใช่ SUPPRESS_OPENING_HELLO ก็พอสำหรับเทสนี้ ("ใคร"/"อะไร" ไม่ใช่ ack เลย
      // จึงควร bargeIn() จริง)
      if (text === 'ใคร' || text === 'อะไร') {
        assert.ok(socket.sent.some(e => e.event === 'clear'), `"${text}" ต้อง bargeIn() จริง (ไม่ใช่ ack ไม่ใช่ greeting-only)`)
      }
    } finally {
      harness.disconnect(socket)
    }
  }
})
