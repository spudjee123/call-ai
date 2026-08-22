// Track P Prewarm Diagnostics (design revision 2026-08-22, Design Review R3 PASS) — pure observability for
// the legacy-only speculative prewarm lifecycle (prewarmPromise/prewarmStartText/etc.). Ungated, no Sheet
// key. Must never affect production decisions (isPrewarmUsable(), the 150ms grace, the 700ms/4-char retrigger
// throttle, Claude request shape) — only adds [PREWARM_DIAG] logging alongside existing behavior.
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
function nextCallSid() { callSidCounter++; return `CA_PREWARM_${callSidCounter}` }

async function connectPastGreeting(callSid, { rolloutPercent = 0, sessionOverrides = {} } = {}) {
  const state = harness.getState()
  state.rolloutPercent = rolloutPercent
  const session = makeSession({ greetingChunks: [Buffer.from('pregenerated-greeting')], ...sessionOverrides })
  callSessions.set(callSid, session)
  const socket = harness.connect({ callSid })
  harness.sendStart(socket)
  await delay(2000) // 300ms greeting timer + 1520ms playback unlock + margin — must be past isSpeaking=true before prewarm can trigger
  socket.sent.length = 0
  return { socket, session, state }
}

// จับ [PREWARM_DIAG] JSON line จากช่วงที่ fn() รัน — คนละ prefix จาก [STT_DIAG]/[STT_SHADOW_DIAG] เจตนา
async function capturePrewarmDiag(fn) {
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
  const diagLines = logs.filter(l => l.includes('[PREWARM_DIAG]')).map(l => JSON.parse(l.slice(l.indexOf('{'))))
  return { diagLines, logs, errors }
}

// เดา Claude แบบควบคุมได้: ให้ generator หยุดรอ gate ระหว่าง yield สองครั้ง เพื่อคุมจังหวะ resolve ของ prewarm ได้แม่นยำ
function controllableClaudeStream(firstChunk) {
  let resolveGate
  const gate = new Promise(resolve => { resolveGate = resolve })
  const impl = async function* () {
    yield firstChunk
    await gate
  }
  return { impl, resolveGate }
}

harness.ensureStubbed()

beforeEach(() => {
  const state = harness.getState()
  state.claudeStreamImpl = async function* () { yield 'default legacy response.' }
  state.claudeStreamChunkedImpl = async function* () {}
  state.ttsImpl = async function* () { yield Buffer.from('audio') }
  state.rolloutPercent = 0
  state.legacyObservedConfig = { percent: 0, campaignId: null }
  state.legacyEarlyTtsConfig = { percent: 0, campaignId: null }
  state.sttA2Config = { percent: 0, campaignId: null }
  state.sttA2ShadowConfig = { percent: 0, campaignId: null }
  state.claudeConditionalImpl = null
})

test('NO_PREWARM: final arrives with no interim ever sent first → synthetic record, no lifecycle', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid)
  try {
    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะ')
    })
    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].outcome, 'NO_PREWARM')
    assert.equal(diagLines[0].prewarmDiagId, null)
    assert.ok(diagLines[0].generationId != null, 'NO_PREWARM ที่มาจาก final จริงต้องมี generationId')
  } finally {
    harness.disconnect(socket)
  }
})

test('first real trigger: interim creates a lifecycle, HIT before final settles prewarmReadyBeforeFinal=true', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'คำตอบพร้อมแล้ว' }
    harness.sendInterim('สวัสดีครับ')
    await delay(30) // ให้ IIFE resolve ก่อน final แน่ๆ

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีครับ')
    })

    assert.equal(diagLines.length, 1)
    const d = diagLines[0]
    assert.equal(d.outcome, 'HIT')
    assert.ok(typeof d.prewarmDiagId === 'number')
    assert.equal(d.retriggerCount, 0)
    assert.equal(d.initialTriggerText, 'สวัสดีครับ')
    assert.equal(d.lastTriggerText, 'สวัสดีครับ')
    assert.equal(d.prewarmStateAtFinal, 'READY_TEXT')
    assert.equal(d.prewarmReadyBeforeFinal, true)
    assert.ok(typeof d.initialPrewarmAgeAtFinalMs === 'number' && d.initialPrewarmAgeAtFinalMs >= 0)
    assert.equal(d.initialPrewarmAgeAtFinalMs, d.lastPrewarmAgeAtFinalMs, 'ไม่มี retrigger — initial/last age ต้องเท่ากัน')
    assert.ok(typeof d.prewarmAttemptSettleRelativeToFinalMs === 'number' && d.prewarmAttemptSettleRelativeToFinalMs < 0, 'settle ก่อน final ต้องเป็นค่าลบ')
    assert.equal(d.prewarmTextRelation, 'EXACT')
    assert.equal(d.prewarmUsable, true)
  } finally {
    harness.disconnect(socket)
  }
})

test('PENDING at final → settles during 150ms grace → HIT with prewarmReadyBeforeFinal=false, positive settle-relative', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { impl, resolveGate } = controllableClaudeStream('คำตอบเดียว')
    state.claudeStreamImpl = impl
    harness.sendInterim('ทดสอบเกรซ')
    await delay(20) // ยังไม่ resolve — prewarm ต้องยัง PENDING ตอน final มาถึง

    setTimeout(resolveGate, 60) // resolve ระหว่าง grace window (150ms) แต่หลัง final แน่นอน

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('ทดสอบเกรซ')
    })

    assert.equal(diagLines.length, 1)
    const d = diagLines[0]
    assert.equal(d.outcome, 'HIT')
    assert.equal(d.prewarmStateAtFinal, 'PENDING')
    assert.equal(d.prewarmReadyBeforeFinal, false)
    assert.ok(typeof d.prewarmAttemptSettleRelativeToFinalMs === 'number' && d.prewarmAttemptSettleRelativeToFinalMs > 0, 'settle หลัง final ต้องเป็นค่าบวก')
  } finally {
    harness.disconnect(socket)
  }
})

test('still pending through the whole grace window → GRACE_TIMEOUT, settle-relative null (never observed settling)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { impl } = controllableClaudeStream('จะไม่มีวันมาถึง') // gate ไม่เคย resolve เลยตลอดเทส
    state.claudeStreamImpl = impl
    harness.sendInterim('ทดสอบไทม์เอาท์')
    await delay(20)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('ทดสอบไทม์เอาท์')
    })

    assert.equal(diagLines.length, 1)
    const d = diagLines[0]
    assert.equal(d.outcome, 'GRACE_TIMEOUT')
    assert.equal(d.prewarmStateAtFinal, 'PENDING')
    assert.equal(d.prewarmAttemptSettleRelativeToFinalMs, null)
    assert.ok(typeof d.graceWaitMs === 'number' && d.graceWaitMs >= 140, 'graceWaitMs ต้องประมาณ 150ms')
  } finally {
    harness.disconnect(socket)
  }
})

test('completed with no text at all → SETTLED_NULL via normal loop completion (not abort, not catch)', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () {} // ไม่ yield อะไรเลย — text ค้างเป็น '' ตลอด
    harness.sendInterim('ไม่มีคำตอบ')
    await delay(30)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('ไม่มีคำตอบ')
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].outcome, 'NULL_RESULT')
    assert.equal(diagLines[0].prewarmStateAtFinal, 'SETTLED_NULL')
  } finally {
    harness.disconnect(socket)
  }
})

test('caught error inside the prewarm IIFE → SETTLED_NULL via the catch path', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { throw new Error('simulated Claude error') }

    const { diagLines, errors } = await capturePrewarmDiag(async () => {
      // [Prewarm] Error: จาก catch fires as soon as the stub throws, well before final — must be inside the
      // capture window from the start, not just around sendFinalTranscript()
      harness.sendInterim('จะพังกลางทาง')
      await delay(30)
      await harness.sendFinalTranscript('จะพังกลางทาง')
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].outcome, 'NULL_RESULT') // grace ยัง "success" (IIFE เองไม่เคย throw ออกมา — catch คืน null แล้ว resolve)
    assert.equal(diagLines[0].prewarmStateAtFinal, 'SETTLED_NULL')
    assert.ok(errors.some(e => e.includes('[Prewarm] Error:')), 'ต้อง log error จาก catch จริง')
  } finally {
    harness.disconnect(socket)
  }
})

test('retrigger via signal-aborted direct return: stale attempt A cannot write SETTLED_NULL into the newer attempt B', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    let resolveAFirst
    const aFirstYielded = new Promise(r => { resolveAFirst = r })
    let resolveAContinue
    const aContinueGate = new Promise(r => { resolveAContinue = r })
    let bResolveGate
    const bGate = new Promise(r => { bResolveGate = r })

    let callCount = 0
    state.claudeStreamImpl = async function* () {
      callCount++
      if (callCount === 1) {
        // attempt A — yields once, signals it has yielded, then blocks on aContinueGate before its SECOND
        // step, giving the test a window to trigger a retrigger (which aborts A) before A resumes
        yield 'A กำลังเริ่ม'
        resolveAFirst()
        await aContinueGate
        yield 'A ยังพยายามต่อ' // fires only after aContinueGate resolves — by then A's signal is already aborted
      } else {
        // attempt B — the retriggered request
        yield 'B'
        await bGate
      }
    }

    harness.sendInterim('เริ่มต้น') // triggers attempt A
    await aFirstYielded // A's outer for-await loop has consumed its first chunk, is now awaiting A's next step (blocked on aContinueGate)

    // fake clock: bypass the real 700ms retrigger throttle deterministically, per Design Review's instruction
    // to never use real sleeps for retrigger timing tests
    const originalDateNow = Date.now
    const fakeNow = originalDateNow() + 800
    Date.now = () => fakeNow
    try {
      harness.sendInterim('เริ่มต้นขยายยาวขึ้นมาก') // >=4 char growth, past the (faked) throttle — real retrigger, aborts A, starts B
    } finally {
      Date.now = originalDateNow
    }
    await delay(20) // let the retrigger's clearPrewarm()/new startPrewarm() synchronous work settle

    resolveAContinue() // let A's stale generator resume — its outer loop will see signal.aborted===true now
    await delay(20)

    const { diagLines } = await capturePrewarmDiag(async () => {
      bResolveGate() // let B settle with real text
      await delay(20)
      await harness.sendFinalTranscript('เริ่มต้นขยายยาวขึ้นมาก')
    })

    assert.equal(diagLines.length, 1)
    const d = diagLines[0]
    assert.equal(d.retriggerCount, 1, 'ต้องนับ 1 retrigger จริง (A→B)')
    assert.equal(d.initialTriggerText, 'เริ่มต้น', 'initialTriggerText ต้องเป็นของ A เสมอ ไม่ถูกทับ')
    assert.equal(d.lastTriggerText, 'เริ่มต้นขยายยาวขึ้นมาก', 'lastTriggerText ต้องเป็นของ B (attempt ปัจจุบัน)')
    // ตัวชี้วัดสำคัญที่สุด: ถ้า A's stale SETTLED_NULL write หลุดเข้า B ได้จริง ผลลัพธ์ที่ observe ได้ตอน final
    // จะกลายเป็น B ถูกมองว่า SETTLED_NULL/NULL_RESULT ทั้งที่ B ให้ text จริง (resolveBGate ให้ 'B' ไปแล้ว) — ต้องไม่เกิด
    assert.equal(d.prewarmStateAtFinal, 'READY_TEXT', 'B ต้อง settle เป็น READY_TEXT ของตัวเองจริง ไม่ถูก A ทับ')
    assert.equal(d.outcome, 'HIT')
  } finally {
    harness.disconnect(socket)
  }
})

test('multiple retriggers increment retriggerCount correctly while preserving initial fields', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'คำตอบสุดท้าย' }

    const originalDateNow = Date.now
    let fakeNow = originalDateNow()
    Date.now = () => fakeNow

    try {
      harness.sendInterim('เริ่ม')
      await delay(5)
      fakeNow += 800
      harness.sendInterim('เริ่มขยายรอบหนึ่ง') // retrigger #1
      await delay(5)
      fakeNow += 800
      harness.sendInterim('เริ่มขยายรอบหนึ่งอีกต่อ') // retrigger #2
      await delay(5)
      fakeNow += 800
      harness.sendInterim('เริ่มขยายรอบหนึ่งอีกต่อเนื่อง') // retrigger #3
      await delay(5)
    } finally {
      Date.now = originalDateNow
    }
    await delay(20)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('เริ่มขยายรอบหนึ่งอีกต่อเนื่อง')
    })

    assert.equal(diagLines.length, 1)
    const d = diagLines[0]
    assert.equal(d.retriggerCount, 3)
    assert.equal(d.initialTriggerText, 'เริ่ม')
    assert.equal(d.lastTriggerText, 'เริ่มขยายรอบหนึ่งอีกต่อเนื่อง')
    assert.ok(d.initialPrewarmAgeAtFinalMs > d.lastPrewarmAgeAtFinalMs, 'attempt แรกต้องมี age มากกว่า attempt ล่าสุดเสมอ (เริ่มก่อน)')
  } finally {
    harness.disconnect(socket)
  }
})

test('next utterance starts with a clean lifecycle — retriggerCount does not leak across turns', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'เทิร์นแรก' }
    const originalDateNow = Date.now
    let fakeNow = originalDateNow()
    Date.now = () => fakeNow
    try {
      harness.sendInterim('เทิร์นแรกเริ่ม')
      await delay(5)
      fakeNow += 800
      harness.sendInterim('เทิร์นแรกเริ่มขยายยาว') // 1 retrigger this turn
      await delay(5)
    } finally {
      Date.now = originalDateNow
    }
    await delay(20)
    await harness.sendFinalTranscript('เทิร์นแรกเริ่มขยายยาว') // settles + resets lifecycle
    await delay(1600) // must clear past the ~1520ms fallback-unlock (sent*20+1500) so isSpeaking=false before turn 2 — a shorter wait leaves isSpeaking=true and the next interim hits bargeIn() instead of startPrewarm()

    state.claudeStreamImpl = async function* () { yield 'เทิร์นสอง' }
    harness.sendInterim('เทิร์นสองเริ่ม')
    await delay(30)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('เทิร์นสองเริ่ม')
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].retriggerCount, 0, 'เทิร์นใหม่ต้องเริ่ม retriggerCount จาก 0 เสมอ ไม่สืบทอดจากเทิร์นก่อน')
    assert.equal(diagLines[0].initialTriggerText, 'เทิร์นสองเริ่ม')
  } finally {
    harness.disconnect(socket)
  }
})

test('prewarmTextRelation=EXACT: prewarm trigger text identical to final', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'ตอบแล้ว' }
    harness.sendInterim('ยืนยันครับ')
    await delay(30)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('ยืนยันครับ')
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].prewarmTextRelation, 'EXACT')
    assert.equal(diagLines[0].prewarmUsable, true)
  } finally {
    harness.disconnect(socket)
  }
})

test('prewarmTextRelation=CONTAINS: final text contains the (shorter) prewarm trigger text', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { impl } = controllableClaudeStream('คำตอบ')
    state.claudeStreamImpl = impl
    harness.sendInterim('สวัสดี')
    await delay(20)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะพี่คนสวย') // includes('สวัสดี') — not exact, not prefix-only
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].prewarmTextRelation, 'CONTAINS')
    assert.equal(diagLines[0].prewarmUsable, true, 'isPrewarmUsable() ต้องยอมรับ CONTAINS ด้วยเหตุผลเดียวกัน (mirror structure)')
  } finally {
    harness.disconnect(socket)
  }
})

test('prewarmTextRelation=PREFIX_HEAD: shares only the first 4 characters, no containment relation either way', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { impl } = controllableClaudeStream('คำตอบ')
    state.claudeStreamImpl = impl
    // ASCII เจตนา กันไม่ให้ Thai substring ชนกันเองโดยไม่ตั้งใจ — "abcd" ตรงกัน 4 ตัวแรก แต่ไม่มีความสัมพันธ์ contains เลย
    harness.sendInterim('abcd1111')
    await delay(20)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('abcd2222222')
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].prewarmTextRelation, 'PREFIX_HEAD')
    assert.equal(diagLines[0].prewarmUsable, true, 'isPrewarmUsable() เองก็ยอมรับ prefix-head match ด้วยเหตุผลเดียวกัน')
  } finally {
    harness.disconnect(socket)
  }
})

test('prewarmTextRelation=MISMATCH, and the classifier never changes isPrewarmUsable()\'s own production decision', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    // prewarm text and final text share no meaningful relation — isPrewarmUsable() itself must reject it
    // (aiText never gets set from prewarm), proving the classifier never overrides that decision.
    const { impl } = controllableClaudeStream('คำตอบที่ไม่เกี่ยวกันเลย')
    state.claudeStreamImpl = impl
    harness.sendInterim('ก')
    await delay(20)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('ประโยคที่ไม่เกี่ยวข้องกันเลยแม้แต่น้อย')
    })

    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].prewarmTextRelation, 'MISMATCH')
    assert.equal(diagLines[0].outcome, 'MISMATCH', 'isPrewarmUsable() ต้องปฏิเสธจริง — grace-wait ไม่ควรเคยเริ่มเลย')
    assert.equal(diagLines[0].prewarmUsable, false)
    assert.equal(diagLines[0].graceWaitMs, null, 'MISMATCH ต้องไม่มี grace-wait เกิดขึ้นเลย')
  } finally {
    harness.disconnect(socket)
  }
})

test('legacy-only scoping: a chunked-rollout turn never emits any [PREWARM_DIAG] record (not even NO_PREWARM)', async () => {
  const callSid = nextCallSid()
  const { socket } = await connectPastGreeting(callSid, { rolloutPercent: 100 }) // guarantees useChunkedStreaming=true for any callSid
  try {
    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('สวัสดีค่ะ')
    })
    assert.equal(diagLines.length, 0, 'chunked path ต้องไม่มี [PREWARM_DIAG] เลยแม้แต่ NO_PREWARM')
  } finally {
    harness.disconnect(socket)
  }
})

test('barge-in vs grace-abort race: settlement fires exactly once (BARGE_IN), settled before transport cleanup', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { impl } = controllableClaudeStream('จะไม่ทันได้ใช้') // ไม่ resolve เลย
    state.claudeStreamImpl = impl
    harness.sendInterim('พูดก่อนโดนขัด')
    await delay(20)

    // ทำให้ turn เข้า grace-wait จริงก่อน แล้วส่ง barge-in (interim ระหว่าง isSpeaking) แข่งกับ grace's own abort
    const finalPromise = harness.sendFinalTranscript('พูดก่อนโดนขัด')
    await delay(10) // ให้ processTranscript() เข้า grace-wait (isSpeaking=true แล้ว)

    const { diagLines } = await capturePrewarmDiag(async () => {
      harness.sendInterim('ขัดจังหวะ') // isSpeaking=true ตอนนี้ → trigger bargeIn() โดยตรง
      await delay(200) // ให้ grace (150ms) + finalPromise ไปจบให้หมด
      await finalPromise
    })

    assert.equal(diagLines.length, 1, 'ต้อง emit แค่ครั้งเดียวไม่ว่า bargeIn() หรือ grace-wait ฝั่งไหนชนะ race')
    assert.ok(['BARGE_IN'].includes(diagLines[0].outcome), `outcome ต้องเป็น BARGE_IN ไม่ว่าจะมาจากจุดไหน ได้ ${diagLines[0].outcome}`)
  } finally {
    harness.disconnect(socket)
  }
})

test('defensive catch settlement is a no-op when primary settlement already ran normally — never double-emits', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'ปกติดี' }
    harness.sendInterim('ทดสอบปกติ')
    await delay(30)

    const { diagLines } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('ทดสอบปกติ')
    })

    // เทิร์นนี้ไม่มี error เลย — primary settlement (HIT) ต้องเป็นตัวเดียวที่ emit ไม่มี defensive-catch settlement ซ้ำ
    assert.equal(diagLines.length, 1)
    assert.equal(diagLines[0].outcome, 'HIT')
  } finally {
    harness.disconnect(socket)
  }
})

test('defensive catch settlement actually fires (not just a no-op check): an exception before primary settlement still produces one ERROR record with real state-at-final data', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    state.claudeStreamImpl = async function* () { yield 'ไม่สำคัญ เพราะจะพังก่อนถึงตรงนี้' }
    // ส่ง interim เป็นตัวเลข (ไม่ใช่ string) โดยตั้งใจ — startPrewarm()/prewarmDiag ไม่ validate ชนิดของ interimText
    // เลย (เก็บตรงๆ) แต่ isPrewarmUsable()/classifyPrewarmTextRelation() เรียก .trim() กับมันตอน final มาถึง ทำให้
    // เกิด TypeError จริงที่จุดนั้น — เร็วกว่า primary settlement (ซึ่งอยู่หลังจากนั้นอีกหลายบรรทัด) เสมอ พิสูจน์ว่า
    // defensive catch settlement ทำงานจริงเมื่อ exception เกิดขึ้นก่อนถึง primary settlement จริงๆ ไม่ใช่แค่ no-op guard
    harness.sendInterim(12345)
    await delay(30)

    const { diagLines, errors } = await capturePrewarmDiag(async () => {
      await harness.sendFinalTranscript('final ปกติ')
    })

    assert.equal(diagLines.length, 1, 'ต้อง settle แค่ครั้งเดียวจาก defensive catch เท่านั้น')
    assert.equal(diagLines[0].outcome, 'ERROR')
    // Review Gate fix — ต้องมี state-at-final จริง ไม่ใช่ null ทั้งหมด เพราะ field พวกนี้ถูก snapshot ไว้ตั้งแต่ก่อน
    // เข้า try block แล้ว (ก่อนที่ exception จะเกิดด้วยซ้ำ)
    assert.notEqual(diagLines[0].prewarmStateAtFinal, null)
    assert.notEqual(diagLines[0].prewarmReadyBeforeFinal, null)
    assert.notEqual(diagLines[0].initialPrewarmAgeAtFinalMs, null)
    assert.notEqual(diagLines[0].lastPrewarmAgeAtFinalMs, null)
    assert.ok(errors.some(e => e.includes('[AI/TTS error]')), 'AI/TTS error handling เดิมต้องยังทำงานตามปกติ ไม่ถูกเปลี่ยนพฤติกรรม')
  } finally {
    harness.disconnect(socket)
  }
})

test('ignored/no-op trigger: an interim that never reaches startPrewarm() (call still busy) creates no lifecycle at all', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    let resumeOldTurn
    const gate = new Promise(resolve => { resumeOldTurn = resolve })
    state.claudeStreamImpl = async function* () { yield 'คำตอบเทิร์นเดิมที่ยังไม่จบ'; await gate }
    const oldTurnPromise = harness.sendFinalTranscript('ขอสอบถามก่อนค่ะ') // ไม่มี interim นำมาก่อนเทิร์นนี้ — เทิร์นนี้เองก็ไม่มี prewarm เช่นกัน
    await delay(30) // isSpeaking=true, sttProcessing=true (ค้างรอ gate)

    harness.sendInterim('พูดแทรกสั้นๆนะครับ') // interim-triggered barge-in — isSpeaking→false, sttProcessing ยัง true (เทิร์นเดิมยังค้างรอ gate)
    await delay(420) // รอให้พ้น bargeInCooldown 400ms ก่อน ไม่งั้น interim ถัดไปจะโดนกันที่ cooldown แทนที่จะถึงจุด sttProcessing check ที่ต้องการทดสอบ

    const { diagLines: ignoredDiag } = await capturePrewarmDiag(async () => {
      // ตอนนี้ isSpeaking=false แต่ sttProcessing=true — onInterim's เองมี `if (sttProcessing) return` กันไว้ก่อน
      // ถึง startPrewarm() เลย ("ไม่ trigger prewarm ระหว่างกำลัง process เทิร์นอื่นอยู่" — บรรทัด comment เดิมในซอร์ส)
      harness.sendInterim('อันนี้ต้องถูกเมิน')
      await delay(20)
    })
    assert.equal(ignoredDiag.length, 0, 'interim ที่ถูก ignore (call ยัง busy) ต้องไม่สร้าง lifecycle/[PREWARM_DIAG] อะไรเลย')

    resumeOldTurn()
    await oldTurnPromise
  } finally {
    harness.disconnect(socket)
  }
})

test('stop → close exactly-once: a call ending while a prewarm is still pending settles CALL_ENDED exactly once, not twice', async () => {
  const callSid = nextCallSid()
  const { socket, state } = await connectPastGreeting(callSid)
  try {
    const { impl } = controllableClaudeStream('จะไม่มีวันได้ใช้') // ไม่ resolve เลยตลอดเทส — ยัง PENDING แน่นอนตอนสายจบ
    state.claudeStreamImpl = impl
    harness.sendInterim('พูดก่อนวางสาย')
    await delay(30) // prewarm ยัง PENDING แน่ๆ ตอนนี้

    const { diagLines } = await capturePrewarmDiag(async () => {
      harness.disconnect(socket) // เรียก 'stop' แล้ว 'close' ติดกันตามลำดับจริง (ดู harness.disconnect() — sendStop() แล้ว emit('close'))
    })

    assert.equal(diagLines.length, 1, 'ต้อง settle แค่ครั้งเดียวไม่ว่า stop หรือ close จะเป็นตัวที่ settle จริง — ไม่ใช่ทั้งสองจุด')
    assert.equal(diagLines[0].outcome, 'CALL_ENDED')
    // ไม่ต้อง disconnect ซ้ำใน finally — เกิดขึ้นแล้วข้างบน (fake socket ในฮาร์เนสไม่เปลี่ยน readyState ตอน
    // disconnect() จริง ทำให้ guard แบบ `if (socket.readyState === OPEN)` จะ true เสมอและ disconnect ซ้ำเงียบๆ)
  } catch (err) {
    harness.disconnect(socket) // เผื่อ assertion พังก่อนถึง disconnect ข้างบน — เคลียร์ timer ค้างกันสาย test แขวน
    throw err
  }
})

// Reachability note (2026-08-22, Review Gate R2) — the `if (signal.aborted) { settleAttempt('SETTLED_NULL');
// return null }` exit inside startPrewarm()'s IIFE (src/websocket/audioStream.js) is exercised by the
// "stale attempt A cannot write..." test above, but ONLY in its rejected/no-op form (A is already superseded
// by the time it resumes, so the identity guard blocks the write). Investigated whether a test could isolate
// the OTHER direction — signal.aborted becoming true for an attempt that is STILL the current one, producing
// an OBSERVABLE SETTLED_NULL emission — and traced every real call site that can set signal.aborted=true
// (all of them go through clearPrewarm(), which is only ever called from: (a) a retrigger, where the new
// attempt object always replaces currentAttempt synchronously before the old IIFE's continuation can ever run
// again — JS has no await between clearPrewarm() and the replacement — so the old attempt is always already
// stale by the time it checks signal.aborted; or (b) one of the terminal settlement sites, all of which now
// call settlePrewarmDiag() BEFORE clearPrewarm() per the Review Gate's ordering fix, so prewarmDiag is
// already null/detached by the time the abort actually fires). Given the current control flow, this specific
// combination (signal.aborted fires AND the attempt is still the live, unsettled one) does not appear
// reachable without either changing production ordering or adding a test-only hook — neither of which this
// gate allows. Not adding a synthetic/misleading test for it; the existing stale-attempt test remains the
// closest real coverage, and it verifies the security-relevant direction (a stale write is correctly
// rejected) rather than the currently-unreachable one.
