// L2b PROTOTYPE — tests for askClaudeConditionalStream() (design revision 2026-08-21, NOT wired into any
// live call path). Same stub pattern as test/claude.test.js (own require.cache entry so this file can run
// standalone or alongside it without state bleed — each test file gets its own module registry state via
// the shared `state` object below, reset in beforeEach).
const { test, beforeEach, mock } = require('node:test')
const assert = require('node:assert/strict')

// deterministic grace-boundary tests need a fake clock (real setTimeout at exactly 149/150/151ms is not
// reliably distinguishable under real scheduler jitter) — node:test's built-in mock.timers patches the
// global setTimeout that both askClaudeConditionalStream()'s internal grace timer AND these tests' fake
// stream delays go through, so tick() controls both deterministically from one place.
async function flushMicrotasks(times = 6) {
  for (let i = 0; i < times; i++) await Promise.resolve()
}

const state = { events: [], streamImpl: null }

function makeFakeStream(events, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < events.length; i++) {
        if (signal?.aborted) {
          const err = new Error('Request was aborted.')
          err.name = 'AbortError'
          throw err
        }
        yield events[i]
      }
    },
  }
}

// สำหรับเทสที่ต้องรอ real delay ระหว่าง event (จำลอง Claude ส่ง delta ถัดไปช้า/เงียบไปพัก)
function makeSlowFakeStream(events, delaysMs, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < events.length; i++) {
        if (delaysMs[i]) await new Promise(r => setTimeout(r, delaysMs[i]))
        if (signal?.aborted) {
          const err = new Error('Request was aborted.')
          err.name = 'AbortError'
          throw err
        }
        yield events[i]
      }
    },
  }
}

class FakeAnthropic {
  constructor() {
    this.messages = {
      stream: (params, options) => state.streamImpl(state.events, options?.signal),
    }
  }
}

const anthropicSdkPath = require.resolve('@anthropic-ai/sdk')
require.cache[anthropicSdkPath] = {
  id: anthropicSdkPath, filename: anthropicSdkPath, loaded: true,
  exports: FakeAnthropic,
}

const { askClaudeConditionalStream } = require('../src/services/claude')

beforeEach(() => {
  state.events = []
  state.streamImpl = makeFakeStream
})

function textDelta(text) {
  return { type: 'content_block_delta', delta: { type: 'text_delta', text } }
}

function makeSession(userText = 'สวัสดีครับ') {
  return {
    name: 'ทดสอบ',
    campaign: { script: 'สคริปต์ทดสอบ' },
    messages: [{ role: 'user', content: userText }],
  }
}

async function collect(gen) {
  const chunks = []
  for await (const c of gen) chunks.push(c)
  return chunks
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)) }

function makeMilestoneRecorder() {
  const events = []
  return { events, onMilestone: (key, value) => events.push({ key, value, at: Date.now() }) }
}

// ---------------------------------------------------------------------------
// SINGLE_SHOT path
// ---------------------------------------------------------------------------

test('SINGLE_SHOT: ไม่เคยเจอ boundary เลยตลอด stream (ข้อความสั้น ไม่มีจุด/soft boundary/space) → yield เต็มก้อนครั้งเดียวตอนจบ', async () => {
  state.events = [textDelta('ครับผม')] // ไม่มี . ? ! \n, ไม่ match Thai soft boundary (ตามด้วยอักษรอื่นไม่ใช่ whitespace/end), ไม่มี space ให้ตัด natural boundary
  const { events, onMilestone } = makeMilestoneRecorder()
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))
  assert.deepEqual(chunks, ['ครับผม'])
  const keys = events.map(e => e.key)
  assert.ok(keys.includes('requestAt'))
  assert.ok(keys.includes('firstDeltaAt'))
  assert.ok(!keys.includes('firstSafeAt'), 'ไม่ควรเจอ boundary เลย')
  assert.equal(events.find(e => e.key === 'mode')?.value, 'SINGLE_SHOT')
  assert.ok(keys.includes('fullAt'))
  assert.equal(events.find(e => e.key === 'finalText')?.value, 'ครับผม', 'finalText ต้องมาจาก rawText accumulator เดียวกับที่ yield ออกไป')
  assert.equal(events.find(e => e.key === 'endCallRequested')?.value, false)
})

test('SINGLE_SHOT: เจอ boundary แล้ว แต่ Claude จบ stream ภายใน grace (150ms) → ยัง yield เต็มก้อนครั้งเดียว ไม่แยกเป็น chunk', async () => {
  const delaysMs = [0, 20] // delta ที่สองมาเร็วมาก (20ms) เร็วกว่า grace (150ms) มาก — stream จบก่อน grace fire
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [textDelta('ประโยคแรกครับ. '), textDelta('ประโยคสอง.')]
  const { events, onMilestone } = makeMilestoneRecorder()
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))
  assert.deepEqual(chunks, ['ประโยคแรกครับ. ประโยคสอง.'], 'ต้องได้ข้อความเต็มก้อนเดียว raw concat ไม่ trim ตรงกลาง')
  assert.equal(events.find(e => e.key === 'mode')?.value, 'SINGLE_SHOT')
  assert.ok(events.some(e => e.key === 'firstSafeAt'), 'ต้องเจอ boundary จริง (แค่ grace ไม่ทันไล่ทัน)')
  assert.equal(events.find(e => e.key === 'finalText')?.value, 'ประโยคแรกครับ. ประโยคสอง.')
})

test('SINGLE_SHOT: ไม่มี history เลย (เหมือน askClaudeStream/askClaudeObservedFullResponse) → yield ค่า default ทันที ไม่มี milestone ใดๆ', async () => {
  const session = { name: 'ทดสอบ', campaign: { script: 's' }, messages: [] }
  const { events, onMilestone } = makeMilestoneRecorder()
  const chunks = await collect(askClaudeConditionalStream(session, null, onMilestone))
  assert.deepEqual(chunks, ['สวัสดีค่ะ'])
  assert.deepEqual(events, [])
})

// ---------------------------------------------------------------------------
// CHUNKED path
// ---------------------------------------------------------------------------

test('CHUNKED: grace หมดก่อน Claude จบ stream → yield chunk แรกทันทีตอน grace fire (ก่อน full completion), แล้ว yield ส่วนที่เหลือต่อ', async () => {
  const delaysMs = [0, 250] // delta ที่สองมาช้ากว่า grace (150ms) มาก — grace ต้อง fire ก่อน แล้วค่อยมี delta ต่อ
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [textDelta('ประโยคแรกครับ. '), textDelta('ประโยคสองยาวกว่านี้หน่อย.')]
  const { events, onMilestone } = makeMilestoneRecorder()

  const gen = askClaudeConditionalStream(makeSession(), null, onMilestone)
  const chunks = []
  const t0 = Date.now()
  let firstChunkAt = null
  for await (const c of gen) {
    if (firstChunkAt === null) firstChunkAt = Date.now() - t0
    chunks.push(c)
  }

  assert.equal(chunks.length, 2, 'ต้องได้ 2 chunk แยกกัน ไม่ใช่ก้อนเดียว')
  assert.equal(chunks[0], 'ประโยคแรกครับ.', 'chunk แรกต้องเป็นข้อความที่ปลอดภัยที่ตัดได้จริง (ไม่รวมช่องว่างท้าย)')
  assert.equal(chunks[1], 'ประโยคสองยาวกว่านี้หน่อย.')
  assert.ok(firstChunkAt < 250, `chunk แรกต้องมาก่อน delta ที่สอง (250ms) จริง มาได้จริงที่ ${firstChunkAt}ms`)
  assert.ok(firstChunkAt >= 140, `chunk แรกต้องรอ grace (~150ms) ก่อน ไม่ใช่ยิงทันทีที่เจอ boundary มาได้จริงที่ ${firstChunkAt}ms`)
  assert.equal(events.find(e => e.key === 'mode')?.value, 'CHUNKED')
  assert.equal(events.find(e => e.key === 'finalText')?.value, 'ประโยคแรกครับ. ประโยคสองยาวกว่านี้หน่อย.',
    'finalText ต้องมาจาก rawText accumulator ตรงๆ ไม่ใช่การต่อ chunk ที่ yield ออกไป (ถึงแม้ผลลัพธ์จะเหมือนกันในเคสนี้ก็ตาม)')
})

test('CHUNKED: numeric-protection timer ยังทำงานระหว่าง phase 2 — ตัดผ่าน wall-clock timer เอง (~800ms) ไม่ต้องรอ delta ถัดไปมาปลุกเลย แม้ stream จะยังไม่จบ (buffer เดียวกับที่ verified แล้วใน test/claude.test.js L2a, ต้องมี trailing space + ยาวพอผ่าน FALLBACK_MIN_LENGTH ไม่งั้นไม่เข้าเงื่อนไข numeric-protection ตั้งแต่แรก)', { timeout: 5000 }, async () => {
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('ประโยคแรกครับ. ') // boundary แรก — trigger grace
      await new Promise(r => setTimeout(r, 250)) // หลัง grace (150ms) แน่นอน — mode ต้องกลายเป็น CHUNKED ไปแล้ว
      yield textDelta('ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 ') // ยาวพอผ่าน FALLBACK_MIN_LENGTH(25) ลงท้ายตัวเลข+space
      await new Promise(r => setTimeout(r, 1200)) // เงียบยาวเกิน HARD_MAX_MS (800ms) ไม่มี delta ใหม่มาปลุกเลยตลอดช่วงนี้ — stream ยังไม่จบ (ยัง await อยู่นี่)
    },
  })

  const { events, onMilestone } = makeMilestoneRecorder()
  const gen = askClaudeConditionalStream(makeSession(), null, onMilestone)
  const chunks = []
  const timestamps = []
  const t0 = Date.now()
  for await (const c of gen) { chunks.push(c); timestamps.push(Date.now() - t0) }

  assert.equal(chunks.length, 2)
  assert.equal(chunks[1], 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000', 'ไม่มี boundary ให้ตัดก่อนเลขเลย ทั้ง segment จึงเป็น chunk เดียว (ตรงกับ L2a\'s เทสเดียวกันที่ verified แล้ว)')
  const elapsedSinceSegment2Start = timestamps[1] - 250 // segment 2 เริ่มตอน delta ที่สองมาถึง (~250ms)
  // margin กว้างกว่าปกติ (600-1050ms รอบ HARD_MAX_MS=800ms) กัน timer jitter จริงของ setTimeout/Promise.race chain
  // ที่นี่ยาวกว่า L2a's เทสเดิม (แค่ setTimeout เดียว ไม่มี Promise.race) — จุดสำคัญคือต้องน้อยกว่า stream-end จริง
  // (~1450ms) มาก ไม่ใช่ต้องตรง 800ms เป๊ะ
  assert.ok(elapsedSinceSegment2Start >= 600 && elapsedSinceSegment2Start < 1050,
    `ต้องมาถึงใกล้ HARD_MAX_MS (~800ms) จาก timer เอง ไม่ใช่รอจนจบ stream ที่ ~1450ms — วัดได้จริง ${elapsedSinceSegment2Start}ms`)
})

// ---------------------------------------------------------------------------
// [END_CALL] bracket-safety — ต้องไม่มีทาง '[' หลุดเข้าไปใน chunk ก่อนจบ stream เด็ดขาด
// ---------------------------------------------------------------------------

test('[END_CALL] bracket-safety (contract lock round 2): SINGLE_SHOT ก็ต้อง strip marker ออกจาก speech เหมือนกัน — ไม่ใช่ raw-yield-แล้ว-caller-strip แบบ askClaudeStream() เดิมอีกต่อไป — endCallRequested/finalText แยกช่องทางชัดเจน', async () => {
  const { events, onMilestone } = makeMilestoneRecorder()
  state.events = [textDelta('ขอบคุณที่สนใจนะคะ [END_CALL]')]
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))
  assert.deepEqual(chunks, ['ขอบคุณที่สนใจนะคะ'], 'speech ห้ามมี marker เลยไม่ว่า mode ไหน (contract ใหม่ ต่างจาก askClaudeStream() เดิม)')
  assert.equal(events.find(e => e.key === 'finalText')?.value, 'ขอบคุณที่สนใจนะคะ')
  assert.equal(events.find(e => e.key === 'endCallRequested')?.value, true)
})

test('[END_CALL] bracket-safety: CHUNKED mode ต้องไม่มี chunk ไหนก่อนจบ stream มี \'[\' ปนอยู่เลย แม้ boundary จะตัดใกล้ marker มากแค่ไหน — endCallRequested milestone ต้องยิงแทน', async () => {
  const delaysMs = [0, 250]
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [
    textDelta('ประโยคแรกยาวพอที่จะเป็นเทิร์นแรกครับ. '), // boundary แรก → trigger grace
    textDelta('ขอบคุณที่สนใจนะคะ [END_CALL]'), // มาหลัง grace fire — มี marker ติดท้ายเลย ไม่มี delta คั่นระหว่างข้อความกับ [END_CALL]
  ]
  const { events, onMilestone } = makeMilestoneRecorder()
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  for (const c of chunks) {
    assert.ok(!c.includes('['), `chunk "${c}" ห้ามมี '[' ปนอยู่เลยไม่ว่ากรณีใด`)
    assert.ok(!c.includes('END_CALL'), `chunk "${c}" ห้ามมีข้อความ marker หลุดออกมาเป็นคำพูดเด็ดขาด`)
  }
  const rejoined = chunks.join(' ')
  assert.ok(rejoined.includes('ขอบคุณที่สนใจนะคะ'), 'เนื้อความจริงต้องยังมาถึงครบ แค่ marker ถูกกันไว้')
  assert.equal(events.find(e => e.key === 'endCallRequested')?.value, true, 'ต้องส่งสัญญาณ end_call ผ่าน milestone แยกต่างหาก เพราะข้อความไม่มี marker ให้ caller เช็คเองอีกแล้ว')
})

test('[END_CALL] bracket-safety: marker ถูกแยกเป็นหลาย delta ("[END" แล้ว "_CALL]" แยกกัน) หลัง grace fire แล้ว → ยังไม่มี chunk ไหนมี \'[\' หลุดออกมา', async () => {
  const delaysMs = [0, 250, 10, 10]
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [
    textDelta('ประโยคแรกยาวพอที่จะเป็นเทิร์นแรกครับ. '),
    textDelta('ขอบคุณค่ะ '),
    textDelta('[END'),
    textDelta('_CALL]'),
  ]
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, null))
  for (const c of chunks) assert.ok(!c.includes('['), `chunk "${c}" ห้ามมี '[' หลุดออกมาแม้ marker จะถูกแยกเป็นหลาย delta`)
})

// ---------------------------------------------------------------------------
// Abort — ต้องไม่แขวน consumer loop ค้างเลย ไม่ว่า abort จะเกิดช่วงไหน
// ---------------------------------------------------------------------------

test('abort ก่อนเจอ boundary ใดๆ (ระหว่าง phase deciding) → generator จบแบบ clean ไม่ค้าง ไม่ throw', async () => {
  const controller = new AbortController()
  const delaysMs = [0, 300] // delta ที่สองมาช้ากว่า abort (ที่ 20ms) มาก — fake stream เช็ค aborted หลัง delay ของตัวเองจบเท่านั้น (ดู makeSlowFakeStream) จึงต้องให้ delay ตัวที่กำลังรอ (index ปัจจุบัน) จบลงก่อน rejection จะเกิดจริง
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [textDelta('เริ่มพูด'), textDelta('ต่ออีกนิด')]

  const gen = askClaudeConditionalStream(makeSession(), controller.signal, null)
  const chunks = []
  const collectPromise = (async () => { for await (const c of gen) chunks.push(c) })()
  await delay(20)
  controller.abort()
  await collectPromise // ต้อง resolve ได้จริงหลัง delta ที่สอง (300ms) ตรวจพบ aborted แล้ว throw ไม่ค้างตลอดไป
  assert.deepEqual(chunks, [], 'abort ก่อนมี chunk ไหนพร้อม ต้องไม่ yield อะไรเลย')
})

test('abort หลัง grace fire แล้ว (ระหว่าง CHUNKED mode กำลังรอ delta ถัดไป) → generator จบแบบ clean ไม่ค้าง', async () => {
  const controller = new AbortController()
  const delaysMs = [0, 300] // delta ที่สองต้องมาหลัง grace (150ms) แน่นอน แต่ยัง short พอให้เทสไม่ช้าเกินไป
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [textDelta('ประโยคแรกครับ. '), textDelta('ประโยคสอง จะไม่มาถึงจริง')]

  const gen = askClaudeConditionalStream(makeSession(), controller.signal, null)
  const chunks = []
  const collectPromise = (async () => { for await (const c of gen) chunks.push(c) })()
  await delay(200) // ผ่าน grace (150ms) ไปแล้ว แน่นอนว่า mode=CHUNKED และ chunk แรกถูก yield ไปแล้ว
  assert.equal(chunks.length, 1, 'chunk แรกต้อง yield ไปแล้วก่อน abort')
  controller.abort()
  await collectPromise
  assert.equal(chunks.length, 1, 'ไม่ควรมี chunk เพิ่มหลัง abort')
})

// ---------------------------------------------------------------------------
// Error propagation (ไม่ใช่ abort)
// ---------------------------------------------------------------------------

test('provider error กลางทาง (ไม่ใช่ abort) → error propagate ออกจาก generator ตรงๆ ไม่ถูกกลืน', async () => {
  state.streamImpl = () => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('ข้อความก่อนพัง')
      throw new Error('boom — จำลอง Claude API ล้มจริงกลางทาง')
    },
  })
  await assert.rejects(collect(askClaudeConditionalStream(makeSession(), null, null)), /boom/)
})

// ---------------------------------------------------------------------------
// 150ms grace boundary — deterministic fake-clock tests (design review round 2, 2026-08-21)
// Real setTimeout at exactly 149/150/151ms is not reliably distinguishable under scheduler jitter, so
// these use node:test's mock.timers to control both the internal grace timer and the fake stream's delay
// deterministically. Each test uses a stream that yields ONE delta (an immediate strong-boundary "."  so
// firstSafeAt fires synchronously, arming grace at t=0) then goes silent for exactly N ms before the
// generator function itself returns (true stream completion, not just "another delta arrives") — isolating
// exactly the race this blocker cares about: does grace or genuine stream-completion resolve first.
// ---------------------------------------------------------------------------

function makeGraceBoundaryStream(silentMs) {
  return () => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('ประโยคแรกครับ. ') // strong boundary "." present immediately — firstSafeAt fires with no search delay
      await new Promise(r => setTimeout(r, silentMs))
      // stream ends here — no further deltas, this IS completion
    },
  })
}

test('grace boundary: completion ~149ms หลัง first-safe (ก่อน grace 150ms) → SINGLE_SHOT เสมอ, ไม่มี early chunk เลย', async () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    state.streamImpl = makeGraceBoundaryStream(149)
    const { events, onMilestone } = makeMilestoneRecorder()
    const collectPromise = collect(askClaudeConditionalStream(makeSession(), null, onMilestone))
    await flushMicrotasks()
    mock.timers.tick(149)
    await flushMicrotasks()
    const chunks = await collectPromise

    assert.equal(events.find(e => e.key === 'mode')?.value, 'SINGLE_SHOT')
    assert.deepEqual(chunks, ['ประโยคแรกครับ.'], 'ต้อง yield ครั้งเดียว ไม่มี early chunk ใดๆ มาก่อนหน้านี้เลย')
  } finally {
    mock.timers.reset()
  }
})

test('grace boundary: completion ~151ms หลัง first-safe (หลัง grace 150ms) → CHUNKED เสมอ, early chunk ถูก yield ครั้งเดียว', async () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    state.streamImpl = makeGraceBoundaryStream(151)
    const { events, onMilestone } = makeMilestoneRecorder()
    const collectPromise = collect(askClaudeConditionalStream(makeSession(), null, onMilestone))
    await flushMicrotasks()
    mock.timers.tick(150) // grace ครบพอดี — ต้องยิงก่อน stream completion ที่ 151ms
    await flushMicrotasks()
    mock.timers.tick(1) // เดินต่อให้ stream completion (151ms) มาถึงด้วย จบ generator จริง
    await flushMicrotasks()
    const chunks = await collectPromise

    assert.equal(events.find(e => e.key === 'mode')?.value, 'CHUNKED')
    assert.equal(chunks.length, 1, 'early chunk ต้องถูก yield แค่ครั้งเดียว ไม่ใช่ซ้ำ')
    assert.equal(chunks[0], 'ประโยคแรกครับ.')
  } finally {
    mock.timers.reset()
  }
})

test('grace boundary: exact/nominal tie ที่ 150ms พอดี → deterministic ผลเดียวเสมอ ไม่ double-yield ไม่หาย (implementation-defined winner — วัดจริงแล้วคือ CHUNKED ชนะ ไม่ใช่ SINGLE_SHOT ตามที่เดาไว้ตอนแรกใน comment เหนือ askClaudeConditionalStream() ข้อกำหนดจริงคือ deterministic ไม่ใช่ฝั่งไหนต้องชนะ)', async () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    state.streamImpl = makeGraceBoundaryStream(150)
    const { events, onMilestone } = makeMilestoneRecorder()
    const collectPromise = collect(askClaudeConditionalStream(makeSession(), null, onMilestone))
    await flushMicrotasks()
    mock.timers.tick(150) // ทั้งสองฝั่ง (grace timer + stream's silentMs delay) armed ที่ 150ms พอดีเหมือนกัน
    await flushMicrotasks()
    const chunks = await collectPromise

    // ข้อกำหนดจริงคือ deterministic + ไม่ double-yield + ไม่หาย ไม่ได้บังคับว่าฝั่งไหนต้องชนะ — แค่ยืนยันว่าได้ผลลัพธ์
    // ที่ถูกต้อง "แบบใดแบบหนึ่ง" ครบถ้วนสมบูรณ์ ไม่ใช่ผสมกันครึ่งๆ กลางๆ (เช่น mode=CHUNKED แต่ไม่มี early chunk เลย)
    const mode = events.find(e => e.key === 'mode')?.value
    assert.ok(mode === 'SINGLE_SHOT' || mode === 'CHUNKED', `mode ต้องถูกตัดสินแน่ชัด ได้จริง: ${mode}`)
    if (mode === 'SINGLE_SHOT') {
      assert.deepEqual(chunks, ['ประโยคแรกครับ.'])
    } else {
      assert.equal(chunks.length, 1)
      assert.equal(chunks[0], 'ประโยคแรกครับ.')
    }
    // ยืนยันความ deterministic จริง: รันซ้ำหลายรอบด้วย setup เดิมทุกประการ ต้องได้ mode เดิมทุกครั้ง ไม่สุ่ม
    for (let i = 0; i < 4; i++) {
      state.streamImpl = makeGraceBoundaryStream(150)
      const { events: events2, onMilestone: onMilestone2 } = makeMilestoneRecorder()
      const p2 = collect(askClaudeConditionalStream(makeSession(), null, onMilestone2))
      await flushMicrotasks()
      mock.timers.tick(150)
      await flushMicrotasks()
      await p2
      assert.equal(events2.find(e => e.key === 'mode')?.value, mode, `รอบที่ ${i + 2} ต้องได้ mode เดิม (${mode}) ทุกครั้ง ไม่สุ่มเปลี่ยนไปมา`)
    }
  } finally {
    mock.timers.reset()
  }
})

test('grace boundary: abort ก่อนถึง 150ms (หลัง first-safe เจอแล้ว) → ไม่มี speech chunk ใดๆ ถูก yield เลย', async () => {
  mock.timers.enable({ apis: ['setTimeout'] })
  try {
    const controller = new AbortController()
    state.streamImpl = () => ({
      async *[Symbol.asyncIterator]() {
        yield textDelta('ประโยคแรกครับ. ')
        await new Promise(r => setTimeout(r, 5000)) // ไม่มีทางมาถึงจริง — จะถูก abort ก่อน
      },
    })
    const chunks = []
    const collectPromise = (async () => {
      for await (const c of askClaudeConditionalStream(makeSession(), controller.signal, null)) chunks.push(c)
    })()
    await flushMicrotasks()
    mock.timers.tick(80) // ผ่าน first-safe ไปแล้ว (เจอทันทีที่ delta แรกมาถึง) แต่ยังไม่ถึง grace (150ms)
    await flushMicrotasks()
    controller.abort()
    mock.timers.tick(5000) // ปลุก fake stream ให้เช็ค signal.aborted แล้ว throw ตามที่ควรเป็น
    await flushMicrotasks()
    await collectPromise

    assert.deepEqual(chunks, [], 'abort ก่อน grace ต้องไม่มี speech chunk ใดๆ ยิงออกไปเลยแม้จะเจอ first-safe ไปแล้วก็ตาม')
  } finally {
    mock.timers.reset()
  }
})

// ---------------------------------------------------------------------------
// Track L (design revision 2026-08-22, Design Gate R3 PASS) — request/response size telemetry, diagnostic
// only. Computed from the EXACT post-slice(-MAX_HISTORY) `history`/`systemPrompt` this request sends —
// never from a caller-side copy of the full session. Must never affect the real Claude request/response.
// ---------------------------------------------------------------------------

function makeMultiMessageSession(messages, scriptOverride) {
  return { name: 'ทดสอบ', campaign: { script: scriptOverride ?? 'สคริปต์ทดสอบ' }, messages }
}

test('Track L inputStats: turn ปกติ — ค่าตรงกับ history/systemPrompt ที่ request จริงใช้เป๊ะ', async () => {
  const messages = [
    { role: 'user', content: 'สวัสดีครับ' },
    { role: 'assistant', content: 'สวัสดีค่ะ ยินดีให้บริการค่ะ' },
    { role: 'user', content: 'สนใจโปรโมชั่นครับ' },
  ]
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeMultiMessageSession(messages), null, onMilestone))

  const inputStats = events.find(e => e.key === 'inputStats')?.value
  assert.ok(inputStats, 'ต้องมี inputStats milestone จริง')
  assert.equal(inputStats.requestMessageCount, 3, 'ยังไม่เกิน MAX_HISTORY — ต้องได้ทั้ง 3 ข้อความ')
  assert.equal(inputStats.currentUserCharCount, 'สนใจโปรโมชั่นครับ'.length, 'current user ต้องเป็นข้อความสุดท้าย')
  const expectedPriorChars = 'สวัสดีครับ'.length + 'สวัสดีค่ะ ยินดีให้บริการค่ะ'.length
  assert.equal(inputStats.priorHistoryCharCount, expectedPriorChars, 'prior history ต้องไม่รวม current user (ข้อความสุดท้าย)')
  assert.ok(typeof inputStats.systemPromptCharCount === 'number' && inputStats.systemPromptCharCount > 0, 'systemPromptCharCount ต้องเป็นตัวเลขจริง ไม่ null (มาจาก buildSystemPrompt() เสมอ)')
  assert.equal(
    inputStats.approxInputTextCharCount,
    inputStats.systemPromptCharCount + inputStats.priorHistoryCharCount + inputStats.currentUserCharCount,
    'approx ต้องเป็นผลรวมของ 3 ตัวเป๊ะ'
  )
})

test('Track L inputStats: MAX_HISTORY truncation semantic — 21 ข้อความ, message[0] ยาวผิดปกติ ต้องไม่ถูกนับเลย ไม่ใช่แค่ count=20', async () => {
  const veryLongFirstMessage = 'ก'.repeat(5000) // ยาวผิดปกติมาก ถ้าหลุดรอดมานับด้วยจะเห็นชัดทันที
  const messages = [{ role: 'user', content: veryLongFirstMessage }]
  for (let i = 0; i < 20; i++) {
    messages.push({ role: i % 2 === 0 ? 'assistant' : 'user', content: `ข้อความที่ ${i}` })
  }
  // รวม 21 ข้อความ (1 ยาวผิดปกติ + 20 ปกติ) — ตัวสุดท้ายต้องเป็น user เพื่อให้ history ไม่ว่าง
  if (messages[messages.length - 1].role !== 'user') messages.push({ role: 'user', content: 'ข้อความสุดท้าย' })

  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeMultiMessageSession(messages), null, onMilestone))

  const inputStats = events.find(e => e.key === 'inputStats')?.value
  assert.equal(inputStats.requestMessageCount, 20, 'MAX_HISTORY=20 — ต้อง cap ที่ 20 เป๊ะ')
  // ยืนยันว่า message[0] (5000 ตัวอักษร) ไม่ได้ถูกนับเลย — ถ้าหลุดรอดมา priorHistoryCharCount จะเกิน 5000 แน่นอน
  assert.ok(inputStats.priorHistoryCharCount < 5000, `message[0] (5000 ตัวอักษร) ต้องไม่ถูกนับ ได้จริง priorHistoryCharCount=${inputStats.priorHistoryCharCount}`)
})

test('Track L inputStats: null-propagation policy — prior message ที่ content ไม่ใช่ string ทำให้ priorHistoryCharCount/approxInputTextCharCount เป็น null ทั้งคู่ ไม่ใช่ partial sum', async () => {
  const messages = [
    { role: 'user', content: 'ข้อความแรกปกติ' },
    { role: 'assistant', content: { unexpected: 'shape' } }, // content ไม่ใช่ string โดยตั้งใจ
    { role: 'user', content: 'ข้อความสุดท้ายปกติ' },
  ]
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeMultiMessageSession(messages), null, onMilestone))

  const inputStats = events.find(e => e.key === 'inputStats')?.value
  assert.equal(inputStats.priorHistoryCharCount, null, 'มี message วัดไม่ได้แม้แต่ตัวเดียวใน prior history ต้อง null ทั้งก้อน ไม่ partial sum')
  assert.equal(inputStats.approxInputTextCharCount, null, 'ต้อง null ตามไปด้วย เพราะพึ่ง priorHistoryCharCount')
  // field อื่นที่ไม่ได้พึ่ง prior history ต้องยังใช้ได้ปกติ ไม่ถูกดึงลงไปเป็น null ไปด้วย
  assert.equal(inputStats.currentUserCharCount, 'ข้อความสุดท้ายปกติ'.length)
  assert.equal(inputStats.requestMessageCount, 3)
  assert.ok(typeof inputStats.systemPromptCharCount === 'number')
})

test('Track L responseCharCount: ตรงกับ finalText.length เป๊ะ (SINGLE_SHOT)', async () => {
  state.events = [textDelta('คำตอบสั้นๆ')]
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  const finalText = events.find(e => e.key === 'finalText')?.value
  const responseCharCount = events.find(e => e.key === 'responseCharCount')?.value
  assert.equal(responseCharCount, finalText.length)
})

test('Track L responseCharCount: ตรงกับ finalText.length เป๊ะ (CHUNKED)', async () => {
  const delaysMs = [0, 250]
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [textDelta('ประโยคแรกครับ. '), textDelta('ประโยคสองยาวกว่านี้หน่อย.')]
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  const finalText = events.find(e => e.key === 'finalText')?.value
  const responseCharCount = events.find(e => e.key === 'responseCharCount')?.value
  assert.equal(responseCharCount, finalText.length, 'responseCharCount ต้องมาจาก rawText/finalText เดียวกัน ไม่ใช่การรวม chunk ที่ yield ออกไป')
})

test('Track L computation-throw: prior history มี message ที่ผิดรูปแบบ (ไม่ใช่ object) จนทำให้ charLen() คำนวณพัง → outer catch จับได้ ยิง inputStats(null) แทน ไม่ throw ทะลุออกไปกระทบ request จริง', async () => {
  // fixture design (verified against source ก่อนเขียนเทส): history=[valid, null, valid] — cachedMsgs.map()
  // (บรรทัดก่อน Track L block) เข้าถึง m.role/m.content เฉพาะ index สุดท้ายเท่านั้น (`i === history.length-1
  // ? {...} : m`), ตัวกลางที่เป็น null จึงผ่าน cachedMsgs ไปได้แบบ pass-through ไม่ throw ที่นั่น — การ throw จริง
  // จึงเกิดครั้งแรกที่ Track L เอง ใน `priorMessages.map(m => charLen(m.content))` (m.content บน null throw
  // TypeError ก่อน charLen() จะถูกเรียกด้วยซ้ำ) ซึ่งอยู่ใน outer try ของ Track L พอดี — ไม่ใช่ throw จาก
  // existing request-preparation code ก่อนหน้า
  const messages = [
    { role: 'user', content: 'ข้อความปกติ' },
    null, // ผิดรูปแบบโดยตั้งใจ — อยู่ไม่ใช่ index สุดท้าย จึงไม่โดน cachedMsgs.map() แตะเลย
    { role: 'user', content: 'ข้อความสุดท้ายปกติ' },
  ]
  state.events = [textDelta('คำตอบทดสอบครับ')] // ไม่มี boundary char → SINGLE_SHOT ยิงเต็มก้อนครั้งเดียวตอนจบ ให้เช็ค output จริงได้

  const { events, onMilestone } = makeMilestoneRecorder()
  const chunks = await collect(askClaudeConditionalStream(makeMultiMessageSession(messages), null, onMilestone))

  // (1) outer catch ถูก execute จริง — พิสูจน์ผ่านค่า: ทาง non-throw ปกติ onMilestone('inputStats', {...}) ส่ง
  // OBJECT เสมอ (ดูเทส "turn ปกติ"/"null-propagation" — .value เป็น object ที่ field ข้างในอาจ null แต่ตัว value
  // เองไม่ใช่ null) มีแค่บรรทัด `onMilestone?.('inputStats', null)` ใน catch(e) เท่านั้นที่ส่ง bare null ตรงๆ
  // ดังนั้น .value===null (ไม่ใช่ object ที่มี field null) คือลายเซ็นเฉพาะของ outer catch เท่านั้น แยกออกจาก
  // null-propagation policy (ซึ่งให้ value เป็น object เสมอ) ได้ชัดเจนไม่กำกวม
  const inputStatsEvents = events.filter(e => e.key === 'inputStats')
  assert.equal(inputStatsEvents.length, 1, 'ต้องยิง inputStats แค่ครั้งเดียว (จาก outer catch fallback) ไม่ใช่ยิงซ้ำหรือไม่ยิงเลย')
  assert.equal(inputStatsEvents[0].value, null, 'ต้องเป็น bare null ตรงๆ (ลายเซ็นเฉพาะของ catch(e) branch) ไม่ใช่ object ที่มี field ข้างในเป็น null')

  // (2) Claude request ยังเกิดจริง (ไม่ได้ short-circuit ออกไปก่อนถึง requestAt)
  assert.ok(events.some(e => e.key === 'requestAt'), 'ต้องเห็น requestAt milestone แปลว่า request ไปต่อหลัง outer catch จับได้แล้วจริง')

  // (3) stream สำเร็จปกติ ได้ finalText ตรงกับที่ fake stream ส่งมา ไม่ถูกทำให้พังหรือ corrupt จาก computation ที่ throw
  const finalTextEvent = events.find(e => e.key === 'finalText')
  assert.equal(finalTextEvent?.value, 'คำตอบทดสอบครับ')

  // (4) yield ออกไปถึง caller จริง ไม่ค้าง ไม่ throw ทะลุออกมาจาก generator
  assert.deepEqual(chunks, ['คำตอบทดสอบครับ'])
})

test('Track L throw-protection: onMilestone throw บน inputStats และ responseCharCount แยกกัน → stream ยังจบปกติ ไม่กระทบ chunk ที่ได้', async () => {
  state.events = [textDelta('คำตอบปกติ')]
  let inputStatsThrew = false, responseCharCountThrew = false
  const throwingMilestone = (key, value) => {
    if (key === 'inputStats') { inputStatsThrew = true; throw new Error('boom inputStats') }
    if (key === 'responseCharCount') { responseCharCountThrew = true; throw new Error('boom responseCharCount') }
  }
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, throwingMilestone))
  assert.deepEqual(chunks, ['คำตอบปกติ'], 'chunk ที่ได้ต้องไม่กระทบแม้ callback จะ throw ทั้งสองจุด')
  assert.ok(inputStatsThrew && responseCharCountThrew, 'ต้องยืนยันว่า callback throw จริงทั้งสองจุด ไม่ใช่ไม่เคยถูกเรียก')
})

// ---------------------------------------------------------------------------
// Track M (design revision 2026-08-22, Design Gate R3 PASS/LOCKED) — chunk boundary telemetry สำหรับ first
// safe chunk เท่านั้น (scope เดียวกับ chunkDelay=t4-t3) diagnostic only ไม่มีการเปลี่ยน timing/threshold ใดๆ
// ---------------------------------------------------------------------------

function findMilestone(events, key) { return events.find(e => e.key === key) }

test('Track M: STRONG_BOUNDARY → l2bChunkFirstCandidateElapsedMs ต้องเท่ากับ chunkDelay (firstSafeAt-firstDeltaAt) เป๊ะ ไม่ใช่แค่ใกล้เคียง', async () => {
  state.events = [textDelta('ขอบคุณค่ะ! ยินดีให้บริการ')] // strong boundary (!) ตัดทันที elapsedMs~0
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  const firstDeltaAt = findMilestone(events, 'firstDeltaAt')?.value
  const firstSafeAt = findMilestone(events, 'firstSafeAt')?.value
  const stats = findMilestone(events, 'chunkReasonStats')?.value
  assert.equal(stats.reason, 'STRONG_BOUNDARY')
  assert.equal(stats.firstCandidateElapsedMs, firstSafeAt - firstDeltaAt, 'ต้องมาจาก timestamp เดิมที่ capture ไว้แล้ว ไม่ใช่ re-measure ใหม่')
  assert.equal(stats.numericProtectionBlocked, false, 'STRONG_BOUNDARY ต้องเป็น false เสมอ (ไม่ใช่ null) เพราะ numeric protection ไม่เกี่ยวข้องกับ path นี้โดยนิยาม')
})

test('Track M: SOFT_BOUNDARY → l2bChunkFirstCandidateElapsedMs เท่ากับ chunkDelay เป๊ะเช่นกัน', async () => {
  state.events = [textDelta('ยินดีค่ะ พี่สนใจไหมคะ')]
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  const firstDeltaAt = findMilestone(events, 'firstDeltaAt')?.value
  const firstSafeAt = findMilestone(events, 'firstSafeAt')?.value
  const stats = findMilestone(events, 'chunkReasonStats')?.value
  assert.equal(stats.reason, 'SOFT_BOUNDARY')
  assert.equal(stats.firstCandidateElapsedMs, firstSafeAt - firstDeltaAt)
  assert.equal(stats.numericProtectionBlocked, false)
})

test('Track M: NATURAL_BOUNDARY_HARD_MAX กับ numeric-protection ที่เคย block จริง → firstCandidateElapsedMs ต้องน้อยกว่า chunkDelay อย่างมีนัยสำคัญ (candidate พร้อมตั้งแต่ก่อน HARD_MAX แต่ emit ช้าไปถึง HARD_MAX)', { timeout: 5000 }, async () => {
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 ') // >=25 ตัวอักษร ลงท้ายตัวเลข+space, elapsedMs~0 ยังไม่ครบ SOFT_TIMEOUT
      await new Promise(r => setTimeout(r, 400)) // ตอนนี้ elapsedMs~400 อยู่ใน [300,800) → candidate eligible+blocked (ไม่มีจุดตัดใหม่ ไม่มี strong/soft trigger)
      yield textDelta('ก') // ตัวอักษรเดี่ยวไม่ trigger boundary ใหม่ ไม่เพิ่ม space ใหม่ — candidate เดิมยัง blocked อยู่
      await new Promise(r => setTimeout(r, 500)) // รวม elapsedMs~900 (>=HARD_MAX 800) — ไม่มี delta ใหม่มาปลุกจนกว่าจะถึงตอนนี้
      yield textDelta('ข') // delta ที่ทำให้ evaluate ใหม่ที่ elapsedMs>=HARD_MAX → cut ที่ตำแหน่งเดิม (ก่อนตัวเลข)
    },
  })
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  const firstDeltaAt = findMilestone(events, 'firstDeltaAt')?.value
  const firstSafeAt = findMilestone(events, 'firstSafeAt')?.value
  const chunkDelay = firstSafeAt - firstDeltaAt
  const stats = findMilestone(events, 'chunkReasonStats')?.value

  assert.equal(stats.reason, 'NATURAL_BOUNDARY_HARD_MAX')
  assert.equal(stats.numericProtectionBlocked, true, 'ต้องจับได้ว่าเคย blocked จริงระหว่างทาง')
  assert.ok(chunkDelay >= 850, `chunkDelay ต้องสูงจริง (~900ms) วัดได้จริง ${chunkDelay}ms`)
  assert.ok(stats.firstCandidateElapsedMs < chunkDelay - 300, `candidate ต้องพร้อมเร็วกว่า emit มากจริง (candidate=${stats.firstCandidateElapsedMs}ms, chunkDelay=${chunkDelay}ms)`)
  assert.ok(stats.firstCandidateElapsedMs >= 350 && stats.firstCandidateElapsedMs < 500, `candidate ต้องมาจากรอบที่ 400ms ไม่ใช่รอบแรก(~0) หรือรอบสุดท้าย(~900) ได้จริง ${stats.firstCandidateElapsedMs}ms`)
  assert.equal(stats.deltaCount, 3, 'ต้องนับ delta ทั้ง 3 ก้อนที่มาก่อน cut (รวมก้อนที่ทำให้ cut ด้วย)')
  assert.ok(stats.preSafeDeltaGapMs >= 450, `gap ระหว่าง delta ก่อนหน้ากับ delta ที่ cut ต้อง ~500ms ได้จริง ${stats.preSafeDeltaGapMs}ms`)
})

test('Track M: SINGLE_SHOT ที่มี firstSafeAt จริง (grace ทัน) → l2bChunk* fields ยัง populate ปกติ ไม่ null เพราะ mode', async () => {
  const delaysMs = [0, 20] // เร็วกว่า grace (150ms) มาก
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delaysMs, signal)
  state.events = [textDelta('ประโยคแรกครับ. '), textDelta('ประโยคสอง.')]
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  assert.equal(findMilestone(events, 'mode')?.value, 'SINGLE_SHOT')
  const stats = findMilestone(events, 'chunkReasonStats')?.value
  assert.ok(stats, 'ต้องมี chunkReasonStats แม้ mode สุดท้ายจะเป็น SINGLE_SHOT เพราะ firstSafeAt ถูกจับไปแล้วก่อนจะรู้ผล grace')
  assert.equal(stats.reason, 'STRONG_BOUNDARY')
})

test('Track M: stream จบก่อนเจอ boundary ใดๆ เลย (chunkDelay=null) → chunkReasonStats ไม่ถูกยิงเลย', async () => {
  state.events = [textDelta('ครับผม')] // ไม่มี boundary ใดๆ เลยตลอด stream
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  assert.equal(findMilestone(events, 'chunkReasonStats'), undefined, 'ไม่เจอ boundary เลย ต้องไม่มี milestone นี้ยิงออกมา (fields เหลือ default null ตามธรรมชาติของ turnMetrics)')
})

test('Track M Blocker 3 regression: delta ที่มาระหว่าง 150ms grace race (หลัง firstSafeAt ถูกจับแล้ว) ต้องไม่ถูกนับเข้า deltaCount', { timeout: 5000 }, async () => {
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('ประโยคแรกครับ. ') // strong boundary ทันที elapsedMs~0 → firstSafeAt จับที่นี่, deltaCount ต้อง freeze ที่ 1
      await new Promise(r => setTimeout(r, 60)) // ยังอยู่ใน grace window (150ms)
      yield textDelta('เพิ่มเข้ามาอีก ') // delta นี้มาระหว่าง grace — mode ยังเป็น null แต่ firstSafeAt ไม่ null แล้ว
      await new Promise(r => setTimeout(r, 60))
      yield textDelta('อีกก้อนนึง ') // อีกก้อนระหว่าง grace เช่นกัน
      await new Promise(r => setTimeout(r, 300)) // เลย grace ไปแน่นอน → mode ต้องกลายเป็น CHUNKED
    },
  })
  const { events, onMilestone } = makeMilestoneRecorder()
  await collect(askClaudeConditionalStream(makeSession(), null, onMilestone))

  const stats = findMilestone(events, 'chunkReasonStats')?.value
  assert.equal(stats.deltaCount, 1, 'ต้อง freeze ที่ 1 (delta แรกที่ทำให้เกิด first-safe) ไม่ขยับตาม delta ที่มาระหว่าง grace')
  assert.equal(stats.preSafeDeltaGapMs, 0, 'delta แรกสุดของ turn ต้อง gap=0 (ไม่มี delta ก่อนหน้า)')
  assert.equal(findMilestone(events, 'mode')?.value, 'CHUNKED', 'sanity: ต้องเลย grace จริง ไม่ใช่ SINGLE_SHOT โดยไม่ตั้งใจ')
})

test('Track M throw-protection: onMilestone throw บน chunkReasonStats → stream/chunk จริงยังจบปกติ ไม่กระทบ', async () => {
  state.events = [textDelta('ขอบคุณค่ะ!')] // strong boundary จริง — ต้องเจอ chunkReasonStats ถูกยิงจริง ไม่ใช่ fixture ที่ไม่มี boundary เลย
  let threw = false
  const throwingMilestone = (key, value) => {
    if (key === 'chunkReasonStats') { threw = true; throw new Error('boom chunkReasonStats') }
  }
  const chunks = await collect(askClaudeConditionalStream(makeSession(), null, throwingMilestone))
  assert.deepEqual(chunks, ['ขอบคุณค่ะ!'])
  assert.ok(threw, 'ต้องยืนยันว่า callback throw จริง ไม่ใช่ไม่เคยถูกเรียก')
})
