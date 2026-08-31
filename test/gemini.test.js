// Dual Conversation Provider A/B — tests for askGeminiConditionalStream(). Same require.cache stub
// technique as test/claudeConditional.test.js so this exercises the real driver/chunking logic against a
// fake @google/genai stream, without ever calling the real Gemini API.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

const state = { chunks: [], streamImpl: null, lastParams: null }

function makeFakeStream(chunks, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < chunks.length; i++) {
        if (signal?.aborted) {
          const err = new Error('This operation was aborted')
          err.name = 'AbortError'
          throw err
        }
        yield chunks[i]
      }
    },
  }
}

function makeSlowFakeStream(chunks, delaysMs, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < chunks.length; i++) {
        if (delaysMs[i]) await new Promise(r => setTimeout(r, delaysMs[i]))
        if (signal?.aborted) {
          const err = new Error('This operation was aborted')
          err.name = 'AbortError'
          throw err
        }
        yield chunks[i]
      }
    },
  }
}

class FakeGoogleGenAI {
  constructor() {
    this.models = {
      generateContentStream: async (params) => {
        state.lastParams = params
        return state.streamImpl(state.chunks, params.config?.abortSignal)
      },
    }
  }
}

const genaiPath = require.resolve('@google/genai')
require.cache[genaiPath] = {
  id: genaiPath, filename: genaiPath, loaded: true,
  exports: { GoogleGenAI: FakeGoogleGenAI },
}

const { askGeminiConditionalStream, GEMINI_MODEL } = require('../src/services/gemini')

beforeEach(() => {
  state.chunks = []
  state.streamImpl = makeFakeStream
  state.lastParams = null
})

function session(messages, overrides = {}) {
  return {
    name: 'คุณทดสอบ',
    campaign: { script: 'ขายโปรโมชั่นทดสอบ', voice_id: 'th-TH-Test' },
    messages,
    ...overrides,
  }
}

test('ไม่มี history เลย (เทิร์นแรกจริงๆ) → yield คำทักทาย fallback ไม่เรียก Gemini เลย', async () => {
  let called = false
  state.streamImpl = () => { called = true; return makeFakeStream([]) }
  const gen = askGeminiConditionalStream(session([]))
  const { value, done } = await gen.next()
  assert.equal(value, 'สวัสดีค่ะ')
  assert.equal(called, false)
  await gen.next()
})

test('SINGLE_SHOT: ตอบสั้นจบภายใน grace 150ms → yield ก้อนเดียว, mode=SINGLE_SHOT, ไม่มี [END_CALL] หลุดออกมา', async () => {
  state.chunks = [{ text: 'สวัสดีค่ะ ยินดีให้บริการ' }]
  const milestones = {}
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'สวัสดี' }]), null, (k, v) => { milestones[k] = v })
  const chunks = []
  for await (const c of gen) chunks.push(c)
  assert.deepEqual(chunks, ['สวัสดีค่ะ ยินดีให้บริการ'])
  assert.equal(milestones.mode, 'SINGLE_SHOT')
  assert.equal(milestones.endCallRequested, false)
  assert.equal(milestones.finalText, 'สวัสดีค่ะ ยินดีให้บริการ')
})

// Track 2 fix (2026-08-30) — mirror ของ claudeConditional.test.js เป๊ะ: SINGLE_SHOT push gate เดิม >=3 ตัวอักษร
// ทำให้คำตอบสั้นจริง (เช่น "คะ") ไม่เคยถูกพูดเลยทั้งที่ milestone finalText บอกว่าตอบสำเร็จแล้ว
test('Track 2: SINGLE_SHOT คำตอบสั้นจริง 1-2 ตัวอักษร (เช่น "คะ") ต้องถูก push เป็น chunk ด้วย ไม่ใช่หายไปเงียบๆ', async () => {
  state.chunks = [{ text: 'คะ' }]
  const milestones = {}
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]), null, (k, v) => { milestones[k] = v })
  const chunks = []
  for await (const c of gen) chunks.push(c)
  assert.deepEqual(chunks, ['คะ'], 'คำตอบสั้นแต่จริง ต้องถูกพูดออกไป ไม่ใช่หายเงียบทั้งที่ milestone finalText บอกว่าตอบสำเร็จ')
  assert.equal(milestones.mode, 'SINGLE_SHOT')
  assert.equal(milestones.finalText, 'คะ')
})

test('CHUNKED: stream ยังไม่จบเมื่อ grace หมดเวลา → เปลี่ยนเป็น mode=CHUNKED และ yield หลายก้อน', async () => {
  state.streamImpl = (chunks, signal) => makeSlowFakeStream(chunks, [0, 250], signal)
  state.chunks = [{ text: 'ประโยคแรกจบแล้วค่ะ ' }, { text: 'ประโยคที่สองตามมา' }]
  const milestones = {}
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'สวัสดี' }]), null, (k, v) => { milestones[k] = v })
  const chunks = []
  for await (const c of gen) chunks.push(c)
  assert.equal(milestones.mode, 'CHUNKED')
  assert.ok(chunks.length >= 2, `ควรมีมากกว่า 1 ก้อนใน CHUNKED mode, ได้ ${chunks.length}`)
})

test('[END_CALL] ไม่หลุดเข้าไปในเสียงที่ยิลด์เลยสักก้อน แต่ endCallRequested/finalText รู้ว่ามี', async () => {
  state.chunks = [{ text: 'ขอบคุณที่ติดต่อมานะคะ [END_CALL]' }]
  const milestones = {}
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ไม่สนใจแล้วค่ะ' }]), null, (k, v) => { milestones[k] = v })
  const chunks = []
  for await (const c of gen) chunks.push(c)
  for (const c of chunks) assert.ok(!c.includes('[END_CALL]'), `speech chunk ต้องไม่มี marker: "${c}"`)
  assert.equal(milestones.endCallRequested, true)
  assert.equal(milestones.finalText, 'ขอบคุณที่ติดต่อมานะคะ')
})

test('abort ระหว่าง stream (barge-in) → generator หยุดสะอาด ไม่ throw, ไม่ yield ก้อนหลัง abort', async () => {
  const controller = new AbortController()
  state.streamImpl = (chunks, signal) => makeSlowFakeStream(chunks, [0, 500], signal)
  state.chunks = [{ text: 'เริ่มพูด' }, { text: 'พูดต่อหลัง abort' }]
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'พูดต่อ' }]), controller.signal, null)
  const chunks = []
  const iterate = (async () => { for await (const c of gen) chunks.push(c) })()
  await new Promise(r => setTimeout(r, 50))
  controller.abort()
  await iterate
  assert.ok(!chunks.some(c => c.includes('พูดต่อหลัง abort')))
})

test('history role mapping: assistant → model, user คงเดิม (mapped เฉพาะตอนส่ง API ไม่แก้ session.messages)', async () => {
  state.chunks = [{ text: 'ตอบกลับค่ะ' }]
  const history = [
    { role: 'user', content: 'สวัสดี' },
    { role: 'assistant', content: 'สวัสดีค่ะ' },
    { role: 'user', content: 'มีโปรอะไรบ้าง' },
  ]
  const gen = askGeminiConditionalStream(session(history))
  for await (const _ of gen) { /* drain */ }
  assert.deepEqual(state.lastParams.contents.map(c => c.role), ['user', 'model', 'user'])
  assert.deepEqual(history.map(m => m.role), ['user', 'assistant', 'user'], 'session.messages ต้องไม่ถูกแก้')
})

test('request config: model/thinkingLevel/maxOutputTokens/abortSignal ถูกส่งไปถูกต้อง', async () => {
  state.chunks = [{ text: 'โอเคค่ะ' }]
  const controller = new AbortController()
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]), controller.signal)
  for await (const _ of gen) { /* drain */ }
  // Asserts against the exported GEMINI_MODEL constant, not a hardcoded string — this stays correct through
  // the Gemini Latency Root-Cause Test's temporary model swaps (3.7 → 3.6 → back) without needing an edit here.
  assert.equal(state.lastParams.model, GEMINI_MODEL)
  assert.equal(state.lastParams.config.thinkingConfig.thinkingLevel, 'MINIMAL')
  assert.equal(state.lastParams.config.maxOutputTokens, 200)
  assert.equal(state.lastParams.config.abortSignal, controller.signal)
})

// ===== Track 2 (Gemini Lifecycle Diagnostics, Implementation Gate 2026-08-31, RCA revision 3 locked spec) =====

test('Track 2: streamCreatedAt mark หลัง generateContentStream() resolve เท่านั้น — ไม่ mark ก่อนหน้านั้นแม้ await ยังค้างอยู่', async () => {
  const milestones = {}
  state.chunks = [{ text: 'สวัสดีค่ะ' }]
  // generateContentStream เองคืน Promise ที่ resolve ช้า (จำลอง await ที่ยังไม่จบ) — ต่างจาก makeSlowFakeStream
  // ที่จำลอง delay ระหว่าง "ทีละ chunk ของ stream ที่สร้างสำเร็จแล้ว" คนละจุดกัน
  state.streamImpl = (chunks, signal) => new Promise(resolve => {
    setTimeout(() => resolve(makeFakeStream(chunks, signal)), 60)
  })
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]), null, (k, v) => { milestones[k] = v })

  const drainPromise = (async () => { for await (const _ of gen) { /* drain */ } })()
  await new Promise(r => setTimeout(r, 25)) // ยังไม่ครบ 60ms ที่ generateContentStream() ใช้ resolve
  assert.equal(milestones.streamCreatedAt, undefined, 'ยังไม่ควร mark เพราะ await generateContentStream() เองยังไม่ resolve')

  await drainPromise
  assert.ok(milestones.streamCreatedAt != null, 'ต้อง mark แล้วหลัง await resolve จริง')
})

test('Track 2: firstRawChunkAt mark ทันทีที่ iterator resolve แม้ chunk แรกจะไม่มี .text (metadata-only) — คนละจุดกับ firstDeltaAt', async () => {
  const milestones = {}
  state.chunks = [{}, { text: 'ข้อความจริง' }] // chunk แรกไม่มี text เลย (เช่น metadata/thinking event)
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]), null, (k, v) => { milestones[k] = v })
  for await (const _ of gen) { /* drain */ }
  assert.ok(milestones.firstRawChunkAt != null, 'ต้อง mark ตั้งแต่ raw chunk แรกแม้ไม่มี text')
  assert.ok(milestones.firstDeltaAt != null)
  assert.ok(milestones.firstRawChunkAt <= milestones.firstDeltaAt, 'raw chunk (metadata) ต้องมาถึงก่อนหรือพร้อมกับ text delta แรกเสมอ ไม่ใช่หลัง')
})

test('Track 2: activeGeminiAttemptCountAtStart นับ LOCAL overlap ถูกต้อง — 0 ตอนไม่มีใครทำงานอยู่, เพิ่มขึ้นตอนมี attempt ค้างพร้อมกัน, กลับมา 0 หลังทุกอย่างจบ (ไม่ leak)', async () => {
  const milestonesB = {}
  const milestonesC = {}
  let resolveA
  const gateA = new Promise(resolve => { resolveA = resolve })
  const sess = session([{ role: 'user', content: 'ทดสอบ' }], { callSid: 'CA_OVERLAP_TEST' })

  let firstCall = true
  state.streamImpl = (chunks, signal) => {
    if (firstCall) { firstCall = false; return gateA.then(() => makeFakeStream([{ text: 'A' }], signal)) }
    return makeFakeStream([{ text: 'ok' }], signal)
  }

  const genA = askGeminiConditionalStream(sess, null, null)
  const drainA = (async () => { for await (const _ of genA) { /* drain */ } })()
  await new Promise(r => setTimeout(r, 10)) // ให้ A เริ่ม request ไปแล้วจริง (ผ่าน increment แล้ว แต่ยังไม่จบ)

  const genB = askGeminiConditionalStream(sess, null, (k, v) => { milestonesB[k] = v })
  for await (const _ of genB) { /* drain */ }
  assert.equal(milestonesB.activeGeminiAttemptCountAtStart, 1, 'B เริ่มขณะ A ยังค้างอยู่ ต้องเห็นว่ามี 1 attempt (A) ทำงานอยู่แล้ว')

  resolveA()
  await drainA

  const genC = askGeminiConditionalStream(sess, null, (k, v) => { milestonesC[k] = v })
  for await (const _ of genC) { /* drain */ }
  assert.equal(milestonesC.activeGeminiAttemptCountAtStart, 0, 'หลัง A และ B จบไปแล้วทั้งคู่ counter ต้องกลับมา 0 ไม่ leak')
})

test('Track 2: generateContentStream() เอง throw ก่อนสร้าง stream สำเร็จ → counter ยัง decrement ถูกต้อง ไม่ leak ไปกระทบ attempt ถัดไป', async () => {
  const sess = session([{ role: 'user', content: 'ทดสอบ' }], { callSid: 'CA_THROW_TEST' })
  state.streamImpl = () => { throw new Error('SDK connection failed') }

  const genA = askGeminiConditionalStream(sess, null, null)
  await assert.rejects(async () => { for await (const _ of genA) { /* drain */ } })

  const milestonesB = {}
  state.streamImpl = (chunks, signal) => makeFakeStream(chunks, signal)
  state.chunks = [{ text: 'โอเค' }]
  const genB = askGeminiConditionalStream(sess, null, (k, v) => { milestonesB[k] = v })
  for await (const _ of genB) { /* drain */ }
  assert.equal(milestonesB.activeGeminiAttemptCountAtStart, 0, 'ความพยายามก่อนหน้าที่ throw ตั้งแต่ generateContentStream() เองต้องไม่ leak counter ค้างไว้')
})

test('Track 2: signalAbortedAt mark ตอน barge-in จริง (childSignal.aborted ถูกตรวจพบครั้งแรก)', async () => {
  const milestones = {}
  const controller = new AbortController()
  state.streamImpl = (chunks, signal) => makeSlowFakeStream(chunks, [0, 500], signal)
  state.chunks = [{ text: 'เริ่มพูด' }, { text: 'พูดต่อ' }]
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]), controller.signal, (k, v) => { milestones[k] = v })
  const iterate = (async () => { for await (const _ of gen) { /* drain */ } })()
  await new Promise(r => setTimeout(r, 50))
  controller.abort()
  await iterate
  assert.ok(milestones.signalAbortedAt != null, 'ต้อง mark เวลาที่ตรวจพบ signal.aborted จริง')
})

test('chunk ที่ไม่มี .text (เช่น metadata-only) ไม่ทำให้พัง — ข้ามไปเฉยๆ', async () => {
  state.chunks = [{}, { text: 'ข้อความจริง' }]
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]))
  const chunks = []
  for await (const c of gen) chunks.push(c)
  assert.deepEqual(chunks, ['ข้อความจริง'])
})
