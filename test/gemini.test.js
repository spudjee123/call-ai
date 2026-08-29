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

test('chunk ที่ไม่มี .text (เช่น metadata-only) ไม่ทำให้พัง — ข้ามไปเฉยๆ', async () => {
  state.chunks = [{}, { text: 'ข้อความจริง' }]
  const gen = askGeminiConditionalStream(session([{ role: 'user', content: 'ทดสอบ' }]))
  const chunks = []
  for await (const c of gen) chunks.push(c)
  assert.deepEqual(chunks, ['ข้อความจริง'])
})
