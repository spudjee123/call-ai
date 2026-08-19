const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// G1 (greeting completion safety) — stub @anthropic-ai/sdk ก่อน require claude.js ครั้งแรก (pattern เดียวกับที่
// ใช้ทั้ง repo มาตลอด) เพื่อควบคุม stop_reason/content ของ response ได้อิสระ โดยไม่ยิง API จริง — ไม่เคยมี test
// ตรงให้ askClaude()/greeting completion logic มาก่อนเลย (harness เดิม mock ที่ boundary claude.js ทั้งไฟล์)
const state = { createImpl: null }

const anthropicPath = require.resolve('@anthropic-ai/sdk')
require.cache[anthropicPath] = {
  id: anthropicPath, filename: anthropicPath, loaded: true,
  exports: class FakeAnthropic {
    constructor() {
      this.messages = { create: (...args) => state.createImpl(...args) }
    }
  },
}

const { askClaude } = require('../src/services/claude')

beforeEach(() => {
  state.createImpl = null
})

function makeSession(overrides = {}) {
  return {
    name: 'จอร์จ',
    direction: 'outbound',
    campaign: { script: 'สคริปต์ทดสอบ' },
    messages: [],
    ...overrides,
  }
}

function textResponse(text, stopReason = 'end_turn') {
  return { content: [{ type: 'text', text }], stop_reason: stopReason }
}

test('askClaude: attempt แรกสมบูรณ์ (end_turn + text) → รับผลตรงๆ ไม่ retry ไม่ fallback', async () => {
  let calls = 0
  state.createImpl = async (params) => { calls++; return textResponse('สวัสดีค่ะคุณจอร์จ สะดวกคุยไหมคะ') }
  const text = await askClaude(makeSession())
  assert.equal(text, 'สวัสดีค่ะคุณจอร์จ สะดวกคุยไหมคะ')
  assert.equal(calls, 1)
})

test('askClaude: request ที่ส่งไป Anthropic จริงต้องมี max_tokens=120 (ล็อก regression ของต้นเหตุ production โดยตรง — เดิมคือ 60)', async () => {
  let capturedMaxTokens = null
  state.createImpl = async (params) => { capturedMaxTokens = params.max_tokens; return textResponse('สวัสดีค่ะคุณจอร์จ สะดวกคุยไหมคะ') }
  await askClaude(makeSession())
  assert.equal(capturedMaxTokens, 120)
})

test('askClaude: attempt แรก max_tokens (ตัดกลางคำ) → retry ครั้งเดียว แล้วสำเร็จ ใช้ผลจาก attempt ที่สอง', async () => {
  let calls = 0
  state.createImpl = async () => {
    calls++
    if (calls === 1) return textResponse('สวัสดีค่ะคุณจอร์จ หนูฟ้าจากพีจีด็อกโทรมาทักทายนะคะ สะดวกคุยส', 'max_tokens')
    return textResponse('สวัสดีค่ะคุณจอร์จ สะดวกคุยไหมคะ', 'end_turn')
  }
  const text = await askClaude(makeSession())
  assert.equal(text, 'สวัสดีค่ะคุณจอร์จ สะดวกคุยไหมคะ')
  assert.equal(calls, 2, 'ต้อง retry แค่ 1 ครั้ง (รวมเป็น 2 call)')
})

test('askClaude: attempt แรกข้อความว่างเปล่าแต่ไม่ใช่ max_tokens → fallback ทันที ไม่ retry (deterministic copy ถูกอยู่แล้ว)', async () => {
  let calls = 0
  state.createImpl = async () => { calls++; return textResponse('', 'end_turn') }
  const text = await askClaude(makeSession({ name: 'จอร์จ', direction: 'outbound' }))
  assert.equal(calls, 1, 'ไม่ควร retry ถ้า stop_reason ไม่ใช่ max_tokens')
  assert.match(text, /จอร์จ/)
  assert.match(text, /สะดวกคุยสักครู่ไหมคะ/, 'ต้องเป็น outbound fallback')
})

test('askClaude: max_tokens ทั้ง 2 attempt → fallback (2 calls)', async () => {
  let calls = 0
  state.createImpl = async () => { calls++; return textResponse('สวัสดีค่ะ...', 'max_tokens') }
  const text = await askClaude(makeSession({ name: 'มานี', direction: 'outbound' }))
  assert.equal(calls, 2)
  assert.match(text, /มานี/)
  assert.match(text, /สะดวกคุยสักครู่ไหมคะ/)
})

test('askClaude: max_tokens แล้ว attempt ที่สองข้อความว่างเปล่า → fallback (2 calls)', async () => {
  let calls = 0
  state.createImpl = async () => {
    calls++
    if (calls === 1) return textResponse('สวัสดีค่ะ...', 'max_tokens')
    return textResponse('', 'end_turn')
  }
  const text = await askClaude(makeSession({ name: 'สมชาย', direction: 'outbound' }))
  assert.equal(calls, 2)
  assert.match(text, /สมชาย/)
})

test('askClaude: fallback แยกตาม direction ถูกต้อง — inbound ต้องไม่ถาม "สะดวกคุยไหม" (regression risk ที่ design review จับได้)', async () => {
  state.createImpl = async () => textResponse('', 'end_turn')
  const inboundText = await askClaude(makeSession({ name: 'ปอ', direction: 'inbound' }))
  assert.match(inboundText, /มีอะไรให้ช่วยไหมคะ/)
  assert.doesNotMatch(inboundText, /สะดวกคุยสักครู่ไหมคะ/, 'inbound fallback ต้องไม่ใช้ประโยค outbound')
})

test('askClaude: fallback แยกตาม direction ถูกต้อง — outbound', async () => {
  state.createImpl = async () => textResponse('', 'end_turn')
  const outboundText = await askClaude(makeSession({ name: 'ปอ', direction: 'outbound' }))
  assert.match(outboundText, /สะดวกคุยสักครู่ไหมคะ/)
  assert.doesNotMatch(outboundText, /มีอะไรให้ช่วยไหมคะ/, 'outbound fallback ต้องไม่ใช้ประโยค inbound')
})

test('askClaude: response ไม่มี text content block เลย (เช่น content ว่าง/ไม่มี type text) → extractGreetingText ไม่ throw คืน "" ให้ fallback logic ตัดสินใจต่อ', async () => {
  let calls = 0
  state.createImpl = async () => { calls++; return { content: [{ type: 'tool_use', name: 'x' }], stop_reason: 'end_turn' } }
  const text = await askClaude(makeSession({ name: 'แนน', direction: 'outbound' }))
  assert.equal(calls, 1, 'stop_reason ไม่ใช่ max_tokens ไม่ควร retry')
  assert.match(text, /แนน/, 'ต้องได้ fallback ไม่ throw')
})

test('askClaude: response.content เป็น undefined ทั้งหมด (เคส edge สุด) → ไม่ throw ยังได้ fallback', async () => {
  state.createImpl = async () => ({ content: undefined, stop_reason: 'end_turn' })
  const text = await askClaude(makeSession({ name: 'บี', direction: 'outbound' }))
  assert.match(text, /บี/)
})

test('askClaude: [GreetingGen] log ยิงทุก attempt พร้อม stopReason/length ถูกต้อง', async () => {
  let calls = 0
  state.createImpl = async () => {
    calls++
    if (calls === 1) return textResponse('ตัดกลางคำ', 'max_tokens')
    return textResponse('ครบประโยคแล้วค่ะ', 'end_turn')
  }
  const originalLog = console.log
  const logs = []
  console.log = (...args) => { logs.push(args.join(' ')) }
  try {
    await askClaude(makeSession())
  } finally {
    console.log = originalLog
  }
  const greetingLogs = logs.filter(l => l.includes('[GreetingGen]'))
  assert.equal(greetingLogs.length, 2, 'ต้องมี log ทุก attempt (2 ครั้งในเคสนี้)')
  assert.match(greetingLogs[0], /stopReason=max_tokens/)
  assert.match(greetingLogs[0], new RegExp(`length=${'ตัดกลางคำ'.length}`))
  assert.match(greetingLogs[1], /stopReason=end_turn/)
})
