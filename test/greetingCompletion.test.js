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

const { askClaude, askClaudeStream } = require('../src/services/claude')

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

// ===== Campaign-Controlled Opening (Design Freeze 2026-09-03) =====

test('askClaude: user instruction เป็นข้อความกลางๆ คงที่ ไม่ inject พฤติกรรมธุรกิจ (ไม่ถามสะดวกคุยไหม/มีอะไรให้ช่วยไหม) ไม่ว่า campaign จะเขียนอะไรมา', async () => {
  let capturedMessages = null
  state.createImpl = async (params) => { capturedMessages = params.messages; return textResponse('สวัสดีค่ะ') }
  await askClaude(makeSession({ direction: 'outbound' }))
  const userContent = capturedMessages[0].content
  assert.doesNotMatch(userContent, /สะดวกคุยสักครู่ไหม/, 'ต้องไม่บังคับถามสะดวกคุยไหมอีกต่อไป — ให้ campaign เป็นคนสั่งเอง')
  assert.doesNotMatch(userContent, /มีอะไรให้ช่วยไหมคะ/)
  assert.match(userContent, /Campaign Prompt/, 'ต้องบอกให้ยึด Campaign Prompt เป็นหลัก')
})

test('askClaude: user instruction บอกทิศทางสายเป็นข้อเท็จจริงเท่านั้น (outbound/inbound ต่างกันแค่คำอธิบายทิศทาง ไม่ต่างพฤติกรรม)', async () => {
  let outboundMsg = null, inboundMsg = null
  state.createImpl = async (params) => { outboundMsg = params.messages[0].content; return textResponse('สวัสดีค่ะ') }
  await askClaude(makeSession({ direction: 'outbound' }))
  state.createImpl = async (params) => { inboundMsg = params.messages[0].content; return textResponse('สวัสดีค่ะ') }
  await askClaude(makeSession({ direction: 'inbound' }))
  assert.match(outboundMsg, /AI โทรออกหาลูกค้า/)
  assert.match(inboundMsg, /ลูกค้าโทรเข้ามาเอง/)
  // ส่วนที่เหลือของ instruction (คำสั่งหลัก) ต้องเหมือนกันทุกตัวอักษร ต่างกันแค่ท้ายวงเล็บทิศทาง
  const stripDirection = s => s.replace(/\(สายนี้เป็นสาย.*?\)$/, '')
  assert.equal(stripDirection(outboundMsg), stripDirection(inboundMsg))
})

test('askClaude: system prompt (buildOpeningSystemPrompt) ต้องไม่มีกฎธุรกิจของบทสนทนาปกติ (ดึงกลับโปรโมชั่น, [END_CALL] policy) ปนอยู่ — กัน campaign ที่สั่ง "ห้ามพูดโปรโมชั่น" โดนกฎกลางบังคับย้อนกลับ', async () => {
  let capturedSystem = null
  state.createImpl = async (params) => { capturedSystem = params.system; return textResponse('สวัสดีค่ะ') }
  await askClaude(makeSession({ campaign: { script: 'คุณคือทีมบริการหลังการขาย ห้ามพูดโปรโมชั่นเด็ดขาด' } }))
  assert.doesNotMatch(capturedSystem, /ดึงกลับมาที่โปรโมชั่น/, 'ต้องไม่มีกฎบังคับดึงกลับโปรโมชั่นจาก buildSystemPrompt เดิมปนมาด้วย')
  assert.doesNotMatch(capturedSystem, /\[END_CALL\]/, 'ต้องไม่มี policy END_CALL ของบทสนทนาปกติปนมาด้วย — Opening เป็นเทิร์นเดียว ไม่เกี่ยวกับการจบสาย')
  assert.match(capturedSystem, /ห้ามพูดโปรโมชั่นเด็ดขาด/, 'เนื้อหา campaign prompt เต็มต้องยังส่งเข้าไปครบ')
})

test('askClaude: system prompt ยังคง technical/voice constraint ไว้ (สั้น, ภาษาไทย, ไม่ใช้ markdown, ลงท้ายเพศหญิง)', async () => {
  let capturedSystem = null
  state.createImpl = async (params) => { capturedSystem = params.system; return textResponse('สวัสดีค่ะ') }
  await askClaude(makeSession())
  assert.match(capturedSystem, /ห้ามใช้ bullet points, markdown/)
  assert.match(capturedSystem, /ห้ามใช้ ครับ เด็ดขาด/)
  assert.match(capturedSystem, /ชื่อลูกค้า: จอร์จ/)
})

test('askClaude: campaign.script และ campaign.system_prompt ว่างเปล่า/whitespace ล้วน → fallback ทันที ไม่เรียก LLM เลย', async () => {
  let calls = 0
  state.createImpl = async () => { calls++; return textResponse('ไม่ควรถูกเรียก') }
  const text = await askClaude(makeSession({ campaign: { script: '   ' } }))
  assert.equal(calls, 0, 'ไม่ควรเรียก Anthropic เลยถ้าไม่มี context ใช้งานได้')
  assert.match(text, /จอร์จ/)
  assert.match(text, /สะดวกคุยสักครู่ไหมคะ/, 'ต้องเป็น outbound fallback เดิม')
})

test('askClaude (IR finding): script เป็น whitespace ล้วนแต่ system_prompt ใช้งานได้จริง → ต้องเลือก system_prompt ไม่ใช่ fallback (bug เดิม: "   " || "VALID" ยังได้ "   " เพราะ || เช็คแค่ truthy ไม่รู้เรื่อง trim)', async () => {
  let calls = 0, capturedSystem = null
  state.createImpl = async (params) => { calls++; capturedSystem = params.system; return textResponse('สวัสดีค่ะ') }
  const text = await askClaude(makeSession({ campaign: { script: '   ', system_prompt: 'คุณคือทีมบริการหลังการขาย ห้ามพูดโปรโมชั่นเด็ดขาด' } }))
  assert.equal(calls, 1, 'ต้องเรียก LLM จริง ไม่ใช่ fallback ทั้งที่ system_prompt ใช้งานได้')
  assert.match(capturedSystem, /ห้ามพูดโปรโมชั่นเด็ดขาด/, 'ต้องใช้เนื้อหาจาก system_prompt ไม่ใช่ whitespace ของ script')
  assert.equal(text, 'สวัสดีค่ะ')
})

test('askClaude: Anthropic throw ตอน attempt แรก (เช่น 429/5xx/network) → fallback ทันที ไม่ throw ออกไปให้ผู้เรียกเห็น ไม่ retry', async () => {
  let calls = 0
  state.createImpl = async () => { calls++; throw new Error('Request failed with status code 429') }
  const text = await askClaude(makeSession({ name: 'มด', direction: 'outbound' }))
  assert.equal(calls, 1, 'throw ไม่ควร retry — ตัดไป fallback ทันทีตาม Design Freeze')
  assert.match(text, /มด/)
  assert.match(text, /สะดวกคุยสักครู่ไหมคะ/)
})

test('askClaude: max_tokens ตอน attempt แรก แล้ว Anthropic throw ตอน retry → fallback ทันที', async () => {
  let calls = 0
  state.createImpl = async () => {
    calls++
    if (calls === 1) return textResponse('สวัสดีค่ะ...', 'max_tokens')
    throw new Error('network timeout')
  }
  const text = await askClaude(makeSession({ name: 'ปุ๊', direction: 'inbound' }))
  assert.equal(calls, 2)
  assert.match(text, /ปุ๊/)
  assert.match(text, /มีอะไรให้ช่วยไหมคะ/)
})

test('askClaude: throw ไม่มีทางทำให้คืนค่า empty string — ต้องได้ fallback ที่ใช้พูดได้จริงเสมอ', async () => {
  state.createImpl = async () => { throw new Error('boom') }
  const text = await askClaude(makeSession())
  assert.ok(text && text.length > 0, 'ห้ามคืน empty opening แม้ generation จะล้มเหลวแบบ throw')
})

test('askClaudeStream(isGreeting=true): ต้องใช้ Opening semantics เดียวกับ requestGreetingOnce() (buildOpeningSystemPrompt + openingInstruction) แม้จะเป็น dead path วันนี้ — กัน "ระบบ Opening สองมาตรฐาน"', async () => {
  let capturedSystem = null, capturedUserMsg = null
  state.createImpl = async (params) => {
    // askClaudeStream ส่ง system เป็น content-block array (มี cache_control) ต่างจาก requestGreetingOnce ที่ส่งเป็น string ตรงๆ
    capturedSystem = params.system[0].text
    capturedUserMsg = params.messages[0].content
    return textResponse('สวัสดีค่ะ')
  }
  const gen = askClaudeStream(makeSession({ campaign: { script: 'ห้ามพูดโปรโมชั่นเด็ดขาด' } }), true)
  const results = []
  for await (const chunk of gen) results.push(chunk)
  assert.doesNotMatch(capturedSystem, /ดึงกลับมาที่โปรโมชั่น/, 'isGreeting=true ต้องไม่ใช้ buildSystemPrompt() เดิมที่มีกฎบังคับโปรโมชั่น')
  assert.doesNotMatch(capturedSystem, /\[END_CALL\]/)
  assert.doesNotMatch(capturedUserMsg, /สะดวกคุยสักครู่ไหม/, 'ต้องใช้ openingInstruction() ที่เป็นกลาง ไม่ใช่ instruction เดิม')
  assert.match(capturedUserMsg, /Campaign Prompt/)
})
