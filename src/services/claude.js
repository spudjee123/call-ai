const Anthropic = require('@anthropic-ai/sdk')
const { performance } = require('perf_hooks')
const { findChunkBoundary, getNumericProtectionRemainingMs, evaluateNumericProtectionDiagnostic, CHUNK_REASON } = require('../utils/speechChunker')

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })

const MAX_HISTORY = 20

// คำสั่งทักทายต่างกันตามทิศทางสาย — outbound คือ AI โทรออกไปหาลูกค้า (ต้องถามว่าสะดวกคุยไหม)
// inbound คือลูกค้าโทรเข้ามาเอง (ไม่ต้องถามว่าสะดวกไหม เพราะลูกค้าเลือกโทรมาเองอยู่แล้ว — ควรถามว่ามีอะไรให้ช่วย)
const OUTBOUND_GREETING_INSTRUCTION = 'ทักทายและแนะนำตัวสั้นๆ แล้วถามว่าสะดวกคุยสักครู่ไหม รวม 1-2 ประโยคเท่านั้น'
const INBOUND_GREETING_INSTRUCTION = 'รับสายลูกค้าที่โทรเข้ามาเอง ทักทายสั้นๆ แนะนำตัวว่าเป็นใคร แล้วถามว่ามีอะไรให้ช่วยไหมคะ รวม 1-2 ประโยคเท่านั้น'

function greetingInstruction(session) {
  return session.direction === 'inbound' ? INBOUND_GREETING_INSTRUCTION : OUTBOUND_GREETING_INSTRUCTION
}

// G1 (production defect 2026-08-20) — greeting เคยถูกตัดกลางคำจริงใน production (max_tokens: 60 ไม่พอสำหรับ
// ประโยคไทยบางประโยค) โดยไม่มี completion check ใดๆ เลย ระบบจึงพูดข้อความที่ตัดกลางคำออกไปให้ลูกค้าฟังตรงๆ
// เพิ่ม margin (60→120) อย่างเดียวไม่พอ เพราะ prompt/ชื่อลูกค้ายาวขึ้นในอนาคตก็ทำให้เกิดซ้ำได้ — ต้องมี completion
// check + fallback ที่ deterministic ด้วย
const GREETING_MAX_TOKENS = 120

// fallback ต้องแยกตาม direction เหมือน greetingInstruction() เอง — inbound (ลูกค้าโทรเข้ามาเอง) ไม่ควรถาม
// "สะดวกคุยไหม" (ดูเหตุผลเดิมที่ greetingInstruction ด้านบน) ถ้าใช้ fallback เดียวปนกันจะทำให้ inbound behavior
// regress เงียบๆ เฉพาะตอน fallback trigger เท่านั้น (เคสที่ test ปกติมักไม่ได้ครอบคลุม)
function outboundFallbackGreeting(name) {
  return `สวัสดีค่ะคุณ${name} ฟ้าจากพีจีด็อกนะคะ โทรมาทักทายและขอบคุณที่เข้ามาเป็นสมาชิกค่ะ สะดวกคุยสักครู่ไหมคะ`
}
function inboundFallbackGreeting(name) {
  return `สวัสดีค่ะคุณ${name} ฟ้าจากพีจีด็อกนะคะ มีอะไรให้ช่วยไหมคะ`
}
function fallbackGreeting(session) {
  return session.direction === 'inbound' ? inboundFallbackGreeting(session.name) : outboundFallbackGreeting(session.name)
}

// defensive: ห้ามอ่าน response.content[0].text ตรงๆ — ถ้า response ไม่มี text block เลย (เช่นตอบด้วย content
// block ชนิดอื่นล้วนๆ) การ index ตรงๆ จะ throw ก่อนถึง fallback logic เสียอีก ทำให้เคส "!text → fallback" ที่
// ตั้งใจไว้ไม่มีทางถูกใช้งานจริง
function extractGreetingText(response) {
  return response.content?.find(block => block.type === 'text')?.text?.trim() || ''
}

async function requestGreetingOnce(session) {
  const { name, campaign } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name)
  const response = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: GREETING_MAX_TOKENS,
    system: systemPrompt,
    messages: [{ role: 'user', content: greetingInstruction(session) }],
  })
  const text = extractGreetingText(response)
  console.log(`[GreetingGen] stopReason=${response.stop_reason} length=${text.length}`)
  return { text, stopReason: response.stop_reason }
}

// ใช้สำหรับ greeting เท่านั้น — Haiku เพราะต้องการ latency ต่ำ
//
// completion policy (G1):
//   attempt 1: end_turn + text        → accept
//              max_tokens             → retry ครั้งเดียว
//              empty text (ไม่ใช่ max_tokens) → fallback ทันที ไม่ retry (deterministic copy ที่ถูกต้องอยู่แล้ว
//                                        retry เพิ่มแค่เสีย latency โดยไม่ได้ประโยชน์)
//   attempt 2: complete + text        → accept
//              max_tokens/empty       → fallback
async function askClaude(session) {
  let result = await requestGreetingOnce(session)
  if (result.stopReason === 'max_tokens') {
    console.error('[GreetingGen] Truncated (max_tokens) — retrying once')
    result = await requestGreetingOnce(session)
  }
  if (result.stopReason === 'max_tokens' || !result.text) {
    console.error(`[GreetingGen] Still incomplete after retry (stopReason=${result.stopReason}, length=${result.text.length}) — using deterministic fallback`)
    return fallbackGreeting(session)
  }
  return result.text
}

async function summarizeCall(session) {
  const transcript = session.messages
    .map(m => `${m.role === 'user' ? 'ลูกค้า' : 'AI'}: ${m.content}`)
    .join('\n')

  const response = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 500,
    messages: [{
      role: 'user',
      content: `วิเคราะห์บทสนทนานี้และตอบในรูปแบบ JSON:
${transcript}

ตอบเป็น JSON ดิบเท่านั้น ห้ามใช้ code block หรือ \`\`\` ห้ามมีข้อความอื่นนอกเหนือจาก JSON:
{
  "outcome": "interested | not_interested | callback | no_answer | angry",
  "summary": "สรุปสั้นๆ 1-2 ประโยค",
  "key_points": "ประเด็นสำคัญที่ลูกค้าพูดถึง",
  "next_action": "สิ่งที่ควรทำต่อ"
}`
    }]
  })

  try {
    return JSON.parse(extractJson(response.content[0].text))
  } catch {
    return { outcome: 'completed', summary: response.content[0].text, key_points: '', next_action: '' }
  }
}

// Claude มักห่อ JSON ด้วย ```json ... ``` แม้สั่งห้ามแล้ว — ดึงเฉพาะเนื้อใน code fence ถ้ามี ก่อน parse
function extractJson(text) {
  const match = text.match(/```(?:json)?\s*([\s\S]*?)```/)
  return match ? match[1].trim() : text.trim()
}

function buildSystemPrompt(campaignPrompt, customerName) {
  return `${campaignPrompt}

ชื่อลูกค้า: ${customerName}
คำตอบต้องสั้นมาก ไม่เกิน 1-2 ประโยคเท่านั้น ภาษาไทย เหมาะกับการพูดทางโทรศัพท์ ห้ามใช้ bullet points, markdown, emoji หรือสัญลักษณ์พิเศษ ตอบกระชับที่สุด

ถ้าลูกค้าคุยนอกเรื่อง ให้ตอบรับสั้นๆ 1 คำ แล้วดึงกลับมาที่โปรโมชั่นทันที ห้ามให้คำแนะนำหรือพูดเรื่องอื่นที่ไม่เกี่ยวกับโปรโมชั่นเด็ดขาด ถ้าลูกค้ายังออกนอกเรื่องซ้ำ ให้โน้มน้าวกลับมาที่โปรโมชั่น ถ้าปฏิเสธซ้ำหลายครั้งจึงจบสาย [END_CALL]

ถ้าถูกถามว่าเป็น AI ให้ตอบตรงๆ ว่าใช่
ถ้าลูกค้าโกรธ ให้พูดเสียงเย็นและพยายามเข้าใจปัญหา
ใช้คำลงท้ายผู้หญิง ค่ะ หรือ คะ เสมอ ห้ามใช้ ครับ เด็ดขาด ห้ามใช้คำว่า ผม เด็ดขาด ให้ใช้คำว่า หนู แทนทุกกรณีโดยไม่มีข้อยกเว้น
เมื่อต้องการตอบรับลูกค้า ตัวอย่างเช่น: ดีใจมากเลยค่ะ / ยินดีค่ะ / ขอบคุณค่ะ / เข้าใจแล้วค่ะ / ดีค่ะ ห้ามใช้คำที่ไม่มีในภาษาไทยปกติหรือคำซ้ำผิดปกติ
ถ้าไม่เข้าใจสิ่งที่ลูกค้าพูด ให้ถามสั้นๆ ว่า พูดซ้ำได้ไหมคะ อย่าทวนคำที่ฟังไม่ชัด
ใช้ [END_CALL] เฉพาะ 2 กรณีเท่านั้น:
1. ลูกค้าขอจบสายเองชัดเจน เช่น วางสายได้เลย / ไม่สะดวกแล้ว / ขอบคุณไม่ต้องแล้ว
2. ลูกค้าปฏิเสธซ้ำหลังจากที่ชักชวนเพิ่มเติมแล้ว — ครั้งแรกที่ปฏิเสธให้โน้มน้าวอีกครั้งก่อนเสมอ
ถ้าลูกค้าบอกรับทราบ / โอเค / ได้ / เข้าใจ / จะลองทำ / สนใจ / อยากลอง / ลองดู → ถามว่า มีอะไรสอบถามเพิ่มเติมไหมคะ ห้ามใช้ [END_CALL]
ถ้าลูกค้าสนใจและต้องการสมัคร → แนะนำขั้นตอนสั้นๆ แล้วต้องถาม มีอะไรสอบถามเพิ่มเติมไหมคะ ก่อนเสมอ ถ้าลูกค้าตอบว่าไม่มีคำถามเพิ่มเติม ให้กล่าว ขอบคุณที่สนใจนะคะ [END_CALL]
ห้ามใช้ [END_CALL] หลังพูดโปรโมชั่น ต้องรอฟังคำตอบก่อนเสมอ
ห้ามพูดข้อมูลซ้ำที่พูดไปแล้วในสายนี้ ตอบต่อจาก context ล่าสุด ไม่ต้องสรุปซ้ำ
STT บนสายโทรศัพท์อาจฟังผิดบ้าง ให้ตีความจาก context การสนทนาเสมอ ไม่ตอบตาม text ตรงๆ ถ้าคำนั้นไม่ make sense ในบริบท`
}

async function* askClaudeStream(session, isGreeting = false, signal = null) {
  const { name, campaign, messages } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name)
  const history = messages.slice(-MAX_HISTORY)
  const msgs = isGreeting
    ? [{ role: 'user', content: greetingInstruction(session) }]
    : history

  if (!msgs.length) { yield 'สวัสดีค่ะ'; return }

  // แคช prefix ของบทสนทนาด้วย — ทำเครื่องหมายที่ข้อความล่าสุดของ request นี้
  // เทิร์นถัดไปจะมี history ชุดเดิม + ข้อความใหม่ต่อท้าย จึงอ่าน prefix เดิมจาก cache ได้
  // (ข้าม greeting เพราะเรียกแค่ครั้งเดียวต่อสาย ไม่มีการใช้ซ้ำให้คุ้มค่าเขียน cache)
  const cachedMsgs = isGreeting ? msgs : msgs.map((m, i) =>
    i === msgs.length - 1
      ? { role: m.role, content: [{ type: 'text', text: m.content, cache_control: { type: 'ephemeral' } }] }
      : m
  )

  const response = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 200,
    // Sonnet 4.6 default effort เป็น high ถ้าไม่ตั้งไว้ — งานนี้แค่ตอบ 1-2 ประโยคสั้นๆ
    // ลดเป็น low ตามคำแนะนำของ Anthropic สำหรับงานแชท/บทสนทนาสั้นๆ เพื่อลดเวลาคิดต่อ turn
    // ระบุ thinking: disabled ตรงๆ (ไม่พึ่ง default ที่ omit แล้วได้ off เฉยๆ) กัน default เปลี่ยนในอนาคตแล้วเผลอเปิด thinking โดยไม่ตั้งใจ
    thinking: { type: 'disabled' },
    output_config: { effort: 'low' },
    // แคช system prompt ไว้ — เหมือนกันทุก turn ในสายเดียวกัน (ต่างแค่ชื่อลูกค้าข้ามสาย)
    // ทำให้ turn ถัดไปในสายเดียวกันไม่ต้องประมวลผล system prompt ซ้ำทั้งก้อน
    system: [
      { type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }
    ],
    messages: cachedMsgs,
  }, { signal })

  if (signal?.aborted) return

  const text = response.content[0].text.trim()
  if (text.length >= 3) yield text
}

// ==========================================================================
// askClaudeStreamChunked — Checkpoint B (stream+ตัดประโยค)
// ตั้งใจแยกจาก askClaudeStream ข้างบนโดยสิ้นเชิง ไม่แก้ฟังก์ชันเดิมแม้แต่บรรทัดเดียว —
// prewarm และ fallback เดิมยังเรียก askClaudeStream ตามปกติ ไม่ถูกกระทบเลย
// ยังไม่ถูกเชื่อมเข้า audioStream.js ในขั้นนี้ (รอ Checkpoint C)
// ==========================================================================

// B2: ย้าย [END_CALL] จาก string marker ในข้อความ → tool call แยกต่างหาก (control channel)
// เหตุผล: string marker เสี่ยงหลุดไปโดน TTS พูดออกไปถ้า chunker ตัดคาบเกี่ยวจุดตัดพอดี — tool_use
// เป็นคนละ content block จาก text อยู่แล้วในตัว ไม่มีทางหลุดปนกับข้อความที่ส่งเข้า TTS ได้เลย
// schema ว่างเปล่าตั้งใจ — ยังไม่มี use case ต้องส่งข้อมูลอะไรเพิ่ม ใส่ field ไปเพิ่ม failure surface เฉยๆ
const END_CALL_TOOL = {
  name: 'end_call',
  description: 'เรียกเมื่อควรจบการสนทนา/วางสายตามเงื่อนไขที่กำหนดไว้ในคำสั่งระบบเท่านั้น',
  input_schema: { type: 'object', properties: {}, additionalProperties: false },
}

// เวอร์ชันแยกของ buildSystemPrompt เฉพาะ path ที่มี end_call tool จริง (มี tools: [END_CALL_TOOL] ส่งไปด้วย)
// จงใจไม่ใช้ฟังก์ชันร่วมกับ buildSystemPrompt เดิม เพราะ path เดิม (askClaude/askClaudeStream) ไม่ได้ประกาศ
// tool นี้ไว้เลย — ถ้าไปสั่งให้ Claude "เรียก tool end_call" ในบททดสอบ/สายจริงที่ไม่มี tool นั้นจริง จะสับสนเปล่าๆ
// มีเนื้อหาซ้ำกับ buildSystemPrompt อยู่บ้างโดยตั้งใจ เพื่อความปลอดภัย/แยกส่วนสมบูรณ์ — รอรวมเป็นฟังก์ชันเดียว
// ตอน Checkpoint C ที่ path เดิมถูกเลิกใช้แล้วจริงๆ
function buildSystemPromptToolBased(campaignPrompt, customerName) {
  return `${campaignPrompt}

ชื่อลูกค้า: ${customerName}
คำตอบต้องสั้นมาก ไม่เกิน 1-2 ประโยคเท่านั้น ภาษาไทย เหมาะกับการพูดทางโทรศัพท์ ห้ามใช้ bullet points, markdown, emoji หรือสัญลักษณ์พิเศษ ตอบกระชับที่สุด

ถ้าลูกค้าคุยนอกเรื่อง ให้ตอบรับสั้นๆ 1 คำ แล้วดึงกลับมาที่โปรโมชั่นทันที ห้ามให้คำแนะนำหรือพูดเรื่องอื่นที่ไม่เกี่ยวกับโปรโมชั่นเด็ดขาด ถ้าลูกค้ายังออกนอกเรื่องซ้ำ ให้โน้มน้าวกลับมาที่โปรโมชั่น ถ้าปฏิเสธซ้ำหลายครั้งจึงเรียก tool end_call เพื่อจบสาย

ถ้าถูกถามว่าเป็น AI ให้ตอบตรงๆ ว่าใช่
ถ้าลูกค้าโกรธ ให้พูดเสียงเย็นและพยายามเข้าใจปัญหา
ใช้คำลงท้ายผู้หญิง ค่ะ หรือ คะ เสมอ ห้ามใช้ ครับ เด็ดขาด ห้ามใช้คำว่า ผม เด็ดขาด ให้ใช้คำว่า หนู แทนทุกกรณีโดยไม่มีข้อยกเว้น
เมื่อต้องการตอบรับลูกค้า ตัวอย่างเช่น: ดีใจมากเลยค่ะ / ยินดีค่ะ / ขอบคุณค่ะ / เข้าใจแล้วค่ะ / ดีค่ะ ห้ามใช้คำที่ไม่มีในภาษาไทยปกติหรือคำซ้ำผิดปกติ
ถ้าไม่เข้าใจสิ่งที่ลูกค้าพูด ให้ถามสั้นๆ ว่า พูดซ้ำได้ไหมคะ อย่าทวนคำที่ฟังไม่ชัด
เรียก tool end_call เฉพาะ 2 กรณีเท่านั้น (ห้ามพิมพ์คำว่า end_call หรือสัญลักษณ์ใดๆ ลงในข้อความที่พูดเด็ดขาด ให้เรียก tool จริงๆ เท่านั้น):
1. ลูกค้าขอจบสายเองชัดเจน เช่น วางสายได้เลย / ไม่สะดวกแล้ว / ขอบคุณไม่ต้องแล้ว
2. ลูกค้าปฏิเสธซ้ำหลังจากที่ชักชวนเพิ่มเติมแล้ว — ครั้งแรกที่ปฏิเสธให้โน้มน้าวอีกครั้งก่อนเสมอ
ถ้าลูกค้าบอกรับทราบ / โอเค / ได้ / เข้าใจ / จะลองทำ / สนใจ / อยากลอง / ลองดู → ถามว่า มีอะไรสอบถามเพิ่มเติมไหมคะ ห้ามเรียก tool end_call
ถ้าลูกค้าสนใจและต้องการสมัคร → แนะนำขั้นตอนสั้นๆ แล้วต้องถาม มีอะไรสอบถามเพิ่มเติมไหมคะ ก่อนเสมอ ถ้าลูกค้าตอบว่าไม่มีคำถามเพิ่มเติม ให้กล่าว ขอบคุณที่สนใจนะคะ แล้วเรียก tool end_call
ห้ามเรียก tool end_call หลังพูดโปรโมชั่น ต้องรอฟังคำตอบก่อนเสมอ
ห้ามพูดข้อมูลซ้ำที่พูดไปแล้วในสายนี้ ตอบต่อจาก context ล่าสุด ไม่ต้องสรุปซ้ำ
STT บนสายโทรศัพท์อาจฟังผิดบ้าง ให้ตีความจาก context การสนทนาเสมอ ไม่ตอบตาม text ตรงๆ ถ้าคำนั้นไม่ make sense ในบริบท`
}

// stream token จริงจาก Claude — yield text เป็น delta ดิบทีละก้อนตามที่ API ส่งมา (เหมือน B1 เป๊ะ
// ไม่ต่อ/ไม่แทรกช่องว่างเอง ผู้เรียก accumulate เอง: buffer += chunk ตรงๆ)
// onControl(...) ถูกเรียกแยกต่างหาก (ไม่ปนกับ yield) เมื่อ Claude เรียก tool end_call สมบูรณ์แล้วเท่านั้น —
// ไม่ผูกกับการวางสายจริงในขั้นนี้ (รอ Checkpoint C ที่มี generationId/state machine มาคุม lifecycle จริง)
async function* askClaudeStreamChunked(session, signal = null, onControl = null) {
  const { name, campaign, messages } = session
  const systemPrompt = buildSystemPromptToolBased(campaign.script || campaign.system_prompt, name)
  const history = messages.slice(-MAX_HISTORY)

  if (!history.length) { yield 'สวัสดีค่ะ'; return }

  const cachedMsgs = history.map((m, i) =>
    i === history.length - 1
      ? { role: m.role, content: [{ type: 'text', text: m.content, cache_control: { type: 'ephemeral' } }] }
      : m
  )

  const stream = client.messages.stream({
    model: 'claude-sonnet-4-6',
    max_tokens: 200,
    thinking: { type: 'disabled' },
    output_config: { effort: 'low' },
    system: [
      { type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }
    ],
    messages: cachedMsgs,
    tools: [END_CALL_TOOL],
  }, { signal })

  // เก็บ state ของแต่ละ content block แยกตาม index — เทิร์นเดียวอาจมีทั้ง text block และ tool_use block ปนกัน
  // (input_json_delta ของ tool_use ต้องสะสมจนกว่า content_block_stop ค่อย validate เพราะระหว่างทางยังเป็น
  // partial JSON parse ไม่ได้ — ตามที่เอกสาร Anthropic แนะนำ)
  const blocks = new Map()

  for await (const event of stream) {
    if (signal?.aborted) return

    if (event.type === 'content_block_start') {
      blocks.set(event.index, { type: event.content_block?.type, name: event.content_block?.name })
      continue
    }

    if (event.type === 'content_block_delta') {
      if (event.delta?.type === 'text_delta') {
        yield event.delta.text
      }
      // input_json_delta ของ tool_use ไม่ต้องสะสมจริงจัง เพราะ schema ว่างเปล่า (ไม่มี field ให้อ่าน) —
      // แค่รอ content_block_stop ของ block ที่เป็น end_call ก็พอจะยืนยันว่าเรียก tool นี้จริง
      continue
    }

    if (event.type === 'content_block_stop') {
      const block = blocks.get(event.index)
      // ต้องเช็คจาก content_block.type/name ที่เก็บไว้ตอน start เท่านั้น ห้ามพึ่ง stop_reason ของทั้ง message
      // เพราะ stop_reason: "tool_use" บอกแค่ว่าหยุดเพราะมีการเรียก tool ไม่ได้บอกว่าเป็น tool ตัวไหน
      if (block?.type === 'tool_use' && block?.name === 'end_call') {
        onControl?.({ type: 'end_call' })
      }
      blocks.delete(event.index)
      continue
    }

    // event ชนิดอื่นที่ไม่รู้จัก (Anthropic อาจเพิ่ม event type ใหม่ได้ในอนาคต) → ignore อย่างปลอดภัย ไม่ throw
  }
}

// ==========================================================================
// askClaudeObservedFullResponse — L2a (legacy Claude instrumentation, design locked 2026-08-20)
// ==========================================================================
// askClaudeStream() ด้านบน "ไม่ได้ streaming จริง" — เรียก client.messages.create() (blocking, รอเต็มก้อน)
// แล้ว yield ข้อความเต็มครั้งเดียว จึงไม่มีทางวัด first-delta/first-safe-sentence จริงได้เลย แต่ askClaudeStream()
// ถูกใช้ร่วมกันถึง 3 จุด (fresh legacy :1029, legacy prewarm :389, runLegacyFallback :205 ของ chunked-path) —
// ห้ามแก้ transport ของฟังก์ชันเดิมเด็ดขาด เพราะจะเปลี่ยน abort/error/timing semantics ของทั้ง 3 จุดพร้อมกันทั้งที่
// ตั้งใจ isolate แค่ fresh-call site เดียวเพื่อสังเกตการณ์ (design review round 2, ยืนยันจาก grep จริง)
//
// ฟังก์ชันนี้จึงแยกต่างหากสมบูรณ์ — clone config จาก askClaudeStream() ทุกจุด (model/max_tokens/thinking/effort/
// system prompt/cache_control/messages) เปลี่ยนแค่ transport เป็น client.messages.stream() แล้วยัง yield ข้อความ
// เต็มครั้งเดียวตอนจบเหมือนเดิมทุกประการ — ผู้เรียก (audioStream.js) ยังเริ่ม TTS หลัง full completion เท่านั้น
// ไม่มีการเริ่มพูดเร็วขึ้นเลยจาก L2a — เป็น instrumentation ล้วนๆ
//
// onMilestone(key, value) ถูกเรียกทันทีที่แต่ละเหตุการณ์เกิดขึ้นจริง (ไม่รอ callback เดียวตอนจบ) เพื่อไม่ให้ turn
// ที่ abort/timeout/error กลางทางเสีย TTFT/first-safe data ไปทั้งหมด (dataset bias ไปทาง successful turns) —
// pattern เดียวกับ onChunkTelemetry ที่ L1c2a ใช้อยู่แล้ว: key ที่เป็นไปได้คือ requestAt/firstDeltaAt/firstSafeAt/
// fullAt — fullAt ถูกเรียกเฉพาะตอน stream จบตามปกติเท่านั้น (ไม่เรียกถ้า abort/error — ปล่อย null ตามจริง)
//
// firstSafeAt ใช้ findChunkBoundary()/getNumericProtectionRemainingMs() จาก speechChunker.js ตรงๆ (import only,
// ไม่แก้ไฟล์นั้นเลย) — elapsedMs คำนวณจาก firstDeltaAt (เวลาที่ตัวอักษรแรกของ buffer นี้มาถึงจริง) ตาม contract
// ของ speechChunker.js ("เวลานับจากตัวอักษรแรกของ buffer ปัจจุบันมาถึง") ไม่ใช่จาก request start — ถ้าใช้ request
// start จะทำให้ turn ที่ Claude TTFT ช้าอยู่แล้ว (เช่น 900ms) ดูเหมือนมี "safe chunk พร้อมเร็ว" เกินจริงทันทีที่
// delta แรกมาถึง ทั้งที่ buffer เพิ่งเริ่มสะสมจริงๆ แค่เสี้ยววินาที
//
// mirror wall-clock numeric-protection re-check ของ createChunkedProducer()'s drainReadyChunks() (chunkedTurn.js)
// ด้วย — ถ้า buffer ลงท้ายด้วยตัวเลข (เช่น "รับ 2,000") findChunkBoundary() จะคืน null จนกว่าจะครบ HARD_MAX_MS แต่
// ถ้าไม่มี delta ใหม่มาปลุกเลย (Claude เงียบ) จะไม่มีใครมา re-check เอง — arm setTimeout ผ่าน
// getNumericProtectionRemainingMs() เหมือนกันเป๊ะ ต่างจาก producer จริงตรงที่ L2a สนใจแค่ safe boundary "ตัวแรก"
// เท่านั้น หยุด watch ทันทีที่เจอ ไม่ต้อง loop/drain หลายก้อนต่อแบบ producer จริงที่ดูแลทั้งเทิร์น
async function* askClaudeObservedFullResponse(session, signal = null, onMilestone = null) {
  const { name, campaign, messages } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name)
  const history = messages.slice(-MAX_HISTORY)

  if (!history.length) { yield 'สวัสดีค่ะ'; return }

  const cachedMsgs = history.map((m, i) =>
    i === history.length - 1
      ? { role: m.role, content: [{ type: 'text', text: m.content, cache_control: { type: 'ephemeral' } }] }
      : m
  )

  const requestAt = performance.now()
  onMilestone?.('requestAt', requestAt)

  const stream = client.messages.stream({
    model: 'claude-sonnet-4-6',
    max_tokens: 200,
    thinking: { type: 'disabled' },
    output_config: { effort: 'low' },
    system: [
      { type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }
    ],
    messages: cachedMsgs,
  }, { signal })

  let text = ''
  let firstDeltaAt = null
  let firstSafeAt = null
  let numericProtectionTimer = null

  function clearNumericProtectionTimer() {
    if (numericProtectionTimer) { clearTimeout(numericProtectionTimer); numericProtectionTimer = null }
  }

  function checkFirstSafe() {
    if (firstSafeAt !== null || firstDeltaAt === null) return
    const elapsedMs = performance.now() - firstDeltaAt
    const result = findChunkBoundary(text, elapsedMs)
    if (result) {
      firstSafeAt = performance.now()
      onMilestone?.('firstSafeAt', firstSafeAt)
      clearNumericProtectionTimer()
      return
    }
    clearNumericProtectionTimer()
    const remainingMs = getNumericProtectionRemainingMs(text, elapsedMs)
    if (remainingMs != null) {
      numericProtectionTimer = setTimeout(() => {
        numericProtectionTimer = null
        if (signal?.aborted) return
        checkFirstSafe()
      }, remainingMs)
    }
  }

  try {
    for await (const event of stream) {
      if (signal?.aborted) return
      if (event.type === 'content_block_delta' && event.delta?.type === 'text_delta') {
        if (firstDeltaAt === null) {
          firstDeltaAt = performance.now()
          onMilestone?.('firstDeltaAt', firstDeltaAt)
        }
        text += event.delta.text
        checkFirstSafe()
      }
    }
  } finally {
    clearNumericProtectionTimer() // stream จบแล้ว (ปกติหรือ error/abort) — ไม่ต้องรอ expiry timer อีกต่อไป
  }

  if (signal?.aborted) return

  const fullAt = performance.now()
  onMilestone?.('fullAt', fullAt)

  const trimmed = text.trim()
  if (trimmed.length >= 3) yield trimmed
}

// ==========================================================================
// askClaudeConditionalStream — L2b PROTOTYPE (design revision 2026-08-21, NOT wired into any live call
// path — local prototype only, no rollout, no commit/push/deploy per gate instructions)
// ==========================================================================
// Design per L2a Expanded Measurement (24 production fresh-observed turns, CONDITIONAL L2b decision):
//   first-safe boundary found → wait CONDITIONAL_GRACE_MS (150ms)
//     Claude finishes within grace → SINGLE_SHOT: yield the canonical text once (measured: SHORT responses
//       get ~0 benefit from chunking, so they stay on a single TTS request — no seam, no continuity risk)
//     Claude still generating past grace → CHUNKED: yield the held first-safe chunk immediately, then
//       continue yielding each subsequent safe chunk as speechChunker finds it (same boundary/numeric-
//       protection logic chunkedTurn.js already uses in production for the L1b chunked-rollout path)
//   On an exact tie (stream-completion and the grace timer scheduled for the identical target time) this
//   implementation resolves to CHUNKED, empirically — verified by a repeated-run test in
//   claudeConditional.test.js that asserts the SAME outcome across multiple iterations, never a coin flip,
//   never a double-yield. (An earlier draft of this comment guessed stream-completion would win via
//   Promise.race's array-order tie-break among already-settled promises — that guess was wrong; measuring
//   it directly showed the grace timer's callback actually fires first under Node's mock-timer scheduling
//   here. What the design review round actually required was determinism, not a specific winner, so this
//   comment now states the measured behavior instead of an unverified mechanism.)
//
// Why this can't reuse askClaudeStreamChunked()/createChunkedProducer() directly (design review finding):
// askClaudeStreamChunked() uses a DIFFERENT system prompt (buildSystemPromptToolBased) and a DIFFERENT
// end_call mechanism (Anthropic tool_use) than legacy's buildSystemPrompt/[END_CALL] text marker — routing
// through it would silently change what Claude generates, not just how it's transported, confounding the
// L2a comparison entirely. This function clones askClaudeStream()'s exact prompt/config (same as
// askClaudeObservedFullResponse() does) and only adds incremental boundary detection + conditional timing.
//
// END_CALL contract (locked design review round 2, 2026-08-21) — three separate channels, none derived
// from the others:
//   Speech (yielded chunks, both modes) — NEVER contains "[END_CALL]" or a partial fragment of it. Bracket-
//     safety: boundary search never looks past the first unmatched '[' in the buffer (safeView()/
//     tryFindBoundary() below), so no chunk yielded before stream-end can ever contain '['. The final
//     post-stream chunk (either mode) also has the marker stripped before yielding.
//   Canonical history (`finalText` milestone) — built ONLY from `rawText`, the raw per-delta accumulator
//     that chunk boundaries never touch, with the marker stripped. A future caller must NOT reconstruct
//     this by concatenating yielded speech chunks — CHUNKED mode's chunks are stripped independently per
//     chunk, so concatenation is not guaranteed byte-for-byte equal to `finalText` and must never be
//     treated as if it were.
//   Control (`endCallRequested` milestone, boolean) — `rawText.includes('[END_CALL]')`. A future
//     audioStream.js integration must use this, not `fullText.includes('[END_CALL]')` on reconstructed
//     text, then run through the exact same premature-end-call guard policy that already exists.
// This deliberately replaces askClaudeStream()'s older "yield raw text incl. marker, caller strips it"
// convention — this function's speech output is unconditionally marker-free in both modes, uniformly.
//
// TTS continuity note (design review, not this prototype's concern): CHUNKED-mode chunks are synthesized
// as independent ElevenLabs requests with no previous_text threading, same as the current L1b chunked path
// (ENABLE_PREVIOUS_TEXT_CONTINUITY is hardcoded false in chunkedTurn.js after a production 400 incident) —
// L2b prototype inherits that same known prosody-seam risk, does not attempt to fix or worsen it.
const CONDITIONAL_GRACE_MS = 150

async function* askClaudeConditionalStream(session, signal = null, onMilestone = null) {
  const { name, campaign, messages } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name)
  const history = messages.slice(-MAX_HISTORY)

  if (!history.length) { yield 'สวัสดีค่ะ'; return }

  const cachedMsgs = history.map((m, i) =>
    i === history.length - 1
      ? { role: m.role, content: [{ type: 'text', text: m.content, cache_control: { type: 'ephemeral' } }] }
      : m
  )

  // Track L (diagnostic only, design revision 2026-08-22, Design Gate R3 PASS) — computed from the EXACT
  // `history`/`systemPrompt` this request sends (post-slice(-MAX_HISTORY), same objects cachedMsgs is built
  // from — verified cachedMsgs never transforms the .content TEXT VALUE itself, only re-wraps the last
  // message into Anthropic's content-block array shape, so counting from `history` directly here is accurate
  // for actual text; only structural/JSON overhead is excluded, which is exactly what "Approx" signals).
  // Strictly typed: non-string content produces null for that count, never a fabricated 0 — and ANY
  // unmeasurable message in prior history poisons the whole priorHistoryCharCount/approxInputTextCharCount
  // to null rather than a partial sum that looks complete but silently has a gap.
  // Both computation AND emission are wrapped in try/catch — this is a NEW milestone with more payload
  // surface area than the existing scalar ones, and nothing upstream (onEarlyTtsMilestone/wrappedMilestone
  // in audioStream.js) currently guards against a throwing callback, so the guard has to live here.
  try {
    const charLen = (c) => typeof c === 'string' ? c.length : null

    const priorMessages = history.slice(0, -1)
    const priorLens = priorMessages.map(m => charLen(m.content))
    const priorHistoryCharCount = priorLens.some(l => l === null) ? null : priorLens.reduce((a, b) => a + b, 0)

    const currentUserCharCount = charLen(history[history.length - 1].content)

    const approxInputTextCharCount =
      (priorHistoryCharCount === null || currentUserCharCount === null)
        ? null
        : systemPrompt.length + priorHistoryCharCount + currentUserCharCount

    try {
      onMilestone?.('inputStats', {
        systemPromptCharCount: systemPrompt.length,
        priorHistoryCharCount,
        requestMessageCount: history.length,
        currentUserCharCount,
        approxInputTextCharCount,
      })
    } catch (_) { /* diagnostic only — must never affect the real request below */ }
  } catch (e) {
    try { onMilestone?.('inputStats', null) } catch (_) { /* diagnostic only */ }
  }

  const requestAt = performance.now()
  onMilestone?.('requestAt', requestAt)

  const stream = client.messages.stream({
    model: 'claude-sonnet-4-6',
    max_tokens: 200,
    thinking: { type: 'disabled' },
    output_config: { effort: 'low' },
    system: [
      { type: 'text', text: systemPrompt, cache_control: { type: 'ephemeral' } }
    ],
    messages: cachedMsgs,
  }, { signal })

  // Internal producer/consumer split mirrors chunkedTurn.js's createChunkedProducer shape (queue +
  // waiter-resolve) — unavoidable because a plain generator can only yield from its own body, never from
  // a detached setTimeout callback, so the grace-vs-stream-completion race has to live in a separate
  // driver that pushes decided items into a queue this generator's body drains.
  const items = []
  let waiter = null
  function push(item) { items.push(item); if (waiter) { const w = waiter; waiter = null; w() } }
  function waitForItem() {
    if (items.length > 0) return Promise.resolve()
    return new Promise(resolve => { waiter = resolve })
  }

  let rawText = ''
  let firstDeltaAt = null
  let firstSafeAt = null
  let mode = null // null (deciding) | 'SINGLE_SHOT' | 'CHUNKED'

  const driver = (async () => {
    let buffer = ''
    let segmentStartMs = null
    let numericProtectionTimer = null
    let graceTimer = null
    let pendingFirstChunk = null

    function clearNumericProtectionTimer() { if (numericProtectionTimer) { clearTimeout(numericProtectionTimer); numericProtectionTimer = null } }
    function clearGraceTimer() { if (graceTimer) { clearTimeout(graceTimer); graceTimer = null } }

    // never search past the first unmatched '[' — see [END_CALL] bracket-safety note above
    function safeView() {
      const idx = buffer.indexOf('[')
      return idx === -1 ? buffer : buffer.slice(0, idx)
    }
    function tryFindBoundary(elapsedMs) {
      const view = safeView()
      const result = findChunkBoundary(view, elapsedMs)
      if (!result) return null
      return { chunk: result.chunk, remainder: result.remainder + buffer.slice(view.length), reason: result.reason }
    }
    function numericProtectionRemainingMs(elapsedMs) {
      return getNumericProtectionRemainingMs(safeView(), elapsedMs)
    }

    // Track M (diagnostic only, design R3 LOCKED 2026-08-22) — state for the FIRST safe chunk only (mirrors
    // chunkDelay's own scope: t3→t4, never anything after). All frozen the instant firstSafeAt is set — see
    // the guard in the delta-processing loop below (mandatory per R2 Blocker 3: deltas arriving during the
    // 150ms grace race, while mode is still null, must NOT keep incrementing/updating these).
    let deltaCount = 0
    let lastDeltaAt = null
    let firstCandidateAt = null
    let numericProtectionEverBlocked = false

    // phase 2 (mode === 'CHUNKED'): drain every safe chunk as found, same continuous-flush shape as
    // chunkedTurn.js's drainReadyChunks() — arms its own wall-clock numeric-protection timer so a
    // protected buffer still resolves even if Claude goes quiet before HARD_MAX_MS
    function drainChunked() {
      clearNumericProtectionTimer()
      while (true) {
        const elapsedMs = performance.now() - segmentStartMs
        const result = tryFindBoundary(elapsedMs)
        if (result) {
          push({ type: 'chunk', text: result.chunk })
          buffer = result.remainder
          segmentStartMs = performance.now()
          continue
        }
        const remainingMs = numericProtectionRemainingMs(elapsedMs)
        if (remainingMs != null) {
          numericProtectionTimer = setTimeout(() => {
            numericProtectionTimer = null
            if (signal?.aborted) return
            drainChunked()
          }, remainingMs)
        }
        return
      }
    }

    function armGrace() {
      return new Promise(resolve => {
        graceTimer = setTimeout(() => resolve('GRACE'), CONDITIONAL_GRACE_MS)
      })
    }

    // exactly-once guarantee: waitForItem() on the consumer side only ever resolves via push() — every
    // driver exit path (abort mid-stream, normal completion, error) MUST push something or the consumer
    // hangs forever awaiting a promise nothing will ever resolve. Caught this before running any test by
    // re-reading the abort-return paths below: a bare `return` after `if (signal?.aborted)` does exactly
    // that. sendDone() makes the "always push before returning" invariant impossible to accidentally skip.
    let doneSent = false
    function sendDone() { if (!doneSent) { doneSent = true; push({ type: 'done' }) } }

    const iterator = stream[Symbol.asyncIterator]()
    let pendingNext = iterator.next()
    let gracePromise = null

    try {
      while (true) {
        const racers = [pendingNext.then(r => ({ kind: 'stream', r }))]
        if (gracePromise) racers.push(gracePromise.then(() => ({ kind: 'grace' })))
        const winner = await Promise.race(racers)

        if (signal?.aborted) { sendDone(); return }

        if (winner.kind === 'grace') {
          gracePromise = null
          clearGraceTimer()
          mode = 'CHUNKED'
          onMilestone?.('mode', mode)
          push({ type: 'chunk', text: pendingFirstChunk })
          segmentStartMs = performance.now() // buffer already holds the remainder from the boundary find below
          drainChunked() // in case more deltas already queued up behind the boundary before grace fired
          continue
        }

        const { value: event, done } = winner.r
        if (done) break
        pendingNext = iterator.next()

        if (signal?.aborted) { sendDone(); return }
        if (event.type !== 'content_block_delta' || event.delta?.type !== 'text_delta') continue

        if (firstDeltaAt === null) { firstDeltaAt = performance.now(); onMilestone?.('firstDeltaAt', firstDeltaAt) }
        rawText += event.delta.text

        const wasEmpty = buffer.length === 0
        buffer += event.delta.text
        if (wasEmpty) segmentStartMs = performance.now()

        if (mode === 'CHUNKED') {
          drainChunked()
        } else if (mode === null) {
          if (firstSafeAt === null) {
            // Track M bookkeeping — ONLY runs before first-safe is found. Once firstSafeAt is set below,
            // this whole block is skipped for any further deltas arriving during the 150ms grace race (mode
            // still null then, per the outer if/else here) — that is exactly the freeze R2 Blocker 3 requires.
            deltaCount++
            const deltaArrivedAt = performance.now()
            const gapFromPreviousDeltaMs = lastDeltaAt === null ? 0 : (deltaArrivedAt - lastDeltaAt)
            lastDeltaAt = deltaArrivedAt

            const elapsedMs = performance.now() - segmentStartMs

            try {
              // read-only diagnostic, mirrors findChunkBoundary()'s own eligibility gate exactly (single
              // source of truth in speechChunker.js) — never used to decide the real cut below
              const diag = evaluateNumericProtectionDiagnostic(safeView(), elapsedMs)
              if (diag.candidateEligible && firstCandidateAt === null) firstCandidateAt = performance.now()
              if (diag.blockedByNumericProtection) numericProtectionEverBlocked = true
            } catch (_) { /* diagnostic only — must never affect the real cut decision below */ }

            const result = tryFindBoundary(elapsedMs)
            if (result) {
              firstSafeAt = performance.now()
              onMilestone?.('firstSafeAt', firstSafeAt)
              try {
                // locked implementation detail (R3 review): for STRONG/SOFT the candidate IS the emit instant
                // by construction — reuse the timestamps already captured directly, never re-measure via a
                // fresh performance.now() call and claim it's "exactly equal"
                const isStrongOrSoft = result.reason === CHUNK_REASON.STRONG_BOUNDARY || result.reason === CHUNK_REASON.SOFT_BOUNDARY
                const firstCandidateElapsedMs = isStrongOrSoft
                  ? (firstSafeAt - firstDeltaAt)
                  : (firstCandidateAt !== null ? (firstCandidateAt - firstDeltaAt) : (firstSafeAt - firstDeltaAt))
                onMilestone?.('chunkReasonStats', {
                  reason: result.reason,
                  charCount: result.chunk.length,
                  deltaCount,
                  firstCandidateElapsedMs,
                  numericProtectionBlocked: isStrongOrSoft ? false : numericProtectionEverBlocked,
                  preSafeDeltaGapMs: gapFromPreviousDeltaMs,
                })
              } catch (_) { /* diagnostic only */ }
              pendingFirstChunk = result.chunk
              buffer = result.remainder
              gracePromise = armGrace()
            }
            // no boundary yet: keep accumulating, nothing to flush
          }
          // else: firstSafeAt already found, still waiting on the grace race (mode still null) — nothing to do
        }
      }

      clearNumericProtectionTimer()
      clearGraceTimer()
      if (signal?.aborted) { sendDone(); return }

      const fullAt = performance.now()
      onMilestone?.('fullAt', fullAt)

      // END_CALL contract (locked design review 2026-08-21) — [END_CALL] never appears in ANY yielded
      // speech chunk in either mode (unlike askClaudeStream()'s raw-yield-let-caller-strip convention;
      // this function replaces that quirk with a clean, uniform contract since a future caller must not
      // depend on `text.includes('[END_CALL]')` for this path at all). Canonical history text and the
      // end-call control signal are exposed as two SEPARATE milestones, both built directly from rawText
      // — the pure delta accumulator, untouched by chunk boundaries — never reconstructed by concatenating
      // yielded speech chunks (CHUNKED mode's chunks have already been marker-stripped independently per
      // chunk, so concatenating them is not guaranteed to equal the canonical text byte-for-byte, and must
      // never be relied on as if it were).
      const finalText = rawText.replace(/\[END_CALL\]/g, '').trim()
      const endCallRequested = rawText.includes('[END_CALL]')
      onMilestone?.('finalText', finalText)
      // Track L — Claude's own canonical response length, captured here (before audioStream.js can ever
      // substitute a recovery phrase or append an END_CALL follow-up) so it reflects what the model actually
      // produced, not what ends up spoken. Guarded independently — see the inputStats block above for why.
      try { onMilestone?.('responseCharCount', finalText.length) } catch (_) { /* diagnostic only */ }
      onMilestone?.('endCallRequested', endCallRequested)

      if (mode === 'CHUNKED') {
        const lastSpeechChunk = buffer.replace(/\[END_CALL\]/g, '').trim()
        if (lastSpeechChunk) push({ type: 'chunk', text: lastSpeechChunk })
      } else {
        mode = 'SINGLE_SHOT'
        onMilestone?.('mode', mode)
        if (finalText.length >= 3) push({ type: 'chunk', text: finalText })
      }
      sendDone()
    } catch (err) {
      clearNumericProtectionTimer()
      clearGraceTimer()
      if (!signal?.aborted) push({ type: 'error', err })
      else sendDone() // abort surfaced as a rejected iterator.next() instead of a clean stream end — still must not hang the consumer
    }
  })()

  try {
    while (true) {
      await waitForItem()
      const item = items.shift() // guaranteed non-empty here — sendDone() ensures every driver exit path pushes exactly one final item
      if (item.type === 'chunk') yield item.text
      else if (item.type === 'done') break
      else if (item.type === 'error') throw item.err
    }
  } finally {
    await driver.catch(() => {}) // let the driver settle (clears its own timers in its own try/catch/finally) before returning control
  }
}

module.exports = { askClaude, askClaudeStream, askClaudeStreamChunked, askClaudeObservedFullResponse, askClaudeConditionalStream, summarizeCall }
