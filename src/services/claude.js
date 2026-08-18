const Anthropic = require('@anthropic-ai/sdk')

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })

const MAX_HISTORY = 20

// คำสั่งทักทายต่างกันตามทิศทางสาย — outbound คือ AI โทรออกไปหาลูกค้า (ต้องถามว่าสะดวกคุยไหม)
// inbound คือลูกค้าโทรเข้ามาเอง (ไม่ต้องถามว่าสะดวกไหม เพราะลูกค้าเลือกโทรมาเองอยู่แล้ว — ควรถามว่ามีอะไรให้ช่วย)
const OUTBOUND_GREETING_INSTRUCTION = 'ทักทายและแนะนำตัวสั้นๆ แล้วถามว่าสะดวกคุยสักครู่ไหม รวม 1-2 ประโยคเท่านั้น'
const INBOUND_GREETING_INSTRUCTION = 'รับสายลูกค้าที่โทรเข้ามาเอง ทักทายสั้นๆ แนะนำตัวว่าเป็นใคร แล้วถามว่ามีอะไรให้ช่วยไหมคะ รวม 1-2 ประโยคเท่านั้น'

function greetingInstruction(session) {
  return session.direction === 'inbound' ? INBOUND_GREETING_INSTRUCTION : OUTBOUND_GREETING_INSTRUCTION
}

// ใช้สำหรับ greeting เท่านั้น — Haiku เพราะต้องการ latency ต่ำ
async function askClaude(session) {
  const { name, campaign } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name)
  const response = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 60,
    system: systemPrompt,
    messages: [{ role: 'user', content: greetingInstruction(session) }],
  })
  return response.content[0].text.trim()
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

module.exports = { askClaude, askClaudeStream, askClaudeStreamChunked, summarizeCall }
