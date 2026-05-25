const Anthropic = require('@anthropic-ai/sdk')

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })

const MAX_OFFTOPIC = parseInt(process.env.MAX_OFFTOPIC_COUNT) || 3
const MAX_HISTORY = 20

async function askClaude(session, isGreeting = false) {
  const { name, campaign, messages, offTopicCount } = session

  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name, offTopicCount)

  // ตัด history ให้ไม่ยาวเกิน
  const history = messages.slice(-MAX_HISTORY)

  // ถ้าเป็นการทักทายครั้งแรก ไม่ต้องรอ user พูดก่อน
  const msgs = isGreeting
    ? [{ role: 'user', content: 'ทักทายและแนะนำตัวสั้นๆ แล้วถามว่าสะดวกคุยสักครู่ไหม รวม 1-2 ประโยคเท่านั้น' }]
    : history

  if (!msgs.length) return 'สวัสดีครับ'

  const response = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: isGreeting ? 60 : 80,
    system: systemPrompt,
    messages: msgs,
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

ตอบแบบนี้เท่านั้น:
{
  "outcome": "interested | not_interested | callback | no_answer | angry",
  "summary": "สรุปสั้นๆ 1-2 ประโยค",
  "key_points": "ประเด็นสำคัญที่ลูกค้าพูดถึง",
  "next_action": "สิ่งที่ควรทำต่อ"
}`
    }]
  })

  try {
    return JSON.parse(response.content[0].text)
  } catch {
    return { outcome: 'completed', summary: response.content[0].text, key_points: '', next_action: '' }
  }
}

function buildSystemPrompt(campaignPrompt, customerName, offTopicCount) {
  let offTopicInstruction = ''

  if (offTopicCount === 0) {
    offTopicInstruction = `ถ้าลูกค้าคุยนอกเรื่อง ให้รับฟัง 1 ประโยคแล้วดึงกลับมาที่จุดประสงค์`
  } else if (offTopicCount < MAX_OFFTOPIC) {
    offTopicInstruction = `ลูกค้านอกเรื่องไปแล้ว ${offTopicCount} ครั้ง ให้ดึงกลับชัดขึ้น`
  } else {
    offTopicInstruction = `ลูกค้านอกเรื่องซ้ำ ให้ขอโทษและสรุปจบสาย พูดว่า [END_CALL] เมื่อต้องการวางสาย`
  }

  return `${campaignPrompt}

ชื่อลูกค้า: ${customerName}
คำตอบต้องสั้นมาก ไม่เกิน 1-2 ประโยคเท่านั้น ภาษาไทย เหมาะกับการพูดทางโทรศัพท์ ห้ามใช้ bullet points, markdown, emoji หรือสัญลักษณ์พิเศษ ตอบกระชับที่สุด

${offTopicInstruction}

ถ้าถูกถามว่าเป็น AI ให้ตอบตรงๆ ว่าใช่
ถ้าลูกค้าโกรธ ให้พูดเสียงเย็นและพยายามเข้าใจปัญหา
ใช้คำลงท้ายผู้หญิง ค่ะ หรือ คะ เสมอ ห้ามใช้ ครับ ห้ามใช้ ผม ให้ใช้ หนู แทนเสมอ
ถ้าไม่เข้าใจสิ่งที่ลูกค้าพูด ให้ถามสั้นๆ ว่า พูดซ้ำได้ไหมคะ อย่าทวนคำที่ฟังไม่ชัด
เมื่อต้องการวางสาย ให้พูดคำว่า [END_CALL] ต่อท้าย`
}

// ตรวจหาจุดสิ้นสุดประโยคสำหรับภาษาไทย
// แยกที่: . ! ? หรือ คำลงท้ายสุภาพ ตามด้วย space หรือ end
function extractSentences(buffer) {
  const re = /(.*?(?:[.!?]|ค่ะ|ครับ|นะคะ|นะครับ|เลยค่ะ|เลยครับ|ด้วยค่ะ|ด้วยครับ))(?=\s|$)/g
  const sentences = []
  let lastIndex = 0
  let match
  re.lastIndex = 0
  while ((match = re.exec(buffer)) !== null) {
    const s = match[1].trim()
    if (s) sentences.push(s)
    lastIndex = re.lastIndex
  }
  return { sentences, remaining: buffer.slice(lastIndex) }
}

// Streaming version — yields ประโยคทีละประโยคทันทีที่ Claude generate
// ElevenLabs เริ่มแปลงเสียงได้โดยไม่ต้องรอ Claude เสร็จทั้งหมด
async function* askClaudeStream(session, isGreeting = false, signal = null) {
  const { name, campaign, messages, offTopicCount } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name, offTopicCount)
  const history = messages.slice(-MAX_HISTORY)
  const msgs = isGreeting
    ? [{ role: 'user', content: 'ทักทายและแนะนำตัวสั้นๆ แล้วถามว่าสะดวกคุยสักครู่ไหม รวม 1-2 ประโยคเท่านั้น' }]
    : history

  if (!msgs.length) { yield 'สวัสดีค่ะ'; return }

  // ใช้ create({ stream: true }) แทน .stream() เพื่อ compatibility ทุก SDK version
  const stream = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 150,
    system: systemPrompt,
    messages: msgs,
    stream: true,
  })

  let buffer = ''
  for await (const event of stream) {
    if (signal?.aborted) return
    if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
      buffer += event.delta.text
      const { sentences, remaining } = extractSentences(buffer)
      buffer = remaining
      for (const s of sentences) {
        if (s) yield s
      }
    }
  }

  // flush ส่วนที่เหลือ (ประโยคสุดท้ายไม่มี punctuation)
  if (buffer.trim()) yield buffer.trim()
}

module.exports = { askClaude, askClaudeStream, summarizeCall }
