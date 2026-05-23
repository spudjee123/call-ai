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
    ? [{ role: 'user', content: 'เริ่มต้นการสนทนา' }]
    : history

  if (!msgs.length) return 'สวัสดีครับ'

  const response = await client.messages.create({
    model: 'claude-haiku-4-5-20251001',
    max_tokens: 300,
    system: systemPrompt,
    messages: msgs,
  })

  let text = response.content[0].text.trim()

  // เช็ค off-topic และนับ
  if (isOffTopic(text)) {
    session.offTopicCount = (session.offTopicCount || 0) + 1
  }

  return text
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
  } else if (offTopicCount === 1) {
    offTopicInstruction = `ลูกค้านอกเรื่องไปแล้ว 1 ครั้ง ให้ดึงกลับชัดขึ้น`
  } else if (offTopicCount >= 2) {
    offTopicInstruction = `ลูกค้านอกเรื่องซ้ำ ให้ขอโทษและสรุปจบสาย พูดว่า [END_CALL] เมื่อต้องการวางสาย`
  }

  return `${campaignPrompt}

ชื่อลูกค้า: ${customerName}
คำตอบต้องสั้น กระชับ ภาษาไทย ไม่เกิน 2-3 ประโยค เหมาะกับการพูด ไม่ใช้ bullet points, markdown, emoji หรือสัญลักษณ์พิเศษใดๆ ทั้งสิ้น ตอบเป็นประโยคธรรมดาเท่านั้น

${offTopicInstruction}

ถ้าถูกถามว่าเป็น AI ให้ตอบตรงๆ ว่าใช่
ถ้าลูกค้าโกรธ ให้พูดเสียงเย็นและพยายามเข้าใจปัญหา
เมื่อต้องการวางสาย ให้พูดคำว่า [END_CALL] ต่อท้าย`
}

function isOffTopic(text) {
  const salesKeywords = ['สินค้า', 'โปรโมชั่น', 'ราคา', 'สนใจ', 'ซื้อ', 'บริการ', 'ติดต่อ', 'นัดหมาย']
  return !salesKeywords.some(k => text.includes(k))
}

module.exports = { askClaude, summarizeCall }
