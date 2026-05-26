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

  if (!msgs.length) return 'สวัสดีค่ะ'

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
    offTopicInstruction = `ถ้าลูกค้าคุยนอกเรื่อง ให้ตอบรับสั้นๆ 1 คำ แล้วดึงกลับมาที่โปรโมชั่นทันที ห้ามให้คำแนะนำหรือพูดเรื่องอื่นที่ไม่เกี่ยวกับโปรโมชั่นเด็ดขาด`
  } else if (offTopicCount < MAX_OFFTOPIC) {
    offTopicInstruction = `ลูกค้านอกเรื่องไปแล้ว ${offTopicCount} ครั้ง ให้ดึงกลับมาที่โปรโมชั่นและพยายามปิดการขายให้ได้`
  } else {
    offTopicInstruction = `ลูกค้านอกเรื่องซ้ำ ให้โน้มน้าวกลับมาที่โปรโมชั่นอีกครั้ง ถ้าปฏิเสธซ้ำอีกจึงจบสาย [END_CALL]`
  }

  return `${campaignPrompt}

ชื่อลูกค้า: ${customerName}
คำตอบต้องสั้นมาก ไม่เกิน 1-2 ประโยคเท่านั้น ภาษาไทย เหมาะกับการพูดทางโทรศัพท์ ห้ามใช้ bullet points, markdown, emoji หรือสัญลักษณ์พิเศษ ตอบกระชับที่สุด

${offTopicInstruction}

ถ้าถูกถามว่าเป็น AI ให้ตอบตรงๆ ว่าใช่
ถ้าลูกค้าโกรธ ให้พูดเสียงเย็นและพยายามเข้าใจปัญหา
ใช้คำลงท้ายผู้หญิง ค่ะ หรือ คะ เสมอ ห้ามใช้ ครับ ห้ามใช้ ผม ให้ใช้ หนู แทนเสมอ
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
  const { name, campaign, messages, offTopicCount } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name, offTopicCount)
  const history = messages.slice(-MAX_HISTORY)
  const msgs = isGreeting
    ? [{ role: 'user', content: 'ทักทายและแนะนำตัวสั้นๆ แล้วถามว่าสะดวกคุยสักครู่ไหม รวม 1-2 ประโยคเท่านั้น' }]
    : history

  if (!msgs.length) { yield 'สวัสดีค่ะ'; return }

  const response = await client.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 200,
    system: systemPrompt,
    messages: msgs,
  })

  if (signal?.aborted) return

  const text = response.content[0].text.trim()
  if (text.length >= 3) yield text
}

module.exports = { askClaude, askClaudeStream, summarizeCall }
