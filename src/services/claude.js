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
    offTopicInstruction = `ถ้าลูกค้าคุยนอกเรื่อง ให้รับฟัง 1 ประโยคแล้วดึงกลับมาที่โปรโมชั่น พร้อมชักชวนให้ลูกค้าสนใจรับโปรให้ได้`
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
ถ้าไม่เข้าใจสิ่งที่ลูกค้าพูด ให้ถามสั้นๆ ว่า พูดซ้ำได้ไหมคะ อย่าทวนคำที่ฟังไม่ชัด
ใช้ [END_CALL] เฉพาะ 2 กรณีเท่านั้น:
1. ลูกค้าขอจบสายเองชัดเจน เช่น วางสายได้เลย / ไม่สะดวกแล้ว / ขอบคุณไม่ต้องแล้ว
2. ลูกค้าปฏิเสธซ้ำหลังจากที่ชักชวนเพิ่มเติมแล้ว — ครั้งแรกที่ปฏิเสธให้โน้มน้าวอีกครั้งก่อนเสมอ
ถ้าลูกค้าบอกรับทราบ / โอเค / ได้ / เข้าใจ / จะลองทำ → ถามว่า มีอะไรให้ช่วยอีกไหมคะ ห้ามใช้ [END_CALL]
ห้ามใช้ [END_CALL] หลังพูดโปรโมชั่น ต้องรอฟังคำตอบก่อนเสมอ
ห้ามพูดข้อมูลซ้ำที่พูดไปแล้วในสายนี้ ตอบต่อจาก context ล่าสุด ไม่ต้องสรุปซ้ำ
STT บนสายโทรศัพท์อาจฟังผิดบ้าง ให้ตีความจาก context การสนทนาเสมอ ไม่ตอบตาม text ตรงๆ ถ้าคำนั้นไม่ make sense ในบริบท`
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
  let prependKha = !isGreeting  // true until first sentence yielded — adds "ค่ะ " acknowledgment
  for await (const event of stream) {
    if (signal?.aborted) return
    if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
      buffer += event.delta.text
      const { sentences, remaining } = extractSentences(buffer)
      buffer = remaining
      for (const s of sentences) {
        if (s && s.length >= 5) {
          if (prependKha) {
            prependKha = false
            yield (s.startsWith('ค่ะ ') || s === 'ค่ะ') ? s : 'ค่ะ ' + s
          } else {
            yield s
          }
        }
      }
    }
  }

  // flush ส่วนที่เหลือ (ประโยคสุดท้ายไม่มี punctuation)
  if (buffer.trim().length >= 3) {
    const flushed = buffer.trim()
    if (prependKha) {
      yield (flushed.startsWith('ค่ะ ') || flushed === 'ค่ะ') ? flushed : 'ค่ะ ' + flushed
    } else {
      yield flushed
    }
  }
}

module.exports = { askClaude, askClaudeStream, summarizeCall }
