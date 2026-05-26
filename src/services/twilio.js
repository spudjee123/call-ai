const twilio = require('twilio')
const callSessions = require('../utils/callSessions')
const { askClaude } = require('./claude')
const { synthesizeSpeech } = require('./tts')

const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)

const FILLER_TEXTS = ['ค่ะ', 'อ่า ค่ะ', 'เดี๋ยวนะคะ']

async function makeOutboundCall(contact, campaign) {
  const session = {
    callSid: null,
    phone: contact.phone,
    name: contact.name || 'คุณลูกค้า',
    campaign,
    messages: [],
    offTopicCount: 0,
    direction: 'outbound',
    startTime: Date.now(),
    greetingChunks: null,
    fillerChunks: null,   // pre-generated filler sounds
  }

  // Pre-generate greeting + fillers ขณะรอสายต่อ (~3-4s) เพื่อลด silence
  const greetingPromise = (async () => {
    try {
      const text = await askClaude(session, true)
      session.messages.push({ role: 'assistant', content: text })
      const [chunks, ...fillerChunks] = await Promise.all([
        synthesizeSpeech(text, campaign.voice_id),
        ...FILLER_TEXTS.map(t => synthesizeSpeech(t, campaign.voice_id)),
      ])
      session.greetingChunks = chunks
      session.greetingText = text
      session.fillerChunks = fillerChunks
      console.log(`[PreGen] Greeting ready: "${text.substring(0, 60)}" (${chunks.length} chunks)`)
      console.log(`[PreGen] Fillers ready: ${fillerChunks.map(f => f.length).join(', ')} chunks`)
    } catch (err) {
      console.error('[PreGen] Failed:', err.message)
    }
  })()

  const call = await client.calls.create({
    to: contact.phone,
    from: process.env.TWILIO_PHONE_NUMBER,
    url: `${process.env.BASE_URL}/webhook/outbound`,
    statusCallback: `${process.env.BASE_URL}/webhook/status`,
    statusCallbackMethod: 'POST',
    statusCallbackEvent: ['completed', 'busy', 'no-answer', 'failed'],
    timeout: 30,
  })

  session.callSid = call.sid
  callSessions.set(call.sid, session)

  // ถ้า pre-gen ยังไม่เสร็จตอน call เชื่อมต่อ รอต่ออีกนิด
  await greetingPromise.catch(() => {})

  console.log(`[Twilio] Calling ${contact.phone} (${call.sid})`)
  return call
}

async function sendSms(to, body) {
  return client.messages.create({
    to,
    from: process.env.TWILIO_PHONE_NUMBER,
    body,
  })
}

module.exports = { makeOutboundCall, sendSms }
