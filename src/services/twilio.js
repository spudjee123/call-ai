const twilio = require('twilio')
const callSessions = require('../utils/callSessions')
const { askClaude } = require('./claude')
const { resolveExplicitProvider } = require('./conversationAI')
const { synthesizeSpeech } = require('./tts')
const { sheetsService } = require('./googleSheets')
const healthMonitor = require('../utils/healthMonitor')

const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)


// เบอร์ที่จะใช้จริงของ campaign นี้ (ของตัวเองถ้าตั้งไว้ ไม่งั้น fallback เบอร์เริ่มต้นของระบบ) — export ให้ callQueue.js เรียกใช้ตัวเดียวกันเป๊ะ
// กันกรณีเบอร์ที่ใช้เป็น key ของคิว (callQueue.js) กับเบอร์ที่โทรออกจริง (ตรงนี้) เพี้ยนไม่ตรงกันถ้าคำนวณแยกกันคนละที่
function effectiveTwilioNumber(campaign) {
  return campaign?.twilio_number || process.env.TWILIO_PHONE_NUMBER
}

async function makeOutboundCall(contact, campaign, onCallCreated) {
  if (await sheetsService.isBlocked(contact.phone)) {
    console.log(`[Twilio] Skipped ${contact.phone} — อยู่ใน Do-not-call list`)
    return null
  }

  // เบอร์ที่ใช้โทรออกจริง — เก็บไว้ใน session เพื่อบันทึกลงผลการโทร (ให้หน้าประวัติการโทรโชว์/กรองตามเบอร์ที่ใช้จริงได้)
  const twilioNumber = effectiveTwilioNumber(campaign)

  // Dual Conversation Provider A/B (design locked) — resolved ONCE here and frozen as a scalar on the
  // session for the rest of the call. Deliberately not re-read from session.campaign later: session.campaign
  // is a live object reference (not a deep copy), so relying on it mid-call would break the "provider frozen
  // per call" invariant if the campaign is edited while this call is still in progress. null means no
  // explicit choice — existing rollout routing (legacyEarlyTts, etc.) stays completely untouched.
  const explicitProvider = resolveExplicitProvider(campaign)

  const session = {
    callSid: null,
    phone: contact.phone,
    name: contact.name || 'คุณลูกค้า',
    campaign,
    messages: [],
    direction: 'outbound',
    startTime: Date.now(),
    greetingChunks: null,
    twilioNumber,
    llmProvider: explicitProvider?.provider || null,
    llmModel: explicitProvider?.model || null,
  }

  // Pre-generate greeting ขณะรอสายต่อ (~3-4s) เพื่อลด silence
  const greetingPromise = (async () => {
    try {
      const text = await askClaude(session)
      session.messages.push({ role: 'assistant', content: text })
      const chunks = await synthesizeSpeech(text, campaign.voice_id)
      session.greetingChunks = chunks
      session.greetingText = text
      console.log(`[PreGen] Greeting ready: "${text.substring(0, 60)}" (${chunks.length} chunks)`)
    } catch (err) {
      console.error('[PreGen] Failed:', err.message)
      healthMonitor.reportError('tts', err.message)
    }
  })()

  const call = await client.calls.create({
    to: contact.phone,
    from: twilioNumber,
    url: `${process.env.BASE_URL}/webhook/outbound`,
    statusCallback: `${process.env.BASE_URL}/webhook/status`,
    statusCallbackMethod: 'POST',
    statusCallbackEvent: ['completed', 'busy', 'no-answer', 'failed'],
    timeout: 30,
  })

  session.callSid = call.sid
  callSessions.set(call.sid, session)
  onCallCreated?.(call) // แจ้งทันทีที่มี callSid จริง — ก่อนรอ greeting pregen เสร็จ (กัน race กับ webhook สถานะที่อาจมาเร็วกว่า)

  // ถ้า pre-gen ยังไม่เสร็จตอน call เชื่อมต่อ รอต่ออีกนิด
  await greetingPromise.catch(() => {})

  console.log(`[Twilio] Calling ${contact.phone} (${call.sid})`)
  return call
}

// วางสายทันที — ใช้กับปุ่มฉุกเฉินใน /admin
async function hangupCall(callSid) {
  return client.calls(callSid).update({ status: 'completed' })
}

// เบอร์ที่ซื้อไว้จริงทั้งหมดในบัญชี Twilio — ให้แอดมินเลือกจากของจริงแทนพิมพ์เอง กัน typo/เบอร์ที่ไม่ได้เป็นเจ้าของ
async function listPhoneNumbers() {
  const numbers = await client.incomingPhoneNumbers.list({ limit: 100 })
  return numbers.map(n => ({ phoneNumber: n.phoneNumber, friendlyName: n.friendlyName || n.phoneNumber }))
}

module.exports = { makeOutboundCall, hangupCall, listPhoneNumbers, effectiveTwilioNumber }
