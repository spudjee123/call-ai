const twilio = require('twilio')
const callSessions = require('../utils/callSessions')

const client = twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)

async function makeOutboundCall(contact, campaign) {
  const callSid_placeholder = `pending_${Date.now()}`

  const call = await client.calls.create({
    to: contact.phone,
    from: process.env.TWILIO_PHONE_NUMBER,
    url: `${process.env.BASE_URL}/webhook/outbound`,
    statusCallback: `${process.env.BASE_URL}/webhook/status`,
    statusCallbackMethod: 'POST',
    statusCallbackEvent: ['completed', 'busy', 'no-answer', 'failed'],
    timeout: 30,
  })

  // บันทึก session ก่อนที่ webhook จะมา
  callSessions.set(call.sid, {
    callSid: call.sid,
    phone: contact.phone,
    name: contact.name || 'คุณลูกค้า',
    campaign,
    messages: [],
    offTopicCount: 0,
    direction: 'outbound',
    startTime: Date.now()
  })

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
