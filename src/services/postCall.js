const { v4: uuidv4 } = require('uuid')
const { summarizeCall } = require('./claude')
const { sheetsService } = require('./googleSheets')
const { sendSms } = require('./twilio')

async function postCallHandler(callSid, callStatus, duration, session) {
  try {
    console.log(`[PostCall] Processing ${callSid} status=${callStatus}`)

    let outcome = callStatus
    let summary = ''
    let keyPoints = ''
    let nextAction = ''

    // ถ้ามีบทสนทนา ให้ Claude Sonnet วิเคราะห์
    if (session.messages.length > 0 && callStatus === 'completed') {
      const analysis = await summarizeCall(session)
      outcome = analysis.outcome
      summary = analysis.summary
      keyPoints = analysis.key_points
      nextAction = analysis.next_action
    }

    const transcript = session.messages
      .map(m => `${m.role === 'user' ? 'ลูกค้า' : 'AI'}: ${m.content}`)
      .join(' | ')

    // บันทึกผลใน Google Sheets
    await sheetsService.saveCallResult({
      call_id: uuidv4(),
      phone: session.phone,
      name: session.name,
      campaign_id: session.campaign?.id || '',
      outcome,
      summary,
      key_points: keyPoints,
      duration: duration || 0,
      transcript,
    })

    // อัปเดต status ใน Contacts sheet
    await sheetsService.updateContactStatus(session.phone, 'called')

    // ส่ง SMS follow-up
    await handleSmsFollowup(session, outcome)

    console.log(`[PostCall] Done ${callSid} → ${outcome}`)
  } catch (err) {
    console.error(`[PostCall] Error for ${callSid}:`, err.message)
  }
}

async function handleSmsFollowup(session, outcome) {
  const { campaign, phone, name } = session

  // เช็ค campaign-level setting
  const campaignAllows = campaign[`sms_${outcome}`] === 'TRUE' || campaign[`sms_${outcome}`] === true
  if (!campaignAllows) return

  // เช็ค contact-level opt-in (ดูจาก session ว่ามี sms_opt ไหม)
  if (session.sms_opt === 'FALSE' || session.sms_opt === false) return

  // ดึง template จาก Sheet
  const template = await sheetsService.getSmsTemplate(outcome)
  if (!template) return

  const body = template.replace('{name}', name)

  await sendSms(phone, body)
  console.log(`[SMS] Sent to ${phone} (${outcome})`)
}

module.exports = { postCallHandler }
