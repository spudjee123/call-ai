const { v4: uuidv4 } = require('uuid')
const { summarizeCall } = require('./claude')
const { sheetsService } = require('./googleSheets')
const { sendSms } = require('./twilio')

const MAX_RETRIES = 3
const RETRY_DELAY_MS = 4 * 60 * 60 * 1000 // 4 ชั่วโมง

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
    } else if (callStatus === 'completed') {
      // เชื่อมสายได้แต่ไม่มีบทสนทนาเลย (เช่น วางสายทันทีที่รับ) — แยกจาก 'completed' เฉยๆ ให้ความหมายชัดกว่า
      outcome = 'no_conversation'
    }

    const transcript = session.messages
      .map(m => `${m.role === 'user' ? 'ลูกค้า' : 'AI'}: ${m.content}`)
      .join(' | ')

    // บันทึกผลใน Google Sheets — แยก call_status (เชื่อมสายได้ไหม) ออกจาก outcome (ผลลัพธ์เชิงธุรกิจ)
    await sheetsService.saveCallResult({
      call_id: uuidv4(),
      call_sid: callSid,
      phone: session.phone,
      name: session.name,
      campaign_id: session.campaign?.id || '',
      outcome,
      call_status: callStatus,
      hangup_reason: session.hangupReason || '',
      summary,
      key_points: keyPoints,
      next_action: nextAction,
      duration: duration || 0,
      transcript,
    })

    // ไม่รับสาย/สายไม่ว่าง → ตั้งโทรซ้ำอัตโนมัติแทนที่จะปิดเป็น called เฉยๆ
    if (['no-answer', 'busy'].includes(callStatus)) {
      await scheduleRetry(session.phone)
    } else {
      await sheetsService.updateContactStatus(session.phone, 'called')
    }

    // ส่ง SMS follow-up
    await handleSmsFollowup(session, outcome)

    console.log(`[PostCall] Done ${callSid} → ${outcome}`)
  } catch (err) {
    console.error(`[PostCall] Error for ${callSid}:`, err.message)
  }
}

async function scheduleRetry(phone) {
  const contact = await sheetsService.getContact(phone)
  if (!contact) return
  const retryCount = Number(contact.retry_count) || 0
  if (retryCount >= MAX_RETRIES) {
    await sheetsService.updateContact(phone, { status: 'called' })
    return
  }
  await sheetsService.updateContact(phone, {
    status: 'retry_pending',
    retry_count: retryCount + 1,
    next_attempt_at: new Date(Date.now() + RETRY_DELAY_MS).toISOString(),
  })
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
