const twilio = require('twilio')
const { v4: uuidv4 } = require('uuid')
const { sheetsService } = require('../services/googleSheets')
const callSessions = require('../utils/callSessions')

// เริ่มบันทึกเสียงทั้งสาย (สองแชนแนลแยก AI/ลูกค้า) แบบขนานกับ media stream — ไม่บล็อก TwiML ที่เหลือ
function addRecording(twiml) {
  twiml.start().recording({
    recordingChannels: 'dual',
    recordingStatusCallback: `${process.env.BASE_URL}/webhook/recording`,
    recordingStatusCallbackMethod: 'POST',
    trim: 'do-not-trim',
  })
}

module.exports = async function webhookRoutes(fastify) {

  // Twilio โทรออกแล้วลูกค้ารับสาย
  fastify.post('/webhook/outbound', async (req, reply) => {
    const callSid = req.body.CallSid
    const to = req.body.To
    const session = callSessions.get(callSid)
    const customerName = session?.name || 'คุณลูกค้า'

    const wsUrl = `${process.env.BASE_URL.replace(/^http/, 'ws')}/stream?callSid=${callSid}`

    const twiml = new twilio.twiml.VoiceResponse()
    addRecording(twiml)
    const connect = twiml.connect()
    connect.stream({ url: wsUrl })

    reply.header('Content-Type', 'text/xml')
    return reply.send(twiml.toString())
  })

  // Twilio รับสายเข้า
  fastify.post('/webhook/inbound', async (req, reply) => {
    const callSid = req.body.CallSid
    const from = req.body.From

    // หา campaign สำหรับ inbound
    const campaign = await sheetsService.getDefaultInboundCampaign()
    callSessions.set(callSid, {
      callSid,
      phone: from,
      name: 'ลูกค้า',
      campaign,
      messages: [],
      direction: 'inbound',
      startTime: Date.now()
    })

    const wsUrl = `${process.env.BASE_URL.replace(/^http/, 'ws')}/stream?callSid=${callSid}`

    const twiml = new twilio.twiml.VoiceResponse()
    addRecording(twiml)
    const connect = twiml.connect()
    connect.stream({ url: wsUrl })

    reply.header('Content-Type', 'text/xml')
    return reply.send(twiml.toString())
  })

  // Twilio แจ้งว่าไฟล์บันทึกเสียงพร้อมแล้ว — ผูกกับแถวผลการโทรผ่าน call_sid
  // (มักมาถึงหลัง webhook/status สักพัก เพราะ Twilio ต้อง process ไฟล์เสียงก่อน)
  fastify.post('/webhook/recording', async (req, reply) => {
    const { CallSid, RecordingUrl, RecordingStatus } = req.body
    if (RecordingStatus === 'completed' && CallSid && RecordingUrl) {
      try {
        const saved = await sheetsService.saveRecordingUrl(CallSid, RecordingUrl)
        if (!saved) console.warn(`[Recording] ไม่พบแถวผลการโทรสำหรับ callSid=${CallSid}`)
      } catch (err) {
        console.error('[Recording] Save error:', err.message)
      }
    }
    return reply.send({ ok: true })
  })

  // Twilio แจ้งสถานะสาย (วางสาย, ไม่รับ ฯลฯ)
  fastify.post('/webhook/status', async (req, reply) => {
    const { CallSid, CallStatus, CallDuration } = req.body
    const session = callSessions.get(CallSid)

    if (!session) return reply.send({ ok: true })

    if (['completed', 'busy', 'no-answer', 'failed', 'canceled'].includes(CallStatus)) {
      const { postCallHandler } = require('../services/postCall')
      await postCallHandler(CallSid, CallStatus, CallDuration, session)
      callSessions.delete(CallSid)
    }

    return reply.send({ ok: true })
  })
}
