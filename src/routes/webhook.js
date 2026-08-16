const twilio = require('twilio')
const { v4: uuidv4 } = require('uuid')
const { sheetsService } = require('../services/googleSheets')
const callSessions = require('../utils/callSessions')
const { synthesizeSpeechWavThai } = require('../services/googleTTS')

// เริ่มบันทึกเสียงทั้งสาย (สองแชนแนลแยก AI/ลูกค้า) แบบขนานกับ media stream — ไม่บล็อก TwiML ที่เหลือ
function addRecording(twiml) {
  twiml.start().recording({
    recordingChannels: 'dual',
    recordingStatusCallback: `${process.env.BASE_URL}/webhook/recording`,
    recordingStatusCallbackMethod: 'POST',
    trim: 'do-not-trim',
  })
}

// ข้อความไทยเมื่อสายเข้ามาแต่ไม่มี campaign type=inbound ในระบบเลย — cache ไว้หลัง gen ครั้งแรก
// ไม่ยิง Google TTS ใหม่ทุกครั้งที่มีสายเข้ามาช่วงระบบยังไม่พร้อม (เคสนี้ควรเกิดไม่บ่อยอยู่แล้ว)
const INBOUND_FALLBACK_TEXT = 'ขออภัยค่ะ ขณะนี้ระบบยังไม่พร้อมรับสาย กรุณาติดต่อใหม่อีกครั้ง'
let inboundFallbackWavPromise = null
function getInboundFallbackWav() {
  if (!inboundFallbackWavPromise) inboundFallbackWavPromise = synthesizeSpeechWavThai(INBOUND_FALLBACK_TEXT)
  return inboundFallbackWavPromise
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
    const to = req.body.To
    reply.header('Content-Type', 'text/xml')

    // หา campaign สำหรับ inbound — ตรงกับเบอร์ที่ลูกค้าโทรเข้ามาจริง (รองรับหลาย campaign ใช้คนละเบอร์พร้อมกัน)
    const campaign = await sheetsService.getInboundCampaignForNumber(to)

    // ไม่มี campaign inbound ที่ตั้งไว้ตรงกับเบอร์นี้เลย — ปฏิเสธสายอย่างสุภาพ แทนที่จะเดาเอา campaign อื่น (เช่น outbound หรือเบอร์อื่น) มาใช้แบบผิดบริบทเงียบๆ
    // ใช้ <Play> เล่นไฟล์เสียงไทยที่ gen เองผ่าน Google TTS แทน <Say> ของ Twilio ตรงๆ
    // เพราะเช็คเอกสาร Twilio แล้วไม่ยืนยันว่า th-TH อยู่ในภาษาที่ <Say> รองรับ — Google TTS ของเรา
    // เป็นตัวเดียวกับที่ใช้พูดจริงทุกสายอยู่แล้ว มั่นใจได้ว่าออกเสียงไทยถูกต้องแน่นอน
    if (!campaign) {
      console.error(`[Inbound] ไม่มี campaign inbound ที่ตั้งเบอร์ตรงกับ ${to} เลย — ปฏิเสธสาย`)
      const twiml = new twilio.twiml.VoiceResponse()
      twiml.play(`${process.env.BASE_URL}/webhook/inbound-fallback.wav`)
      twiml.hangup()
      return reply.send(twiml.toString())
    }

    callSessions.set(callSid, {
      callSid,
      phone: from,
      name: 'ลูกค้า',
      campaign,
      messages: [],
      direction: 'inbound',
      startTime: Date.now(),
      twilioNumber: to,
    })

    const wsUrl = `${process.env.BASE_URL.replace(/^http/, 'ws')}/stream?callSid=${callSid}`

    const twiml = new twilio.twiml.VoiceResponse()
    addRecording(twiml)
    const connect = twiml.connect()
    connect.stream({ url: wsUrl })

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

  // ไฟล์เสียงไทยที่ /webhook/inbound เรียกด้วย <Play> ตอนไม่มี campaign inbound ให้ใช้ — gen ครั้งแรกแล้ว cache ไว้
  fastify.get('/webhook/inbound-fallback.wav', async (req, reply) => {
    try {
      const wav = await getInboundFallbackWav()
      reply.header('Content-Type', 'audio/wav')
      return reply.send(wav)
    } catch (err) {
      inboundFallbackWavPromise = null // reset cache กัน error ค้างตลอดไป ให้ครั้งถัดไป retry ใหม่ได้
      console.error('[Inbound fallback audio] Gen error:', err.message)
      return reply.code(502).send()
    }
  })

  // ThaiBulkSMS Delivery Report — ยิงมาแบบ GET เท่านั้น (ช่องทาง SMS รองรับแค่ Method GET)
  // query: Transaction (=message_id จากตอนส่ง), Status, Time — ต้องตั้ง Webhook URL นี้ไว้ที่หน้า API Key ของ ThaiBulkSMS เอง
  // (ใช้งานได้เฉพาะเครดิตประเภท Corporate เท่านั้น ถ้าส่งด้วยเครดิต standard จะไม่มี DR ยิงเข้ามา)
  fastify.get('/webhook/sms-dr', async (req, reply) => {
    const { Transaction, Status, Time } = req.query
    console.log(`[SMS DR] Transaction=${Transaction} Status=${Status} Time=${Time}`)
    // ผูกกลับเข้าแถวผลการโทรที่มี sms_message_id ตรงกัน (บันทึกไว้ตอนส่ง SMS ใน postCall.js)
    // ไม่มีแถวไหนตรง (เช่น column ยังไม่ถูกสร้าง หรือ template.sender ผิด) แค่ log เตือน ไม่ทำให้ webhook นี้ error
    if (Transaction && Status) {
      try {
        const updated = await sheetsService.updateSmsDeliveryStatus(Transaction, Status)
        if (!updated) console.warn(`[SMS DR] ไม่พบแถวที่มี sms_message_id=${Transaction}`)
      } catch (err) {
        console.error('[SMS DR] Update error:', err.message)
      }
    }
    return reply.send({ ok: true })
  })
}
