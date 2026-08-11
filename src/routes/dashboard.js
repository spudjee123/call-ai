const axios = require('axios')
const { sheetsService } = require('../services/googleSheets')
const callSessions = require('../utils/callSessions')
const twilioService = require('../services/twilio')
const healthMonitor = require('../utils/healthMonitor')

module.exports = async function dashboardRoutes(fastify) {

  // Apps Script ดึง call logs
  fastify.get('/api/calls', async (req, reply) => {
    const { limit = 50, campaignId } = req.query
    const calls = await sheetsService.getCallResults({ limit: Number(limit), campaignId })
    return reply.send(calls)
  })

  // Active calls ตอนนี้
  fastify.get('/api/calls/active', async (req, reply) => {
    const active = []
    for (const [callSid, session] of callSessions.entries()) {
      active.push({
        callSid,
        phone: session.phone,
        name: session.name,
        duration: Math.floor((Date.now() - session.startTime) / 1000),
        messageCount: session.messages.length
      })
    }
    return reply.send(active)
  })

  // บทสนทนาสด ๆ ของสายที่กำลังคุยอยู่ — ให้ /admin poll ดูระหว่างสายยังไม่จบ
  fastify.get('/api/calls/active/:callSid', async (req, reply) => {
    const session = callSessions.get(req.params.callSid)
    if (!session) return reply.code(404).send({ error: 'Call not found or already ended' })
    return reply.send({
      callSid: req.params.callSid,
      phone: session.phone,
      name: session.name,
      duration: Math.floor((Date.now() - session.startTime) / 1000),
      messages: session.messages,
    })
  })

  // สถิติ summary — ?days=7|30|90
  fastify.get('/api/stats', async (req, reply) => {
    const days = Number(req.query.days) || 7
    const stats = await sheetsService.getStats({ days })
    return reply.send(stats)
  })

  // วางสายทันที — ปุ่มฉุกเฉินใน /admin
  fastify.post('/api/calls/active/:callSid/hangup', async (req, reply) => {
    const session = callSessions.get(req.params.callSid)
    if (!session) return reply.code(404).send({ error: 'Call not found or already ended' })
    session.hangupReason = 'manual_hangup'
    try {
      await twilioService.hangupCall(req.params.callSid)
      return reply.send({ message: 'Call ended' })
    } catch (err) {
      return reply.code(500).send({ error: err.message })
    }
  })

  // สถานะระบบ — TTS/STT/AI error ล่าสุดในช่วง 10 นาทีที่ผ่านมา
  fastify.get('/api/health/status', async (req, reply) => {
    return reply.send(healthMonitor.getStatus())
  })

  // Proxy ไฟล์บันทึกเสียงจาก Twilio (ต้อง Basic Auth ด้วย Account SID/Auth Token ที่ frontend ไม่ควรถือ)
  fastify.get('/api/recordings/proxy', async (req, reply) => {
    const { url } = req.query
    const prefix = `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/`
    if (!url || !url.startsWith(prefix)) {
      return reply.code(400).send({ error: 'Invalid recording url' })
    }
    try {
      const twilioRes = await axios.get(`${url}.mp3`, {
        auth: { username: process.env.TWILIO_ACCOUNT_SID, password: process.env.TWILIO_AUTH_TOKEN },
        responseType: 'stream',
      })
      reply.header('Content-Type', 'audio/mpeg')
      return reply.send(twilioRes.data)
    } catch (err) {
      return reply.code(502).send({ error: 'Fetch recording failed' })
    }
  })
}
