const { sheetsService } = require('../services/googleSheets')
const callSessions = require('../utils/callSessions')

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

  // สถิติ summary
  fastify.get('/api/stats', async (req, reply) => {
    const stats = await sheetsService.getStats()
    return reply.send(stats)
  })
}
