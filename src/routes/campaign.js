const { sheetsService } = require('../services/googleSheets')
const { callQueue } = require('../utils/callQueue')
const twilioService = require('../services/twilio')

module.exports = async function campaignRoutes(fastify) {

  // เริ่ม campaign — Apps Script เรียก
  fastify.post('/api/campaign/start', async (req, reply) => {
    const { campaignId } = req.body
    if (!campaignId) return reply.code(400).send({ error: 'campaignId required' })

    const campaign = await sheetsService.getCampaign(campaignId)
    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' })

    const contacts = await sheetsService.getPendingContacts(campaignId)
    if (!contacts.length) return reply.send({ message: 'No pending contacts', count: 0 })

    contacts.forEach(contact => callQueue.add({ contact, campaign }))

    return reply.send({ message: 'Campaign started', count: contacts.length })
  })

  // หยุด campaign
  fastify.post('/api/campaign/stop', async (req, reply) => {
    const { campaignId } = req.body
    callQueue.clear(campaignId)
    return reply.send({ message: 'Campaign stopped' })
  })

  // สถานะ queue
  fastify.get('/api/campaign/status', async (req, reply) => {
    return reply.send({
      queueSize: callQueue.size(),
      running: callQueue.runningCount()
    })
  })
}
