const { sheetsService } = require('../services/googleSheets')
const { callQueue } = require('../utils/callQueue')
const { isValidPhone, normalizePhone } = require('../utils/phone')

module.exports = async function campaignRoutes(fastify) {

  // รายชื่อ campaign ทั้งหมด (สำหรับ dropdown + ตาราง)
  fastify.get('/api/campaigns', async (req, reply) => {
    const campaigns = await sheetsService.getCampaigns()
    return reply.send(campaigns)
  })

  // สร้าง campaign ใหม่
  fastify.post('/api/campaigns', async (req, reply) => {
    const { id, name, type, voice_id, script, status } = req.body || {}
    if (!id || !name) return reply.code(400).send({ error: 'id and name required' })

    const existing = await sheetsService.getCampaign(id)
    if (existing) return reply.code(409).send({ error: 'Campaign id already exists' })

    await sheetsService.addCampaign({ id, name, type, voice_id, script, status })
    return reply.send({ message: 'Campaign created' })
  })

  // ดู campaign เดียว (สำหรับเปิดแก้ prompt)
  fastify.get('/api/campaigns/:id', async (req, reply) => {
    const campaign = await sheetsService.getCampaign(req.params.id)
    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' })
    return reply.send(campaign)
  })

  // แก้ไข campaign เช่น prompt, voice_id, sms flags
  fastify.patch('/api/campaigns/:id', async (req, reply) => {
    const updated = await sheetsService.updateCampaign(req.params.id, req.body || {})
    if (!updated) return reply.code(404).send({ error: 'Campaign not found' })
    return reply.send({ message: 'Campaign updated' })
  })

  // ยิงโทรทดสอบเบอร์เดียว — ใช้ pipeline เดียวกับ campaign จริง แต่ไม่แตะ Contacts sheet
  fastify.post('/api/calls/test', async (req, reply) => {
    const { name, campaignId } = req.body || {}
    const phone = normalizePhone(req.body?.phone)
    if (!phone || !campaignId) {
      return reply.code(400).send({ error: 'phone and campaignId required' })
    }
    if (!isValidPhone(phone)) {
      return reply.code(400).send({ error: 'Invalid phone number' })
    }
    const campaign = await sheetsService.getCampaign(campaignId)
    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' })

    callQueue.add({ contact: { phone, name: name || 'ทดสอบ' }, campaign })
    return reply.send({ message: 'Test call queued', phone })
  })

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
