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
    // ห้าม destructure เฉพาะบางฟิลด์ — เคยพลาดตัดฟิลด์ sms_<outcome> ทิ้งไปตอน create ทั้งที่ PATCH (updateCampaign) ส่ง body ทั้งก้อนอยู่แล้ว ทำให้เลือก SMS template ตอนสร้างใหม่แล้วหายเงียบๆ
    const { id, name } = req.body || {}
    if (!id || !name) return reply.code(400).send({ error: 'id and name required' })

    const existing = await sheetsService.getCampaign(id)
    if (existing) return reply.code(409).send({ error: 'Campaign id already exists' })

    await sheetsService.addCampaign(req.body)
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
    // ตัด id ออกจาก body เสมอ — กันแก้ primary key ผ่าน PATCH โดยไม่เช็ค unique (ต่างจาก POST ที่เช็คซ้ำก่อนสร้าง)
    const { id, ...updates } = req.body || {}
    const updated = await sheetsService.updateCampaign(req.params.id, updates)
    if (!updated) return reply.code(404).send({ error: 'Campaign not found' })
    return reply.send({ message: 'Campaign updated' })
  })

  // ลบ campaign ทิ้งจริง (hard delete) — ไม่กระทบ contact/ประวัติการโทรเก่าที่อ้างอิง id นี้อยู่
  // (แค่จะแสดงเป็น id ดิบแทนชื่อ campaign แทน — ดู campaignName() ใน admin.html)
  fastify.delete('/api/campaigns/:id', async (req, reply) => {
    const deleted = await sheetsService.deleteCampaign(req.params.id)
    if (!deleted) return reply.code(404).send({ error: 'Campaign not found' })
    return reply.send({ message: 'Campaign deleted' })
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
