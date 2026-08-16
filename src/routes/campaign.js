const { sheetsService } = require('../services/googleSheets')
const { callQueue } = require('../utils/callQueue')
const { isValidPhone, normalizePhone } = require('../utils/phone')
const twilioService = require('../services/twilio')

module.exports = async function campaignRoutes(fastify) {

  // รายชื่อ campaign ทั้งหมด (สำหรับ dropdown + ตาราง)
  fastify.get('/api/campaigns', async (req, reply) => {
    const campaigns = await sheetsService.getCampaigns()
    return reply.send(campaigns)
  })

  // เบอร์ Twilio ที่ซื้อไว้จริงทั้งหมดในบัญชี — ให้แอดมินเลือกจากของจริงตอนตั้งค่า campaign แทนพิมพ์เอง
  fastify.get('/api/twilio/numbers', async (req, reply) => {
    try {
      const numbers = await twilioService.listPhoneNumbers()
      return reply.send(numbers)
    } catch (err) {
      return reply.code(502).send({ error: 'ดึงรายชื่อเบอร์จาก Twilio ไม่สำเร็จ' })
    }
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

  // ลบ campaign ทิ้งจริง (hard delete) — ไม่กระทบประวัติการโทรเก่าที่อ้างอิง id นี้อยู่
  // (แค่จะแสดงเป็น id ดิบแทนชื่อ campaign แทน — ดู campaignName() ใน admin.html)
  // ถ้ายังมีเบอร์ที่ยังไม่ได้โทร (pending) หรือรอโทรซ้ำ (retry_pending) ผูกอยู่ ต้องระบุวิธีจัดการก่อนถึงจะลบได้ — กันเบอร์ค้างแบบไม่มีใครรู้ตัวหลังลบ
  // ใช้ getContacts ธรรมดาแล้ว filter เอง (ไม่ใช้ getPendingContacts ที่เช็คแค่ pending) เพราะ retry_pending ก็ต้องกันด้วย —
  // retryScheduler.js เจอ campaign ที่ถูกลบไปแล้วจะข้ามเงียบๆ (if (!campaign) continue) ทำให้เบอร์เหล่านี้ค้างสถานะ retry_pending ตลอดไปถ้าไม่กันตรงนี้
  fastify.delete('/api/campaigns/:id', async (req, reply) => {
    const id = req.params.id
    const contactsInCampaign = await sheetsService.getContacts({ campaignId: id })
    const pending = contactsInCampaign.filter(c => c.status === 'pending' || c.status === 'retry_pending')

    if (pending.length) {
      const { resolution, targetCampaignId } = req.body || {}
      if (resolution === 'move') {
        if (!targetCampaignId || targetCampaignId === id) {
          return reply.code(400).send({ error: 'ต้องเลือก Campaign ปลายทางอื่นจากที่จะลบ' })
        }
        const target = await sheetsService.getCampaign(targetCampaignId)
        if (!target) return reply.code(400).send({ error: 'ไม่พบ Campaign ปลายทาง' })
        await Promise.all(pending.map(c => sheetsService.updateContact(c.phone, { campaign: targetCampaignId })))
      } else if (resolution === 'cancel') {
        await Promise.all(pending.map(c => sheetsService.updateContact(c.phone, { status: 'deleted' })))
      } else {
        return reply.code(409).send({
          error: `มีเบอร์ที่ยังไม่เสร็จสิ้น (รอโทร/รอโทรซ้ำ) ${pending.length} เบอร์ผูกกับ Campaign นี้อยู่ ต้องเลือกวิธีจัดการก่อนลบ`,
          pendingCount: pending.length,
          needsResolution: true,
        })
      }
    }

    const deleted = await sheetsService.deleteCampaign(id)
    if (!deleted) return reply.code(404).send({ error: 'Campaign not found' })
    return reply.send({ message: 'Campaign deleted' })
  })

  // ยิงโทรทดสอบเบอร์เดียว — ใช้ pipeline เดียวกับ campaign จริง แต่ไม่แตะ Contacts sheet
  fastify.post('/api/calls/test', async (req, reply) => {
    const { name, campaignId, twilioNumber } = req.body || {}
    const phone = normalizePhone(req.body?.phone)
    if (!phone || !campaignId) {
      return reply.code(400).send({ error: 'phone and campaignId required' })
    }
    if (!isValidPhone(phone)) {
      return reply.code(400).send({ error: 'Invalid phone number' })
    }
    const campaign = await sheetsService.getCampaign(campaignId)
    if (!campaign) return reply.code(404).send({ error: 'Campaign not found' })

    // เบอร์ที่แอดมินเลือกตอนทดสอบ (ถ้ามี) ทับเบอร์ที่ตั้งไว้ระดับ campaign เฉพาะสายทดสอบนี้ — ไม่แก้ campaign จริง
    callQueue.add({ contact: { phone, name: name || 'ทดสอบ' }, campaign: twilioNumber ? { ...campaign, twilio_number: twilioNumber } : campaign })
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

  // หยุด campaign หรือหยุดคิวของเบอร์ใดเบอร์หนึ่งเจาะจง (ระบุ twilioNumber แทน campaignId) — ไม่กระทบคิว/สายที่กำลังคุยอยู่ของเบอร์อื่น
  fastify.post('/api/campaign/stop', async (req, reply) => {
    const { campaignId, twilioNumber } = req.body || {}
    if (twilioNumber) {
      const removed = callQueue.clearByNumber(twilioNumber)
      return reply.send({ message: 'Queue stopped', removed })
    }
    callQueue.clear(campaignId)
    return reply.send({ message: 'Campaign stopped' })
  })

  // สถานะ queue — รวมทุกเบอร์ และแยกตามเบอร์ (สำหรับให้เลือกว่าจะหยุดคิวของเบอร์ไหน)
  fastify.get('/api/campaign/status', async (req, reply) => {
    return reply.send({
      queueSize: callQueue.size(),
      running: callQueue.runningCount(),
      byNumber: callQueue.statusByNumber(),
    })
  })
}
