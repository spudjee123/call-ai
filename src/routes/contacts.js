const { sheetsService } = require('../services/googleSheets')
const { isValidPhone, normalizePhone } = require('../utils/phone')
const { callQueue } = require('../utils/callQueue')

module.exports = async function contactsRoutes(fastify) {

  // รายชื่อ contacts (กรองตาม campaign/status ได้)
  fastify.get('/api/contacts', async (req, reply) => {
    const { campaignId, status } = req.query
    const contacts = await sheetsService.getContacts({ campaignId, status })
    return reply.send(contacts)
  })

  // ลงชื่อเบอร์ใหม่
  fastify.post('/api/contacts', async (req, reply) => {
    const { name, campaign, status } = req.body || {}
    const phone = normalizePhone(req.body?.phone)
    if (!phone || !campaign) {
      return reply.code(400).send({ error: 'phone and campaign required' })
    }
    if (!isValidPhone(phone)) {
      return reply.code(400).send({ error: 'Invalid phone number' })
    }
    await sheetsService.addContact({ phone, name: name || '', campaign, status: status || 'pending' })
    return reply.send({ message: 'Contact added' })
  })

  // นำเข้าหลายเบอร์พร้อมกัน (paste จาก Excel/Google Sheets) — 1 Sheets API call
  fastify.post('/api/contacts/bulk', async (req, reply) => {
    const { contacts, campaign } = req.body || {}
    if (!Array.isArray(contacts) || !contacts.length || !campaign) {
      return reply.code(400).send({ error: 'contacts[] and campaign required' })
    }
    const rows = contacts
      .map(c => ({ phone: normalizePhone(c.phone), name: c.name || '' }))
      .filter(c => isValidPhone(c.phone))
      .map(c => ({ ...c, campaign, status: 'pending' }))
    if (!rows.length) return reply.code(400).send({ error: 'No valid rows' })

    await sheetsService.addContactsBulk(rows)
    return reply.send({ message: 'Contacts imported', count: rows.length })
  })

  // โทรออกหลายเบอร์ที่เลือกพร้อมกัน — เข้าคิวเดียวกับ campaign จริง (คุมสูงสุด MAX_CONCURRENT_CALLS สายพร้อมกัน)
  // ใช้ campaign ที่ผูกกับแต่ละ contact อยู่แล้วในชีต ไม่ต้องเลือกใหม่
  fastify.post('/api/contacts/call', async (req, reply) => {
    const phones = Array.isArray(req.body?.phones) ? req.body.phones.map(normalizePhone) : []
    if (!phones.length) return reply.code(400).send({ error: 'phones[] required' })

    // ดึงทีเดียวแล้ว lookup ใน memory — กันยิง Sheets API ทีละเบอร์ (ช้า + เสี่ยงชน quota เวลาเลือกเยอะๆ)
    // และกันเคส error กลางลูปหลังจากโทรบางเบอร์ไปแล้ว (ผู้ใช้กด retry ซ้ำจะโทรซ้ำเบอร์เดิม)
    const [allContacts, allCampaigns] = await Promise.all([
      sheetsService.getContacts(),
      sheetsService.getCampaigns(),
    ])
    const contactByPhone = new Map(allContacts.map(c => [c.phone, c]))
    const campaignById = new Map(allCampaigns.map(c => [c.id, c]))

    let queued = 0
    const skipped = []
    for (const phone of phones) {
      const contact = contactByPhone.get(phone)
      if (!contact || contact.status === 'deleted') { skipped.push(phone); continue }
      const campaign = campaignById.get(contact.campaign)
      if (!campaign) { skipped.push(phone); continue }
      callQueue.add({ contact, campaign })
      queued++
    }
    return reply.send({ message: 'Queued', queued, skipped })
  })

  // Do-not-call list
  fastify.get('/api/blocklist', async (req, reply) => {
    return reply.send(await sheetsService.getBlocklist())
  })

  fastify.post('/api/blocklist', async (req, reply) => {
    const phone = normalizePhone(req.body?.phone)
    const { reason } = req.body || {}
    if (!phone) return reply.code(400).send({ error: 'phone required' })
    await sheetsService.addToBlocklist(phone, reason)
    return reply.send({ message: 'Blocked' })
  })

  // แก้ไข contact (เช่น เปลี่ยน status, ชื่อ, campaign)
  fastify.patch('/api/contacts/:phone', async (req, reply) => {
    const updated = await sheetsService.updateContact(normalizePhone(req.params.phone), req.body || {})
    if (!updated) return reply.code(404).send({ error: 'Contact not found' })
    return reply.send({ message: 'Contact updated' })
  })

  // ลบแบบ soft delete — เปลี่ยน status เป็น deleted แทนการลบแถวจริง
  fastify.delete('/api/contacts/:phone', async (req, reply) => {
    const updated = await sheetsService.updateContact(normalizePhone(req.params.phone), { status: 'deleted' })
    if (!updated) return reply.code(404).send({ error: 'Contact not found' })
    return reply.send({ message: 'Contact deleted' })
  })
}
