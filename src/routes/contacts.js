const { sheetsService } = require('../services/googleSheets')
const { isValidPhone } = require('../utils/phone')

module.exports = async function contactsRoutes(fastify) {

  // รายชื่อ contacts (กรองตาม campaign/status ได้)
  fastify.get('/api/contacts', async (req, reply) => {
    const { campaignId, status } = req.query
    const contacts = await sheetsService.getContacts({ campaignId, status })
    return reply.send(contacts)
  })

  // ลงชื่อเบอร์ใหม่
  fastify.post('/api/contacts', async (req, reply) => {
    const { phone, name, campaign, status } = req.body || {}
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
      .filter(c => isValidPhone(c.phone))
      .map(c => ({ phone: c.phone, name: c.name || '', campaign, status: 'pending' }))
    if (!rows.length) return reply.code(400).send({ error: 'No valid rows' })

    await sheetsService.addContactsBulk(rows)
    return reply.send({ message: 'Contacts imported', count: rows.length })
  })

  // Do-not-call list
  fastify.get('/api/blocklist', async (req, reply) => {
    return reply.send(await sheetsService.getBlocklist())
  })

  fastify.post('/api/blocklist', async (req, reply) => {
    const { phone, reason } = req.body || {}
    if (!phone) return reply.code(400).send({ error: 'phone required' })
    await sheetsService.addToBlocklist(phone, reason)
    return reply.send({ message: 'Blocked' })
  })

  // แก้ไข contact (เช่น เปลี่ยน status, ชื่อ, campaign)
  fastify.patch('/api/contacts/:phone', async (req, reply) => {
    const updated = await sheetsService.updateContact(req.params.phone, req.body || {})
    if (!updated) return reply.code(404).send({ error: 'Contact not found' })
    return reply.send({ message: 'Contact updated' })
  })

  // ลบแบบ soft delete — เปลี่ยน status เป็น deleted แทนการลบแถวจริง
  fastify.delete('/api/contacts/:phone', async (req, reply) => {
    const updated = await sheetsService.updateContact(req.params.phone, { status: 'deleted' })
    if (!updated) return reply.code(404).send({ error: 'Contact not found' })
    return reply.send({ message: 'Contact deleted' })
  })
}
