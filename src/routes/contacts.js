const { sheetsService } = require('../services/googleSheets')

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
    await sheetsService.addContact({ phone, name: name || '', campaign, status: status || 'pending' })
    return reply.send({ message: 'Contact added' })
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
