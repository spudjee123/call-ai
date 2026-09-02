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

  // ลงชื่อเบอร์ใหม่ — status ไม่รับจาก client เสมอเป็น pending (แก้ผ่าน PATCH/DELETE แทน)
  fastify.post('/api/contacts', async (req, reply) => {
    const { name, campaign } = req.body || {}
    const phone = normalizePhone(req.body?.phone)
    if (!phone || !campaign) {
      return reply.code(400).send({ error: 'phone and campaign required' })
    }
    if (!isValidPhone(phone)) {
      return reply.code(400).send({ error: 'Invalid phone number' })
    }
    const campaignExists = await sheetsService.getCampaign(campaign)
    if (!campaignExists) return reply.code(400).send({ error: 'ไม่พบ Campaign นี้ในระบบ' })
    await sheetsService.addContact({ phone, name: name || '', campaign, status: 'pending' })
    return reply.send({ message: 'Contact added' })
  })

  // นำเข้าหลายเบอร์พร้อมกัน (paste จาก Excel/Google Sheets) — 1 Sheets API call
  fastify.post('/api/contacts/bulk', async (req, reply) => {
    const { contacts, campaign } = req.body || {}
    if (!Array.isArray(contacts) || !contacts.length || !campaign) {
      return reply.code(400).send({ error: 'contacts[] and campaign required' })
    }
    const campaignExists = await sheetsService.getCampaign(campaign)
    if (!campaignExists) return reply.code(400).send({ error: 'ไม่พบ Campaign นี้ในระบบ' })
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
    const twilioNumber = req.body?.twilioNumber || ''
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
      // เบอร์ที่แอดมินเลือกตอนกดโทร (ถ้ามี) ทับเบอร์ที่ตั้งไว้ระดับ campaign เฉพาะสายชุดนี้ — ไม่แก้ campaign จริง
      callQueue.add({ contact, campaign: twilioNumber ? { ...campaign, twilio_number: twilioNumber } : campaign })
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

  fastify.delete('/api/blocklist/:phone', async (req, reply) => {
    const removed = await sheetsService.removeFromBlocklist(normalizePhone(req.params.phone))
    if (!removed) return reply.code(404).send({ error: 'Phone not found in blocklist' })
    return reply.send({ message: 'Unblocked' })
  })

  // แก้ไข contact (เช่น เปลี่ยน status, ชื่อ, campaign, หรือแก้เบอร์เอง)
  fastify.patch('/api/contacts/:phone', async (req, reply) => {
    const originalPhone = normalizePhone(req.params.phone)
    const body = { ...(req.body || {}) }
    if (body.phone) {
      body.phone = normalizePhone(body.phone)
      if (!isValidPhone(body.phone)) return reply.code(400).send({ error: 'Invalid phone number' })
      if (body.phone !== originalPhone) {
        const existing = await sheetsService.getContact(body.phone)
        if (existing) return reply.code(409).send({ error: 'มีเบอร์นี้อยู่ในระบบแล้ว' })
      }
    }
    const updated = await sheetsService.updateContact(originalPhone, body)
    if (!updated) return reply.code(404).send({ error: 'Contact not found' })
    return reply.send({ message: 'Contact updated' })
  })

  // ลบแบบ soft delete — เปลี่ยน status เป็น deleted แทนการลบแถวจริง
  fastify.delete('/api/contacts/:phone', async (req, reply) => {
    const updated = await sheetsService.updateContact(normalizePhone(req.params.phone), { status: 'deleted' })
    if (!updated) return reply.code(404).send({ error: 'Contact not found' })
    return reply.send({ message: 'Contact deleted' })
  })

  // Bulk soft-delete (Design Freeze "Contacts Bulk Soft-Delete + Row-Safe Undo", 2026-09-02) — 1 request
  // instead of N sequential DELETEs (ทีละเบอร์ทำให้ 63 เบอร์ = ~126 Sheets API operations เรียงคิวกัน + client
  // เดิม (Promise.all ที่ไม่เช็ค res.ok) ไม่มีทางรู้เลยว่าเบอร์ไหนลบไม่สำเร็จจริง) — response ด้านล่างรายงานผลจริง
  // ให้ frontend เช็ค res.ok + ตัวเลขที่คืนมาก่อนประกาศสำเร็จ แทนที่จะสมมติว่าสำเร็จเสมอ
  fastify.post('/api/contacts/bulk-delete', async (req, reply) => {
    const phones = Array.isArray(req.body?.phones)
      ? [...new Set(req.body.phones.map(normalizePhone).filter(Boolean))]
      : []
    if (!phones.length) return reply.code(400).send({ error: 'phones[] required' })
    const result = await sheetsService.bulkDeleteContacts(phones)
    return reply.send(result)
  })

  // Undo สำหรับ bulk-delete ด้านบน — body คือ undo array ที่ endpoint ด้านบนคืนมาตรงๆ (แต่ละ entry มี
  // rowNumber/phone/previousStatus) sheetsService.bulkRestoreContacts จะ verify ทีละ entry ก่อน restore จริง
  fastify.post('/api/contacts/bulk-restore', async (req, reply) => {
    const undo = Array.isArray(req.body?.undo)
      ? req.body.undo.filter(e => e && Number.isInteger(e.rowNumber) && typeof e.phone === 'string')
      : []
    if (!undo.length) return reply.code(400).send({ error: 'undo[] required' })
    const result = await sheetsService.bulkRestoreContacts(undo)
    return reply.send(result)
  })
}
