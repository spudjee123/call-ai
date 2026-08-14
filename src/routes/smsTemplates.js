const { sheetsService } = require('../services/googleSheets')
const { sendSms } = require('../services/thaiBulkSms')

module.exports = async function smsTemplateRoutes(fastify) {

  fastify.get('/api/sms-templates', async (req, reply) => {
    return reply.send(await sheetsService.getSmsTemplates())
  })

  fastify.post('/api/sms-templates', async (req, reply) => {
    const { id, name, template_text, sender } = req.body || {}
    if (!id || !name || !template_text) return reply.code(400).send({ error: 'id, name and template_text required' })

    const existing = await sheetsService.getSmsTemplateById(id)
    if (existing) return reply.code(409).send({ error: 'Template id already exists' })

    await sheetsService.addSmsTemplate({ id, name, template_text, sender })
    return reply.send({ message: 'Template created' })
  })

  fastify.patch('/api/sms-templates/:id', async (req, reply) => {
    // ตัด id ออกจาก body เสมอ — กันแก้ primary key ผ่าน PATCH โดยไม่เช็ค unique (ต่างจาก POST ที่เช็คซ้ำก่อนสร้าง)
    const { id, ...updates } = req.body || {}
    const updated = await sheetsService.updateSmsTemplate(req.params.id, updates)
    if (!updated) return reply.code(404).send({ error: 'Template not found' })
    return reply.send({ message: 'Template updated' })
  })

  // ลบ template ทิ้งจริง — campaign ที่เคยเลือก template นี้ไว้จะไม่ส่ง SMS สำหรับ outcome นั้นอีก (เงียบๆ ไม่ error)
  fastify.delete('/api/sms-templates/:id', async (req, reply) => {
    const deleted = await sheetsService.deleteSmsTemplate(req.params.id)
    if (!deleted) return reply.code(404).send({ error: 'Template not found' })
    return reply.send({ message: 'Template deleted' })
  })

  // ส่ง SMS จริงด้วย template นี้ไปเบอร์ที่ระบุ — ไว้ทดสอบ template+sender ก่อนเอาไปผูกกับ campaign จริง เสียเครดิตจริง ไม่บันทึกอะไรลง Sheet
  fastify.post('/api/sms-templates/:id/test-send', async (req, reply) => {
    const { phone, name } = req.body || {}
    if (!phone) return reply.code(400).send({ error: 'phone required' })

    const template = await sheetsService.getSmsTemplateById(req.params.id)
    if (!template) return reply.code(404).send({ error: 'Template not found' })

    let senderName
    if (template.sender) {
      const senderRow = await sheetsService.getSenderNameById(template.sender)
      senderName = senderRow?.name
    }
    const body = template.template_text.replace(/\{name\}/g, name || 'ทดสอบ')

    try {
      const result = await sendSms(phone, body, senderName)
      // sendSms() ไม่ throw ตอนเบอร์ถูก reject (แค่ log บนเซิร์ฟเวอร์) — ปล่อยผ่านตรงนี้จะทำให้ปุ่มทดสอบขึ้น "ส่งแล้ว" ทั้งที่จริงไม่ถึงเบอร์
      if (result?.bad_phone_number_list?.length) {
        return reply.code(502).send({ error: `ThaiBulkSMS ปฏิเสธเบอร์นี้: ${JSON.stringify(result.bad_phone_number_list)}` })
      }
      return reply.send({ message: 'ส่งแล้ว' })
    } catch (err) {
      return reply.code(502).send({ error: err.message })
    }
  })
}
