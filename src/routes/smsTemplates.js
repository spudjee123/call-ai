const { sheetsService } = require('../services/googleSheets')

module.exports = async function smsTemplateRoutes(fastify) {

  fastify.get('/api/sms-templates', async (req, reply) => {
    return reply.send(await sheetsService.getSmsTemplates())
  })

  fastify.post('/api/sms-templates', async (req, reply) => {
    const { id, name, template_text } = req.body || {}
    if (!id || !name || !template_text) return reply.code(400).send({ error: 'id, name and template_text required' })

    const existing = await sheetsService.getSmsTemplateById(id)
    if (existing) return reply.code(409).send({ error: 'Template id already exists' })

    await sheetsService.addSmsTemplate({ id, name, template_text })
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
}
