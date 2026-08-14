const { sheetsService } = require('../services/googleSheets')

module.exports = async function senderNameRoutes(fastify) {

  fastify.get('/api/sender-names', async (req, reply) => {
    return reply.send(await sheetsService.getSenderNames())
  })

  fastify.post('/api/sender-names', async (req, reply) => {
    const { id, name } = req.body || {}
    if (!id || !name) return reply.code(400).send({ error: 'id and name required' })

    const existing = await sheetsService.getSenderNameById(id)
    if (existing) return reply.code(409).send({ error: 'Sender id already exists' })

    await sheetsService.addSenderName({ id, name })
    return reply.send({ message: 'Sender name created' })
  })

  fastify.patch('/api/sender-names/:id', async (req, reply) => {
    // ตัด id ออกจาก body เสมอ — กันแก้ primary key ผ่าน PATCH โดยไม่เช็ค unique (ต่างจาก POST ที่เช็คซ้ำก่อนสร้าง)
    const { id, ...updates } = req.body || {}
    const updated = await sheetsService.updateSenderName(req.params.id, updates)
    if (!updated) return reply.code(404).send({ error: 'Sender name not found' })
    return reply.send({ message: 'Sender name updated' })
  })

  // ลบทิ้งจริง — template ที่เคยเลือกชื่อนี้ไว้จะ resolve ไม่เจอตอนส่ง SMS จริง (เงียบๆ ไม่ error ดู postCall.js)
  fastify.delete('/api/sender-names/:id', async (req, reply) => {
    const deleted = await sheetsService.deleteSenderName(req.params.id)
    if (!deleted) return reply.code(404).send({ error: 'Sender name not found' })
    return reply.send({ message: 'Sender name deleted' })
  })
}
