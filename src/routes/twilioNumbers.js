const { sheetsService } = require('../services/googleSheets')
const twilioService = require('../services/twilio')

module.exports = async function twilioNumberRoutes(fastify) {

  // เบอร์ที่ลงทะเบียนไว้ในระบบนี้ — แหล่งข้อมูลเดียวกันที่ Dashboard/เบอร์ติดต่อ/ทดสอบ/ประวัติการโทรใช้ร่วมกัน
  fastify.get('/api/twilio-numbers', async (req, reply) => {
    return reply.send(await sheetsService.getTwilioNumbers())
  })

  // ลงทะเบียนเบอร์ที่ซื้อไว้แล้วในบัญชี Twilio เข้าระบบนี้ (ไม่ใช่ซื้อเบอร์ใหม่) — เช็คกับบัญชี Twilio จริงก่อนเสมอ กัน typo/เบอร์ที่ไม่ได้เป็นเจ้าของ
  fastify.post('/api/twilio-numbers', async (req, reply) => {
    const { phone_number, label, campaign_id, notes } = req.body || {}
    if (!phone_number) return reply.code(400).send({ error: 'phone_number required' })

    const existing = await sheetsService.getTwilioNumberByPhone(phone_number)
    if (existing) return reply.code(409).send({ error: 'เบอร์นี้ลงทะเบียนไว้แล้ว' })

    let owned
    try {
      owned = await twilioService.listPhoneNumbers()
    } catch (err) {
      return reply.code(502).send({ error: 'ตรวจสอบเบอร์กับ Twilio ไม่สำเร็จ ลองใหม่อีกครั้ง' })
    }
    if (!owned.some(n => n.phoneNumber === phone_number)) {
      return reply.code(400).send({ error: 'ไม่พบเบอร์นี้ในบัญชี Twilio ของคุณ' })
    }

    if (campaign_id) {
      const campaign = await sheetsService.getCampaign(campaign_id)
      if (!campaign) return reply.code(400).send({ error: 'ไม่พบ Campaign ที่เลือก' })
    }

    await sheetsService.addTwilioNumber({ phone_number, label, campaign_id, notes })
    return reply.send({ message: 'เพิ่มเบอร์แล้ว' })
  })

  // แก้ label/campaign ที่ผูกไว้/notes — เปลี่ยนตัวเบอร์เองไม่ได้ (ลบแล้วเพิ่มใหม่แทน)
  fastify.patch('/api/twilio-numbers/:phone', async (req, reply) => {
    const { phone_number, ...updates } = req.body || {}
    if (updates.campaign_id) {
      const campaign = await sheetsService.getCampaign(updates.campaign_id)
      if (!campaign) return reply.code(400).send({ error: 'ไม่พบ Campaign ที่เลือก' })
    }
    const updated = await sheetsService.updateTwilioNumber(req.params.phone, updates)
    if (!updated) return reply.code(404).send({ error: 'ไม่พบเบอร์นี้ในระบบ' })
    return reply.send({ message: 'บันทึกแล้ว' })
  })

  // เอาออกจากระบบนี้เท่านั้น — เบอร์ยังอยู่ในบัญชี Twilio เหมือนเดิม ไม่ถูกยกเลิก/release
  fastify.delete('/api/twilio-numbers/:phone', async (req, reply) => {
    const deleted = await sheetsService.deleteTwilioNumber(req.params.phone)
    if (!deleted) return reply.code(404).send({ error: 'ไม่พบเบอร์นี้ในระบบ' })
    return reply.send({ message: 'ลบแล้ว' })
  })
}
