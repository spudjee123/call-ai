const { sheetsService } = require('../services/googleSheets')
const twilioService = require('../services/twilio')

// เขียนลงชีต "Twilio Numbers" ล้มเหลวบ่อยสุดเพราะผู้ใช้ยังไม่ได้สร้างแท็บนี้ในไฟล์ Google Sheets จริง —
// ถ้าไม่ครอบ error ตรงนี้ Fastify จะโยน error ดิบของ Google API ("Unable to parse range: ...") ออกไปเป็น
// "Bad Request" เฉยๆ ที่หน้าแอดมิน ผู้ใช้ไม่รู้ว่าต้องแก้ยังไง
function friendlySheetError(err) {
  if (/Unable to parse range/i.test(err?.message || '')) {
    return 'ยังไม่ได้สร้างแท็บชีตชื่อ "Twilio Numbers" ใน Google Sheets — กรุณาสร้างแท็บนี้ก่อน (คอลัมน์: Phone Number, Label, Campaign Id, Notes)'
  }
  return 'บันทึกไม่สำเร็จ ลองใหม่อีกครั้ง'
}

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

    try {
      await sheetsService.addTwilioNumber({ phone_number, label, campaign_id, notes })
    } catch (err) {
      return reply.code(500).send({ error: friendlySheetError(err) })
    }
    return reply.send({ message: 'เพิ่มเบอร์แล้ว' })
  })

  // แก้ label/campaign ที่ผูกไว้/notes — เปลี่ยนตัวเบอร์เองไม่ได้ (ลบแล้วเพิ่มใหม่แทน)
  fastify.patch('/api/twilio-numbers/:phone', async (req, reply) => {
    const { phone_number, ...updates } = req.body || {}
    if (updates.campaign_id) {
      const campaign = await sheetsService.getCampaign(updates.campaign_id)
      if (!campaign) return reply.code(400).send({ error: 'ไม่พบ Campaign ที่เลือก' })
    }
    let updated
    try {
      updated = await sheetsService.updateTwilioNumber(req.params.phone, updates)
    } catch (err) {
      return reply.code(500).send({ error: friendlySheetError(err) })
    }
    if (!updated) return reply.code(404).send({ error: 'ไม่พบเบอร์นี้ในระบบ' })
    return reply.send({ message: 'บันทึกแล้ว' })
  })

  // เอาออกจากระบบนี้เท่านั้น — เบอร์ยังอยู่ในบัญชี Twilio เหมือนเดิม ไม่ถูกยกเลิก/release
  fastify.delete('/api/twilio-numbers/:phone', async (req, reply) => {
    let deleted
    try {
      deleted = await sheetsService.deleteTwilioNumber(req.params.phone)
    } catch (err) {
      return reply.code(500).send({ error: friendlySheetError(err) })
    }
    if (!deleted) return reply.code(404).send({ error: 'ไม่พบเบอร์นี้ในระบบ' })
    return reply.send({ message: 'ลบแล้ว' })
  })
}
