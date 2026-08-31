const { sheetsService } = require('../services/googleSheets')
const { callQueue } = require('../utils/callQueue')
const { isValidPhone, normalizePhone } = require('../utils/phone')
const twilioService = require('../services/twilio')

// Campaign field validation (Hardening Batch, 2026-08-30) — audit finding: PATCH/POST accepted any
// llm_provider string as-is; a typo (e.g. 'gemni') silently falls back to Claude at the router
// (resolveExplicitProvider in conversationAI.js returns null for anything it doesn't recognize) instead
// of surfacing the mistake to whoever set it. Blank stays valid — it means "use the system default", not
// an error. Only validates the one field this codebase actually branches on; does not attempt a full
// column whitelist (updateRowByKey/appendRowByFields already silently ignore unknown field names, so
// unrecognized keys are a no-op today, not a corruption risk).
const VALID_LLM_PROVIDERS = new Set(['', 'claude', 'gemini'])

// Dual STT Provider (design frozen 2026-08-31) — validateCampaignFields() below mirrors the exact same
// pattern for stt_provider. hasCampaignColumn/validateSttProviderSchema is the separate, async, Lock-3
// check for whether the Sheet's Campaigns tab actually HAS the stt_provider column yet — a typo/type
// check alone (sync, below) can't catch "column doesn't exist," which would otherwise let a save look
// successful while silently never persisting (updateRowByKey/appendRowByFields already skip unknown
// column names — see googleSheets.js comment on that behavior).
const VALID_STT_PROVIDERS = new Set(['', 'google', 'deepgram'])

function validateCampaignFields(fields) {
  if (!fields) return null
  if (fields.llm_provider !== undefined) {
    const raw = fields.llm_provider
    if (typeof raw !== 'string') {
      return `llm_provider ต้องเป็นข้อความ (ค่าว่าง, 'claude', หรือ 'gemini') ไม่ใช่ ${typeof raw}`
    }
    if (!VALID_LLM_PROVIDERS.has(raw.trim().toLowerCase())) {
      return `llm_provider ต้องเป็นค่าว่าง, 'claude', หรือ 'gemini' เท่านั้น (ได้รับ: '${raw}')`
    }
  }
  if (fields.stt_provider !== undefined) {
    const raw = fields.stt_provider
    if (typeof raw !== 'string') {
      return `stt_provider ต้องเป็นข้อความ (ค่าว่าง, 'google', หรือ 'deepgram') ไม่ใช่ ${typeof raw}`
    }
    if (!VALID_STT_PROVIDERS.has(raw.trim().toLowerCase())) {
      return `stt_provider ต้องเป็นค่าว่าง, 'google', หรือ 'deepgram' เท่านั้น (ได้รับ: '${raw}')`
    }
  }
  return null
}

// Active schema check (Lock 3, locked design) — only runs when stt_provider is being set to a NON-blank
// value, so installations that never touch the Dual STT feature never hit an extra Sheets read on every
// campaign save. Blank stt_provider skips this entirely (blank never touches the column at all).
//
// Review hardening — this function must be safe standing alone, not just at its current two call sites.
// Both current callers happen to run validateCampaignFields() first (which rejects a non-string
// stt_provider before this ever runs), but this function shouldn't rely on that ordering to avoid
// throwing — a future caller invoking it directly would hit `.trim()` on a non-string otherwise. The type
// check below makes that impossible regardless of call order.
async function validateSttProviderSchema(fields) {
  if (!fields || fields.stt_provider === undefined) return null
  const raw = typeof fields.stt_provider === 'string' ? fields.stt_provider.trim().toLowerCase() : ''
  if (!raw) return null
  const hasColumn = await sheetsService.hasCampaignColumn('stt_provider')
  if (!hasColumn) {
    return `ตั้งค่า stt_provider='${raw}' ไม่ได้ เพราะ Google Sheet (แท็บ Campaigns) ยังไม่มีคอลัมน์ "stt_provider" — ต้องเพิ่มคอลัมน์นี้ในชีตก่อนถึงจะเลือก STT provider ต่อ Campaign ได้`
  }
  return null
}

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

    const validationError = validateCampaignFields(req.body)
    if (validationError) return reply.code(400).send({ error: validationError })

    const sttSchemaError = await validateSttProviderSchema(req.body)
    if (sttSchemaError) return reply.code(400).send({ error: sttSchemaError })

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
    const validationError = validateCampaignFields(updates)
    if (validationError) return reply.code(400).send({ error: validationError })

    const sttSchemaError = await validateSttProviderSchema(updates)
    if (sttSchemaError) return reply.code(400).send({ error: sttSchemaError })

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

    // เลิกผูกเบอร์ที่เคยผูกกับ campaign นี้ไว้ (หน้า "จัดการเบอร์") — กัน campaign_id ค้างชี้ไปหา campaign ที่ไม่มีอยู่แล้ว
    // ไม่งั้นเบอร์นั้นจะรับสายเข้า/ใช้เป็นเบอร์เริ่ม campaign อัตโนมัติไม่ได้อีกแบบเงียบๆ โดยไม่มีใครรู้ตัว
    const boundNumbers = (await sheetsService.getTwilioNumbers()).filter(n => n.campaign_id === id)
    await Promise.all(boundNumbers.map(n => sheetsService.updateTwilioNumber(n.phone_number, { campaign_id: '' })))

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

    // ใช้เบอร์ที่ผูก campaign นี้ไว้จากหน้า "จัดการเบอร์" เป็นเบอร์โทรออกเริ่มต้น (ถ้ามี) — ไม่มีก็ fallback เบอร์เริ่มต้นของระบบตามปกติ
    const numberRow = await sheetsService.getTwilioNumberForCampaign(campaignId)
    const effectiveCampaign = numberRow ? { ...campaign, twilio_number: numberRow.phone_number } : campaign
    contacts.forEach(contact => callQueue.add({ contact, campaign: effectiveCampaign }))

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
