const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const Fastify = require('fastify')

// stub googleSheets ก่อน require route — กันยิง Sheets API จริงและคุมข้อมูลทดสอบเองได้
const state = { campaigns: {}, contactsByCampaign: {}, pendingByCampaign: {}, twilioNumbers: [], updates: [], deleted: [], queued: [], numberUpdates: [], added: [], campaignUpdates: [], sttColumnExists: true }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      getContacts: async ({ campaignId } = {}) => state.contactsByCampaign[campaignId] || [],
      getPendingContacts: async (campaignId) => state.pendingByCampaign[campaignId] || [],
      getCampaign: async (id) => state.campaigns[id] || null,
      addCampaign: async (fields) => { state.added.push(fields) },
      updateCampaign: async (id, updates) => { state.campaignUpdates.push({ id, updates }); return true },
      hasCampaignColumn: async (columnName) => columnName === 'stt_provider' ? state.sttColumnExists : true,
      updateContact: async (phone, updates) => { state.updates.push({ phone, updates }); return true },
      deleteCampaign: async (id) => { state.deleted.push(id); return true },
      getTwilioNumberForCampaign: async (campaignId) => state.twilioNumbers.find(n => n.campaign_id === campaignId) || null,
      getTwilioNumbers: async () => state.twilioNumbers,
      updateTwilioNumber: async (phone, updates) => {
        state.numberUpdates.push({ phone, updates })
        const n = state.twilioNumbers.find(x => x.phone_number === phone)
        if (n) Object.assign(n, updates)
        return true
      },
    },
  },
}

// stub callQueue — กันไม่ให้ /api/campaign/start ไปสั่งโทรออกจริงผ่าน twilio.js ตอนเทส (callQueue.add ปกติจะ processQueue ทันที)
const callQueuePath = require.resolve('../src/utils/callQueue')
require.cache[callQueuePath] = {
  id: callQueuePath, filename: callQueuePath, loaded: true,
  exports: {
    callQueue: {
      add: (job) => { state.queued.push(job) },
      clear: () => {},
      clearByNumber: () => 0,
      size: () => 0,
      runningCount: () => 0,
      statusByNumber: () => ({}),
    },
  },
}

const campaignRoutes = require('../src/routes/campaign')

async function buildApp() {
  const fastify = Fastify()
  fastify.register(campaignRoutes)
  await fastify.ready()
  return fastify
}

beforeEach(() => {
  state.campaigns = { camp1: { id: 'camp1' }, camp2: { id: 'camp2' } }
  state.contactsByCampaign = {}
  state.pendingByCampaign = {}
  state.twilioNumbers = []
  state.updates = []
  state.deleted = []
  state.queued = []
  state.numberUpdates = []
  state.added = []
  state.campaignUpdates = []
  state.sttColumnExists = true
})

test('POST /api/campaigns: llm_provider ผิด (พิมพ์ผิด) ต้องปฏิเสธ 400 ไม่เขียนลง Sheet แบบเงียบๆ', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new1', name: 'ทดสอบ', llm_provider: 'gemni' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.added.length, 0)
})

test('POST /api/campaigns: llm_provider ว่างเปล่าหรือค่าที่ถูกต้อง (claude/gemini) ต้องผ่าน', async () => {
  const app = await buildApp()
  const res1 = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new1', name: 'ทดสอบ' } })
  assert.equal(res1.statusCode, 200)
  const res2 = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new2', name: 'ทดสอบ2', llm_provider: 'Gemini' } })
  assert.equal(res2.statusCode, 200)
  assert.equal(state.added.length, 2)
})

test('PATCH /api/campaigns/:id: llm_provider ผิด ต้องปฏิเสธ 400 ไม่แตะ Sheet เลย', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { llm_provider: 'chatgpt' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.campaignUpdates.length, 0)
})

test('PATCH /api/campaigns/:id: llm_provider เป็น null (ไม่ใช่ string) ต้องปฏิเสธ 400 เช่นกัน ไม่ใช่หลุดผ่านไปเงียบๆ (Review Gate finding)', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { llm_provider: null } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.campaignUpdates.length, 0)
})

test('PATCH /api/campaigns/:id: field อื่นที่ไม่เกี่ยวกับ llm_provider ยังแก้ได้ปกติ', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { name: 'ชื่อใหม่' } })
  assert.equal(res.statusCode, 200)
  assert.deepEqual(state.campaignUpdates, [{ id: 'camp1', updates: { name: 'ชื่อใหม่' } }])
})

// ===== Dual STT Provider (design frozen 2026-08-31) — mirrors llm_provider validation tests exactly,
// plus the Lock-3 active schema check that llm_provider doesn't need (llm_provider's column already
// exists in every real installation; stt_provider is a brand-new optional column) =====

test('POST /api/campaigns: stt_provider ผิด (พิมพ์ผิด) ต้องปฏิเสธ 400 ไม่เขียนลง Sheet แบบเงียบๆ', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new1', name: 'ทดสอบ', stt_provider: 'deepgrm' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.added.length, 0)
})

test('POST /api/campaigns: stt_provider ว่างเปล่าหรือค่าที่ถูกต้อง (google/deepgram) ต้องผ่าน', async () => {
  const app = await buildApp()
  const res1 = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new1', name: 'ทดสอบ' } })
  assert.equal(res1.statusCode, 200)
  const res2 = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new2', name: 'ทดสอบ2', stt_provider: 'Deepgram' } })
  assert.equal(res2.statusCode, 200)
  assert.equal(state.added.length, 2)
})

test('PATCH /api/campaigns/:id: stt_provider ผิด ต้องปฏิเสธ 400 ไม่แตะ Sheet เลย', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { stt_provider: 'whisper' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.campaignUpdates.length, 0)
})

test('PATCH /api/campaigns/:id: stt_provider เป็น null (ไม่ใช่ string) ต้องปฏิเสธ 400', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { stt_provider: null } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.campaignUpdates.length, 0)
})

test('Lock 3 — POST /api/campaigns: stt_provider=deepgram แต่ Sheet ยังไม่มีคอลัมน์ stt_provider ต้องปฏิเสธ 400 พร้อมข้อความบอกวิธีแก้ ไม่บันทึกแบบครึ่งๆ กลางๆ', async () => {
  state.sttColumnExists = false
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/campaigns', payload: { id: 'new1', name: 'ทดสอบ', stt_provider: 'deepgram' } })
  assert.equal(res.statusCode, 400)
  assert.match(res.json().error, /stt_provider/)
  assert.match(res.json().error, /คอลัมน์/)
  assert.equal(state.added.length, 0, 'ห้ามบันทึกลง Sheet เลยถ้าคอลัมน์ยังไม่มี')
})

test('Lock 3 — PATCH /api/campaigns/:id: stt_provider=google แต่ Sheet ยังไม่มีคอลัมน์ ต้องปฏิเสธ 400 เช่นกัน (ไม่ใช่แค่ deepgram)', async () => {
  state.sttColumnExists = false
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { stt_provider: 'google' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.campaignUpdates.length, 0)
})

test('Lock 3 — stt_provider ว่างเปล่า (blank) ต้องผ่านได้เสมอแม้คอลัมน์ยังไม่มีในชีต (blank ไม่แตะคอลัมน์เลย)', async () => {
  state.sttColumnExists = false
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { stt_provider: '', name: 'ชื่อใหม่' } })
  assert.equal(res.statusCode, 200)
  assert.equal(state.campaignUpdates.length, 1)
})

test('Lock 3 — field อื่นที่ไม่เกี่ยวกับ stt_provider ยังแก้ได้ปกติแม้คอลัมน์ stt_provider ยังไม่มีในชีต', async () => {
  state.sttColumnExists = false
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/campaigns/camp1', payload: { name: 'ชื่อใหม่' } })
  assert.equal(res.statusCode, 200)
})

test('DELETE campaign: เบอร์สถานะ retry_pending (รอโทรซ้ำ) ต้องถูกนับเป็น "ยังไม่เสร็จ" เหมือนเบอร์ pending — ไม่ใช่แค่ pending เฉยๆ', async () => {
  // บั๊กเดิม: เดิมเช็คแค่ getPendingContacts (status==='pending') ทำให้เบอร์ retry_pending หลุดผ่านไปได้
  // แล้ว retryScheduler.js จะข้ามเงียบๆ เพราะ campaign หาไม่เจอ (if (!campaign) continue) เบอร์เหล่านี้เลยค้างสถานะ retry_pending ตลอดไป
  state.contactsByCampaign.camp1 = [
    { phone: '081', campaign: 'camp1', status: 'pending' },
    { phone: '082', campaign: 'camp1', status: 'retry_pending' },
    { phone: '083', campaign: 'camp1', status: 'called' }, // ไม่ควรถูกนับ
    { phone: '084', campaign: 'camp1', status: 'deleted' }, // ไม่ควรถูกนับ
  ]
  const app = await buildApp()

  const blocked = await app.inject({ method: 'DELETE', url: '/api/campaigns/camp1' })
  assert.equal(blocked.statusCode, 409)
  assert.equal(blocked.json().pendingCount, 2)
  assert.equal(state.deleted.length, 0)

  const moved = await app.inject({ method: 'DELETE', url: '/api/campaigns/camp1', payload: { resolution: 'move', targetCampaignId: 'camp2' } })
  assert.equal(moved.statusCode, 200)
  assert.equal(state.updates.length, 2)
  assert.ok(state.updates.every(u => u.updates.campaign === 'camp2'))
  assert.deepEqual(state.deleted, ['camp1'])
})

test('DELETE campaign: ไม่มีเบอร์ pending/retry_pending เลย → ลบได้ตรงๆ ไม่ต้องระบุ resolution', async () => {
  state.contactsByCampaign.camp1 = [{ phone: '081', campaign: 'camp1', status: 'called' }]
  const app = await buildApp()
  const res = await app.inject({ method: 'DELETE', url: '/api/campaigns/camp1' })
  assert.equal(res.statusCode, 200)
  assert.deepEqual(state.deleted, ['camp1'])
})

test('DELETE campaign: resolution=move ไป campaign ปลายทางที่ไม่มีจริง ต้องปฏิเสธและไม่ลบ', async () => {
  state.contactsByCampaign.camp1 = [{ phone: '081', campaign: 'camp1', status: 'pending' }]
  const app = await buildApp()
  const res = await app.inject({ method: 'DELETE', url: '/api/campaigns/camp1', payload: { resolution: 'move', targetCampaignId: 'ghost' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.deleted.length, 0)
})

test('DELETE campaign: resolution=cancel ปิดเบอร์ที่ค้างทั้งหมดเป็น deleted แล้วลบสำเร็จ', async () => {
  state.contactsByCampaign.camp1 = [
    { phone: '081', campaign: 'camp1', status: 'pending' },
    { phone: '082', campaign: 'camp1', status: 'retry_pending' },
  ]
  const app = await buildApp()
  const res = await app.inject({ method: 'DELETE', url: '/api/campaigns/camp1', payload: { resolution: 'cancel' } })
  assert.equal(res.statusCode, 200)
  assert.equal(state.updates.length, 2)
  assert.ok(state.updates.every(u => u.updates.status === 'deleted'))
})

test('DELETE campaign: เบอร์ที่ผูก campaign นี้ไว้ในหน้า "จัดการเบอร์" ต้องถูกเลิกผูก (campaign_id ล้าง) อัตโนมัติ กัน campaign_id ค้างชี้ไปหา campaign ที่ลบไปแล้ว', async () => {
  state.contactsByCampaign.camp1 = [{ phone: '081', campaign: 'camp1', status: 'called' }]
  state.twilioNumbers = [
    { phone_number: '+66811111111', campaign_id: 'camp1' },
    { phone_number: '+66822222222', campaign_id: 'camp2' }, // ผูกกับ campaign อื่น ไม่ควรถูกแตะ
  ]
  const app = await buildApp()
  const res = await app.inject({ method: 'DELETE', url: '/api/campaigns/camp1' })
  assert.equal(res.statusCode, 200)
  assert.deepEqual(state.numberUpdates, [{ phone: '+66811111111', updates: { campaign_id: '' } }])
  assert.equal(state.twilioNumbers.find(n => n.phone_number === '+66822222222').campaign_id, 'camp2')
})

test('POST /api/campaign/start: campaign ที่ผูกเบอร์ไว้ในหน้า "จัดการเบอร์" ใช้เบอร์นั้นเป็นเบอร์โทรออกของทุกสายที่เข้าคิว', async () => {
  state.campaigns.camp1 = { id: 'camp1', name: 'A' }
  state.pendingByCampaign.camp1 = [{ phone: '081', campaign: 'camp1', status: 'pending' }]
  state.twilioNumbers = [{ phone_number: '+66811111111', campaign_id: 'camp1' }]
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/campaign/start', payload: { campaignId: 'camp1' } })
  assert.equal(res.statusCode, 200)
  assert.equal(state.queued.length, 1)
  assert.equal(state.queued[0].campaign.twilio_number, '+66811111111')
})

test('POST /api/campaign/start: campaign ที่ไม่มีเบอร์ผูกไว้เลย ไม่ตั้ง twilio_number ให้ campaign (fallback เบอร์เริ่มต้นของระบบตามปกติผ่าน effectiveTwilioNumber)', async () => {
  state.campaigns.camp1 = { id: 'camp1', name: 'A' }
  state.pendingByCampaign.camp1 = [{ phone: '081', campaign: 'camp1', status: 'pending' }]
  state.twilioNumbers = []
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/campaign/start', payload: { campaignId: 'camp1' } })
  assert.equal(res.statusCode, 200)
  assert.equal(state.queued.length, 1)
  assert.equal(state.queued[0].campaign.twilio_number, undefined)
})
