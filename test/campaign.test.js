const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const Fastify = require('fastify')

// stub googleSheets ก่อน require route — กันยิง Sheets API จริงและคุมข้อมูลทดสอบเองได้
const state = { campaigns: {}, contactsByCampaign: {}, pendingByCampaign: {}, twilioNumbers: [], updates: [], deleted: [], queued: [], numberUpdates: [] }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      getContacts: async ({ campaignId } = {}) => state.contactsByCampaign[campaignId] || [],
      getPendingContacts: async (campaignId) => state.pendingByCampaign[campaignId] || [],
      getCampaign: async (id) => state.campaigns[id] || null,
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
