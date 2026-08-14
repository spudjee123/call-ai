const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const Fastify = require('fastify')

// stub googleSheets ก่อน require route — กันยิง Sheets API จริงและคุมข้อมูลทดสอบเองได้
const state = { campaigns: {}, contactsByCampaign: {}, updates: [], deleted: [] }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      getContacts: async ({ campaignId } = {}) => state.contactsByCampaign[campaignId] || [],
      getCampaign: async (id) => state.campaigns[id] || null,
      updateContact: async (phone, updates) => { state.updates.push({ phone, updates }); return true },
      deleteCampaign: async (id) => { state.deleted.push(id); return true },
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
  state.updates = []
  state.deleted = []
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
