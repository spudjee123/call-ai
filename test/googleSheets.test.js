const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// เจตนารันใน timezone ที่ไม่ใช่ UTC — บั๊กเดิม (parse วันที่แบบ local timezone แล้ว toISOString() เลื่อนวันไป 1 วัน)
// จะจับได้เฉพาะตอนรันใน timezone ที่ไม่ใช่ UTC เท่านั้น รันเป็น UTC เทสนี้จะผ่านทั้งที่โค้ดพังจริงก็ได้
process.env.TZ = 'Asia/Bangkok'

// stub googleapis ก่อน require googleSheets.js — กันยิง Sheets API จริงและคุมข้อมูลทดสอบเองได้
// (pattern เดียวกับที่ใช้พิสูจน์บั๊กจริงมาตลอดทั้งเซสชันตรวจสอบระบบนี้ ผ่าน require.cache injection)
const state = { data: {}, calls: [] }
const fakeClient = {
  spreadsheets: {
    values: {
      get: async ({ range }) => { state.calls.push({ method: 'get', range }); return { data: { values: state.data[range] || [] } } },
      update: async (params) => { state.calls.push({ method: 'update', ...params }); return {} },
      append: async (params) => { state.calls.push({ method: 'append', ...params }); return {} },
    },
    get: async () => ({ data: { sheets: [] } }),
    batchUpdate: async (params) => { state.calls.push({ method: 'batchUpdate', ...params }); return {} },
  },
}
const googleapisPath = require.resolve('googleapis')
require.cache[googleapisPath] = {
  id: googleapisPath, filename: googleapisPath, loaded: true,
  exports: { google: { auth: { GoogleAuth: function () {} }, sheets: () => fakeClient } },
}

const { sheetsService } = require('../src/services/googleSheets')

beforeEach(() => { state.data = {}; state.calls = [] })

function lastCall(method) {
  return [...state.calls].reverse().find(c => c.method === method)
}

test('appendRowByFields วางค่าตามตำแหน่ง column header จริงในชีต ไม่ใช่ตามลำดับ field ที่ส่งเข้ามา', async () => {
  state.data['Contacts'] = [['Status', 'Campaign', 'Name', 'Phone']] // จงใจสลับลำดับให้ต่างจากลำดับปกติ
  await sheetsService.addContact({ phone: '0812345678', name: 'ทดสอบ', campaign: 'camp1' })
  const call = lastCall('append')
  assert.deepEqual(call.requestBody.values[0], ['pending', 'camp1', 'ทดสอบ', '0812345678'])
})

test('getRows normalize header เป็น lowercase + underscore และเติม field ที่ขาด/แถวสั้นกว่า header เป็นค่าว่าง', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Retry Count'],
    ['0899999999', 'ชื่อลูกค้า', 'camp2'], // แถวสั้นกว่า header — ไม่มีคอลัมน์สุดท้าย
  ]
  const contacts = await sheetsService.getContacts()
  assert.deepEqual(contacts, [{ phone: '0899999999', name: 'ชื่อลูกค้า', campaign: 'camp2', retry_count: '' }])
})

test('updateRowByKey แก้เฉพาะ column ที่ระบุ คงค่าคอลัมน์อื่นไว้ครบ และคืน false ถ้าไม่เจอแถว', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0811111111', 'เก่า', 'camp1', 'pending'],
  ]
  const ok = await sheetsService.updateContact('0811111111', { name: 'ใหม่' })
  assert.equal(ok, true)
  assert.deepEqual(lastCall('update').requestBody.values[0], ['0811111111', 'ใหม่', 'camp1', 'pending'])

  const notFound = await sheetsService.updateContact('0899999999', { name: 'x' })
  assert.equal(notFound, false)
})

test('updateRowByKey throw ถ้า key column ไม่มีอยู่ในชีตเลย (กัน silent no-op ที่ตรวจจับไม่ได้)', async () => {
  state.data['Call Results'] = [['Phone', 'Outcome']] // ไม่มีคอลัมน์ call_sid
  await assert.rejects(() => sheetsService.updateCallResultSmsStatus('CA123', { messageId: 'm1', status: 'sent' }))
})

test('getDefaultInboundCampaign เลือกเฉพาะ campaign type=inbound ไม่ fallback ไปแถวแรกที่เป็น outbound (บั๊กเดิม)', async () => {
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['out1', 'Outbound A', 'active', 'outbound'],
    ['in1', 'Inbound A', 'active', 'inbound'],
  ]
  const campaign = await sheetsService.getDefaultInboundCampaign()
  assert.equal(campaign.id, 'in1')
})

test('getDefaultInboundCampaign คืน null ถ้าไม่มี inbound campaign เลย (ไม่เดา campaign อื่นมาใช้แทน)', async () => {
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['out1', 'Outbound A', 'active', 'outbound'],
  ]
  const campaign = await sheetsService.getDefaultInboundCampaign()
  assert.equal(campaign, null)
})

test('getStats: ช่วงวันที่กำหนดเองต้องไม่เลื่อนวันตาม timezone ของเครื่อง (บั๊กเดิม: parse ไม่ต่อท้าย Z ทำให้เลื่อน 1 วันใน timezone ที่ไม่ใช่ UTC)', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'camp1', 'interested', '30', '2026-03-01T10:00:00.000Z'],
    ['CA2', '082', 'camp1', 'interested', '30', '2026-03-03T23:59:00.000Z'],
  ]
  const stats = await sheetsService.getStats({ dateFrom: '2026-03-01', dateTo: '2026-03-03' })
  assert.deepEqual(stats.dailyTrend.labels, ['2026-03-01', '2026-03-02', '2026-03-03'])
  assert.equal(stats.total, 2)
})

test('getStats: ช่วงวันที่เกิน 365 วัน ต้องครอบ total และกราฟรายวันด้วยขอบเขตเดียวกัน (บั๊กเดิม: total นับเกินขณะกราฟถูกตัด ทำให้ตัวเลขไม่ตรงกัน)', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'camp1', 'interested', '30', '2020-01-01T00:00:00.000Z'], // อยู่ในขอบเขตที่ถูกตัดแล้ว
    ['CA2', '082', 'camp1', 'interested', '30', '2029-01-01T00:00:00.000Z'], // เกินขอบเขตไปมาก ต้องไม่ถูกนับ
  ]
  const stats = await sheetsService.getStats({ dateFrom: '2020-01-01', dateTo: '2030-01-01' })
  const sumFromChart = stats.dailyTrend.calls.reduce((a, b) => a + b, 0)
  assert.equal(stats.total, sumFromChart)
  assert.equal(stats.total, 1)
})
