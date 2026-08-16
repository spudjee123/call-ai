const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// เจตนารันใน timezone ที่ไม่ใช่ UTC — บั๊กเดิม (parse วันที่แบบ local timezone แล้ว toISOString() เลื่อนวันไป 1 วัน)
// จะจับได้เฉพาะตอนรันใน timezone ที่ไม่ใช่ UTC เท่านั้น รันเป็น UTC เทสนี้จะผ่านทั้งที่โค้ดพังจริงก็ได้
process.env.TZ = 'Asia/Bangkok'

// stub googleapis ก่อน require googleSheets.js — กันยิง Sheets API จริงและคุมข้อมูลทดสอบเองได้
// (pattern เดียวกับที่ใช้พิสูจน์บั๊กจริงมาตลอดทั้งเซสชันตรวจสอบระบบนี้ ผ่าน require.cache injection)
const state = { data: {}, calls: [], throwOnRange: new Set() }
const fakeClient = {
  spreadsheets: {
    values: {
      get: async ({ range }) => {
        state.calls.push({ method: 'get', range })
        if (state.throwOnRange.has(range)) throw new Error(`Unable to parse range: ${range}`) // จำลอง error จริงของ Sheets API เมื่อชีตแท็บนี้ยังไม่ถูกสร้าง
        return { data: { values: state.data[range] || [] } }
      },
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

beforeEach(() => { state.data = {}; state.calls = []; state.throwOnRange = new Set() })

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

test('getInboundCampaignForNumber เลือกเฉพาะ campaign type=inbound ไม่ fallback ไปแถวแรกที่เป็น outbound (บั๊กเดิม)', async () => {
  state.data['Twilio Numbers'] = [
    ['Phone Number', 'Label', 'Campaign Id', 'Notes'],
    ['+66800000000', '', 'in1', ''],
  ]
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['out1', 'Outbound A', 'active', 'outbound'],
    ['in1', 'Inbound A', 'active', 'inbound'],
  ]
  const campaign = await sheetsService.getInboundCampaignForNumber('+66800000000')
  assert.equal(campaign.id, 'in1')
})

test('getInboundCampaignForNumber คืน null ถ้าเบอร์ลงทะเบียนไว้แต่ไม่ได้ผูก campaign_id เลย (ไม่เดา campaign อื่นมาใช้แทน)', async () => {
  state.data['Twilio Numbers'] = [
    ['Phone Number', 'Label', 'Campaign Id', 'Notes'],
    ['+66800000000', '', '', ''],
  ]
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['out1', 'Outbound A', 'active', 'outbound'],
  ]
  const campaign = await sheetsService.getInboundCampaignForNumber('+66800000000')
  assert.equal(campaign, null)
})

test('getInboundCampaignForNumber เลือก campaign ตามเบอร์ที่ผูกไว้ในหน้าจัดการเบอร์ (รองรับหลายเบอร์พร้อมกัน)', async () => {
  state.data['Twilio Numbers'] = [
    ['Phone Number', 'Label', 'Campaign Id', 'Notes'],
    ['+66811111111', 'สาย A', 'in1', ''],
    ['+66822222222', 'สาย B', 'in2', ''],
  ]
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['in1', 'สาขา A', 'active', 'inbound'],
    ['in2', 'สาขา B', 'active', 'inbound'],
  ]
  const campaignA = await sheetsService.getInboundCampaignForNumber('+66811111111')
  const campaignB = await sheetsService.getInboundCampaignForNumber('+66822222222')
  assert.equal(campaignA.id, 'in1')
  assert.equal(campaignB.id, 'in2')
})

test('getInboundCampaignForNumber คืน null ถ้าเบอร์ที่โทรเข้ามาไม่เคยลงทะเบียนในระบบเลย (ไม่เดาเบอร์อื่นมาใช้แทน)', async () => {
  state.data['Twilio Numbers'] = [
    ['Phone Number', 'Label', 'Campaign Id', 'Notes'],
    ['+66811111111', 'สาย A', 'in1', ''],
  ]
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['in1', 'สาขา A', 'active', 'inbound'],
  ]
  const campaign = await sheetsService.getInboundCampaignForNumber('+66899999999')
  assert.equal(campaign, null)
})

test('getInboundCampaignForNumber คืน null ถ้า campaign ที่ผูกไว้ไม่ active หรือไม่ใช่ inbound แล้ว (กันเบอร์ผูกค้างกับ campaign ที่ปิด/เปลี่ยนประเภทไปแล้ว)', async () => {
  state.data['Twilio Numbers'] = [
    ['Phone Number', 'Label', 'Campaign Id', 'Notes'],
    ['+66800000000', '', 'in1', ''],
  ]
  state.data['Campaigns'] = [
    ['Id', 'Name', 'Status', 'Type'],
    ['in1', 'สาขา A', 'inactive', 'inbound'],
  ]
  const campaign = await sheetsService.getInboundCampaignForNumber('+66800000000')
  assert.equal(campaign, null)
})

test('addTwilioNumber/updateTwilioNumber/deleteTwilioNumber: เขียน/แก้/ลบแถวในชีต Twilio Numbers ตาม phone_number', async () => {
  state.data['Twilio Numbers'] = [['Phone Number', 'Label', 'Campaign Id', 'Notes']]
  await sheetsService.addTwilioNumber({ phone_number: '+66811111111', label: 'สายหลัก', campaign_id: 'in1', notes: 'ทดสอบ' })
  assert.deepEqual(lastCall('append').requestBody.values[0], ['+66811111111', 'สายหลัก', 'in1', 'ทดสอบ'])

  state.data['Twilio Numbers'].push(['+66811111111', 'สายหลัก', 'in1', 'ทดสอบ'])
  const found = await sheetsService.getTwilioNumberByPhone('+66811111111')
  assert.equal(found.label, 'สายหลัก')

  const updated = await sheetsService.updateTwilioNumber('+66811111111', { label: 'สายใหม่' })
  assert.equal(updated, true)
  assert.deepEqual(lastCall('update').requestBody.values[0], ['+66811111111', 'สายใหม่', 'in1', 'ทดสอบ'])

  const missing = await sheetsService.updateTwilioNumber('+66899999999', { label: 'x' })
  assert.equal(missing, false)
})

test('getInboundCampaignForNumber ไม่ throw ถ้าชีต "Twilio Numbers" ยังไม่ถูกสร้างเลย (ฟีเจอร์ใหม่ ผู้ใช้อาจยังไม่ได้เพิ่มแท็บนี้) — ไม่งั้น webhook สายเข้าจริงจะล่มทั้งระบบ', async () => {
  state.throwOnRange.add('Twilio Numbers')
  state.data['Campaigns'] = [['Id', 'Name', 'Status', 'Type'], ['in1', 'สาขา A', 'active', 'inbound']]
  const campaign = await sheetsService.getInboundCampaignForNumber('+66800000000')
  assert.equal(campaign, null)
})

test('getTwilioNumberForCampaign คืนแถวเบอร์ที่ผูก campaign นี้ไว้ (ใช้เป็นเบอร์โทรออกเริ่มต้นตอนสั่งเริ่ม campaign)', async () => {
  state.data['Twilio Numbers'] = [
    ['Phone Number', 'Label', 'Campaign Id', 'Notes'],
    ['+66811111111', '', 'camp1', ''],
    ['+66822222222', '', 'camp2', ''],
  ]
  const found = await sheetsService.getTwilioNumberForCampaign('camp1')
  assert.equal(found.phone_number, '+66811111111')
  const none = await sheetsService.getTwilioNumberForCampaign('camp3')
  assert.equal(none, null)
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

test('getStats: แยกจำนวนสายและจำนวนที่สนใจตามเบอร์ Twilio ที่ใช้โทรจริง (สำหรับ Dashboard มุมมอง "ต่อเบอร์")', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Twilio Number', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'camp1', '+66811111111', 'interested', '30', '2026-03-01T10:00:00.000Z'],
    ['CA2', '082', 'camp1', '+66811111111', 'not_interested', '30', '2026-03-01T11:00:00.000Z'],
    ['CA3', '083', 'camp2', '+66822222222', 'interested', '30', '2026-03-01T12:00:00.000Z'],
    ['CA4', '084', 'camp2', '', 'interested', '30', '2026-03-01T13:00:00.000Z'], // ไม่มี twilio_number บันทึกไว้ (แถวเก่าก่อนมีฟีเจอร์นี้) — ไม่ควรถูกนับในกลุ่มไหนเลย ไม่ใช่ไปรวมกับเบอร์ว่าง
  ]
  const stats = await sheetsService.getStats({ allTime: true })
  assert.deepEqual(stats.byTwilioNumber, { '+66811111111': 2, '+66822222222': 1 })
  assert.deepEqual(stats.interestedByTwilioNumber, { '+66811111111': 1, '+66822222222': 1 })
})

test('getStats: สรุปอัตราส่ง SMS สำเร็จจากสถานะ sms_status ที่บันทึกไว้ต่อแถวอยู่แล้ว', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp', 'SMS Status'],
    ['CA1', '081', 'camp1', 'interested', '30', '2026-03-01T10:00:00.000Z', 'delivery'],
    ['CA2', '082', 'camp1', 'interested', '30', '2026-03-01T10:00:00.000Z', 'delivery'],
    ['CA3', '083', 'camp1', 'not_interested', '30', '2026-03-01T10:00:00.000Z', 'failed'],
    ['CA4', '084', 'camp1', 'callback', '30', '2026-03-01T10:00:00.000Z', 'sent'],
    ['CA5', '085', 'camp1', 'no_answer', '30', '2026-03-01T10:00:00.000Z', ''], // ไม่มีการส่ง SMS เลย — ไม่ควรถูกนับในตัวส่วนของอัตราสำเร็จ
  ]
  const stats = await sheetsService.getStats({ allTime: true })
  assert.deepEqual(stats.smsStats, { sent: 1, delivered: 2, failed: 1, attempted: 4, deliveryRate: 50 })
})

test('getStats: แยกจำนวนสายตามชั่วโมง/วันในสัปดาห์ตามเวลาไทย (UTC+7) ต้องข้ามวันถูกต้องถ้าเวลา UTC บวก 7 ชม. แล้วเลยเที่ยงคืน', async () => {
  process.env.TZ = 'UTC' // กันเครื่องที่รันเทสอยู่คนละ timezone มามีผลกับผลลัพธ์ — โค้ดต้องคำนวณเวลาไทยเองไม่พึ่ง timezone ของเครื่อง
  state.data['Call Results'] = [
    // UTC วันอาทิตย์ 18:00 → เวลาไทย (บวก 7) กลายเป็นวันจันทร์ 01:00 (ข้ามวันจริง ไม่ใช่แค่ข้ามชั่วโมง)
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'camp1', 'interested', '30', '2026-03-01T18:00:00.000Z'],
  ]
  const stats = await sheetsService.getStats({ allTime: true })
  assert.equal(stats.byHour[1].calls, 1, 'ชั่วโมง 01:00 ตามเวลาไทยต้องมี 1 สาย')
  assert.equal(stats.byHour[1].interested, 1)
  assert.equal(stats.byDayOfWeek[1].calls, 1, 'ต้องนับเป็นวันจันทร์ (index 1) ไม่ใช่วันอาทิตย์ตาม UTC ดิบ (index 0)')
  assert.equal(stats.byDayOfWeek[0].calls, 0, 'ต้องไม่ค้างอยู่ที่วันอาทิตย์ตาม UTC ดิบ')
})
