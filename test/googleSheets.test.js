const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// เจตนารันใน timezone ที่ไม่ใช่ UTC — บั๊กเดิม (parse วันที่แบบ local timezone แล้ว toISOString() เลื่อนวันไป 1 วัน)
// จะจับได้เฉพาะตอนรันใน timezone ที่ไม่ใช่ UTC เท่านั้น รันเป็น UTC เทสนี้จะผ่านทั้งที่โค้ดพังจริงก็ได้
process.env.TZ = 'Asia/Bangkok'

// stub googleapis ก่อน require googleSheets.js — กันยิง Sheets API จริงและคุมข้อมูลทดสอบเองได้
// (pattern เดียวกับที่ใช้พิสูจน์บั๊กจริงมาตลอดทั้งเซสชันตรวจสอบระบบนี้ ผ่าน require.cache injection)
const state = { data: {}, calls: [], throwOnRange: new Set(), failGetNTimes: 0 }
const fakeClient = {
  spreadsheets: {
    values: {
      get: async ({ range }) => {
        state.calls.push({ method: 'get', range })
        if (state.throwOnRange.has(range)) throw new Error(`Unable to parse range: ${range}`) // จำลอง error จริงของ Sheets API เมื่อชีตแท็บนี้ยังไม่ถูกสร้าง
        if (state.failGetNTimes > 0) {
          state.failGetNTimes--
          const err = new Error("Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute per user'")
          err.code = 429
          throw err
        }
        return { data: { values: state.data[range] || [] } }
      },
      update: async (params) => { state.calls.push({ method: 'update', ...params }); return {} },
      append: async (params) => { state.calls.push({ method: 'append', ...params }); return {} },
      // ชื่อ method log แยกจาก spreadsheets.batchUpdate (deleteDimension) ด้านล่างตั้งใจ — คนละ endpoint จริง
      // ของ Sheets API (values.batchUpdate เขียนหลาย cell/range พร้อมกัน, spreadsheets.batchUpdate ทำ
      // structural change เช่นลบแถว) ใช้ชื่อเดียวกันจะทำให้ lastCall() แยกไม่ออกว่า test เจาะจงตัวไหน
      batchUpdate: async (params) => { state.calls.push({ method: 'valuesBatchUpdate', ...params }); return {} },
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

const { sheetsService, isRetryableSheetsError, withRetry, _resetSchemaWarningsForTest } = require('../src/services/googleSheets')

beforeEach(() => {
  state.data = {}; state.calls = []; state.throwOnRange = new Set(); state.failGetNTimes = 0
  _resetSchemaWarningsForTest()
})

function lastCall(method) {
  return [...state.calls].reverse().find(c => c.method === method)
}

test('appendRowByFields วางค่าตามตำแหน่ง column header จริงในชีต ไม่ใช่ตามลำดับ field ที่ส่งเข้ามา', async () => {
  state.data['Contacts'] = [['Status', 'Campaign', 'Name', 'Phone']] // จงใจสลับลำดับให้ต่างจากลำดับปกติ
  await sheetsService.addContact({ phone: '0812345678', name: 'ทดสอบ', campaign: 'camp1' })
  const call = lastCall('append')
  assert.deepEqual(call.requestBody.values[0], ['pending', 'camp1', 'ทดสอบ', '0812345678'])
})

test('Schema guard: Contacts ชีตขาดคอลัมน์สำคัญ (status) ต้อง log [SCHEMA_WARNING] หนึ่งครั้ง ไม่ throw ไม่กระทบผลลัพธ์', async () => {
  state.data['Contacts'] = [['Phone', 'Name', 'Campaign']] // ขาด "Status" ไปเลย
  const originalError = console.error
  const logs = []
  console.error = (...args) => logs.push(args.join(' '))
  try {
    await sheetsService.getContacts()
    await sheetsService.getContacts() // เรียกซ้ำ — ต้อง warn แค่ครั้งเดียวต่อ process ไม่ spam log
  } finally {
    console.error = originalError
  }
  const warnings = logs.filter(l => l.includes('[SCHEMA_WARNING]'))
  assert.equal(warnings.length, 1)
  assert.match(warnings[0], /Contacts/)
  assert.match(warnings[0], /status/)
})

test('Schema guard: ชีตที่มีคอลัมน์สำคัญครบ ต้องไม่ log warning ใดๆ', async () => {
  state.data['Campaigns'] = [['Id', 'Name', 'Status']]
  const originalError = console.error
  const logs = []
  console.error = (...args) => logs.push(args.join(' '))
  try {
    await sheetsService.getCampaigns()
  } finally {
    console.error = originalError
  }
  assert.equal(logs.filter(l => l.includes('[SCHEMA_WARNING]')).length, 0)
})

// ===== Dual STT Provider (design frozen 2026-08-31, Lock 3) — hasCampaignColumn() =====

test('hasCampaignColumn: คืน true ถ้าคอลัมน์นั้นมีอยู่จริงในแท็บ Campaigns (ไม่สนตัวพิมพ์ใหญ่เล็ก/ช่องว่าง — ผ่าน normalize header เดิม)', async () => {
  state.data['Campaigns'] = [['Id', 'Name', 'Stt Provider']]
  assert.equal(await sheetsService.hasCampaignColumn('stt_provider'), true)
})

test('hasCampaignColumn: คืน false ถ้าคอลัมน์ไม่มีอยู่เลย', async () => {
  state.data['Campaigns'] = [['Id', 'Name', 'Status']]
  assert.equal(await sheetsService.hasCampaignColumn('stt_provider'), false)
})

test('hasCampaignColumn: ชีต Campaigns ยังไม่มีข้อมูล/แท็บว่างเปล่า → คืน false ไม่ throw', async () => {
  state.data['Campaigns'] = []
  assert.equal(await sheetsService.hasCampaignColumn('stt_provider'), false)
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

test('isRetryableSheetsError: แยก error โควตา/rate-limit จริง (429/503/ข้อความ quota) ออกจาก error อื่นๆ ที่ไม่ควรลองใหม่', () => {
  assert.equal(isRetryableSheetsError({ code: 429 }), true)
  assert.equal(isRetryableSheetsError({ response: { status: 429 } }), true)
  assert.equal(isRetryableSheetsError({ response: { status: 503 } }), true)
  assert.equal(isRetryableSheetsError({ message: "Quota exceeded for quota metric 'Read requests'" }), true)
  assert.equal(isRetryableSheetsError({ code: 400, message: "Column 'x' not found" }), false)
  assert.equal(isRetryableSheetsError({ message: 'Unable to parse range: Foo' }), false)
})

test('withRetry: ลองใหม่อัตโนมัติเมื่อเจอ error โควตา แล้วสำเร็จตอนลองรอบถัดไป — ผู้เรียกไม่เห็น error เลย', async () => {
  let calls = 0
  const result = await withRetry(async () => {
    calls++
    if (calls < 3) { const err = new Error('Quota exceeded'); err.code = 429; throw err }
    return 'ok'
  }, { baseDelayMs: 5 })
  assert.equal(result, 'ok')
  assert.equal(calls, 3)
})

test('withRetry: error ที่ไม่ใช่ rate-limit ต้อง throw ทันที ไม่เสียเวลาลองใหม่เลย', async () => {
  let calls = 0
  await assert.rejects(
    () => withRetry(async () => { calls++; throw new Error("Column 'x' not found") }, { baseDelayMs: 5 }),
    /Column 'x' not found/
  )
  assert.equal(calls, 1, 'ต้องเรียกแค่ครั้งเดียว ไม่ลองใหม่กับ error ที่ไม่ใช่ rate-limit')
})

test('withRetry: โควตาเต็มติดต่อกันเกินจำนวนที่กำหนด ต้อง throw error เดิมกลับไปหลังลองครบทุกครั้ง', async () => {
  let calls = 0
  await assert.rejects(
    () => withRetry(async () => { calls++; const err = new Error('Quota exceeded'); err.code = 429; throw err }, { baseDelayMs: 5, maxAttempts: 3 }),
    /Quota exceeded/
  )
  assert.equal(calls, 3)
})

test('getRows: เจอ error โควตาจาก Sheets API ครั้งเดียวแล้วสำเร็จตอนลองใหม่ — ไม่ throw ออกไปให้ผู้เรียกเห็น', async () => {
  state.data['Contacts'] = [['Phone', 'Name'], ['0811111111', 'ทดสอบ']]
  state.failGetNTimes = 1
  const rows = await sheetsService.getContacts()
  assert.deepEqual(rows, [{ phone: '0811111111', name: 'ทดสอบ' }])
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
    ['CA2', '082', 'camp1', 'interested', '30', '2026-03-03T12:00:00.000Z'], // Bangkok 2026-03-03 19:00 — อยู่ในช่วงชัดเจน ไม่ใกล้ขอบเขตวันตาม Bangkok (เดิมใช้ 23:59:00Z ซึ่งจริงๆ คือ Bangkok 2026-03-04 06:59 — เข้าใจผิดเป็นวันที่ 3 ตอนยังใช้ UTC ดิบ)
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

// ===== Dashboard Daily Active Campaigns + Exact Bangkok Date Boundaries (Design Freeze 2026-09-02) — ก่อนหน้านี้
// dateFrom/dateTo, days=N, callsToday, dailyTrend อ่านวันจาก timestamp UTC ดิบด้วย .slice(0,10) ตรงๆ ต่างจาก
// byHour/byDayOfWeek ในฟังก์ชันเดียวกันที่แปลงเป็นเวลาไทยถูกต้องอยู่แล้ว — ทำให้สายช่วง 00:00-06:59 น. เวลาไทย
// ถูกนับเป็นของ "เมื่อวาน" แทนที่จะเป็นวันนี้ =====

test('getStats: ขอบเขตวันตาม Bangkok (UTC+7) ต้องตัดที่ 17:00 UTC ไม่ใช่เที่ยงคืน UTC (บั๊กเดิม: dateFrom/dateTo กรองด้วย .slice(0,10) ของ UTC ดิบ)', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'interested', '30', '2026-09-01T16:59:59.999Z'], // Bangkok 2026-09-01 23:59:59.999 → exclude
    ['CA2', '082', 'campB', 'interested', '30', '2026-09-01T17:00:00.000Z'], // Bangkok 2026-09-02 00:00:00.000 → include
    ['CA3', '083', 'campC', 'interested', '30', '2026-09-02T16:59:59.999Z'], // Bangkok 2026-09-02 23:59:59.999 → include
    ['CA4', '084', 'campD', 'interested', '30', '2026-09-02T17:00:00.000Z'], // Bangkok 2026-09-03 00:00:00.000 → exclude
  ]
  const stats = await sheetsService.getStats({ dateFrom: '2026-09-02', dateTo: '2026-09-02' })
  assert.equal(stats.total, 2)
  assert.deepEqual(Object.keys(stats.byCampaign).sort(), ['campB', 'campC'])
})

test('getStats: per-campaign metrics ใหม่ — answeredByCampaign ดูจาก call_status (เชื่อมสายได้ไหม) แยกจาก outcome (ผลลัพธ์ธุรกิจ) และ byCampaign นับทุก attempt ไม่ว่าผลจะเป็นอะไร', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Call Status', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'completed', 'interested', '30', '2026-09-02T10:00:00.000Z'],
    ['CA2', '082', 'campA', 'completed', 'callback', '30', '2026-09-02T10:00:00.000Z'],
    ['CA3', '083', 'campA', 'no-answer', 'no-answer', '0', '2026-09-02T10:00:00.000Z'],
    ['CA4', '084', 'campA', 'busy', 'busy', '0', '2026-09-02T10:00:00.000Z'],
    ['CA5', '085', 'campA', 'failed', 'failed', '0', '2026-09-02T10:00:00.000Z'],
  ]
  const stats = await sheetsService.getStats({ dateFrom: '2026-09-02', dateTo: '2026-09-02' })
  assert.equal(stats.byCampaign.campA, 5, 'ต้องนับทุก attempt รวม no-answer/busy/failed ไม่ใช่แค่ completed')
  assert.equal(stats.answeredByCampaign.campA, 2, 'รับสาย = call_status===completed เท่านั้น')
  assert.equal(stats.interestedByCampaign.campA, 1)
  assert.equal(stats.callbackByCampaign.campA, 1)
})

test('getStats: dailyTrend bucket ต้องกรุ๊ปตามวันไทยวันเดียวกับที่ total/byCampaign ใช้ (แถวเดียวกันต้องไม่ตกคนละวันระหว่าง field ในผลลัพธ์เดียวกัน)', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'interested', '30', '2026-09-01T17:30:00.000Z'], // Bangkok 2026-09-02 00:30
  ]
  const stats = await sheetsService.getStats({ dateFrom: '2026-09-02', dateTo: '2026-09-02' })
  assert.equal(stats.total, 1)
  const idx = stats.dailyTrend.labels.indexOf('2026-09-02')
  assert.notEqual(idx, -1)
  assert.equal(stats.dailyTrend.calls[idx], 1, 'bucket ของกราฟต้องนับแถวนี้เป็นวันที่ 2026-09-02 ตามเวลาไทย ไม่ใช่ 2026-09-01 ตาม UTC ดิบ')
})

test('getStats: callsToday ต้องใช้ Bangkok today จริง ไม่ใช่ UTC — freeze "now" ที่ขอบเขต 17:00 UTC เพื่อพิสูจน์ (เวอร์ชันก่อนหน้าที่ใช้ new Date().toISOString() ตรงๆ เป็น timestamp ผ่านได้แม้ implementation ยังเป็น UTC ดิบ เพราะ row timestamp กับ "now" เป็น instant เดียวกันเสมอ ไม่ discriminate อะไรเลย — IR finding)', async (t) => {
  t.mock.timers.enable({ apis: ['Date'] })
  t.mock.timers.setTime(new Date('2026-09-01T17:30:00.000Z').getTime()) // Bangkok "ตอนนี้" = 2026-09-02 00:30
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'interested', '30', '2026-09-01T16:59:59.999Z'], // Bangkok 2026-09-01 23:59:59.999 → เมื่อวาน ไม่นับ
    ['CA2', '082', 'campA', 'interested', '30', '2026-09-01T17:00:00.000Z'], // Bangkok 2026-09-02 00:00:00.000 → วันนี้ นับ
    ['CA3', '083', 'campA', 'interested', '30', '2026-09-02T16:59:59.999Z'], // Bangkok 2026-09-02 23:59:59.999 → วันนี้ นับ
    ['CA4', '084', 'campA', 'interested', '30', '2026-09-02T17:00:00.000Z'], // Bangkok 2026-09-03 00:00:00.000 → พรุ่งนี้ ไม่นับ
  ]
  const stats = await sheetsService.getStats({ allTime: true })
  assert.equal(stats.callsToday, 2, 'ต้องนับเฉพาะ 2 แถวที่ Bangkok date ตรงกับ "วันนี้" (2026-09-02) ตาม freeze time ที่ตั้งไว้')
})

test('getStats: days=N ต้องนับเป็น Bangkok calendar days ปิดทั้งสองด้าน (today-N+1..today) — สายเมื่อ 10 วันก่อนไม่ถูกนับใน days=7 (ใช้ margin 10 วันกัน edge case ใกล้ขอบเขต ไม่ผูกกับเวลาจริงตอนรันเทส)', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'interested', '30', new Date().toISOString()],
    ['CA2', '082', 'campB', 'interested', '30', new Date(Date.now() - 10 * 86400000).toISOString()],
  ]
  const stats = await sheetsService.getStats({ days: 7 })
  assert.equal(stats.total, 1)
  assert.deepEqual(stats.byCampaign, { campA: 1 })
})

test('getStats: days=N ต้องมี upper bound ด้วย — แถวที่ Bangkok date ล้ำไปกว่า "today" (เช่น timestamp ผิดปกติ/นาฬิกาเครื่องคลาดเคลื่อน) ต้องไม่หลุดเข้า total/byCampaign ทั้งที่ไม่มี bucket ใน dailyTrend รองรับ (IR finding: เดิมมีแค่ lower-bound filter ทำให้ total กับผลรวม dailyTrend ไม่ตรงกันได้)', async (t) => {
  t.mock.timers.enable({ apis: ['Date'] })
  t.mock.timers.setTime(new Date('2026-09-02T10:00:00.000Z').getTime()) // Bangkok "วันนี้" = 2026-09-02
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'interested', '30', '2026-09-02T10:00:00.000Z'], // Bangkok 2026-09-02 (วันนี้) → include
    ['CA2', '082', 'campB', 'interested', '30', '2026-09-02T17:00:00.000Z'], // Bangkok 2026-09-03 (ล้ำอนาคตไปกว่า today) → exclude
  ]
  const stats = await sheetsService.getStats({ days: 7 })
  assert.equal(stats.total, 1)
  assert.deepEqual(stats.byCampaign, { campA: 1 })
  const sumFromChart = stats.dailyTrend.calls.reduce((a, b) => a + b, 0)
  assert.equal(stats.total, sumFromChart, 'total ต้องเท่ากับผลรวมของ dailyTrend เสมอ (invariant เดียวกับที่ dateFrom/dateTo path บังคับไว้)')
})

test('getStats: timestamp ว่าง/parse ไม่ได้ ต้องไม่ crash และถูกกรองทิ้งจาก dateFrom/dateTo path แต่ allTime ยังนับรวมเหมือนพฤติกรรมเดิม', async () => {
  state.data['Call Results'] = [
    ['Call SID', 'Phone', 'Campaign ID', 'Outcome', 'Duration', 'Timestamp'],
    ['CA1', '081', 'campA', 'interested', '30', ''],
    ['CA2', '082', 'campA', 'interested', '30', 'not-a-real-date'],
    ['CA3', '083', 'campA', 'interested', '30', '2026-09-02T10:00:00.000Z'],
  ]
  const rangedStats = await sheetsService.getStats({ dateFrom: '2026-09-02', dateTo: '2026-09-02' })
  assert.equal(rangedStats.total, 1, 'timestamp ว่าง/พังต้องถูกกรองทิ้งจากช่วงวันที่ระบุ ไม่ใช่ crash หรือหลุดเข้ามานับผิด')
  const allTimeStats = await sheetsService.getStats({ allTime: true })
  assert.equal(allTimeStats.total, 3, 'allTime ไม่กรองตามวัน ต้องยังนับครบทุกแถวเหมือนพฤติกรรมเดิม แม้ timestamp จะพังก็ตาม')
})

// ===== Contacts Bulk Soft-Delete + Row-Safe Undo (Design Freeze 2026-09-02) — replaces N sequential
// updateContact() calls (each its own full-sheet read) with 1 read + 1 values.batchUpdate, and fixes the
// separate structural bug found during Review: findRowIndex()'s first-match-only findIndex(), combined
// with no phone-uniqueness enforcement anywhere in this file, meant a duplicate-phone contact could never
// be fully deleted via the old single-delete path — deterministically, not intermittently. Semantics locked
// as "A": phone is the unit, deleting a phone deletes EVERY row with that phone. =====

test('bulkDeleteContacts: N unique phones → เพียง 1 Sheets GET + 1 values.batchUpdate เท่านั้น ไม่ว่าจะกี่เบอร์ (แก้บั๊กเดิม ~126 operations สำหรับ 63 เบอร์)', async () => {
  const N = 63
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ...Array.from({ length: N }, (_, i) => [`08${String(i).padStart(8, '0')}`, `name${i}`, 'camp1', 'pending']),
  ]
  const phones = Array.from({ length: N }, (_, i) => `08${String(i).padStart(8, '0')}`)
  const result = await sheetsService.bulkDeleteContacts(phones)

  const getCalls = state.calls.filter(c => c.method === 'get' && c.range === 'Contacts')
  const batchCalls = state.calls.filter(c => c.method === 'valuesBatchUpdate')
  assert.equal(getCalls.length, 1, 'ต้องอ่านชีตแค่ครั้งเดียวไม่ว่าจะลบกี่เบอร์')
  assert.equal(batchCalls.length, 1, 'ต้องเขียนกลับแค่ครั้งเดียว (1 batchUpdate call รวมทุก cell)')
  assert.equal(batchCalls[0].requestBody.data.length, N, 'batch request ต้องมี N ranges (1 ต่อแถว)')
  assert.equal(result.requestedPhones, N)
  assert.equal(result.deletedPhones, N)
  assert.equal(result.deletedRows, N)
  assert.equal(result.notFoundPhones.length, 0)
})

test('bulkDeleteContacts: เบอร์เดียวกันซ้ำ 3 แถว (คนละ campaign, คนละ status) → ทุกแถวถูกลบ ไม่ใช่แค่แถวแรก (แก้บั๊ก findIndex เดิม) และ undo เก็บ previousStatus แยกตาม row ถูกต้อง', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0812345678', 'a', 'campA', 'pending'],
    ['0812345678', 'b', 'campB', 'retry_pending'],
    ['0812345678', 'c', 'campC', 'pending'],
  ]
  const result = await sheetsService.bulkDeleteContacts(['0812345678'])

  assert.equal(result.deletedPhones, 1, 'นับเป็น 1 เบอร์ที่ขอ')
  assert.equal(result.deletedRows, 3, 'แต่เปลี่ยนจริง 3 แถว')

  const batchCall = state.calls.find(c => c.method === 'valuesBatchUpdate')
  assert.equal(batchCall.requestBody.data.length, 3)
  assert.deepEqual(batchCall.requestBody.data.map(d => d.range).sort(), ['Contacts!D2', 'Contacts!D3', 'Contacts!D4'])
  batchCall.requestBody.data.forEach(d => assert.deepEqual(d.values, [['deleted']]))

  assert.equal(result.undo.length, 3)
  result.undo.forEach(u => assert.equal(u.phone, '0812345678'))
  const byRow = Object.fromEntries(result.undo.map(u => [u.rowNumber, u.previousStatus]))
  assert.deepEqual(byRow, { 2: 'pending', 3: 'retry_pending', 4: 'pending' }, 'undo ต้องเก็บ status เดิมของแต่ละแถวแยกกัน ไม่ใช่ค่าเดียวใช้ร่วม')
})

test('bulkDeleteContacts: แถวที่ status=deleted อยู่แล้ว → ไม่ถูกเขียนซ้ำ ไม่เข้า undo แต่นับใน alreadyDeletedRows', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0812345678', 'a', 'campA', 'deleted'],
    ['0812345678', 'b', 'campB', 'pending'],
  ]
  const result = await sheetsService.bulkDeleteContacts(['0812345678'])
  assert.equal(result.alreadyDeletedRows, 1)
  assert.equal(result.deletedRows, 1, 'เขียนแค่แถวที่ยังไม่ deleted')
  assert.equal(result.undo.length, 1)
  assert.equal(result.undo[0].rowNumber, 3, 'undo ต้องมีแค่แถวที่ยังไม่ deleted (แถวที่ 2 deleted อยู่แล้ว ต้องไม่เข้า undo)')
  const batchCall = state.calls.find(c => c.method === 'valuesBatchUpdate')
  assert.equal(batchCall.requestBody.data.length, 1)
})

test('bulkDeleteContacts: เบอร์ที่ไม่มีในชีตเลย → อยู่ใน notFoundPhones ไม่นับใน deletedPhones', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0812345678', 'a', 'campA', 'pending'],
  ]
  const result = await sheetsService.bulkDeleteContacts(['0812345678', '0899999999'])
  assert.equal(result.requestedPhones, 2)
  assert.equal(result.deletedPhones, 1)
  assert.deepEqual(result.notFoundPhones, ['0899999999'])
  assert.equal(result.requestedPhones, result.deletedPhones + result.notFoundPhones.length, 'arithmetic ต้องลงตัวเสมอ: requested = deleted + notFound')
})

test('bulkDeleteContacts: เบอร์เดียวกันซ้ำใน request array เอง → dedupe ก่อนนับ requestedPhones', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0812345678', 'a', 'campA', 'pending'],
  ]
  const result = await sheetsService.bulkDeleteContacts(['0812345678', '0812345678', '0812345678'])
  assert.equal(result.requestedPhones, 1, 'array ที่ส่งมามีเบอร์ซ้ำกัน 3 ครั้ง ต้อง dedupe เหลือ 1 ก่อนนับ')
})

test('bulkRestoreContacts: undo entries ถูกต้องครบ → restore previousStatus แยกตาม row ถูกต้อง แม้เบอร์ซ้ำกันมีสถานะต่างกัน — ด้วย 1 GET + 1 batchUpdate', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0812345678', 'a', 'campA', 'deleted'],
    ['0812345678', 'b', 'campB', 'deleted'],
  ]
  const result = await sheetsService.bulkRestoreContacts([
    { rowNumber: 2, phone: '0812345678', previousStatus: 'pending' },
    { rowNumber: 3, phone: '0812345678', previousStatus: 'retry_pending' },
  ])
  assert.equal(result.restoredRows, 2)
  assert.equal(result.conflicts.length, 0)

  const getCalls = state.calls.filter(c => c.method === 'get' && c.range === 'Contacts')
  const batchCalls = state.calls.filter(c => c.method === 'valuesBatchUpdate')
  assert.equal(getCalls.length, 1)
  assert.equal(batchCalls.length, 1)
  const byRange = Object.fromEntries(batchCalls[0].requestBody.data.map(d => [d.range, d.values[0][0]]))
  assert.deepEqual(byRange, { 'Contacts!D2': 'pending', 'Contacts!D3': 'retry_pending' })
})

test('bulkRestoreContacts: rowNumber ตรงแต่ phone ไม่ตรง (แถวถูกแก้ไปแล้วระหว่าง delete กับ undo) → conflict ไม่ restore ทับ', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0899999999', 'other', 'campX', 'deleted'], // เบอร์ที่ row 2 เปลี่ยนไปแล้วจาก undo entry เดิม
  ]
  const result = await sheetsService.bulkRestoreContacts([
    { rowNumber: 2, phone: '0812345678', previousStatus: 'pending' },
  ])
  assert.equal(result.restoredRows, 0)
  assert.equal(result.conflicts.length, 1)
  assert.equal(state.calls.some(c => c.method === 'valuesBatchUpdate'), false, 'ไม่มีอะไรให้เขียนเลย ต้องไม่ยิง batchUpdate เปล่าๆ')
  const rows = await sheetsService.getContacts()
  assert.equal(rows[0].status, 'deleted', 'ต้องไม่ถูกแก้ status ของแถวที่ mismatch')
})

test('bulkRestoreContacts: current status ไม่ใช่ deleted แล้ว (ถูกแก้เป็นอย่างอื่นระหว่างนั้น) → conflict ไม่ restore ทับของใหม่', async () => {
  state.data['Contacts'] = [
    ['Phone', 'Name', 'Campaign', 'Status'],
    ['0812345678', 'a', 'campA', 'retry_pending'], // ไม่ใช่ deleted แล้ว — เช่นถูกกด PATCH สถานะใหม่ระหว่างที่ toast undo ยังค้างอยู่
  ]
  const result = await sheetsService.bulkRestoreContacts([
    { rowNumber: 2, phone: '0812345678', previousStatus: 'pending' },
  ])
  assert.equal(result.restoredRows, 0)
  assert.equal(result.conflicts.length, 1)
  const rows = await sheetsService.getContacts()
  assert.equal(rows[0].status, 'retry_pending', 'ต้องไม่ถูกเขียนทับค่าที่เปลี่ยนไปแล้วระหว่างนั้น')
})
