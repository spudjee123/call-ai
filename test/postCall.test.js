const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
require('dotenv').config()

// stub googleSheets และ thaiBulkSms ก่อน require postCall.js — กันยิง Sheets/SMS API จริง
// และคุมผลลัพธ์การ resolve template/sender เองได้ (pattern เดียวกับ test/googleSheets.test.js)
const state = { templates: {}, senders: {}, smsCalls: [] }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      getSmsTemplateById: async (id) => state.templates[id] || null,
      getSenderNameById: async (id) => state.senders[id] || null,
      updateCallResultSmsStatus: async () => true,
    },
  },
}

const thaiBulkSmsPath = require.resolve('../src/services/thaiBulkSms')
require.cache[thaiBulkSmsPath] = {
  id: thaiBulkSmsPath, filename: thaiBulkSmsPath, loaded: true,
  exports: {
    sendSms: async (phone, body, senderName) => {
      state.smsCalls.push({ phone, body, senderName })
      return { phone_number_list: [{ message_id: 'MSG1' }], bad_phone_number_list: [] }
    },
  },
}

const { handleSmsFollowup } = require('../src/services/postCall')

beforeEach(() => { state.templates = {}; state.senders = {}; state.smsCalls = [] })

test('resolve template ตาม campaign.sms_<outcome> แล้วแทน {name} ในเนื้อความก่อนส่งจริง', async () => {
  state.templates['tpl1'] = { template_text: 'สวัสดีคุณ {name} สนใจติดต่อกลับด้วยนะคะ' }
  const session = { campaign: { sms_interested: 'tpl1' }, phone: '0812345678', name: 'สมชาย', callSid: 'CA1' }
  await handleSmsFollowup(session, 'interested')
  assert.equal(state.smsCalls.length, 1)
  assert.equal(state.smsCalls[0].body, 'สวัสดีคุณ สมชาย สนใจติดต่อกลับด้วยนะคะ')
  assert.equal(state.smsCalls[0].phone, '0812345678')
})

test('ไม่ส่ง SMS ถ้า campaign ไม่ได้ตั้ง template สำหรับ outcome นี้ไว้ (ว่าง = ไม่ส่ง)', async () => {
  const session = { campaign: {}, phone: '0812345678', name: 'สมชาย', callSid: 'CA1' }
  await handleSmsFollowup(session, 'interested')
  assert.equal(state.smsCalls.length, 0)
})

test('ไม่ส่ง SMS และไม่ throw ถ้า template id ที่ campaign อ้างอิงถูกลบไปแล้ว', async () => {
  const session = { campaign: { sms_interested: 'tpl-deleted' }, phone: '0812345678', name: 'สมชาย', callSid: 'CA1' }
  await handleSmsFollowup(session, 'interested')
  assert.equal(state.smsCalls.length, 0)
})

test('resolve sender name จาก template.sender เป็นชื่อจริงก่อนส่ง', async () => {
  state.templates['tpl1'] = { template_text: 'ข้อความ', sender: 'sender1' }
  state.senders['sender1'] = { name: 'MyShop' }
  const session = { campaign: { sms_interested: 'tpl1' }, phone: '0812345678', name: 'สมชาย', callSid: 'CA1' }
  await handleSmsFollowup(session, 'interested')
  assert.equal(state.smsCalls[0].senderName, 'MyShop')
})

test('sender ที่ template อ้างอิงถูกลบไปแล้ว → ส่ง senderName เป็น undefined ให้ thaiBulkSms ไป fallback ค่า default เอง (ไม่ throw)', async () => {
  state.templates['tpl1'] = { template_text: 'ข้อความ', sender: 'sender-deleted' }
  const session = { campaign: { sms_interested: 'tpl1' }, phone: '0812345678', name: 'สมชาย', callSid: 'CA1' }
  await handleSmsFollowup(session, 'interested')
  assert.equal(state.smsCalls.length, 1)
  assert.equal(state.smsCalls[0].senderName, undefined)
})
