const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const Fastify = require('fastify')

// stub googleSheets + twilio.js ก่อน require route — กันยิง Sheets/Twilio API จริงและคุมข้อมูลทดสอบเองได้
const state = { numbers: {}, campaigns: {}, accountNumbers: [] }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      getTwilioNumbers: async () => Object.values(state.numbers),
      getTwilioNumberByPhone: async (phone) => state.numbers[phone] || null,
      addTwilioNumber: async (fields) => {
        if (state.sheetMissing) throw new Error('Unable to parse range: Twilio Numbers: Unable to parse range: Twilio Numbers')
        if (state.headerMismatch) return // จำลอง header ในชีตพิมพ์ไม่ตรง — เขียนแถวว่างเปล่าแบบไม่ throw ไม่มีอะไรถูกเก็บจริง
        state.numbers[fields.phone_number] = { ...fields }
        return true
      },
      updateTwilioNumber: async (phone, updates) => {
        if (state.sheetMissing) throw new Error('Unable to parse range: Twilio Numbers: Unable to parse range: Twilio Numbers')
        if (!state.numbers[phone]) return false
        state.numbers[phone] = { ...state.numbers[phone], ...updates }
        return true
      },
      deleteTwilioNumber: async (phone) => {
        if (state.sheetMissing) throw new Error('Unable to parse range: Twilio Numbers: Unable to parse range: Twilio Numbers')
        if (!state.numbers[phone]) return false
        delete state.numbers[phone]
        return true
      },
      getCampaign: async (id) => state.campaigns[id] || null,
    },
  },
}

const twilioServicePath = require.resolve('../src/services/twilio')
require.cache[twilioServicePath] = {
  id: twilioServicePath, filename: twilioServicePath, loaded: true,
  exports: {
    listPhoneNumbers: async () => state.accountNumbers,
  },
}

const twilioNumberRoutes = require('../src/routes/twilioNumbers')

async function buildApp() {
  const fastify = Fastify()
  fastify.register(twilioNumberRoutes)
  await fastify.ready()
  return fastify
}

beforeEach(() => {
  state.numbers = {}
  state.campaigns = { camp1: { id: 'camp1' } }
  state.accountNumbers = [{ phoneNumber: '+66811111111', friendlyName: '+66811111111' }]
  state.sheetMissing = false
  state.headerMismatch = false
})

test('POST /api/twilio-numbers: ยังไม่ได้สร้างแท็บชีต "Twilio Numbers" → ต้องได้ข้อความบอกสาเหตุชัดเจน ไม่ใช่ "Bad Request" เฉยๆ (บั๊กจริงที่เจอในโปรดักชัน)', async () => {
  state.sheetMissing = true
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/twilio-numbers', payload: { phone_number: '+66811111111' } })
  assert.equal(res.statusCode, 500)
  assert.match(res.json().error, /สร้างแท็บชีต.*Twilio Numbers/)
})

test('POST /api/twilio-numbers: หัวคอลัมน์ในชีตพิมพ์ไม่ตรง "Phone Number" เป๊ะๆ → เขียนแถวว่างเปล่าแบบไม่ throw ต้องตรวจจับได้ ไม่ใช่ตอบสำเร็จทั้งที่ข้อมูลหายไป (บั๊กจริงที่เจอในโปรดักชัน)', async () => {
  state.headerMismatch = true
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/twilio-numbers', payload: { phone_number: '+66811111111' } })
  assert.equal(res.statusCode, 500)
  assert.match(res.json().error, /หัวคอลัมน์/)
})

test('POST /api/twilio-numbers: ลงทะเบียนเบอร์ที่มีจริงในบัญชี Twilio สำเร็จ', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/twilio-numbers', payload: { phone_number: '+66811111111', label: 'สายหลัก', campaign_id: 'camp1' } })
  assert.equal(res.statusCode, 200)
  assert.equal(state.numbers['+66811111111'].label, 'สายหลัก')
})

test('POST /api/twilio-numbers: ปฏิเสธเบอร์ที่ไม่มีในบัญชี Twilio จริง (กัน typo/เบอร์ที่ไม่ได้เป็นเจ้าของ)', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/twilio-numbers', payload: { phone_number: '+66899999999' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.numbers['+66899999999'], undefined)
})

test('POST /api/twilio-numbers: ปฏิเสธถ้าเบอร์นี้ลงทะเบียนไว้แล้ว (กันซ้ำ)', async () => {
  state.numbers['+66811111111'] = { phone_number: '+66811111111' }
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/twilio-numbers', payload: { phone_number: '+66811111111' } })
  assert.equal(res.statusCode, 409)
})

test('POST /api/twilio-numbers: ปฏิเสธถ้า campaign_id ที่ระบุไม่มีอยู่จริง', async () => {
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/twilio-numbers', payload: { phone_number: '+66811111111', campaign_id: 'ghost' } })
  assert.equal(res.statusCode, 400)
  assert.equal(state.numbers['+66811111111'], undefined)
})

test('PATCH /api/twilio-numbers/:phone: แก้ label/campaign ได้ และคืน 404 ถ้าไม่เคยลงทะเบียนไว้', async () => {
  state.numbers['+66811111111'] = { phone_number: '+66811111111', label: 'เก่า' }
  const app = await buildApp()
  const ok = await app.inject({ method: 'PATCH', url: '/api/twilio-numbers/' + encodeURIComponent('+66811111111'), payload: { label: 'ใหม่' } })
  assert.equal(ok.statusCode, 200)
  assert.equal(state.numbers['+66811111111'].label, 'ใหม่')

  const missing = await app.inject({ method: 'PATCH', url: '/api/twilio-numbers/' + encodeURIComponent('+66899999999'), payload: { label: 'x' } })
  assert.equal(missing.statusCode, 404)
})

test('PATCH /api/twilio-numbers/:phone: ปฏิเสธถ้า campaign_id ใหม่ที่ระบุไม่มีอยู่จริง', async () => {
  state.numbers['+66811111111'] = { phone_number: '+66811111111' }
  const app = await buildApp()
  const res = await app.inject({ method: 'PATCH', url: '/api/twilio-numbers/' + encodeURIComponent('+66811111111'), payload: { campaign_id: 'ghost' } })
  assert.equal(res.statusCode, 400)
})

test('DELETE /api/twilio-numbers/:phone: เอาออกจากระบบเราเท่านั้น (ไม่แตะบัญชี Twilio จริง) และคืน 404 ถ้าไม่เคยลงทะเบียนไว้', async () => {
  state.numbers['+66811111111'] = { phone_number: '+66811111111' }
  const app = await buildApp()
  const ok = await app.inject({ method: 'DELETE', url: '/api/twilio-numbers/' + encodeURIComponent('+66811111111') })
  assert.equal(ok.statusCode, 200)
  assert.equal(state.numbers['+66811111111'], undefined)
  assert.equal(state.accountNumbers.length, 1, 'ไม่ควรไปแตะรายชื่อเบอร์ในบัญชี Twilio จริงเลย')

  const missing = await app.inject({ method: 'DELETE', url: '/api/twilio-numbers/' + encodeURIComponent('+66899999999') })
  assert.equal(missing.statusCode, 404)
})
