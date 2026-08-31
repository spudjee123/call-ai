// Twilio Webhook Signature Verification (Hardening Batch, 2026-08-30) — audit finding: /webhook/* never
// checked X-Twilio-Signature. Default mode is LOG_ONLY (never blocks — a BASE_URL/proxy mismatch must
// never drop a real customer call); ENFORCE is opt-in only, tested separately here.
const { test, beforeEach, afterEach } = require('node:test')
const assert = require('node:assert/strict')
const twilio = require('twilio')

const ENV_KEYS = ['TWILIO_AUTH_TOKEN', 'BASE_URL', 'WEBHOOK_SIGNATURE_MODE']
let savedEnv

beforeEach(() => {
  savedEnv = Object.fromEntries(ENV_KEYS.map(k => [k, process.env[k]]))
  process.env.TWILIO_AUTH_TOKEN = 'test_auth_token_12345'
  process.env.BASE_URL = 'https://call-ai.example.com'
  delete process.env.WEBHOOK_SIGNATURE_MODE
  delete require.cache[require.resolve('../src/utils/twilioSignature')]
})

afterEach(() => {
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k]
    else process.env[k] = savedEnv[k]
  }
})

function fakeReq({ url = '/webhook/status', body = { CallSid: 'CA123', CallStatus: 'completed' }, signature } = {}) {
  const fullUrl = `${process.env.BASE_URL}${url}`
  const sig = signature !== undefined ? signature : twilio.getExpectedTwilioSignature(process.env.TWILIO_AUTH_TOKEN, fullUrl, body)
  return { url, body, headers: { 'x-twilio-signature': sig } }
}

test('isValidTwilioSignature: ยอมรับ signature ที่ถูกต้องจริง', () => {
  const { isValidTwilioSignature } = require('../src/utils/twilioSignature')
  assert.equal(isValidTwilioSignature(fakeReq()), true)
})

test('isValidTwilioSignature: ปฏิเสธ signature ที่ผิด', () => {
  const { isValidTwilioSignature } = require('../src/utils/twilioSignature')
  assert.equal(isValidTwilioSignature(fakeReq({ signature: 'not-a-real-signature' })), false)
})

test('isValidTwilioSignature: ไม่มี header เลย ต้องถือว่า invalid ไม่ throw', () => {
  const { isValidTwilioSignature } = require('../src/utils/twilioSignature')
  assert.equal(isValidTwilioSignature(fakeReq({ signature: '' })), false)
})

test('isValidTwilioSignature: body ถูกแก้ระหว่างทาง (signature ไม่ตรงกับ params จริง) ต้อง invalid', () => {
  const { isValidTwilioSignature } = require('../src/utils/twilioSignature')
  const req = fakeReq({ body: { CallSid: 'CA123', CallStatus: 'completed' } })
  req.body.CallStatus = 'failed' // แก้ค่าหลัง sign แล้ว
  assert.equal(isValidTwilioSignature(req), false)
})

test('getMode: default (ไม่ตั้ง env) ต้องเป็น LOG_ONLY เสมอ — ต้องไม่ enforce เองโดยไม่ได้ตั้งใจ', () => {
  const { getMode } = require('../src/utils/twilioSignature')
  assert.equal(getMode(), 'LOG_ONLY')
})

test('getMode: ค่าอื่นที่ไม่ใช่ ENFORCE (เช่น พิมพ์ผิด) ต้อง fallback เป็น LOG_ONLY ปลอดภัยไว้ก่อน', () => {
  process.env.WEBHOOK_SIGNATURE_MODE = 'ENFORCEE'
  const { getMode } = require('../src/utils/twilioSignature')
  assert.equal(getMode(), 'LOG_ONLY')
})

test('getMode: ตั้ง ENFORCE ตรงตัว (case-insensitive) ต้องได้ ENFORCE จริง', () => {
  process.env.WEBHOOK_SIGNATURE_MODE = 'enforce'
  const { getMode } = require('../src/utils/twilioSignature')
  assert.equal(getMode(), 'ENFORCE')
})

test('twilioSignatureGuard: LOG_ONLY + signature ผิด → ยัง done() ผ่านไปปกติ ไม่ reply.code(403)', () => {
  const { twilioSignatureGuard } = require('../src/utils/twilioSignature')
  const req = fakeReq({ signature: 'bad' })
  let doneCalled = false
  let replyCode = null
  const reply = { code: (c) => { replyCode = c; return reply }, send: () => {} }
  twilioSignatureGuard(req, reply, () => { doneCalled = true })
  assert.equal(doneCalled, true)
  assert.equal(replyCode, null)
})

test('twilioSignatureGuard: ENFORCE + signature ผิด → reply 403 และไม่เรียก done()', () => {
  process.env.WEBHOOK_SIGNATURE_MODE = 'ENFORCE'
  const { twilioSignatureGuard } = require('../src/utils/twilioSignature')
  const req = fakeReq({ signature: 'bad' })
  let doneCalled = false
  let replyCode = null
  const reply = { code: (c) => { replyCode = c; return reply }, send: () => {} }
  twilioSignatureGuard(req, reply, () => { doneCalled = true })
  assert.equal(doneCalled, false)
  assert.equal(replyCode, 403)
})

test('twilioSignatureGuard: ENFORCE + signature ถูกต้อง → done() ผ่านปกติ ไม่ reply', () => {
  process.env.WEBHOOK_SIGNATURE_MODE = 'ENFORCE'
  const { twilioSignatureGuard } = require('../src/utils/twilioSignature')
  const req = fakeReq()
  let doneCalled = false
  let replyCode = null
  const reply = { code: (c) => { replyCode = c; return reply }, send: () => {} }
  twilioSignatureGuard(req, reply, () => { doneCalled = true })
  assert.equal(doneCalled, true)
  assert.equal(replyCode, null)
})

test('buildRequestUrl: ต่อ BASE_URL (ตัด trailing slash) กับ req.url ตรงๆ ไม่เติม query แปลกปลอม', () => {
  process.env.BASE_URL = 'https://call-ai.example.com/'
  const { buildRequestUrl } = require('../src/utils/twilioSignature')
  assert.equal(buildRequestUrl({ url: '/webhook/status' }), 'https://call-ai.example.com/webhook/status')
})
