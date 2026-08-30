const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

process.env.MAX_CONCURRENT_CALLS = '2'
process.env.TWILIO_PHONE_NUMBER = '+66800000000'
process.env.MAX_CALL_DURATION_SECONDS = '300'

// stub twilio.js — คุมเองว่า makeOutboundCall "สำเร็จ" เมื่อไหร่ ไม่ยิง Twilio จริง
// onCallCreated ต้องเรียกก่อน resolve เสมอ (ตาม pattern จริงใน twilio.js ที่เรียกทันทีที่ได้ callSid ก่อนรอ greeting pregen)
let callCounter = 0
const createdCalls = []
const twilioPath = require.resolve('../src/services/twilio')
require.cache[twilioPath] = {
  id: twilioPath, filename: twilioPath, loaded: true,
  exports: {
    makeOutboundCall: async (contact, campaign, onCallCreated) => {
      const sid = 'CA' + (++callCounter)
      createdCalls.push({ sid, phone: contact.phone, twilioNumber: campaign.twilio_number || process.env.TWILIO_PHONE_NUMBER })
      onCallCreated({ sid })
      return { sid }
    },
    // ต้องมีจริงเหมือน twilio.js ตัวจริง — callQueue.js เรียกใช้ตัวนี้เป๊ะๆ เพื่อคำนวณ key ของคิว (ดู effectiveNumber ใน callQueue.js)
    effectiveTwilioNumber: (campaign) => campaign?.twilio_number || process.env.TWILIO_PHONE_NUMBER,
  },
}

const { callQueue } = require('../src/utils/callQueue')

const wait = (ms) => new Promise(r => setTimeout(r, ms))

beforeEach(() => {
  createdCalls.length = 0
})

// เบอร์ในแต่ละเทสตั้งใจใช้เลขไม่ซ้ำกันข้ามเทส (ต่างจากเทสอื่นในไฟล์นี้) — callQueue เป็น module-level singleton
// ที่ state (running/queue ต่อเบอร์) ไม่ถูกรีเซ็ตระหว่างเทส ถ้าใช้เบอร์ซ้ำกันแล้วมีสายค้าง "กำลังคุยอยู่" จากเทสก่อนหน้า
// (ที่ตั้งใจไม่ release ให้ครบเพื่อทดสอบพฤติกรรมคิวเต็ม) จะไปกระทบผลของเทสถัดไปแบบเงียบๆ

test('คิวของแต่ละเบอร์เป็นอิสระจากกัน — เบอร์นึงเต็มโควตาไม่กระทบอีกเบอร์นึง', async () => {
  const campaignA = { id: 'campA', twilio_number: '+66811110001' }
  const campaignB = { id: 'campB', twilio_number: '+66811110002' }

  // ยิง 3 สายให้เบอร์ A (โควตาสูงสุด 2) — สายที่ 3 ต้องรอคิว
  callQueue.add({ contact: { phone: '+66890000001' }, campaign: campaignA })
  callQueue.add({ contact: { phone: '+66890000002' }, campaign: campaignA })
  callQueue.add({ contact: { phone: '+66890000003' }, campaign: campaignA })
  await wait(20)

  assert.equal(callQueue.runningCount('+66811110001'), 2, 'เบอร์ A ต้องรันเต็มโควตา 2 สาย')
  assert.equal(callQueue.size('+66811110001'), 1, 'เบอร์ A ต้องมี 1 สายเหลือรอคิว')

  // ยิงสายให้เบอร์ B พร้อมกัน — ต้องเริ่มได้ทันทีแม้เบอร์ A จะเต็มโควตาอยู่
  callQueue.add({ contact: { phone: '+66890000004' }, campaign: campaignB })
  await wait(20)
  assert.equal(callQueue.runningCount('+66811110002'), 1, 'เบอร์ B ต้องเริ่มโทรได้ทันที ไม่ต้องรอเบอร์ A')
  assert.equal(callQueue.size('+66811110002'), 0)

  // release สายแรกของเบอร์ A -> สายที่ 3 ที่รอคิวอยู่ต้องเริ่มโทรทันที
  callQueue.release(createdCalls[0].sid)
  await wait(20)
  assert.equal(callQueue.runningCount('+66811110001'), 2, 'หลัง release สายที่ 3 ของเบอร์ A ต้องเริ่มโทรจนครบโควตาอีกครั้ง')
  assert.equal(callQueue.size('+66811110001'), 0)

  // ปิดท้าย release ให้ครบทุกสายที่ยังค้างอยู่ กันรบกวนเทสถัดไป (แม้ใช้เบอร์คนละชุดกันแล้วก็ตาม เป็นสุขลักษณะทดสอบที่ดี)
  createdCalls.filter(c => c.twilioNumber.startsWith('+6681111000')).forEach(c => callQueue.release(c.sid))
})

test('clearByNumber ล้างคิวเฉพาะเบอร์ที่ระบุ ไม่กระทบคิวของเบอร์อื่น', async () => {
  const campaignA = { id: 'campA', twilio_number: '+66811110003' }
  const campaignB = { id: 'campB', twilio_number: '+66811110004' }
  for (let i = 0; i < 3; i++) callQueue.add({ contact: { phone: '+6689000000' + i }, campaign: campaignA })
  for (let i = 0; i < 3; i++) callQueue.add({ contact: { phone: '+6689100000' + i }, campaign: campaignB })
  await wait(20)

  const removed = callQueue.clearByNumber('+66811110003')
  assert.equal(removed, 1, 'เบอร์ A มี 1 สายรอคิวอยู่ (โควตาเต็ม 2 ไปแล้ว) ต้องถูกล้าง')
  assert.equal(callQueue.size('+66811110003'), 0)
  assert.equal(callQueue.size('+66811110004'), 1, 'คิวของเบอร์ B ต้องไม่ถูกแตะเลย')

  createdCalls.forEach(c => callQueue.release(c.sid))
})

test('statusByNumber แสดงเฉพาะเบอร์ที่มีกิจกรรมจริง (คิว หรือ กำลังคุยอยู่) เท่านั้น', async () => {
  const campaignA = { id: 'campA', twilio_number: '+66811110005' }
  callQueue.add({ contact: { phone: '+66890000001' }, campaign: campaignA })
  await wait(20)
  const status = callQueue.statusByNumber()
  assert.equal(status['+66811110005'].running, 1)
  assert.equal(status['+66811110005'].queueSize, 0)

  createdCalls.forEach(c => callQueue.release(c.sid))
})

test('campaign ที่ไม่ได้ตั้ง twilio_number ไว้เอง ใช้คิวร่วมกันของเบอร์เริ่มต้นระบบ', async () => {
  const campaignNoNumber = { id: 'campC' } // ไม่มี twilio_number
  callQueue.add({ contact: { phone: '+66890000001' }, campaign: campaignNoNumber })
  await wait(20)
  assert.equal(callQueue.runningCount(process.env.TWILIO_PHONE_NUMBER), 1)

  createdCalls.forEach(c => callQueue.release(c.sid))
})

test('Track 5 — add() ซ้ำด้วยเบอร์เดียวกันก่อนสายแรกจบ ต้องข้าม ไม่โทรซ้ำสอง', async () => {
  const campaignA = { id: 'campA', twilio_number: '+66811110008' }
  callQueue.add({ contact: { phone: '+66892220001' }, campaign: campaignA })
  // เรียกซ้ำทันที (จำลอง double-click / client retry) ก่อนสายแรกจะ release เลย
  callQueue.add({ contact: { phone: '+66892220001' }, campaign: campaignA })
  callQueue.add({ contact: { phone: '+66892220001' }, campaign: campaignA })
  await wait(20)

  assert.equal(createdCalls.length, 1, 'เบอร์เดียวกันต้องถูกโทรแค่ครั้งเดียวแม้เรียก add() ซ้ำสามครั้ง')
  assert.equal(callQueue.runningCount('+66811110008'), 1)
  assert.equal(callQueue.size('+66811110008'), 0, 'สอง add() ที่ซ้ำต้องไม่ถูกเก็บเข้าคิวเลย ไม่ใช่แค่ไม่โทร')

  callQueue.release(createdCalls[0].sid)
  await wait(20)

  // หลัง release แล้ว เบอร์เดิมต้อง add() ใหม่ได้อีกครั้งตามปกติ (ไม่ใช่ถูกแบนถาวร)
  callQueue.add({ contact: { phone: '+66892220001' }, campaign: campaignA })
  await wait(20)
  assert.equal(createdCalls.length, 2, 'หลัง release แล้วเบอร์เดิมต้องโทรซ้ำได้ปกติ')
  callQueue.release(createdCalls[1].sid)
})

test('Track 5 — clearByNumber ต้องปล่อยเบอร์ที่ยังไม่ได้โทร ให้ add() ซ้ำได้อีกครั้ง', async () => {
  const campaignA = { id: 'campA', twilio_number: '+66811110009' }
  // เติมให้เต็มโควตา 2 ก่อน แล้วเบอร์ที่ 3 จะติดอยู่ในคิว (ไม่ถูกโทร)
  callQueue.add({ contact: { phone: '+66893330001' }, campaign: campaignA })
  callQueue.add({ contact: { phone: '+66893330002' }, campaign: campaignA })
  callQueue.add({ contact: { phone: '+66893330003' }, campaign: campaignA })
  await wait(20)
  assert.equal(callQueue.size('+66811110009'), 1)

  callQueue.clearByNumber('+66811110009')
  // เบอร์ที่ 3 ถูกล้างออกจากคิวไปแล้ว ต้อง add() ใหม่ได้ทันที ไม่ถูกมองว่ายัง "in flight"
  callQueue.add({ contact: { phone: '+66893330003' }, campaign: campaignA })
  await wait(20)
  assert.equal(callQueue.size('+66811110009'), 1, 'add() ใหม่หลัง clearByNumber ต้องเข้าคิวได้ปกติ ไม่ถูกกันซ้ำผิดๆ')

  callQueue.clearByNumber('+66811110009')
  createdCalls.filter(c => c.twilioNumber === '+66811110009').forEach(c => callQueue.release(c.sid))
})

test('clear(campaignId) แบบเดิม: ไม่ระบุ campaignId ต้องล้างทุกคิวทุกเบอร์ (คงพฤติกรรมเดิมไว้)', async () => {
  const campaignA = { id: 'campA', twilio_number: '+66811110006' }
  const campaignB = { id: 'campB', twilio_number: '+66811110007' }
  for (let i = 0; i < 3; i++) callQueue.add({ contact: { phone: '+6689000000' + i }, campaign: campaignA })
  for (let i = 0; i < 3; i++) callQueue.add({ contact: { phone: '+6689100000' + i }, campaign: campaignB })
  await wait(20)

  callQueue.clear()
  assert.equal(callQueue.size('+66811110006'), 0)
  assert.equal(callQueue.size('+66811110007'), 0)

  createdCalls.forEach(c => callQueue.release(c.sid))
})
