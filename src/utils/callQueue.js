const twilioService = require('../services/twilio')

const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT_CALLS) || 5
const DEFAULT_NUMBER = process.env.TWILIO_PHONE_NUMBER || '__default__'

// คิวแยกอิสระต่อเบอร์ Twilio — เดิมทั้งระบบใช้คิวเดียวรวมกัน คนนึงยิงเบอร์ตัวเองเต็มโควตา (MAX_CONCURRENT_CALLS)
// จะไปบล็อกคิวของอีกเบอร์นึงด้วยทั้งที่ไม่เกี่ยวกัน ตอนนี้แต่ละเบอร์มีคิวและโควตาพร้อมกันของตัวเอง เป็นอิสระจากเบอร์อื่นเต็มที่
// campaign ที่ไม่ได้ตั้ง twilio_number ไว้เอง ถือว่าอยู่ในคิวของเบอร์เริ่มต้นของระบบร่วมกัน
const numberStates = new Map() // twilioNumber -> { queue: [], running: 0 }

// ใช้ effectiveTwilioNumber ตัวเดียวกับที่ twilio.js ใช้ตอนโทรออกจริงเป๊ะๆ (ไม่คำนวณแยกกันเองอีกชุด) กัน key ของคิวกับเบอร์ที่โทรจริงเพี้ยนไม่ตรงกัน
// DEFAULT_NUMBER ('__default__') เป็นแค่ safety net เผื่อ TWILIO_PHONE_NUMBER ไม่ได้ตั้งไว้เลยจริงๆ (ซึ่งไม่ควรเกิดในระบบที่ตั้งค่าถูกต้อง)
function effectiveNumber(campaign) {
  return twilioService.effectiveTwilioNumber(campaign) || DEFAULT_NUMBER
}

function getState(number) {
  if (!numberStates.has(number)) numberStates.set(number, { queue: [], running: 0 })
  return numberStates.get(number)
}

// callSid ที่กำลังถือ slot อยู่จริง (สายยังไม่จบ) — คืน slot ต่อเมื่อสายจบจริงเท่านั้น (ผ่าน release())
// ไม่ใช่ตอนแค่สั่งโทรออกสำเร็จ เพราะ Twilio รับคำสั่งไวมาก (~1-3s) แต่บทสนทนาจริงอาจกินเวลานาทีนึง
// เดิมโค้ดคืน slot ทันทีที่สั่งโทรเสร็จ ทำให้ MAX_CONCURRENT_CALLS ไม่ได้จำกัดสายคุยสดจริงเลย
// เก็บเบอร์ที่ผูกกับแต่ละสายไว้ด้วย — ตอน release ต้องรู้ว่าจะไปลด running ของคิวเบอร์ไหน
const activeCallNumbers = new Map() // callSid -> twilioNumber
const releaseTimers = new Map() // callSid -> timer กันเคส webhook/status ไม่มาถึง (network blip) แล้ว slot ค้างตลอดไป

// สายจริงยาวสุด MAX_CALL_DURATION_SECONDS (จาก audioStream.js) + เวลาสำรองให้ webhook ของ Twilio ส่งมาถึง
const RELEASE_SAFETY_TIMEOUT_MS = ((parseInt(process.env.MAX_CALL_DURATION_SECONDS) || 300) + 90) * 1000

function processQueue(number) {
  const state = getState(number)
  while (state.queue.length > 0 && state.running < MAX_CONCURRENT) {
    const job = state.queue.shift()
    state.running++
    runCall(job, number)
  }
}

function trackActive(callSid, number) {
  activeCallNumbers.set(callSid, number)
  const timer = setTimeout(() => {
    console.warn(`[CallQueue] ไม่ได้รับ webhook สถานะของสาย ${callSid} ภายในเวลาที่ควร — ปล่อย slot คืนเอง`)
    release(callSid)
  }, RELEASE_SAFETY_TIMEOUT_MS)
  timer.unref?.() // ไม่ให้ timer นี้ค้าง process ไว้เวลา shutdown
  releaseTimers.set(callSid, timer)
}

async function runCall({ contact, campaign }, number) {
  try {
    const call = await twilioService.makeOutboundCall(contact, campaign, (createdCall) => {
      // เรียกทันทีที่ได้ callSid จาก Twilio (ก่อนรอ pre-gen เสียงทักทายเสร็จ)
      // กัน race: ถ้าสายพังทันที webhook สถานะอาจมาถึงเร็วกว่า makeOutboundCall จะ return กลับมาที่นี่
      trackActive(createdCall.sid, number)
    })
    if (!call) {
      // ไม่ได้โทรออกจริง (เช่นเบอร์ติด Do-not-call list) — ไม่มี callSid ให้ release ทีหลัง คืน slot ทันที
      getState(number).running--
      processQueue(number)
    }
  } catch (err) {
    console.error(`Call failed for ${contact.phone}:`, err.message)
    getState(number).running--
    processQueue(number)
  }
}

// เรียกจาก postCallHandler ตอนสายจบจริง (ผ่าน webhook/status) หรือจาก safety timeout — คืน slot ให้คิวของเบอร์นั้นเดินต่อ
function release(callSid) {
  if (!callSid || !activeCallNumbers.has(callSid)) return
  const number = activeCallNumbers.get(callSid)
  activeCallNumbers.delete(callSid)
  clearTimeout(releaseTimers.get(callSid))
  releaseTimers.delete(callSid)
  getState(number).running--
  processQueue(number)
}

const callQueue = {
  add(job) {
    const number = effectiveNumber(job.campaign)
    getState(number).queue.push(job)
    processQueue(number)
  },
  // เดิม: ไม่ระบุ campaignId = เคลียร์ทั้งคิวทุกเบอร์ (ใช้กับปุ่มหยุด Campaign) — พฤติกรรมเดิมคงไว้เผื่อยังมีที่เรียกแบบนี้อยู่
  clear(campaignId) {
    let removed = 0
    for (const state of numberStates.values()) {
      const before = state.queue.length
      state.queue = state.queue.filter(job => campaignId && job.campaign.id !== campaignId)
      removed += before - state.queue.length
    }
    console.log(`Cleared ${removed} jobs from queue`)
  },
  // เคลียร์เฉพาะคิวของเบอร์ที่ระบุเบอร์เดียว — ไม่กระทบคิว/สายที่กำลังคุยอยู่ของเบอร์อื่นเลย
  clearByNumber(number) {
    const state = getState(number)
    const removed = state.queue.length
    state.queue = []
    console.log(`Cleared ${removed} jobs from queue for number=${number}`)
    return removed
  },
  release,
  size(number) {
    if (number) return getState(number).queue.length
    let total = 0
    for (const state of numberStates.values()) total += state.queue.length
    return total
  },
  runningCount(number) {
    if (number) return getState(number).running
    let total = 0
    for (const state of numberStates.values()) total += state.running
    return total
  },
  // สรุปคิว+สายที่กำลังคุยอยู่ แยกตามเบอร์ — ใช้แสดงในหน้าแอดมินตอนเลือกว่าจะหยุดคิวของเบอร์ไหน
  statusByNumber() {
    const result = {}
    for (const [number, state] of numberStates) {
      if (state.queue.length || state.running) result[number] = { queueSize: state.queue.length, running: state.running }
    }
    return result
  },
}

module.exports = { callQueue }
