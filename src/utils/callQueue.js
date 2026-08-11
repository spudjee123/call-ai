const twilioService = require('../services/twilio')
const callSessions = require('./callSessions')

const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT_CALLS) || 5

const queue = []
let running = 0
// callSid ที่กำลังถือ slot อยู่จริง (สายยังไม่จบ) — คืน slot ต่อเมื่อสายจบจริงเท่านั้น (ผ่าน release())
// ไม่ใช่ตอนแค่สั่งโทรออกสำเร็จ เพราะ Twilio รับคำสั่งไวมาก (~1-3s) แต่บทสนทนาจริงอาจกินเวลานาทีนึง
// เดิมโค้ดคืน slot ทันทีที่สั่งโทรเสร็จ ทำให้ MAX_CONCURRENT_CALLS ไม่ได้จำกัดสายคุยสดจริงเลย
const activeCallSids = new Set()
const releaseTimers = new Map() // callSid -> timer กันเคส webhook/status ไม่มาถึง (network blip) แล้ว slot ค้างตลอดไป

// สายจริงยาวสุด MAX_CALL_DURATION_SECONDS (จาก audioStream.js) + เวลาสำรองให้ webhook ของ Twilio ส่งมาถึง
const RELEASE_SAFETY_TIMEOUT_MS = ((parseInt(process.env.MAX_CALL_DURATION_SECONDS) || 300) + 90) * 1000

function processQueue() {
  while (queue.length > 0 && running < MAX_CONCURRENT) {
    const job = queue.shift()
    running++
    runCall(job)
  }
}

function trackActive(callSid) {
  activeCallSids.add(callSid)
  const timer = setTimeout(() => {
    console.warn(`[CallQueue] ไม่ได้รับ webhook สถานะของสาย ${callSid} ภายในเวลาที่ควร — ปล่อย slot คืนเอง`)
    release(callSid)
  }, RELEASE_SAFETY_TIMEOUT_MS)
  timer.unref?.() // ไม่ให้ timer นี้ค้าง process ไว้เวลา shutdown
  releaseTimers.set(callSid, timer)
}

async function runCall({ contact, campaign }) {
  try {
    const call = await twilioService.makeOutboundCall(contact, campaign, (createdCall) => {
      // เรียกทันทีที่ได้ callSid จาก Twilio (ก่อนรอ pre-gen เสียงทักทายเสร็จ)
      // กัน race: ถ้าสายพังทันที webhook สถานะอาจมาถึงเร็วกว่า makeOutboundCall จะ return กลับมาที่นี่
      trackActive(createdCall.sid)
    })
    if (!call) {
      // ไม่ได้โทรออกจริง (เช่นเบอร์ติด Do-not-call list) — ไม่มี callSid ให้ release ทีหลัง คืน slot ทันที
      running--
      processQueue()
    }
  } catch (err) {
    console.error(`Call failed for ${contact.phone}:`, err.message)
    running--
    processQueue()
  }
}

// เรียกจาก postCallHandler ตอนสายจบจริง (ผ่าน webhook/status) หรือจาก safety timeout — คืน slot ให้คิวเดินต่อ
function release(callSid) {
  if (!callSid || !activeCallSids.has(callSid)) return
  activeCallSids.delete(callSid)
  clearTimeout(releaseTimers.get(callSid))
  releaseTimers.delete(callSid)
  running--
  processQueue()
}

const callQueue = {
  add(job) {
    queue.push(job)
    processQueue()
  },
  clear(campaignId) {
    const before = queue.length
    for (let i = queue.length - 1; i >= 0; i--) {
      if (!campaignId || queue[i].campaign.id === campaignId) {
        queue.splice(i, 1)
      }
    }
    console.log(`Cleared ${before - queue.length} jobs from queue`)
  },
  release,
  size() { return queue.length },
  runningCount() { return running }
}

module.exports = { callQueue }
