// สแกนเบอร์ที่ถูกตั้งให้โทรซ้ำ (no_answer/busy) แล้วดันกลับเข้าคิวเมื่อถึงเวลา
const { sheetsService } = require('../services/googleSheets')
const { callQueue } = require('./callQueue')

const SCAN_INTERVAL_MS = 15 * 60 * 1000 // ทุก 15 นาที

async function scanRetries() {
  try {
    const contacts = await sheetsService.getContacts({ status: 'retry_pending' })
    const now = Date.now()
    const due = contacts.filter(c => c.next_attempt_at && new Date(c.next_attempt_at).getTime() <= now)
    if (!due.length) return

    console.log(`[Retry] ${due.length} เบอร์ถึงเวลาโทรซ้ำ`)
    for (const contact of due) {
      const campaign = await sheetsService.getCampaign(contact.campaign)
      if (!campaign) continue
      // เปลี่ยนกลับเป็น pending ก่อนกันสแกนรอบถัดไปดันซ้ำระหว่างรอคิวโทร
      await sheetsService.updateContact(contact.phone, { status: 'pending' })
      callQueue.add({ contact, campaign })
    }
  } catch (err) {
    console.error('[Retry] Scan error:', err.message)
  }
}

function startRetryScheduler() {
  // สแกนทันทีตอน start ด้วย — ไม่งั้นทุกครั้งที่แอป restart/redeploy เบอร์ที่ถึงคิวโทรซ้ำแล้วต้องรอ
  // รอบ setInterval แรกอีกสูงสุด 15 นาทีถัดไป ทั้งที่จริงๆ ถึงเวลาโทรซ้ำไปแล้วตั้งแต่ก่อน restart
  scanRetries()
  setInterval(scanRetries, SCAN_INTERVAL_MS)
  console.log('[Retry] Scheduler started (สแกนทันที แล้วทุก 15 นาทีถัดจากนี้)')
}

module.exports = { startRetryScheduler }
