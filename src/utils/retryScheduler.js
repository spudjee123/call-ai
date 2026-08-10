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
  setInterval(scanRetries, SCAN_INTERVAL_MS)
  console.log('[Retry] Scheduler started (ทุก 15 นาที)')
}

module.exports = { startRetryScheduler }
