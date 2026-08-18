// Checkpoint C5 — Google Sheets เป็น config source สำหรับ % rollout ของ chunked path พร้อม background
// polling ของตัวเอง (ไม่ใช่ lazy-on-call-only) เพราะ config นี้ทำหน้าที่เป็น runtime kill switch — สายแรกหลัง
// idle ต้องไม่เจอค่าที่ค้างมาจาก cache เก่าที่หมดอายุไปแล้วเงียบๆ โดยไม่มีใครไป trigger refresh ก่อนหน้านั้นเลย
//
// getCurrentRolloutPercent() อ่านจาก memory เท่านั้น เป็น synchronous เสมอ ไม่มีทาง block การเริ่มสายด้วย
// Sheets I/O — start() เปิด background polling ที่เป็นกลไกหลักคอย refresh ค่าให้ทันสมัยอยู่เสมอ ไม่ใช่พึ่งมีสาย
// เข้าก่อนค่อย trigger refresh (refreshIfStale ใน getCurrentRolloutPercent เป็นแค่ backup เผื่อลืม start())
//
// Cold start (ยังไม่เคย fetch สำเร็จเลยตั้งแต่ process เริ่ม) → 0% เสมอ ไม่ default 100% เด็ดขาด
// Sheets ล่ม/แถวหาย/แถวซ้ำ/format ผิด → ถือเป็น "refresh failed" ทั้งหมด ไม่แตะ cachedPercent เดิม (last-known-good)
// lastAttemptAt แยกจาก lastSuccessAt โดยเจตนา — ให้ refreshIfStale คุมจาก "พยายามครั้งล่าสุดเมื่อไหร่" ไม่ใช่
// "สำเร็จครั้งล่าสุดเมื่อไหร่" ไม่งั้น Sheets ที่พังต่อเนื่องจะโดน hammer ซ้ำทุกครั้งที่มีสายเข้าระหว่างที่ยังพังอยู่
const { sheetsService } = require('../services/googleSheets')

const DEFAULT_REFRESH_INTERVAL_MS = 30000

// รับเฉพาะ key "rollout_percent" ที่เป็น non-negative integer ล้วนๆ ในช่วง 0-100 เท่านั้น — ปฏิเสธ "-1"/"5.5"/
// "abc"/"" ทั้งหมด (bucket มีแค่ 100 ช่อง 0..99 จริง เศษทศนิยมไม่มีความหมาย) และถ้ามีแถว rollout_percent
// มากกว่า 1 แถว ถือว่า config ทั้งชุด invalid ไปเลย ไม่เดาว่าจะใช้แถวแรกหรือแถวสุดท้าย (แก้ผิดบน sheet ไม่ควร
// เปลี่ยน traffic แบบเงียบๆ โดยไม่มีใครรู้ตัว)
function parseRolloutPercent(rows) {
  const matches = rows.filter(r => r.key === 'rollout_percent')
  if (matches.length !== 1) return null
  const raw = String(matches[0].value ?? '').trim()
  if (!/^\d+$/.test(raw)) return null
  const n = Number(raw)
  return n >= 0 && n <= 100 ? n : null
}

function createRolloutConfig({ getRows = () => sheetsService.getStreamingConfig(), refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS } = {}) {
  let cachedPercent = null // null = cold start, ยังไม่เคย fetch สำเร็จเลย
  let lastAttemptAt = null
  let lastSuccessAt = null
  let refreshInFlight = null
  let timer = null

  async function refresh() {
    if (refreshInFlight) return refreshInFlight
    refreshInFlight = (async () => {
      lastAttemptAt = Date.now() // อัปเดตทุกครั้งที่ "พยายาม" ไม่ว่าผลจะสำเร็จหรือไม่ — คุม backoff ของ refreshIfStale
      try {
        const rows = await getRows()
        const percent = parseRolloutPercent(rows)
        if (percent != null) {
          cachedPercent = percent
          lastSuccessAt = Date.now()
        }
        // percent == null (ไม่มีแถว/ซ้ำ/format ผิด) → ไม่แตะ cachedPercent เดิมเลย ถือเป็น refresh failed เหมือน error
      } catch (err) {
        // เงียบโดยเจตนา — ใช้ last-known-good ต่อไป ไม่ throw ให้กระทบ caller (ทั้ง background poll และ manual call)
      } finally {
        refreshInFlight = null
      }
    })()
    return refreshInFlight
  }

  function start() {
    if (timer) return // กัน start() ซ้ำสร้าง timer ซ้อนกันหลายตัว
    refresh() // fetch ทันทีตอน bootstrap — ไม่รอสายแรกเข้ามาก่อนค่อย trigger (นี่คือประเด็นหลักที่ C5 แก้จาก lazy-only)
    timer = setInterval(refresh, refreshIntervalMs)
    timer.unref?.() // ไม่ให้ timer ตัวนี้เป็นเหตุผลเดียวที่ทำให้ process ค้างไม่ยอมจบ (สำคัญกับ test/graceful shutdown)
  }

  function stop() {
    if (timer) { clearInterval(timer); timer = null }
  }

  // backup เฉยๆ ไม่ใช่กลไกหลัก — เผื่อ start() ไม่เคยถูกเรียก (เช่น ลืม wire ตอน bootstrap) ยังพอ self-heal ได้บ้าง
  // ตอนมีสายเข้าจริง แต่ไม่ควรพึ่งพาเส้นทางนี้เป็นหลักสำหรับ kill-switch semantics ที่ต้องการ propagation เร็ว
  function refreshIfStale() {
    const isStale = lastAttemptAt == null || Date.now() - lastAttemptAt >= refreshIntervalMs
    if (isStale) refresh()
  }

  function getCurrentRolloutPercent() {
    refreshIfStale()
    return cachedPercent ?? 0
  }

  return { getCurrentRolloutPercent, start, stop, refresh }
}

module.exports = { createRolloutConfig, parseRolloutPercent }
