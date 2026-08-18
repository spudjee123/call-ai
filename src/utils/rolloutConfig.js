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
//
// คืน { value, reason } เสมอ — reason ไม่ null เฉพาะตอน value เป็น null (ใช้บอกสาเหตุใน log ตอน refresh ล้มเหลว
// แบบ "invalid config" ไม่ใช่ error จริง — parseRolloutPercent ด้านล่างเป็น wrapper คืนแค่ value ตาม contract เดิม)
function classifyRolloutPercent(rows) {
  const matches = rows.filter(r => r.key === 'rollout_percent')
  if (matches.length === 0) return { value: null, reason: 'missing_rollout_percent' }
  if (matches.length > 1) return { value: null, reason: 'duplicate_rollout_percent' }
  const raw = String(matches[0].value ?? '').trim()
  if (!/^\d+$/.test(raw)) return { value: null, reason: `invalid_value value=${JSON.stringify(raw)}` }
  const n = Number(raw)
  if (n < 0 || n > 100) return { value: null, reason: `invalid_value value=${JSON.stringify(raw)}` }
  return { value: n, reason: null }
}

function parseRolloutPercent(rows) {
  return classifyRolloutPercent(rows).value
}

function createRolloutConfig({ getRows = () => sheetsService.getStreamingConfig(), refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS } = {}) {
  let cachedPercent = null // null = cold start, ยังไม่เคย fetch สำเร็จเลย
  let lastAttemptAt = null
  let lastSuccessAt = null
  let refreshInFlight = null
  let timer = null

  // C6b — log แบบ state-change เท่านั้น ไม่ log ทุกรอบ poll (30s) เพราะจะ spam production logs โดยไม่มีประโยชน์
  // log signature เปลี่ยนเมื่อไหร่ก็ต่อเมื่อผลลัพธ์เปลี่ยนจริง (success→success ค่าเดิม ไม่ log ซ้ำ, error เดิมซ้ำๆ
  // ไม่ log ซ้ำ) แต่เปลี่ยนค่า/ฟื้นจาก error/เปลี่ยนชนิด error จะ log ทันที — ทำให้ audit rollout เปลี่ยนแปลงได้ง่าย
  // และแยกแยะ "Sheets control plane ทำงานจริง" ออกจาก "แค่ cold-start default 0" ได้จาก log บรรทัดเดียว
  let lastLogSignature = null
  function logOnChange(signature, logFn) {
    if (signature === lastLogSignature) return
    lastLogSignature = signature
    logFn()
  }

  async function refresh() {
    if (refreshInFlight) return refreshInFlight
    refreshInFlight = (async () => {
      lastAttemptAt = Date.now() // อัปเดตทุกครั้งที่ "พยายาม" ไม่ว่าผลจะสำเร็จหรือไม่ — คุม backoff ของ refreshIfStale
      try {
        const rows = await getRows()
        const { value, reason } = classifyRolloutPercent(rows)
        if (value != null) {
          cachedPercent = value
          lastSuccessAt = Date.now()
          logOnChange(`success:${value}`, () => console.log(`[RolloutConfig] refresh success percent=${value}`))
        } else {
          // ไม่มีแถว/ซ้ำ/format ผิด → ไม่แตะ cachedPercent เดิมเลย ถือเป็น refresh failed เหมือน error (last-known-good)
          logOnChange(`invalid:${reason}`, () => console.warn(`[RolloutConfig] refresh invalid reason=${reason} using_lkg=${cachedPercent ?? 0}`))
        }
      } catch (err) {
        // ใช้ last-known-good ต่อไป ไม่ throw ให้กระทบ caller (ทั้ง background poll และ manual call)
        logOnChange(`failure:${err.message}`, () => console.error(`[RolloutConfig] refresh failed error=${JSON.stringify(err.message)} using_lkg=${cachedPercent ?? 0}`))
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
