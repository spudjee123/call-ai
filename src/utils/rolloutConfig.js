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

// L2a production exposure gate — คนละ fail-safe policy จาก rollout_percent โดยตั้งใจ (design revision 2026-08-20):
// rollout_percent ใช้ last-known-good เพราะเป็น production feature rollout ปกติ แต่ legacy_observed_percent เป็น
// experimental transport switch ที่แตะ fresh legacy call site ตรงๆ ("เบรก" ให้ a982fdf ก่อนขึ้น production) จึงต้อง
// fail CLOSED เป็น 0 เสมอทุกกรณีที่ไม่ใช่ "parse สำเร็จและถูกต้องครบ" — ไม่มี LKG ไม่มี cold-start "ยังไม่รู้ค่า"
//
// กติกาสำคัญที่สุด: "ไม่มี campaign_id" ต้องไม่แปลว่า "ทุก campaign" — ยังไม่มี all-campaign mode ใน L2a นี้เลย
// ดังนั้น percent>0 ต้องมี campaignId ที่ valid ไม่ว่างเปล่าคู่กันเสมอ ไม่งั้นทั้งคู่ fail-closed กลับเป็น {0, null}
// (เช่น คนแก้ชีตใส่ legacy_observed_percent=100 แต่ลืมใส่ campaign_id — ต้องได้ CONTROL ทุกสาย ไม่ใช่ observed 100%)
//
// คืนเป็น atomic snapshot เดียว {percent, campaignId} เสมอ (ไม่ใช่ getter แยก 2 ตัว) กัน caller ในอนาคตอ่านค่า
// สอง field จากคนละรอบ refresh กันโดยไม่ตั้งใจ แม้ตอนนี้ background poll จะ single-threaded ก็ตาม
function classifyLegacyObservedConfig(rows) {
  const percentRows = rows.filter(r => r.key === 'legacy_observed_percent')
  const campaignRows = rows.filter(r => r.key === 'legacy_observed_campaign_id')

  let percent = 0
  if (percentRows.length === 1) {
    const raw = String(percentRows[0].value ?? '').trim()
    if (/^\d+$/.test(raw)) {
      const n = Number(raw)
      if (n >= 0 && n <= 100) percent = n
    }
  }
  // percentRows.length === 0 (missing) หรือ > 1 (duplicate) หรือ format ผิด → percent คง 0 (fail-closed default)

  let campaignId = null
  let campaignIdValid = true
  if (campaignRows.length === 1) {
    const raw = String(campaignRows[0].value ?? '').trim()
    campaignId = raw || null // ค่าว่างเปล่าถือเป็น "ไม่มี" เหมือน missing แถวไปเลย
  } else if (campaignRows.length > 1) {
    campaignIdValid = false // duplicate แถว → invalid เสมอ ไม่เดาว่าจะใช้แถวไหน
  }

  if (percent > 0 && (campaignId == null || !campaignIdValid)) {
    return { percent: 0, campaignId: null } // fail-closed: percent>0 ต้องมี campaign ที่ valid คู่กันเสมอ
  }
  // percent=0 → คืน campaignId เป็น null เสมอด้วย แม้แถว campaign_id จะมีค่าอยู่ก็ตาม (atomic snapshot ต้องสอดคล้อง
  // กันเอง: percent=0 แปลว่า "ไม่มี exposure" เต็มที ไม่ควรมี campaignId ค้างอยู่ให้อ่านผิดความหมายทีหลังได้)
  if (percent === 0) return { percent: 0, campaignId: null }
  return { percent, campaignId }
}

// L2b production exposure gate (design revision 2026-08-21) — same fail-closed policy and atomic-snapshot
// shape as classifyLegacyObservedConfig() above, own key names, own cache/log-signature state entirely
// (see createRolloutConfig() below) — completely independent kill switch from legacy_observed_*, since L2b
// changes actual TTS start timing (a stronger production risk than L2a's pure instrumentation) and must be
// disable-able without touching L2a's exposure at all. Same "percent>0 requires a valid campaignId" rule.
function classifyLegacyEarlyTtsConfig(rows) {
  const percentRows = rows.filter(r => r.key === 'legacy_early_tts_percent')
  const campaignRows = rows.filter(r => r.key === 'legacy_early_tts_campaign_id')

  let percent = 0
  if (percentRows.length === 1) {
    const raw = String(percentRows[0].value ?? '').trim()
    if (/^\d+$/.test(raw)) {
      const n = Number(raw)
      if (n >= 0 && n <= 100) percent = n
    }
  }

  let campaignId = null
  let campaignIdValid = true
  if (campaignRows.length === 1) {
    const raw = String(campaignRows[0].value ?? '').trim()
    campaignId = raw || null
  } else if (campaignRows.length > 1) {
    campaignIdValid = false
  }

  if (percent > 0 && (campaignId == null || !campaignIdValid)) {
    return { percent: 0, campaignId: null }
  }
  if (percent === 0) return { percent: 0, campaignId: null }
  return { percent, campaignId }
}

function createRolloutConfig({ getRows = () => sheetsService.getStreamingConfig(), refreshIntervalMs = DEFAULT_REFRESH_INTERVAL_MS } = {}) {
  let cachedPercent = null // null = cold start, ยังไม่เคย fetch สำเร็จเลย
  let lastAttemptAt = null
  let lastSuccessAt = null
  let refreshInFlight = null
  let timer = null
  // L2a — fail-closed เสมอ (ไม่มี "cold start ยังไม่รู้ค่า" แบบ cachedPercent) เริ่มต้นที่ {0, null} ตรงๆ ตั้งแต่
  // ก่อน fetch ครั้งแรกเลย — ถ้า Sheets ยังไม่เคยเพิ่มแถว legacy_observed_percent เลย นี่คือค่าที่ปลอดภัยอยู่แล้ว
  let cachedObservedConfig = { percent: 0, campaignId: null }
  // L2b — cache/policy แยกจาก L2a โดยสิ้นเชิง (ดู comment ที่ classifyLegacyEarlyTtsConfig ด้านบน)
  let cachedEarlyTtsConfig = { percent: 0, campaignId: null }

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

  // แยก signature tracking คนละตัวจาก rollout_percent เจตนา — สอง config เปลี่ยนคนละจังหวะกัน ถ้าใช้ signature
  // ร่วมกันจะกดข้าม log ของอีกฝั่งไปเงียบๆ โดยไม่ตั้งใจ (เช่น rollout เปลี่ยนค่าพอดีจังหวะเดียวกับ observed ไม่เปลี่ยน)
  let lastObservedLogSignature = null
  function logOnChangeObserved(signature, logFn) {
    if (signature === lastObservedLogSignature) return
    lastObservedLogSignature = signature
    logFn()
  }

  // L2b — signature ของตัวเองอีกชุด แยกจากทั้ง rollout_percent และ legacy_observed_* (เหตุผลเดียวกัน)
  let lastEarlyTtsLogSignature = null
  function logOnChangeEarlyTts(signature, logFn) {
    if (signature === lastEarlyTtsLogSignature) return
    lastEarlyTtsLogSignature = signature
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

        // L2a — เขียนทับ cachedObservedConfig ทุกรอบ refresh ที่ fetch สำเร็จ ไม่ว่าผลจะเป็นอะไร (fail-closed
        // ไม่ใช่ LKG) — rows ชุดเดียวกับที่ใช้ parse rollout_percent ด้านบน ไม่ fetch ซ้ำ
        const observedResult = classifyLegacyObservedConfig(rows)
        cachedObservedConfig = observedResult
        logOnChangeObserved(
          `observed:${observedResult.percent}:${observedResult.campaignId}`,
          () => console.log(`[RolloutConfig] observed config percent=${observedResult.percent} campaignId=${observedResult.campaignId ?? 'null'}`)
        )

        // L2b — เขียนทับ cachedEarlyTtsConfig ทุกรอบเช่นกัน (fail-closed ไม่ใช่ LKG) rows ชุดเดียวกัน ไม่ fetch ซ้ำ
        const earlyTtsResult = classifyLegacyEarlyTtsConfig(rows)
        cachedEarlyTtsConfig = earlyTtsResult
        logOnChangeEarlyTts(
          `earlyTts:${earlyTtsResult.percent}:${earlyTtsResult.campaignId}`,
          () => console.log(`[RolloutConfig] early-tts config percent=${earlyTtsResult.percent} campaignId=${earlyTtsResult.campaignId ?? 'null'}`)
        )
      } catch (err) {
        // ใช้ last-known-good ต่อไปเฉพาะ rollout_percent ไม่ throw ให้กระทบ caller (ทั้ง background poll และ manual call)
        logOnChange(`failure:${err.message}`, () => console.error(`[RolloutConfig] refresh failed error=${JSON.stringify(err.message)} using_lkg=${cachedPercent ?? 0}`))
        // L2a/L2b — fetch ล้มเหลวเอง (ไม่ใช่แค่ parse ล้มเหลว) ก็ fail-closed เหมือนกันทั้งคู่ ไม่ preserve ค่าเดิม
        cachedObservedConfig = { percent: 0, campaignId: null }
        logOnChangeObserved('observed:fetch_failed', () => console.warn('[RolloutConfig] observed config fetch failed — fail-closed percent=0'))
        cachedEarlyTtsConfig = { percent: 0, campaignId: null }
        logOnChangeEarlyTts('earlyTts:fetch_failed', () => console.warn('[RolloutConfig] early-tts config fetch failed — fail-closed percent=0'))
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

  // L2a — atomic snapshot เดียว {percent, campaignId} จาก refresh รอบเดียวกันเสมอ (ไม่ใช่ getter แยก 2 ตัว) กัน
  // caller ในอนาคตอ่านค่าคนละ field จากคนละรอบ refresh กันโดยไม่ตั้งใจ
  function getCurrentLegacyObservedConfig() {
    refreshIfStale()
    return cachedObservedConfig
  }

  // L2b — atomic snapshot เดียวกัน pattern เป๊ะ แยก state จาก L2a โดยสิ้นเชิง
  function getCurrentLegacyEarlyTtsConfig() {
    refreshIfStale()
    return cachedEarlyTtsConfig
  }

  return { getCurrentRolloutPercent, getCurrentLegacyObservedConfig, getCurrentLegacyEarlyTtsConfig, start, stop, refresh }
}

module.exports = { createRolloutConfig, parseRolloutPercent, classifyLegacyObservedConfig, classifyLegacyEarlyTtsConfig }
