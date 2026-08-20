// C1 — per-turn latency instrumentation, standalone (ไม่แก้ logic เดิมเลย แค่วัดเวลา)
// legacy path ไม่ streaming จริง (askClaudeStream yield ข้อความเต็มก้อนเดียว ไม่มี delta ทีละคำ) จึงไม่มี
// t3 (first Claude delta) / t4 (first safe chunk) ที่มีความหมายจริง — ผู้เรียกต้องปล่อยเป็น null ตามจริง
// ห้ามฝืน markOnce ให้ครบทุกตัวเพื่อความสวยงาม เพราะจะปลอมความหมาย metric ให้ดูเหมือน streaming ทั้งที่ไม่ใช่
const { performance } = require('perf_hooks')

function createTurnMetrics({ callSid, generationId, path, rolloutBucket, rolloutPercent }) {
  return {
    callSid,
    generationId,
    path,
    rolloutBucket,
    rolloutPercent,
    startedAt: new Date().toISOString(), // สำหรับ correlate กับ log บรรทัดอื่น — ไม่ใช้คำนวณ latency (wall clock กระโดดได้)
    t1: null, // STT final
    t2: null, // Claude request sent (หรือเริ่มรอ prewarm ที่ในไฟลท์อยู่แล้ว)
    t3: null, // first Claude delta
    t4: null, // first safe chunk
    t5: null, // TTS request sent
    t6: null, // first TTS audio chunk received
    t7: null, // first audio chunk sent to Twilio
    fallbackTriggered: false,
    fallbackReason: null, // C4c — เหตุผลที่ "เริ่ม" fallback: CLAUDE_ERROR/TTS_ERROR/CLAUDE_FIRST_DELTA_TIMEOUT/CHUNK_READY_TIMEOUT/TTS_FIRST_AUDIO_TIMEOUT
    fallbackStartedAt: null, // C4a — performance.now() ตอนเริ่มพยายาม fallback (ถ้ามี) ใช้แยกวิเคราะห์ turn ที่ fallback ออกจาก pure-chunked latency percentile
    fallbackOutcome: null, // C4c — สิ่งที่เกิดกับ "ความพยายาม fallback เอง": SPOKEN/STALE/FALLBACK_TIMEOUT/FALLBACK_ERROR
    audioCommitted: false,
    // L1b — prewarm telemetry แยกจาก canonical t1-t7 โดยตั้งใจ (ห้ามปนกัน — speculation อาจเริ่มก่อน t1/STT-final
    // ด้วยซ้ำ ถ้าเอาไป mark t2 ตรงๆ จะทำให้ t2 < t1 ผิด sequence) ทุกค่า relative ต่อ prewarmStartedAt เอง ไม่ใช่
    // absolute performance.now() — t3/t4 (canonical) ยังหมายถึง "accepted-path availability time" เหมือนเดิมเป๊ะ
    prewarmStartedAt: null, // performance.now() ตอน speculation เริ่มจริง (ถ้ามี speculation ให้ turn นี้)
    prewarmFirstDeltaMs: null, // เวลาที่ delta แรกของ speculation มาถึง สัมพัทธ์กับ prewarmStartedAt
    prewarmFirstChunkMs: null, // เวลาที่ safe chunk แรกของ speculation พร้อม สัมพัทธ์กับ prewarmStartedAt
    prewarmAgeAtFinalMs: null, // อายุของ speculation ณ ตอน final transcript มาถึงจริง (snapshot ก่อน grace ใดๆ)
    prewarmOutcome: null, // BUFFERED_HIT/READY_HIT/CONTROL_ONLY_HIT/DELTA_ONLY_HIT/GRACE_HIT/GRACE_TIMEOUT_FRESH/MISMATCH_FRESH/EMPTY_FRESH/ERROR_FRESH/ABORTED
    prewarmBufferedChunks: null, // จำนวน chunk ที่ค้างใน queue ตอน adopt (หรือ null ถ้าไม่มี speculation)
    // L2a (legacy Claude instrumentation, design locked 2026-08-20) — แยกจาก canonical t2/t3/t4 โดยตั้งใจ ห้ามปน
    // กัน: t2 เดิม mark ก่อนรอ prewarm grace (150ms) เสมอ ถ้า populate t3 แล้วคำนวณ t3-t2 จะได้ "grace + provider
    // TTFT" ปนกัน ไม่ใช่ Claude TTFT จริงล้วนๆ — field ชุดนี้ผูกกับ askClaudeObservedFullResponse() เท่านั้น (fresh
    // legacy call path เดียว) เขียนค่าทันทีที่แต่ละ milestone มาถึงจริง ไม่รอ callback เดียวตอนจบ เพื่อไม่ให้ turn
    // ที่ abort/timeout/error กลางทางเสีย data ไปทั้งหมด (ยังคง null ตามจริงถ้าไม่ถึง milestone นั้น ไม่ fabricate)
    legacyClaudeRequestAt: null,
    legacyClaudeFirstDeltaAt: null,
    legacyClaudeFirstSafeAt: null,
    legacyClaudeFullAt: null,
    // COMPLETED/ABORTED/TIMEOUT/ERROR/EMPTY — แยกจาก legacyClaudeFirstSafeAt=null โดยตั้งใจ เพราะ "ไม่มี early
    // boundary เลย" (Claude ตอบสั้นจนจบก่อนเจอ boundary) กับ "transport ล้มเหลวก่อนตอบ" เป็นคนละคำถามกัน — ไม่งั้น
    // วิเคราะห์ทีหลังจะแยกไม่ออกว่า null เพราะอะไร
    legacyClaudeOutcome: null,
  }
}

// set ครั้งแรกครั้งเดียวต่อ key — เรียกซ้ำได้อย่างปลอดภัย (เช่นทุก chunk ในลูป TTS) โดยค่าจะไม่ขยับตามก้อนหลังๆ
function markOnce(metrics, key) {
  if (metrics[key] == null) metrics[key] = performance.now()
}

function duration(a, b) {
  return a != null && b != null ? b - a : null
}

function computeDerivedMetrics(metrics) {
  return {
    sttToTwilio: duration(metrics.t1, metrics.t7),
    claudeTTFT: duration(metrics.t2, metrics.t3),
    chunkDelay: duration(metrics.t3, metrics.t4),
    ttsTTFB: duration(metrics.t5, metrics.t6),
    requestToAudio: duration(metrics.t2, metrics.t7),
    // L2a — คนละ namespace จาก t2-t7 โดยตั้งใจ (ดู comment ที่ createTurnMetrics) legacyEarlyOpportunityMs คือ
    // ตัวเลขหลักที่ใช้ตัดสิน L2b: ช่วงเวลาที่ TTS "อาจจะ" เริ่มพูดได้เร็วกว่าที่เป็นอยู่ตอนนี้ ถ้ามี first-safe-sentence
    // TTS architecture จริง (ยังไม่มีตอนนี้ — L2a วัดเฉยๆ ไม่เริ่มพูดเร็วขึ้นเลย)
    legacyClaudeTTFTMs: duration(metrics.legacyClaudeRequestAt, metrics.legacyClaudeFirstDeltaAt),
    legacyFirstSafeMs: duration(metrics.legacyClaudeRequestAt, metrics.legacyClaudeFirstSafeAt),
    legacyFullCompletionMs: duration(metrics.legacyClaudeRequestAt, metrics.legacyClaudeFullAt),
    legacyEarlyOpportunityMs: duration(metrics.legacyClaudeFirstSafeAt, metrics.legacyClaudeFullAt),
  }
}

module.exports = { createTurnMetrics, markOnce, duration, computeDerivedMetrics }
