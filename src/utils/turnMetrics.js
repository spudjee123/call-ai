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
  }
}

module.exports = { createTurnMetrics, markOnce, duration, computeDerivedMetrics }
