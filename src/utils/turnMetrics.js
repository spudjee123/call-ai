// C1 — per-turn latency instrumentation, standalone (ไม่แก้ logic เดิมเลย แค่วัดเวลา)
// legacy path ไม่ streaming จริง (askClaudeStream yield ข้อความเต็มก้อนเดียว ไม่มี delta ทีละคำ) จึงไม่มี
// t3 (first Claude delta) / t4 (first safe chunk) ที่มีความหมายจริง — ผู้เรียกต้องปล่อยเป็น null ตามจริง
// ห้ามฝืน markOnce ให้ครบทุกตัวเพื่อความสวยงาม เพราะจะปลอมความหมาย metric ให้ดูเหมือน streaming ทั้งที่ไม่ใช่
const { performance } = require('perf_hooks')

function createTurnMetrics({
  callSid, generationId, path, rolloutBucket, rolloutPercent,
  legacyObserved = null, legacyObservedBucket = null, legacyObservedPercentAtStart = null, legacyObservedCampaignMatched = null,
  legacyEarlyTts = null, legacyEarlyTtsBucket = null, legacyEarlyTtsPercentAtStart = null, legacyEarlyTtsCampaignMatched = null,
}) {
  return {
    callSid,
    generationId,
    path,
    rolloutBucket,
    rolloutPercent,
    // L2a production exposure gate (design revision 2026-08-20) — frozen ครั้งเดียวตอน WS 'start' เหมือน
    // rollout/rolloutBucket ข้างบน เขียนลง [Metrics] ทุก turn เสมอ (ไม่ใช่แค่ [Rollout] แยกบรรทัด) เพื่อให้ join
    // กลุ่ม CONTROL/OBSERVED กับตัวเลข legacyClaude* ด้านล่างได้จาก log บรรทัดเดียว ไม่ต้อง join ข้าม log ด้วย callSid
    legacyObserved,
    legacyObservedBucket,
    legacyObservedPercentAtStart,
    legacyObservedCampaignMatched,
    // L2b production exposure gate (design revision 2026-08-21) — frozen ครั้งเดียวตอน WS 'start' เหมือนกัน
    // safety precedence: chunked=true → legacyObserved=false ก่อน → legacyEarlyTts=false เสมอ (ดู audioStream.js
    // freeze block) สาม flag นี้ mutual exclusive กันโดยสมบูรณ์ ไม่มีทาง active พร้อมกันสองตัว
    legacyEarlyTts,
    legacyEarlyTtsBucket,
    legacyEarlyTtsPercentAtStart,
    legacyEarlyTtsCampaignMatched,
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
    // L2b (conditional legacy early TTS, design locked 2026-08-21) — telemetry namespace ของตัวเอง แยกจาก
    // legacyClaude* (L2a, ไม่เคยเริ่มพูดเร็วขึ้นเลย) เพราะ L2b ตัวนี้ "ทำจริง" (เริ่ม TTS ก่อน full completion
    // ถ้า mode=CHUNKED) — canonical t3/t4 ก็ยัง markOnce ตามปกติสำหรับ L2b โดยเจตนา (ต่างจาก L2a ที่ปล่อย null
    // เสมอ) เพราะ L2b คือ streaming จริงเหมือน chunked path ไม่ใช่แค่วัดเฉยๆ
    legacyEarlyTtsRequestAt: null,
    legacyEarlyTtsFirstDeltaAt: null,
    legacyEarlyTtsFirstSafeAt: null,
    legacyEarlyTtsFullAt: null,
    legacyEarlyTtsMode: null, // 'SINGLE_SHOT' | 'CHUNKED'
    // COMPLETED/EMPTY/ABORTED/ERROR/TIMEOUT_PRECOMMIT/TIMEOUT_POSTCOMMIT/ERROR_POSTCOMMIT — POSTCOMMIT variants
    // แยกจาก precommit โดยตั้งใจ (design review round 3, mandatory refinement 1): ถ้า audio ถูก commit ไปแล้ว
    // (turnState.audioCommitted) ก่อนที่ Claude tail จะ timeout/error ห้าม fabricate recovery/replay ทับเสียงที่
    // พูดไปแล้ว — ต้องแยกให้วิเคราะห์ทีหลังรู้ว่า "ไม่มีคำตอบเลย" กับ "มีคำตอบบางส่วนแล้วท้ายพัง" เป็นคนละเคสกัน
    legacyEarlyTtsOutcome: null,
    // Track L (diagnostic only, design revision 2026-08-22, Design Gate R3 PASS) — request/response size
    // telemetry for the L2b fresh-Claude-request path only (askClaudeConditionalStream()), prefixed l2b*
    // deliberately so a future reader of [Metrics] never mistakes these for applying to every Claude path.
    // Computed inside claude.js itself from the EXACT post-slice(-MAX_HISTORY) `history`/`systemPrompt` this
    // request actually sends — never from audioStream.js's own copy of session.messages, which would
    // over-count once a conversation exceeds MAX_HISTORY. null means "not measured" (out of scope, or the
    // content at that point wasn't a string) — never fabricated as 0, since 0 and "unmeasurable" mean very
    // different things for a size-vs-latency correlation analysis. No safeBoundaryDelayMs here on purpose —
    // it would be byte-for-byte identical to the existing chunkDelay (duration(t3,t4) above), since L2b maps
    // the same firstDeltaAt/firstSafeAt milestones to t3/t4.
    l2bSystemPromptCharCount: null,
    l2bPriorHistoryCharCount: null,
    l2bRequestMessageCount: null,
    l2bCurrentUserCharCount: null,
    l2bApproxInputTextCharCount: null,
    l2bResponseCharCount: null, // from Claude's own finalText.length, captured before audioStream.js can substitute/append anything (recovery phrase, END_CALL follow-up)
    // Track M (diagnostic only, design R3 LOCKED 2026-08-22) — explains WHY chunkDelay (t4-t3, first-safe-chunk
    // latency) has the value it does, for the L2b fresh-Claude-request path only. Scoped to the FIRST safe
    // chunk exclusively (same scope as chunkDelay itself) — never anything from later chunks in CHUNKED mode.
    // All 6 fields null together whenever chunkDelay is null (no boundary ever found before stream completed).
    l2bChunkReason: null, // STRONG_BOUNDARY | SOFT_BOUNDARY | NATURAL_BOUNDARY | NATURAL_BOUNDARY_HARD_MAX
    l2bChunkCharCount: null, // length of the first safe chunk's text
    l2bChunkDeltaCount: null, // count of Claude text deltas from turn start up to and including the one that produced first-safe — frozen there, never counts deltas arriving during the 150ms grace race
    l2bChunkFirstCandidateElapsedMs: null, // elapsed ms from firstDeltaAt to when a cut candidate first became eligible — equals chunkDelay exactly for STRONG_BOUNDARY/SOFT_BOUNDARY (no separate candidate state exists for those); can be strictly less than chunkDelay for NATURAL_BOUNDARY* when numeric-tail protection or the SOFT_TIMEOUT/HARD_MAX gate delayed the actual cut
    l2bChunkNumericProtectionBlocked: null, // true only if a natural-boundary candidate was policy-eligible AND held back by numeric-tail protection at least once before the eventual cut — false (not null) for STRONG_BOUNDARY/SOFT_BOUNDARY since numeric protection never applies to those by construction
    l2bChunkPreSafeDeltaGapMs: null, // DELTA trigger: observed wall-clock gap between the delta immediately preceding the causal one and the causal delta itself — 0 if first-safe came from the very first delta of the turn. HARD_MAX_TIMER trigger: gap between the last real delta observed and the timer-established firstSafeAt instant (no causal delta exists for this trigger). Correlational only — cannot by itself prove Claude paced generation slowly vs. simply nothing more being needed once the HARD_MAX window closed. (Track N, design R6 LOCKED 2026-08-22 — the deciding phase now HAS a proactive HARD_MAX wall-clock re-check via l2bChunkFirstSafeTrigger='HARD_MAX_TIMER'; it previously did not, matching drainChunked()'s CHUNKED-phase timer.)
    // Track N (design R6 LOCKED 2026-08-22) — 'DELTA' | 'HARD_MAX_TIMER'. Distinguishes a first-safe cut
    // caused by a real Claude delta from one caused by the new HARD_MAX proactive-wakeup timer (which no
    // longer depends on a future delta once a numeric-protected candidate's protection window expires).
    // Always populated whenever l2bChunkReason is non-null (never a third, unpopulated state for this field
    // specifically) — HARD_MAX_TIMER can only ever co-occur with l2bChunkReason='NATURAL_BOUNDARY_HARD_MAX'.
    l2bChunkFirstSafeTrigger: null,
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
    // L2b — คนละ namespace จาก legacyClaude* (L2a) โดยตั้งใจเหมือนกัน
    legacyEarlyTtsTTFTMs: duration(metrics.legacyEarlyTtsRequestAt, metrics.legacyEarlyTtsFirstDeltaAt),
    legacyEarlyTtsFirstSafeMs: duration(metrics.legacyEarlyTtsRequestAt, metrics.legacyEarlyTtsFirstSafeAt),
    legacyEarlyTtsFullCompletionMs: duration(metrics.legacyEarlyTtsRequestAt, metrics.legacyEarlyTtsFullAt),
    legacyEarlyTtsEarlyOpportunityMs: duration(metrics.legacyEarlyTtsFirstSafeAt, metrics.legacyEarlyTtsFullAt),
  }
}

module.exports = { createTurnMetrics, markOnce, duration, computeDerivedMetrics }
