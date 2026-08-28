const callSessions = require('../utils/callSessions')
const { transcribeStream } = require('../services/googleSTT')
const { askClaude, askClaudeStream, askClaudeObservedFullResponse, askClaudeConditionalStream } = require('../services/claude')
const { synthesizeSpeechStream } = require('../services/tts')
const healthMonitor = require('../utils/healthMonitor')
const { createCallState, bumpGeneration, isCurrentGeneration, endCall } = require('../utils/generationGuard')
const { decideRollout, getLegacyObservedBucket, getLegacyEarlyTtsBucket, getSttA2Bucket, getSttA2ShadowBucket } = require('../utils/rolloutBucket')
const { createTurnMetrics, markOnce, computeDerivedMetrics } = require('../utils/turnMetrics')
const { createTurnState, markTtsPending, markAudioCommitted, markDone, claimFallback } = require('../utils/turnState')
const { runChunkedTurn, speakFixedText, createChunkedProducer, adoptChunkedProducer } = require('./chunkedTurn')
const { runAttemptWithWatchdog, bridgeAbort } = require('../utils/attemptWithWatchdog')
const { isSpeculationMatch, classifyForAdoption } = require('../utils/chunkedSpeculation')
const { createRolloutConfig } = require('../utils/rolloutConfig')
const { CHUNK_REASON } = require('../utils/speechChunker')
const { performance } = require('perf_hooks')

const MAX_CALL_DURATION_MS = (parseInt(process.env.MAX_CALL_DURATION_SECONDS) || 300) * 1000

// L1a (latency optimization, rollout-scoped STT endpoint experiment) — googleSTT.js's default
// INTERIM_FINALIZE_MS (900ms) เป็นค่าเดียวที่ legacy และ chunked path ใช้ร่วมกันมาตลอด ห้ามแก้ default ตรงๆ
// เพราะจะกระทบ legacy production ทุกสายทันทีโดยไม่ผูกกับ chunked rollout เลย (rollout=0% จะไม่ป้องกันอะไรเลย)
// ต้องคำนวณค่านี้ "หลัง" rollout ถูก freeze ต่อสายแล้วเท่านั้น (จุดเดียวกับที่ rollout เองถูก freeze — ดู 'start')
//
// ผลการทดลอง 600ms (controlled production test, 2026-08-19): REJECT — natural thinking pause ของลูกค้าถูกตัด
// เป็น utterance แยกกัน 2 ครั้งภายในประโยคเดียวกัน ทำให้ Claude generation เริ่มก่อนลูกค้าพูดจบจริง (ลูกค้าพูดต่อ
// เนื่องจึง trigger barge-in ก่อนถึง TTS เลยทั้งคู่ — t5/t6/t7 เป็น null ไม่มีเสียงหลุดออกไปจริง safety guard
// กันไว้ได้ แต่เสีย Claude+watchdog cycle ไปฟรี 2 รอบ และ conversation history ถูกแบ่งผิดธรรมชาติ) — fail
// acceptance criterion ที่ตั้งไว้ตั้งแต่ต้น ("ต้องไม่ตัด natural thinking pause") กลับมาใช้ 900ms เหมือน legacy
// ไปก่อน แต่ยังคง mechanism/wiring/log ทั้งหมดไว้ เผื่อวันหลังอยากทดลองค่าอื่น (เช่น 750-800ms) ใหม่
const STT_INTERIM_FINALIZE_MS_CHUNKED = 900

// Commit A (L1b prep, production incident 2026-08-19) — legacy's prewarm consumption เดิม `await myPrewarm`
// ไม่มี deadline เลย ทำให้ speculative optimization กลายเป็น blocking dependency ได้จริง: production trace แสดง
// prewarm ที่ยัง PENDING ตอน final มาถึง กลับทำให้ sttToTwilio ยาวถึง ~11.8s เพราะ Claude ตอบช้าผิดปกติ (tail
// latency) บน request เก่าที่เริ่มไว้ก่อนแล้ว ทั้งที่ตัว prewarm ควรช่วยให้เร็วขึ้นเท่านั้น ไม่ใช่ทำให้ช้าลง
//
// invariant ที่ต้องคง: prewarm อาจลด latency ได้ แต่ต้องไม่ทำให้ latency แย่ลงกว่า main path เดิมเด็ดขาด — จึงจำกัด
// เวลารอ prewarm ที่ยัง pending ไว้ที่ PREWARM_GRACE_MS เท่านั้น หมดเวลาแล้วยัง pending → abort request เดิมทิ้ง
// แล้ว fall through ไปเรียก fresh call ตามปกติ (เหมือนไม่เคยมี prewarm เลย) — ใช้ runAttemptWithWatchdog ตัวเดิม
// (ออกแบบมาสำหรับ compose child abort จาก outer signal + แยก success/aborted/timeout/error อยู่แล้วจาก C4b)
//
const PREWARM_GRACE_MS = 150

// L1b (chunked speculative prewarm, design locked 2026-08-19) — reuse PREWARM_GRACE_MS as the budget for the
// *only* remaining pre-adoption wait: "zero progress at all" (no first delta yet). Any state where a first
// delta already exists adopts immediately instead of waiting (see classifyForAdoption in chunkedSpeculation.js)
// — same invariant as Commit A: speculative optimization must never become a blocking dependency.
const CHUNKED_SPEC_PROGRESS_GRACE_TIMEOUT_MS = PREWARM_GRACE_MS

// Commit A2 — legacy's "fresh" Claude call (ไม่ว่าจะไม่เคยมี prewarm เลย หรือ prewarm miss/timeout จาก Commit A
// แล้วก็ตาม) ยังเป็น `for await (chunk of askClaudeStream(...))` เปล่าๆ ไม่มี deadline เหมือนกัน — production
// data (หลัง Commit A ปิด incident เดิมแล้ว) ยืนยันว่า fresh call ปกติใช้เวลา 1.1-3.4s แต่ยังไม่มี upper bound
// ถ้า Claude เกิด tail latency ซ้ำแบบเดิมอีกจะกลับไปค้างไม่มีขอบเขตเหมือนก่อน Commit A
//
// ตั้งใจไม่ใช้ retry/hedge (เพิ่ม Claude call คู่ขนาน) เพราะเพิ่ม cost + race ใหม่ที่ต้องจัดการโดยไม่จำเป็นตอนนี้ —
// เลือก "timeout แล้วพูด recovery phrase คงที่" แทน (รูปแบบเดียวกับที่ silence-timeout ใช้อยู่แล้ว: บอกลูกค้าตรงๆ
// แล้วรอฟังใหม่ ดีกว่าปล่อยเงียบไม่มีคำตอบ) — genuine Claude error (ไม่ใช่ timeout ไม่ใช่ barge-in) ก็ใช้ recovery
// phrase เดียวกัน เพราะจากมุมลูกค้าทั้งสองกรณีคือ "ไม่ได้คำตอบ" เหมือนกัน
//
// 6000ms กว้างกว่า chunked's CLAUDE_FIRST_DELTA_TIMEOUT_MS (3000ms) มาก เพราะ legacy รอ "คำตอบเต็มก้อน" ไม่ใช่
// token แรกเหมือน chunked — เผื่อ margin ~2x เหนือค่าปกติสูงสุดที่เห็นจริง (3.4s) ก่อน ปรับได้ทีหลังจากข้อมูลจริง
//
// override ผ่าน env var สำหรับเทสเท่านั้น (ต้องตั้งก่อน require ไฟล์นี้ครั้งแรก) — บังคับด้วย NODE_ENV==='test'
// จริงๆ ไม่ใช่แค่ "convention ว่าไม่ตั้งใน production" เฉยๆ เพราะถ้า Render มี env ตัวนี้หลงค้างอยู่ (ผิดพลาดจาก
// การตั้งค่าที่ไหนสักที่) legacy production ทั้งหมดจะ timeout ที่ค่านั้นทันทีโดยไม่มีใครตั้งใจ — validate ว่าต้อง
// เป็นตัวเลขจำกัดค่าและ > 0 ด้วย กัน override ที่ผิดรูปแบบ (เช่น string ว่าง/ติดลบ) หลุดเข้ามาโดยไม่ได้ตั้งใจ
const legacyClaudeTimeoutTestOverride = Number(process.env.LEGACY_CLAUDE_TIMEOUT_MS_OVERRIDE)
const LEGACY_FRESH_CLAUDE_TIMEOUT_MS =
  process.env.NODE_ENV === 'test' && Number.isFinite(legacyClaudeTimeoutTestOverride) && legacyClaudeTimeoutTestOverride > 0
    ? legacyClaudeTimeoutTestOverride
    : 6000

// สั้น เป็นธรรมชาติ ไม่พูดคำว่า "ระบบ"/infrastructure — เป้าหมายคือชวนลูกค้าพูดใหม่ ไม่ใช่อธิบายว่าเกิดอะไรขึ้นข้างใน
const LEGACY_RECOVERY_PHRASE = 'ขอโทษค่ะ เมื่อกี้ตอบช้าไปนิดนึง รบกวนพูดอีกครั้งได้ไหมคะ'

// Checkpoint C5: % rollout ของ chunked-streaming path มาจาก Google Sheets แล้ว (แท็บ "Streaming Config")
// ผ่าน background polling ของตัวเอง ไม่ใช่ hardcoded คงที่แบบ C0 อีกต่อไป — start() ครั้งเดียวตอน module load
// (เท่ากับตอน process bootstrap เพราะไฟล์นี้ถูก require ครั้งเดียวตอนเริ่ม server) ให้เริ่ม poll ทันที ไม่ต้องรอ
// สายแรกเข้ามาก่อนถึงจะ trigger fetch ครั้งแรก — cold start ก่อน fetch สำเร็จครั้งแรกยังคง 0% เสมอ (ปลอดภัยไว้ก่อน)
const rolloutConfig = createRolloutConfig()
rolloutConfig.start()

// Checkpoint C4b — เวลาสูงสุดที่ยอมรอ Claude ส่ง delta แรกของ chunked path มา ก่อนจะเลิกรอแล้ว fallback ไป
// legacy แทน ตั้งไว้เผื่อเหนือ TTFT ปกติของ Claude พอสมควร (ปกติต่ำกว่า 1-2s มาก) แต่ไม่นานจนลูกค้ารอเงียบเกินไป
const CLAUDE_FIRST_DELTA_TIMEOUT_MS = 3000

// Watchdog B — เวลาสูงสุดที่ยอมรอ "safe chunk" ก้อนแรกจาก speechChunker หลังจาก delta แรกมาถึงแล้ว (t3→t4)
// ต้องเผื่อ margin เหนือ speechChunker เอง (HARD_MAX_MS=800 ในนั้น) พอสมควร ไม่งั้น watchdog ภายนอกจะฆ่า turn
// ก่อนที่ chunker จะได้โอกาสทำ fallback boundary ของตัวเองครบตามสัญญา — เผื่อ margin ~2.5 เท่า
const CHUNK_READY_TIMEOUT_MS = 2000

// Watchdog C — เวลาสูงสุดที่ยอมรอ ElevenLabs ส่ง audio ก้อนแรกกลับมาหลังจาก TTS request แรกของทั้งเทิร์นเริ่ม
// (t5→t6) ยังไม่มี baseline ttsTTFB จริงจาก production (rollout ยัง 0%) ใช้ค่าตาม roadmap เดิมไปก่อน — ควร tune
// ด้วยข้อมูลจริงจาก [Metrics] log ก่อนเปิด rollout เกิน 0%
const TTS_FIRST_AUDIO_TIMEOUT_MS = 2000

// Checkpoint C4c follow-up (production incident 2026-08-19, C6c barge-in retry call) — fallback watchdog เดิม
// เป็น total-duration timeout ครอบ "ความพยายาม fallback ทั้งก้อน" เดียว ซึ่งพิสูจน์แล้วจาก production ว่าอันตราย:
// ถ้า audio ก้อนแรกคอมมิตไปถึงลูกค้าแล้วก่อน 8s แต่ ElevenLabs stream ที่เหลือช้า/ค้าง watchdog เดิมจะ abort()
// signal ที่ synthesizeAndSend ใช้อยู่ ตัดเสียงกลางประโยคทั้งที่ลูกค้าได้ยินไปบางส่วนแล้ว แถม runAttemptWithWatchdog
// ทิ้งผลลัพธ์ของฝ่ายแพ้ race เสมอ (by design ถูกต้องสำหรับ watchdog A/B/C ที่ไม่มีอะไรคอมมิตมาก่อน) ทำให้
// totalSent/fullText ของ audio ที่ส่งไปแล้วจริงหายไปเป็น 0/'' ด้วย — turn ถัดไปไม่รู้ว่า AI เพิ่งพูดอะไรไป
//
// แก้เป็น 2 phase แยกจากกันชัดเจน ผ่าน armWatchdog rearm (กลไกเดียวกับ Watchdog A→B→C):
//   Phase 1 (pre-commit)  — FALLBACK_PRECOMMIT_TIMEOUT_MS ตั้งแต่ fallback เริ่ม จนกว่าจะมี audio commit ก้อนแรก
//                           ถ้าหมดเวลาก่อน commit: abort ได้เต็มที่เหมือนเดิม (audioCommitted ยังเป็น false แน่นอน)
//   Phase 2 (post-commit) — FALLBACK_IDLE_TIMEOUT_MS แบบ rolling (rearm ใหม่ทุกครั้งที่มีก้อนเสียงส่งสำเร็จ ผ่าน
//                           onAudioSent hook ใน chunkedTurn.js) วัด "หยุดนิ่งไปนานแค่ไหน" ไม่ใช่ "รวมนานแค่ไหน"
//                           เพราะเทิร์นที่พูดยาวโดยธรรมชาติ (>8s ทั้งเทิร์น) ต้องไม่ถูกตัดกลางคันทั้งที่ progress ปกติ
//
// commit (turnState.audioCommitted) คือ source of truth เดียวที่บอกว่าอยู่ phase ไหน — ไม่ใช่ timestamp/ตัวนับเอง
const FALLBACK_PRECOMMIT_TIMEOUT_MS = 8000
const FALLBACK_IDLE_TIMEOUT_MS = 6000

// L1b — populate prewarm telemetry จาก handle เดียวกันไม่ว่า outcome จะเป็นอะไร (ADOPT_NOW/DROP/GRACE_HIT/
// MISMATCH_FRESH ฯลฯ) — ต้องเรียกให้ครบทุก branch ที่มี speculation จริง ไม่ใช่แค่ตอน exact match เท่านั้น
// เพราะข้อมูล "เสีย speculative work ไปเท่าไรตอน miss" (มี delta แล้วหรือยัง/มี chunk พร้อมหรือยัง) สำคัญพอกับ
// ตอน hit สำหรับ tuning hit-rate ในอนาคต (blocker จาก review รอบ commit gate — MISMATCH_FRESH เคย populate
// แค่ prewarmOutcome เฉยๆ ทิ้ง field อื่นที่มีความหมายจริงไปหมด)
function recordChunkedPrewarmMetrics(metrics, handle, finalAcceptedAt, outcome) {
  metrics.prewarmOutcome = outcome
  metrics.prewarmStartedAt = handle.startedAt
  metrics.prewarmAgeAtFinalMs = Math.round(finalAcceptedAt - handle.startedAt)
  metrics.prewarmFirstDeltaMs = handle.producer.firstDeltaAt != null ? Math.round(handle.producer.firstDeltaAt - handle.startedAt) : null
  metrics.prewarmFirstChunkMs = handle.producer.firstChunkAt != null ? Math.round(handle.producer.firstChunkAt - handle.startedAt) : null
  metrics.prewarmBufferedChunks = handle.producer.queue.length
}

// Design B (short-ack lifecycle fix, production incident 2026-08-20, design rounds 1-6) — คำรับคำสั้นที่ลูกค้า
// พูดจริง (ครับ/ค่ะ/โอเค/ok ฯลฯ) เคยหายไปเงียบๆ เพราะ echo/short-fragment filter ที่ตั้งใจกัน PSTN echo ของ AI
// เอง — แก้ด้วย whitelist ที่แยก 2 tier ตามโครงสร้างคำ (ไม่ใช่ความยาว เพราะภาษาไทยไม่มี space ระหว่างคำ ทำให้
// wordCount เดิมไม่มีความหมาย): Tier 1 (คำผสม เช่น "โอเคครับ") เสี่ยง false-echo ต่ำกว่า อนุญาต bargeIn ทันทีได้
// แม้ระหว่าง AI พูดอยู่ ส่วน Tier 2 (particle เดี่ยว เช่น "ครับ"/"ค่ะ") คือคำที่ AI เองพูดลงท้ายประโยคบ่อยที่สุด
// จึงเสี่ยง false-echo สูงสุด — ต้อง defer ไว้จนกว่าจะยืนยันได้ว่า AI พูดจบจริง (ผ่าน owned mark หรือ no-audio
// completion) ก่อนค่อยส่งเป็น turn ใหม่ ไม่ bargeIn ทันทีเด็ดขาด
//
// normalizeForClassification ใช้เปรียบเทียบกับ whitelist เท่านั้น — ค่าที่เก็บใน pendingShortAck/ส่งเข้า Claude
// ยังเป็น raw text จาก STT เป๊ะเสมอ ไม่ตัด punctuation/case ทิ้งจากข้อความจริง
function normalizeForClassification(text) {
  return text
    .trim()
    .replace(/[.!?,]+$/, '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '')
}

const TIER1_ACKS = new Set(
  ['โอเคครับ', 'โอเคค่ะ', 'ใช่ครับ', 'ใช่ค่ะ', 'ไม่ครับ', 'ไม่ค่ะ', 'ไม่มีแล้ว', 'ok ครับ', 'ok ค่ะ']
    .map(normalizeForClassification)
)
const TIER2_ACKS = new Set(
  ['ครับ', 'ค่ะ', 'ใช่', 'ไม่', 'โอเค', 'ok']
    .map(normalizeForClassification)
)

function classifyAck(rawText) {
  const norm = normalizeForClassification(rawText)
  if (TIER1_ACKS.has(norm)) return 'TIER1'
  if (TIER2_ACKS.has(norm)) return 'TIER2'
  return null
}

// Lightweight Post-Mark Echo Guard (design locked) — replaces the old "short = suspicious"
// heuristic (whitespace word-count + char length), which is invalid for Thai: Thai doesn't
// space-delimit words, so a real, complete short answer like "สะดวกค่ะ" was indistinguishable
// from a meaningless echo fragment by that measure alone and got dropped as a false positive
// (verified against real production log: utteranceId=1, "สะดวกค่ะ" dropped as POST_MARK_ECHO).
// New default: ECHO EVIDENCE REQUIRED TO DROP, not SHORT = SUSPICIOUS. Deterministic, synchronous,
// bounded string comparison only — no network/LLM/async, must never add latency to the decision path.
const ECHO_TAIL_CHARS = 100

function normalizeForEchoCompare(text) {
  return (text || '')
    .replace(/[.!?,]+/g, '')
    .replace(/\s+/g, '')
    .toLowerCase()
}

// Review Blocker 2 fix — a bare "any suffix >= 2 chars" match is not strong evidence: Thai sentences
// overwhelmingly end in one of a small closed set of clause-final politeness particles (ค่ะ/ครับ/คะ/...),
// so a customer's genuine short answer sharing just that trailing particle with the AI's own sentence
// (e.g. AI "...แจ้งได้ค่ะ" / customer "ได้ค่ะ") is common, expected, and NOT echo — only real reproduction
// of the AI's actual CONTENT (beyond the closing particle) counts as strong evidence. Longest-particle-first
// so "นะคะ" is matched whole rather than leaving a dangling "นะ" after stripping "คะ" alone.
const TRAILING_PARTICLES = ['นะคะ', 'นะครับ', 'ค่ะ', 'ครับ', 'ค่า', 'คะ', 'จ้ะ', 'จ้า', 'จ๊ะ']
const MIN_ECHO_CONTENT_CHARS = 5 // ระยะห่างจริงระหว่างเคส ACCEPT ("สนใจ"=4 หลังตัด particle) กับเคส DROP ("คุยไหม"=6) ที่ยืนยันจากตัวอย่างจริง

// Active-Playback Speech Guard R1 (design locked) — 2-signal interim barge-in confirmation window. A
// candidate interim expires (resets to a fresh candidate) if the next coherent interim doesn't arrive within
// this many ms of the first one. Initial tuning value from observed production interim cadence (~100-400ms
// between consecutive interims) with margin — NOT proven-optimal, needs production measurement to refine.
const BARGE_CONFIRM_WINDOW_MS = 1000

function stripTrailingParticle(text) {
  for (const p of TRAILING_PARTICLES) {
    if (text.endsWith(p) && text.length > p.length) return text.slice(0, -p.length)
  }
  return text
}

// หลักฐาน echo ที่หนักแน่นคือ "เนื้อหา" ของ transcript (ไม่นับ particle ลงท้ายทั่วไป) เป็นหางเนื้อหา
// ของสิ่งที่ AI เพิ่งพูดจริง (exact suffix match หลัง normalize+ตัด particle) และยาวพอจะไม่ใช่คำร่วมโดย
// บังเอิญ — ถ้าไม่ตรงตามนี้ไม่ถือเป็น echo แม้จะสั้นหรือใช้คำร่วมกับสิ่งที่ AI เพิ่งพูดก็ตาม (เช่น ลูกค้า
// ตอบ "สะดวกค่ะ"/"สนใจค่ะ"/"ได้ค่ะ" ต่อคำถามที่ลงท้ายด้วยคำเดียวกันต้องไม่ถูกทิ้ง)
//
// Active-Playback Speech Guard R1 (design locked) — reused as-is (not duplicated) for the isSpeaking=true
// case too: same "echo evidence required to drop" principle, just compared against a different reference
// (activeSpokenRef — AI text whose audio has actually started being sent, while still speaking — instead of
// lastMarkedSpokenText — AI text confirmed fully played, within 500ms after its mark). Two different
// reference lifetimes, one shared comparison algorithm. `recentAiSpokenText` being null/empty (no reference
// available yet) always returns false — ACCEPT by default, never fall back to a length/word-count heuristic.
function isLikelyPostMarkEcho(transcript, recentAiSpokenText) {
  const t = stripTrailingParticle(normalizeForEchoCompare(transcript))
  if (t.length < MIN_ECHO_CONTENT_CHARS) return false // เนื้อหาสั้นเกินจะเป็นหลักฐานได้ — ไม่ทิ้งโดยไม่มีหลักฐาน
  const aiTail = stripTrailingParticle(normalizeForEchoCompare(recentAiSpokenText).slice(-ECHO_TAIL_CHARS))
  if (!aiTail) return false // ไม่มี AI text ให้เทียบ (เช่น เทิร์นแรกสุดของสาย) — ACCEPT เสมอ ไม่ fallback ไป heuristic เดิม
  return aiTail.endsWith(t)
}

// Owned-mark encoding (design round 5-6) — mark เดิมเป็นแค่ชื่อ (เช่น "ai_done") ไม่มี owner identity เลย ทำให้
// mark ที่มาช้า (จาก pipeline เก่าที่ถูก barge-in ไปแล้ว หรือ turn อื่นที่เพิ่งจบ) มา unlock isSpeaking/consume
// pendingShortAck ของ pipeline ปัจจุบันผิดตัวได้ (race จริงที่ยืนยันจาก code review) — ฝัง owner id (activePipelineId
// ตอนที่ mark session เริ่ม) ต่อท้ายชื่อ mark เป็น "kind:ownerId" แล้วตรวจตอนรับกลับว่าตรงกับ activePipelineId
// ปัจจุบันจริงก่อนทำ side effect ใดๆ — KNOWN_MARK_KINDS + positive-integer-only owner กัน mark ที่ผิดรูปแบบ
// (mismatched kind, "0", ลบ, ทศนิยม, ไม่ใช่ตัวเลข) หลุดเข้ามาโดยไม่ตั้งใจ
const KNOWN_MARK_KINDS = new Set(['ai_done', 'greeting_done', 'silence_done'])

function ownedMarkName(kind, ownerId) {
  return `${kind}:${ownerId}`
}

function parseMarkName(name) {
  if (typeof name !== 'string') return { kind: null, ownerId: null }
  const idx = name.lastIndexOf(':')
  const kind = idx === -1 ? name : name.slice(0, idx)
  if (!KNOWN_MARK_KINDS.has(kind)) return { kind: null, ownerId: null }
  const ownerIdStr = idx === -1 ? '' : name.slice(idx + 1)
  if (!/^[1-9]\d*$/.test(ownerIdStr)) return { kind, ownerId: null }
  const ownerId = Number(ownerIdStr)
  // regex เดียวยอมให้เลขยาวเกิน Number.MAX_SAFE_INTEGER ผ่านมาได้ (เช่น "9007199254740992" ยัง match ^[1-9]\d*$)
  // แต่ Number(...) จะปัดเศษ/สูญเสีย precision จริง ทำให้ ownerId ที่ได้อาจไม่ตรงกับ activePipelineId ที่ตั้งใจ
  // ส่งมาเป๊ะ ต้องเช็ค safe-integer ซ้ำอีกชั้นหลังแปลงแล้วเสมอ
  if (!Number.isSafeInteger(ownerId) || ownerId <= 0) return { kind, ownerId: null }
  return { kind, ownerId }
}

function shouldBlockEndCall(session, aiResponse) {
  const userMessages = session.messages.filter(m => m.role === 'user')
  const lastUserMsg = userMessages.at(-1)?.content ?? ''
  const hasNegation = lastUserMsg.includes('ไม่') || lastUserMsg.includes('ยังไม่')
  const hasInterest = ['สนใจ', 'อยากลอง', 'อยากสมัคร', 'สมัครเลย'].some(k => lastUserMsg.includes(k))
  if (!hasInterest || hasNegation) return false
  return !aiResponse.includes('เพิ่มเติม')
}

// Checkpoint C4a — เมื่อ chunked path พังก่อน commit เสียง ลอง fallback ไปใช้ Claude/TTS เดิม (legacy)
// สำหรับเทิร์นเดียวกันนี้ แทนที่จะปล่อยให้ลูกค้าเงียบไปเฉยๆ ไม่ bump generation เพราะนี่คือการกู้ turn เดิม
// ไม่ใช่ turn ใหม่ — เป็น adapter ระหว่าง legacy transport ([END_CALL] string marker) กับรูปแบบที่ chunked
// branch ใช้อยู่แล้ว (endCallRequested boolean) ก่อน TTS เสมอ กัน marker หลุดเข้าไปให้ speakFixedText() พูดออกไปจริง
async function runLegacyFallback({ session, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onAudioSent, onChunkAudioStart }) {
  let rawText = null
  for await (const chunk of askClaudeStream(session, false, signal)) {
    if (signal.aborted) break
    rawText = (rawText ? rawText + ' ' : '') + chunk
  }
  if (!rawText || signal.aborted) return { fullText: '', endCallRequested: false, totalSent: 0 }

  const legacyEndCallRequested = rawText.includes('[END_CALL]')
  const spokenText = rawText.replace(/\[END_CALL\]/g, '').trim()

  if (!spokenText || !isCurrentGeneration(callState, generationId)) {
    return { fullText: spokenText, endCallRequested: legacyEndCallRequested, totalSent: 0 }
  }

  const result = await speakFixedText({ text: spokenText, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, startingSentCount: 0, onAudioSent, onChunkAudioStart })
  return { fullText: spokenText, endCallRequested: legacyEndCallRequested, totalSent: result.sentCount }
}

// ใช้ policy เดียวกันไม่ว่า end_call intent จะมาจาก tool call ปกติของ chunked path หรือจาก [END_CALL] marker
// ที่ normalize มาจาก legacy fallback แล้ว — ทั้งสองทางเข้าที่นี่ในรูป endCallRequested boolean เดียวกันเสมอ
async function applyChunkedEndCallGuard({ endCallRequested, fullText, totalSent, currentSession, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onChunkAudioStart }) {
  if (!endCallRequested || !shouldBlockEndCall(currentSession, fullText)) {
    return { fullText, endCallRequested, totalSent }
  }
  console.log('[Guard] Premature END_CALL blocked — injecting follow-up question (chunked)')
  const followUp = 'มีอะไรสอบถามเพิ่มเติมไหมคะ'
  let newTotalSent = totalSent
  try {
    const followUpResult = await speakFixedText({ text: followUp, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, startingSentCount: totalSent, onChunkAudioStart })
    newTotalSent += followUpResult.sentCount
  } catch (err) {
    if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
      console.error('[Guard TTS error]', err.message)
      healthMonitor.reportError('tts', err.message)
    }
  }
  return { fullText: fullText.trim() + ' ' + followUp, endCallRequested: false, totalSent: newTotalSent }
}

// Track M Review Fix 1 (2026-08-22) — `value && typeof value === 'object'` alone lets a structurally
// malformed chunkReasonStats payload (wrong enum string, non-numeric/NaN/negative in a numeric field,
// non-boolean flag) still pass through and get written into turnMetrics. Validate every field BEFORE
// assigning any of them — atomic: if one field fails, the whole payload is rejected and all 6 fields stay
// at their turnMetrics null default, never a partial mix of real + garbage values. Checks reason against
// CHUNK_REASON (imported from speechChunker.js, the single source of truth) rather than a hardcoded string
// list, so this can never silently drift from the real enum.
// Track N (design R6 LOCKED 2026-08-22) — extended with the 7th field, firstSafeTrigger, same atomic rule.
const VALID_CHUNK_REASONS = new Set(Object.values(CHUNK_REASON))
const VALID_FIRST_SAFE_TRIGGERS = new Set(['DELTA', 'HARD_MAX_TIMER'])
function isValidChunkReasonStats(value) {
  if (!value || typeof value !== 'object') return false
  if (!VALID_CHUNK_REASONS.has(value.reason)) return false
  if (!Number.isFinite(value.charCount) || value.charCount < 0) return false
  if (!Number.isInteger(value.deltaCount) || value.deltaCount < 1) return false
  if (!Number.isFinite(value.firstCandidateElapsedMs) || value.firstCandidateElapsedMs < 0) return false
  if (typeof value.numericProtectionBlocked !== 'boolean') return false
  if (!Number.isFinite(value.preSafeDeltaGapMs) || value.preSafeDeltaGapMs < 0) return false
  if (!VALID_FIRST_SAFE_TRIGGERS.has(value.firstSafeTrigger)) return false
  return true
}

// Track O0 Review Fix 1 (2026-08-25) — original wiring checked only `typeof value === 'object'` then wrote
// each field independently via `?? null`, which only catches null/undefined and lets any other type (string,
// NaN, negative) flow straight into turnMetrics, contradicting the "malformed payload → both fields stay at
// their null default" contract the comment claimed. Same atomic-validate-before-any-write pattern as
// isValidChunkReasonStats above — a non-negative finite integer or null is valid, anything else rejects the
// whole payload (both fields stay at their prior/default value, never a partial mix).
function isValidCacheToken(v) {
  return v === null || (Number.isInteger(v) && v >= 0)
}
function isValidCacheUsage(value) {
  if (!value || typeof value !== 'object') return false
  if (!isValidCacheToken(value.cacheCreationInputTokens)) return false
  if (!isValidCacheToken(value.cacheReadInputTokens)) return false
  return true
}

// All-Campaigns L2b + STT-A2 (2026-08-25) — shared campaign-match predicate for legacy_early_tts_campaign_id
// and stt_a2_campaign_id ONLY (legacy_observed_* and stt_a2_shadow_* keep their original inline exact-match
// checks unchanged — explicitly out of scope). `configCampaignId == null` (missing/empty Sheet cell) never
// matches anything — the existing fail-closed rule that "no campaign_id" must never mean "all campaigns" is
// preserved exactly as before. `'*'` is treated as an explicit, ordinary non-empty campaignId value by the
// classifier in rolloutConfig.js (no special-casing needed there — confirmed by inspection: percent>0 only
// requires a non-empty campaignId, and '*' is non-empty) — the wildcard semantics live entirely here, at the
// point where a config's campaignId is compared against the session's actual campaign id.
function isCampaignMatched(configCampaignId, sessionCampaignId) {
  if (configCampaignId == null) return false
  if (configCampaignId === '*') return sessionCampaignId != null
  return sessionCampaignId === configCampaignId
}

function registerWebSocket(fastify) {
  fastify.get('/stream', { websocket: true }, (connection, req) => {
    const socket = (typeof connection.send === 'function') ? connection
      : (typeof req?.send === 'function') ? req
      : (connection.socket || connection)

    const rawUrl = (connection?.url || req?.url || '')
    const qs = rawUrl.includes('?') ? rawUrl.split('?')[1] : ''
    let callSid = new URLSearchParams(qs).get('callSid')
    let streamSid = null
    let sttStream = null
    let isSpeaking = false
    let callActive = true
    let greetingAbortController = null  // barge-in: cancel greeting audio
    let ttsAbortController = null       // barge-in: cancel STT→TTS pipeline
    let sttProcessing = false     // mutex: ป้องกัน concurrent Claude calls
    let bargeInCooldown = false   // cooldown หลัง barge-in ป้องกัน echo false-trigger
    let silenceTimer = null
    let silencePromptCount = 0
    let durationTimer = null
    let lastMarkTime = 0
    // Lightweight Post-Mark Echo Guard fix (Review Blocker 1) — session.messages is NOT a reliable source of
    // "what was actually just played," because silence prompts (`silence_done`) speak fixed text that is never
    // pushed into conversation history. Track the text tied to the OWNED pipeline about to speak at each of
    // the 3 owned-mark sites (ai_done/greeting_done/silence_done) instead, and only promote it into the value
    // POST_MARK_ECHO actually compares against once the existing owner-verification check (below, at the
    // `msg.event === 'mark'` handler) confirms the mark that came back really belongs to that same pipeline —
    // a stale/mismatched mark can never move this reference (same invariant `isSpeaking`/`lastMarkTime` already rely on).
    let pendingSpokenText = null   // { pipelineId, text } — set right before sending an owned mark
    let lastMarkedSpokenText = null // promoted from pendingSpokenText only on verified-owner mark — what POST_MARK_ECHO compares against
    // Active-Playback Speech Guard R1 (design locked) — separate lifecycle from pendingSpokenText/
    // lastMarkedSpokenText above (those two are POST-mark, i.e. after AI finished playing). activeSpokenRef
    // covers the DURING-playback window (isSpeaking still true): { pipelineId, text }, appended only via
    // noteActiveSpokenChunk() at the moment a chunk's audio has actually started being sent to Twilio (see
    // onChunkAudioStart in chunkedTurn.js) — never from Claude-generated text alone, which may not have
    // reached TTS yet (that would risk suppressing a customer who is legitimately asking about something the
    // AI hasn't said out loud yet). Bounded to ECHO_TAIL_CHARS, reset per-pipeline (never merges across a
    // barge-in boundary) — see noteActiveSpokenChunk() below.
    let activeSpokenRef = null
    // 2-signal interim barge-in confirmation (R1) — { pipelineId, previousText (normalized), firstAt } for the
    // isSpeaking=true interim path. A single interim never barges in on its own anymore; a second COHERENT
    // interim (same normalized text, or a forward extension of it) within BARGE_CONFIRM_WINDOW_MS does. Reset
    // whenever the pipeline changes, the window expires, or the interim isn't a coherent continuation.
    let bargeCandidate = null
    let pendingEndCall = false
    let pendingTranscript = null  // C6c follow-up: barge-in ที่มาถึงตอนเทิร์นเดิมยังไม่ปล่อย sttProcessing — ช่องเดียว, latest-wins (ดูหมายเหตุที่ processTranscript())
    let bargeInPendingFinal = false  // C6c follow-up (STT listening): true หลัง interim trigger bargeIn() ไปแล้ว — บอก onTranscript ว่า final ตัวถัดไปคือประโยคที่พูดแทรกจริง (ดูหมายเหตุที่ onTranscript ด้านล่าง)
    let activePipelineId = 0
    let pendingShortAck = null       // Design B: { text, pipelineId, capturedAt } — Tier2 ack ที่ deferred ไว้ระหว่าง AI พูด รอ owned mark/no-audio completion ของ pipeline เดียวกัน
    let processTranscriptDispatch = null  // Design B: ref ไปยัง processTranscript (ประกาศใน start block) ให้ mark handler/no-audio sites อื่นที่อยู่นอก scope นั้นเรียกได้ โดยไม่ต้อง hoist ฟังก์ชันใหญ่ทั้งก้อน
    let prewarmPromise = null    // pre-warmed Claude response Promise<string|null>
    let prewarmStartText = null  // interim text that triggered prewarm
    let prewarmAbort = null      // AbortController for prewarm call
    let prewarmStartedAt = 0     // performance.now() ตอนเริ่ม request จริง — สำหรับ telemetry (prewarmAgeMs), คนละตัวกับ prewarmRetriggerAt (Date.now()-based throttle)
    let prewarmRetriggerAt = 0   // เวลาต่ำสุดที่อนุญาตเดาใหม่รอบถัดไป (throttle กันยิง Claude ถี่เกิน)
    // Track P Prewarm Diagnostics (design revision 2026-08-22, Design Review R3 PASS) — diagnostic-only,
    // ungated, legacy-prewarm-only (chunked never calls startPrewarm() at all, see onInterim below — this
    // stays structurally null for every chunked call, no extra guard needed there). NOT reset by
    // clearPrewarm() — that function is transport cleanup only, called mid-lifecycle on every retrigger (see
    // startPrewarm() below), so diagnostic state has to live entirely separate from it or every retrigger
    // would wipe retriggerCount/initialTriggerAt right when we want them to accumulate. See settlePrewarmDiag()
    // for the only place this is ever settled/reset — { prewarmDiagId, initialTriggerAt, initialTriggerText,
    // retriggerCount, settled, currentAttempt: { triggerAt, triggerText, settledAt, resultState } } while
    // active; currentAttempt is a FRESH object every real (re)trigger, never mutated across retriggers — that
    // per-attempt reference (not the outer lifecycle object) is what a superseded async attempt's identity
    // guard checks against.
    let prewarmDiag = null
    let prewarmDiagIdCounter = 0   // connection-scoped, increments once per NEW lifecycle (not per retrigger)
    // L1b — chunked speculative prewarm: คนละ mechanism จาก prewarmPromise ข้างบน (Promise<string|null> เดิม
    // เป็นของ legacy เท่านั้น) ไม่แชร์ clock/state กันเลยแม้ในทางปฏิบัติสายหนึ่งจะใช้แค่ทางใดทางหนึ่งเสมอ (rollout
    // freeze ครั้งเดียวต่อสาย) — แยกไว้ชัดเจนกันความสับสน/regression ในอนาคตหากมีคนแก้แค่ฝั่งเดียว
    let chunkedPrewarmHandle = null   // { transcript, producer, abortController, startedAt, adopted, generationGuard }
    let chunkedSpecRetriggerAt = 0    // throttle deadline แยกจาก prewarmRetriggerAt โดยตั้งใจ

    // Checkpoint C0: safety infrastructure ของ B.5 ผูกไว้กับสายนี้แล้ว แต่ยังไม่มีจุดไหนอ่าน/ใช้งานจริง
    // (ไม่มี chunked path ให้ guard ในไฟล์นี้เลยตอนนี้) — legacy path ทั้งหมดด้านล่างยังทำงานเหมือนเดิมทุกบรรทัด
    const callState = createCallState()
    let rollout = null // freeze ตอน 'start' event เพราะ callSid อาจยังไม่ resolve ตอน connection เปิด
    // L2a production exposure gate (design revision 2026-08-20) — freeze ครั้งเดียวพร้อมกับ rollout ข้างบน
    // ไม่คำนวณใหม่กลางสาย เหมือนกัน — false/null เป็นค่าเริ่มต้นปลอดภัย (CONTROL) ก่อนถึง 'start' event เสมอ
    let legacyObserved = false
    let legacyObservedBucket = null
    let legacyObservedPercentAtStart = null
    let legacyObservedCampaignMatched = null
    // L2b production exposure gate (design revision 2026-08-21) — freeze ครั้งเดียวพร้อมกับ rollout/legacyObserved
    // ข้างบน same หลักการเป๊ะ ห้ามคำนวณใหม่กลางสาย — false/null ปลอดภัยเสมอก่อนถึง 'start' event
    let legacyEarlyTts = false
    let legacyEarlyTtsBucket = null
    let legacyEarlyTtsPercentAtStart = null
    let legacyEarlyTtsCampaignMatched = null
    // STT-A2 diagnostic gate (design revision 2026-08-21) — freeze ครั้งเดียวพร้อมกับตัวอื่นข้างบน same หลักการ
    // เป๊ะ ห้ามคำนวณใหม่กลางสาย — false/null ปลอดภัยเสมอก่อนถึง 'start' event — independent จาก chunked/L2a/L2b
    // โดยสิ้นเชิง (ไม่ผูก precedence chain ใดๆ กับสามตัวนั้นเลย ตามที่ล็อกไว้ตอน design)
    let sttA2 = false
    let sttA2Bucket = null
    let sttA2PercentAtStart = null
    let sttA2CampaignMatched = null
    // A2.1 Shadow Google Final Diagnostics gate (design revision 2026-08-21, Design Gate v2 PASS) — freeze
    // ครั้งเดียวพร้อมกับตัวอื่นข้างบน same หลักการเป๊ะ — false/null ปลอดภัยเสมอก่อนถึง 'start' event — independent
    // gate จาก A2 เอง (คนละ Sheet keys/bucket namespace) แต่ activation ยัง require sttA2===true เพิ่มด้วย (ดู
    // 'start' handler ด้านล่าง) — ไม่ reuse stt_a2_percent/stt_a2_campaign_id เด็ดขาด (Design Review Blocker 1)
    let sttA2Shadow = false
    let sttA2ShadowBucket = null
    let sttA2ShadowPercentAtStart = null
    let sttA2ShadowCampaignMatched = null

    console.log(`[WS] Connected callSid=${callSid}`)

    function clearSilenceTimer() {
      if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null }
    }

    function startSilenceTimer() {
      clearSilenceTimer()
      if (!callActive || isSpeaking || sttProcessing) return
      silenceTimer = setTimeout(handleSilence, 8000)
    }

    function clearDurationTimer() {
      if (durationTimer) { clearTimeout(durationTimer); durationTimer = null }
    }

    // ตัดจบสายอัตโนมัติเมื่อคุยนานเกิน MAX_CALL_DURATION_SECONDS กันสายค้าง/ค่าใช้จ่ายบานปลาย
    async function handleMaxDuration() {
      durationTimer = null
      if (!callActive || pendingEndCall) return
      console.log(`[MaxDuration] Call exceeded ${MAX_CALL_DURATION_MS / 1000}s — closing`)

      const currentSession = callSessions.get(callSid)
      pendingEndCall = true
      if (currentSession) currentSession.hangupReason = 'max_duration'

      clearSilenceTimer()
      if (prewarmDiag) settlePrewarmDiag(prewarmDiag, 'CALL_ENDED') // Track P: settle before transport cleanup below
      clearPrewarm()
      abortChunkedSpeculation()
      if (greetingAbortController) { greetingAbortController.abort(); greetingAbortController = null }
      if (ttsAbortController) { ttsAbortController.abort(); ttsAbortController = null }

      if (!currentSession || socket.readyState !== socket.OPEN) {
        if (socket.readyState === socket.OPEN) socket.close()
        return
      }

      isSpeaking = true
      sttProcessing = true
      const closeAbort = new AbortController()
      let sent = 0
      try {
        const closingText = 'ขอบคุณมากนะคะที่สละเวลาคุยกับหนู หนูขอตัวก่อนนะคะ'
        for await (const chunk of synthesizeSpeechStream(closingText, currentSession.campaign.voice_id, closeAbort.signal)) {
          if (socket.readyState !== socket.OPEN) break
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          sent++
        }
      } catch (err) {
        console.error('[MaxDuration TTS error]', err.message)
        healthMonitor.reportError('tts', err.message)
      } finally {
        sttProcessing = false
      }

      const closeDelay = sent * 20 + 4000
      setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, closeDelay)
    }

    function isPrewarmUsable(interimText, finalText) {
      if (!interimText || !finalText) return false
      const a = interimText.trim(), b = finalText.trim()
      if (a.length >= 2 && (b.includes(a) || a.includes(b))) return true
      const n = Math.min(4, a.length, b.length)
      return n >= 2 && a.substring(0, n) === b.substring(0, n)
    }

    // Track P (design revision 2026-08-22, Design Review R3 PASS) — diagnostic-only relation classifier
    // between the last prewarm attempt's trigger text and the actual final transcript. Deliberately a
    // SEPARATE function from isPrewarmUsable() above — never called from it, never referenced by the
    // production decision at all — mirrors isPrewarmUsable()'s exact two-branch structure (EXACT is a named
    // special-case of the first branch) so its output is directly comparable to what production actually
    // decided, without being able to influence it.
    function classifyPrewarmTextRelation(prewarmText, finalText) {
      if (!prewarmText || !finalText) return 'MISMATCH'
      const a = prewarmText.trim(), b = finalText.trim()
      if (a === b) return 'EXACT'
      if (a.length >= 2 && (b.includes(a) || a.includes(b))) return 'CONTAINS'
      const n = Math.min(4, a.length, b.length)
      if (n >= 2 && a.substring(0, n) === b.substring(0, n)) return 'PREFIX_HEAD'
      return 'MISMATCH'
    }

    // Adaptive re-trigger: ถ้าลูกค้าพูดยาวกว่าที่เดาไว้พอสมควร ยกเลิกคำเดาเก่าแล้วเดาใหม่จากข้อความล่าสุด
    // เดาแค่ครั้งเดียวตอนแรกมักพลาดเวลาลูกค้าพูดยาวกว่านั้น (isPrewarmUsable ปฏิเสธ ต้องเริ่มนับ latency ใหม่ทั้งหมด)
    //
    // เคยลองเปลี่ยนเป็น debounce (รอ interim นิ่ง 350ms ก่อนค่อยยิง) เมื่อ 18 ส.ค. แล้ว rollback กลับมาใช้แบบนี้
    // เพราะวัดผลจริงจาก log พบว่า debounce แย่ลง (median ช่วงรอคำตอบพร้อมเพิ่มจาก 1.8s เป็น 3.0s) — สาเหตุคือระหว่าง
    // ลูกค้าพูดต่อเนื่อง STT ส่ง interim ใหม่มาเรื่อยๆ แทบไม่หยุดนิ่งเลยจนพูดจบจริง debounce เลย "นิ่ง" แทบไม่ทัน
    // และเสียข้อดีเดิมที่การเดาตัวแรกมักไม่โดนยกเลิก (ขยับทีละน้อยกว่า 4 ตัวอักษร) จึงสะสม head start มาตลอดทั้งประโยคได้
    // L1b: parameterize เป็น retriggerAtMs (แทนอ่าน prewarmRetriggerAt จาก closure ตรงๆ) — ให้ chunked speculation
    // ใช้ throttle clock ของตัวเอง (chunkedSpecRetriggerAt) ได้โดยไม่ผูกกับ legacy prewarm's clock โดยไม่ตั้งใจ
    // legacy call site (startPrewarm ด้านล่าง) ยังส่ง prewarmRetriggerAt ตัวเดิมเข้ามาเหมือนเดิมทุกประการ —
    // พฤติกรรม legacy ไม่เปลี่ยนแม้แต่นิดเดียว
    function shouldRetriggerPrewarm(oldText, newText, retriggerAtMs) {
      const oldLen = (oldText || '').trim().length
      const newLen = (newText || '').trim().length
      if (newLen - oldLen < 4) return false // เพิ่มขึ้นน้อยไป ไม่คุ้มยิงใหม่ (แค่ STT ขยับคำเล็กน้อย)
      if (Date.now() < retriggerAtMs) return false // กันยิง Claude รัวๆ ระหว่างลูกค้าพูดยาว
      return true
    }

    function startPrewarm(session, interimText) {
      if (!callActive || isSpeaking || sttProcessing) return
      if (prewarmPromise) {
        if (!shouldRetriggerPrewarm(prewarmStartText, interimText, prewarmRetriggerAt)) return
        console.log(`[Prewarm] Re-trigger — interim grew: "${prewarmStartText}" → "${interimText}"`)
        clearPrewarm()
      }
      prewarmRetriggerAt = Date.now() + 700
      // Track P: one capture reused for prewarmStartedAt AND the diagnostic trigger timestamps below — never
      // call performance.now() a second time to fabricate an "equivalent" moment for the same real event.
      const startedAt = performance.now()
      prewarmStartedAt = startedAt
      prewarmStartText = interimText
      prewarmAbort = new AbortController()
      const signal = prewarmAbort.signal
      const snap = { ...session, messages: [...session.messages, { role: 'user', content: interimText }] }
      console.log(`[Prewarm] Starting for: "${interimText}"`)

      // Track P — diagnostic-only lifecycle/per-attempt state. currentAttempt is a FRESH object every real
      // trigger (first trigger and every retrigger alike), never mutated across retriggers — a superseded
      // attempt's captured `myAttempt` reference stops matching `prewarmDiag.currentAttempt` the instant a
      // retrigger replaces it, regardless of clearTimeout/abort timing (same identity-binding lesson as
      // A2.1's settleShadow fix — checking the outer object's identity alone is not enough when the outer
      // object gets mutated in place across retriggers rather than replaced).
      if (!prewarmDiag) {
        prewarmDiag = {
          prewarmDiagId: ++prewarmDiagIdCounter,
          initialTriggerAt: startedAt,
          initialTriggerText: interimText,
          retriggerCount: 0,
          settled: false,
          currentAttempt: null,
        }
      } else {
        prewarmDiag.retriggerCount++
      }
      prewarmDiag.currentAttempt = { triggerAt: startedAt, triggerText: interimText, settledAt: null, resultState: 'PENDING' }
      const myLifecycle = prewarmDiag
      const myAttempt = prewarmDiag.currentAttempt
      const settleAttempt = (resultState) => {
        if (prewarmDiag === myLifecycle && prewarmDiag.currentAttempt === myAttempt) {
          myAttempt.resultState = resultState
          myAttempt.settledAt = performance.now()
        }
      }

      prewarmPromise = (async () => {
        try {
          let text = ''
          for await (const chunk of askClaudeStream(snap, false, signal)) {
            // Track P: this is a real, distinct exit from the IIFE — bypasses the catch block entirely, so
            // it needs its own settleAttempt() call (askClaudeStream() is blocking/full-response, not delta
            // streaming, so this loop body runs at most once — still a genuine terminal path in its own right)
            if (signal.aborted) { settleAttempt('SETTLED_NULL'); return null }
            text += (text ? ' ' : '') + chunk
          }
          if (text) console.log(`[Prewarm] Ready: "${text.substring(0, 60)}"`)
          settleAttempt(text ? 'READY_TEXT' : 'SETTLED_NULL')
          return text || null
        } catch (err) {
          if (err.name !== 'AbortError') console.error('[Prewarm] Error:', err.message)
          settleAttempt('SETTLED_NULL')
          return null
        }
      })()
    }

    function clearPrewarm() {
      if (prewarmAbort) { prewarmAbort.abort(); prewarmAbort = null }
      prewarmPromise = null
      prewarmStartText = null
    }

    // Track P — dumb emitter, same pattern as emitSttDiag/emitSttShadowDiag: only serializes already-decided
    // fields, never derives outcome/decision logic itself. [PREWARM_DIAG] is legacy-prewarm-only by
    // construction — chunked calls never call startPrewarm() at all (see onInterim below), so prewarmDiag
    // stays null for their whole lifetime and this never fires for a chunked turn.
    //
    // Review Gate fix — full schema defaults live here, spread UNDER whatever the caller passes, so every
    // [PREWARM_DIAG] line (including the synthetic NO_PREWARM record, which only ever supplies 3 fields)
    // always carries the complete, consistent field set — missing fields are explicit `null`, never an
    // absent key that would make NO_PREWARM records structurally different from settled ones downstream.
    const emitPrewarmDiag = (payload) => {
      try {
        console.log(`[PREWARM_DIAG] ${JSON.stringify({
          callSid,
          prewarmDiagId: null,
          generationId: null,
          outcome: null,
          initialTriggerText: null,
          lastTriggerText: null,
          retriggerCount: null,
          initialPrewarmAgeAtFinalMs: null,
          lastPrewarmAgeAtFinalMs: null,
          prewarmStateAtFinal: null,
          prewarmReadyBeforeFinal: null,
          prewarmAttemptSettleRelativeToFinalMs: null,
          prewarmTextRelation: null,
          prewarmUsable: null,
          graceWaitMs: null,
          ...payload,
        })}`)
      } catch (e) {
        console.error('[PREWARM_DIAG] emit failed (non-fatal, ignored):', e.message)
      }
    }

    // Track P — the ONLY place a prewarmDiag lifecycle is ever settled/cleared. Exactly-once by construction:
    // identity check (both the lifecycle object itself AND diag.settled) → mark settled → detach shared
    // ownership → snapshot payload → emit. clearPrewarm() above is completely untouched by this — it's called
    // mid-lifecycle on every retrigger and must never reset diagnostic state, so this lives entirely separate
    // from it, called explicitly at each of its own terminal sites (primary legacy settlement, defensive
    // catch settlement, bargeIn, handleMaxDuration, 'stop'/'close') — always BEFORE any clearPrewarm()/abort
    // cleanup at that same site, so the diagnostic reason is locked in before transport teardown can race it.
    //
    // Review Gate fix — the whole settle+snapshot+emit body is now inside its own try/catch, not just
    // emitPrewarmDiag()'s console.log: "diagnostics can never affect call flow" must hold even if payload
    // construction itself throws, not only if serialization does.
    function settlePrewarmDiag(diag, outcome, extra = {}) {
      if (!diag || prewarmDiag !== diag || diag.settled) return false
      diag.settled = true
      prewarmDiag = null

      try {
        // NOTE: spread `extra` directly rather than re-keying each field as `extra.field` — an object
        // literal with an explicit `key: undefined` still produces that key (with value undefined), which
        // JSON.stringify then DROPS entirely, silently defeating emitPrewarmDiag()'s base-defaults fill-in
        // for any field the caller didn't pass. Spreading `extra` means a field genuinely absent from the
        // caller's extra object stays genuinely absent here too, so emitPrewarmDiag()'s `{...base, ...payload}`
        // correctly fills it with an explicit null instead of silently omitting the key.
        emitPrewarmDiag({
          prewarmDiagId: diag.prewarmDiagId,
          outcome,
          initialTriggerText: diag.initialTriggerText,
          lastTriggerText: diag.currentAttempt.triggerText,
          retriggerCount: diag.retriggerCount,
          ...extra,
        })
      } catch (e) {
        console.error('[PREWARM_DIAG] settlement payload construction failed (non-fatal, ignored):', e.message)
      }
      return true
    }

    // L1b — เริ่ม chunked speculative Claude call จาก interim text ก่อน final transcript มาถึง สร้าง producer
    // ผ่าน createChunkedProducer() เท่านั้น (ไม่มี consumer แนบเลย) จนกว่า final จะยืนยันว่าใช้ได้ (adoption ใน
    // processTranscript ด้านล่าง) — ไม่มี TTS/Twilio side effect ใดๆ เกิดขึ้นได้ก่อนหน้านั้นเลยโดยโครงสร้าง
    //
    // getIsValid ของ producer นี้ผูกกับ handle เอง (handle === chunkedPrewarmHandle) ก่อน adoption แล้ว "upgrade"
    // เป็น generation guard จริง (handle.generationGuard) หลัง adoption — ดู processTranscript สำหรับจุด upgrade
    // (design correction #2: ต้องคง generation-guard defense-in-depth ไว้ ไม่ใช่พึ่ง AbortSignal อย่างเดียว)
    function startChunkedSpeculation(session, interimText) {
      if (!callActive || isSpeaking || sttProcessing) return
      if (chunkedPrewarmHandle) {
        if (!shouldRetriggerPrewarm(chunkedPrewarmHandle.transcript, interimText, chunkedSpecRetriggerAt)) return
        console.log(`[ChunkedPrewarm] Re-trigger — interim grew: "${chunkedPrewarmHandle.transcript}" → "${interimText}"`)
        abortChunkedSpeculation()
      }
      chunkedSpecRetriggerAt = Date.now() + 700
      const abortController = new AbortController()
      const handle = { transcript: interimText, abortController, startedAt: performance.now(), adopted: false, generationGuard: null, producer: null }
      // ต้อง assign chunkedPrewarmHandle = handle ก่อนเรียก createChunkedProducer() เสมอ — ถ้า askClaudeStreamChunked
      // ตอบแบบ synchronous ล้วนๆ (ไม่มี yield/await เลยก่อน return เช่น end_call ล้วนๆ ไม่มี text) for-await loop
      // ภายใน createChunkedProducer() จะรัน iteration แรกจบสมบูรณ์ (รวมเรียก emitControl) แบบ synchronous ทันที
      // ภายใน call เดียวกันนี้เลย ก่อนบรรทัดถัดไปจะได้รันด้วยซ้ำ — ถ้า assign ทีหลัง getIsValid() จะเห็น
      // chunkedPrewarmHandle เป็นค่าเก่า (null/handle อื่น) ทำให้ end_call แรกสุดหลุดโดนกัน guard ทิ้งอย่างผิดๆ
      // (บั๊กจริงที่เจอจาก integration test 45 — CONTROL_ONLY_HIT ที่ควร adopt กลับกลายเป็น EMPTY_FRESH)
      const snap = { ...session, messages: [...session.messages, { role: 'user', content: interimText }] }
      chunkedPrewarmHandle = handle
      handle.producer = createChunkedProducer({
        session: snap,
        signal: abortController.signal,
        getIsValid: () => (handle.generationGuard ? handle.generationGuard() : chunkedPrewarmHandle === handle),
      })
      console.log(`[ChunkedPrewarm] Starting for: "${interimText}"`)
    }

    // ยกเลิก speculation ที่ยังไม่ถูก adopt เท่านั้น (adopted handle มีเจ้าของอื่นดูแล lifecycle ต่อแล้ว ผ่าน
    // bridgeAbort ที่ผูกกับ turn's real signal — ห้ามแตะ abortController ของมันซ้ำที่นี่)
    function abortChunkedSpeculation() {
      if (chunkedPrewarmHandle && !chunkedPrewarmHandle.adopted) chunkedPrewarmHandle.abortController.abort()
      chunkedPrewarmHandle = null
    }

    // Design B — จุดเดียวที่ consume pendingShortAck จริง เรียกจาก 4 ที่ (mark handler, processTranscript tail,
    // speakAndWait's sent===0, handleSilence's totalSent===0) ทุกจุดต้อง await เสมอ (ไม่ fire-and-forget) เพราะ
    // sttProcessing/ownership guard ต้องยัง valid ตลอดจน dispatch จริงเสร็จ ไม่ใช่แค่ตอนเริ่มเรียก
    //
    // เช็ค requestedPipelineId === activePipelineId ซ้ำอีกชั้น (นอกจาก pendingShortAck.pipelineId === requestedPipelineId)
    // เป็น defense-in-depth กัน race ระหว่างตอน caller ตัดสินใจเรียกกับตอน helper รันจริง (โดยเฉพาะ mark handler
    // ที่เป็น async event แยกจาก call site อื่นๆ) — ทั้งสอง check ต้อง pass พร้อมกันเสมอก่อน consume
    //
    // dispatch failure (Claude/TTS error จริงระหว่าง turn ใหม่ที่เพิ่งเปิด) ไม่ retry — ack ถูก claim ไปครั้งเดียว
    // แล้ว (pendingShortAck = null ไปแล้วก่อน dispatch) retry จะกลายเป็น turn ซ้ำที่ไม่มีใครขอ แค่ log แล้วปล่อย
    async function tryDeliverPendingShortAck(requestedPipelineId, session) {
      if (!pendingShortAck) return
      if (pendingShortAck.pipelineId !== requestedPipelineId) return
      if (requestedPipelineId !== activePipelineId) return
      const ackText = pendingShortAck.text
      pendingShortAck = null
      if (!callActive || pendingEndCall || sttProcessing || socket.readyState !== socket.OPEN) return
      if (typeof processTranscriptDispatch !== 'function') return
      try {
        await processTranscriptDispatch(session, ackText)
      } catch (err) {
        console.error('[ShortAck] Dispatch failed — dropping (no retry):', err.message)
        // ต้อง reset state ให้สายทำงานต่อได้ ไม่ใช่แค่ log เฉยๆ — ถ้า exception เกิดก่อนถึง finally หลักของ
        // processTranscript() เอง (เช่น throw ระหว่าง setup ก่อนเข้า try/catch ของ Claude/TTS) sttProcessing/
        // isSpeaking จะค้าง true ตลอดไป ทุก transcript ถัดไปจะถูกทิ้งเป็น "busy" ตลอดกาล สายฟังไม่ได้อีกเลย
        // (พิสูจน์จริงจาก code review ที่จำลอง failure แล้วเจอ nextCalls=0) — ownership ยังเป็นของ pipeline นี้
        // แน่นอนตอนนี้ (sttProcessing gate กันทุก entry point อื่นไว้แล้วตลอดที่ await ค้างอยู่ ไม่มีใครอื่นบุกมาตั้ง
        // activePipelineId ใหม่ได้ระหว่างนี้) เช็คซ้ำไว้เพื่อความชัดเจน/สอดคล้องกับจุดอื่นในฟังก์ชันนี้ที่ paranoid เหมือนกัน
        if (activePipelineId === requestedPipelineId) {
          sttProcessing = false
          isSpeaking = false
          startSilenceTimer()
        }
      }
    }

    async function handleSilence() {
      silenceTimer = null
      if (!callActive || isSpeaking || sttProcessing) return
      const currentSession = callSessions.get(callSid)
      if (!currentSession) return

      silencePromptCount++
      console.log(`[Silence] Timeout #${silencePromptCount}`)
      isSpeaking = true
      sttProcessing = true
      const pipelineId = ++activePipelineId  // Design B: silence prompt เป็น speaking session ของตัวเอง ต้อง bump owner แยกจาก pipeline ก่อนหน้า (เดิมไม่ bump เลย ทำให้ ack ที่พูดระหว่าง silence ผูกกับ owner เก่าผิดตัว)
      ttsAbortController = new AbortController()
      const signal = ttsAbortController.signal
      let totalSent = 0

      const promptText = silencePromptCount >= 2
        ? 'ไม่ได้ยินเสียงค่ะ ขอบคุณที่รับสายนะคะ'
        : 'ได้ยินอยู่ไหมคะ'

      try {
        for await (const chunk of synthesizeSpeechStream(promptText, currentSession.campaign.voice_id, signal)) {
          if (socket.readyState !== socket.OPEN || signal.aborted) break
          if (totalSent === 0) noteActiveSpokenChunk(pipelineId, promptText)
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          totalSent++
        }
      } catch (err) {
        if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
          console.error('[Silence TTS error]', err.message)
          healthMonitor.reportError('tts', err.message)
        }
      } finally {
        ttsAbortController = null
        sttProcessing = false
      }

      const hasAudio = totalSent > 0 && isSpeaking && socket.readyState === socket.OPEN
      if (hasAudio) {
        pendingSpokenText = { pipelineId, text: promptText }
        socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: ownedMarkName('silence_done', pipelineId) } }))
      } else {
        isSpeaking = false
        if (silencePromptCount < 2) startSilenceTimer()
      }

      if (silencePromptCount >= 2) {
        pendingEndCall = true
        currentSession.hangupReason = 'silence_timeout'
        const closeDelay = totalSent * 20 + 4000
        setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, closeDelay)
      }

      // Design B: no-audio completion — เฉพาะกรณีไม่มี audio ส่งเลย (ไม่มี mark ที่จะกลับมาจริง) ต้องเรียกหลัง
      // pendingEndCall ถูกกำหนดค่าสุดท้ายแล้วเท่านั้น (ไม่ใช่ก่อนหน้า) ไม่งั้น ack อาจถูก deliver ไปแล้วก่อนที่ turn
      // นี้จะรู้ตัวว่ากลายเป็น terminal จริง — กรณีมี audio (hasAudio=true) ต้องรอ owned mark กลับมาจริงเท่านั้น
      // ไม่ใช่ deliver ทันทีตรงนี้ (เสียงยังไม่ได้เล่นจบบนโทรศัพท์ลูกค้าเลย)
      if (!hasAudio) await tryDeliverPendingShortAck(pipelineId, currentSession)
    }

    // Active-Playback Speech Guard R1 — append text to the OWNED pipeline's activeSpokenRef, only once its
    // audio has actually started being sent (called from onChunkAudioStart / the inline TTS loops' own
    // first-audio checkpoints, never from Claude text alone). pipelineId !== activePipelineId means this is a
    // stale callback from an already-superseded pipeline (e.g. a barge-in happened, a new pipeline started,
    // and this old chunk's audio callback is only now settling) — must never poison the CURRENT reference.
    // Starts fresh (not merged) whenever the owning pipeline changes, and stays bounded to ECHO_TAIL_CHARS —
    // same bounding concept as lastMarkedSpokenText/POST_MARK_ECHO, no unbounded speech history.
    function noteActiveSpokenChunk(pipelineId, text) {
      if (!text || pipelineId !== activePipelineId) return
      const prevText = (activeSpokenRef && activeSpokenRef.pipelineId === pipelineId) ? activeSpokenRef.text : ''
      activeSpokenRef = { pipelineId, text: (prevText + text).slice(-ECHO_TAIL_CHARS) }
    }

    // หยุด AI พูดทันที เมื่อลูกค้าพูดแทรก
    function bargeIn() {
      if (!isSpeaking) return
      console.log('[Barge-in] Customer interrupted — stopping AI audio')
      bumpGeneration(callState) // C2: invalidate ก่อนทุกอย่าง — ยัง observational, ไม่ได้ใช้ gate การ abort จริงที่อยู่ถัดไป
      clearSilenceTimer()
      silencePromptCount = 0
      bargeCandidate = null // R1 — this pipeline's turn is over one way or another; a fresh candidate must always start from the NEXT pipeline's own interims, never inherit this one's progress
      if (prewarmDiag) settlePrewarmDiag(prewarmDiag, 'BARGE_IN') // Track P: settle before transport cleanup below
      clearPrewarm()
      abortChunkedSpeculation() // L1b — เหมือน clearPrewarm() ข้างบนทุกประการ แค่คนละ mechanism (chunked speculative producer)
      pendingShortAck = null // Design B: bargeIn() คือ central supersession point — turn ที่แข็งแรงกว่ากำลังมาแทนที่ ack สั้นที่ค้างไว้ก่อนหน้าต้องถูกทิ้งเสมอ ไม่พึ่งให้ caller แต่ละจุดจำ clear เอง
      if (greetingAbortController) { greetingAbortController.abort(); greetingAbortController = null }
      if (ttsAbortController) { ttsAbortController.abort(); ttsAbortController = null }
      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({ event: 'clear', streamSid }))
      }
      isSpeaking = false
      // C6c follow-up: ไม่ set sttProcessing = false ที่นี่อีกต่อไป — ถ้าเทิร์นเดิมยัง await Claude/TTS อยู่จริง
      // (sttProcessing ยังเป็น true) นี่ยังไม่ใช่จังหวะปลอดภัยที่จะเริ่ม pipeline ใหม่ซ้อนกัน ปล่อยให้เทิร์นเดิมเป็น
      // เจ้าของ sttProcessing แต่ผู้เดียว (single writer) — reset เองใน finally ของมันตามปกติเมื่อ async work จริงๆ จบ
      // (generation guard ที่มีอยู่แล้วทำให้ async work ที่เหลือของเทิร์นเดิมเป็น no-op ทั้งหมดอยู่แล้วหลัง bumpGeneration ด้านบน)
      // ถ้า sttProcessing เป็น false อยู่แล้ว (เทิร์นเดิมทำงานเสร็จจริง แค่รอ mark/playback-unlock) บรรทัดนี้ไม่มีผลอะไรเช่นกัน
    }

    // Streaming TTS — ส่ง chunk ไป Twilio ทันทีที่ ElevenLabs generate
    // ไม่ต้องรอ audio ทั้งหมดก่อน → ลด latency 2-3 วินาที
    async function speakAndWait(text, session, markName, pipelineId = -1) {
      if (!callActive || socket.readyState !== socket.OPEN) return

      greetingAbortController = new AbortController()
      const signal = greetingAbortController.signal
      let sent = 0

      try {
        for await (const chunk of synthesizeSpeechStream(text, session.campaign.voice_id, signal)) {
          if (socket.readyState !== socket.OPEN || signal.aborted) break
          if (sent === 0) noteActiveSpokenChunk(pipelineId, text)
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          sent++
        }
      } catch (err) {
        if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
          console.error('[Audio Stream error]', err.message)
          healthMonitor.reportError('tts', err.message)
        }
      } finally {
        greetingAbortController = null
      }

      console.log(`[Audio] Streamed ${sent} chunks for mark=${markName}`)

      // ถ้า barge-in เกิดขึ้นระหว่างส่ง → ไม่ส่ง mark (isSpeaking=false แล้ว)
      if (!isSpeaking) return
      if (sent === 0) {
        isSpeaking = false
        // Design B: greeting fallback ที่ TTS คืน 0 chunks — ไม่มี mark ที่จะกลับมาจริง ต้อง hand off ทันที
        // ไม่งั้น ack ที่ capture ระหว่าง generate จะรอ mark ที่ไม่มีวันมา (ownedMarkName ไม่ถูกส่งเลยในกรณีนี้)
        await tryDeliverPendingShortAck(pipelineId, session)
        return
      }

      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: ownedMarkName(markName, pipelineId) } }))
      }

      const playbackMs = sent * 20 + 1500
      setTimeout(() => {
        if (activePipelineId === pipelineId && isSpeaking) {
          console.log(`[Audio] Fallback unlock after ${playbackMs}ms`)
          isSpeaking = false
          startSilenceTimer()
        }
      }, playbackMs)
    }

    socket.on('message', async (rawMsg) => {
      let msg
      try {
        msg = JSON.parse(rawMsg)
      } catch (e) {
        console.error('[WS] Parse error:', e.message)
        return
      }
      console.log(`[WS] Event received: ${msg.event}`)

      if (msg.event === 'start') {
        streamSid = msg.start.streamSid
        if (!callSid) callSid = msg.start.callSid
        console.log(`[WS] callSid resolved: ${callSid}`)
        const session = callSessions.get(callSid)
        if (!session) return

        // Freeze ครั้งเดียวต่อสาย ไม่คำนวณใหม่ทุกเทิร์น — อ่าน % ปัจจุบันจาก memory cache (rolloutConfig.js)
        // แค่ตอนนี้จุดเดียว ไม่อ่านซ้ำอีกเลยตลอดสาย แม้ % ใน Sheets จะเปลี่ยนกลางสายก็ไม่กระทบสายที่ freeze ไปแล้ว
        rollout = decideRollout(callSid, rolloutConfig.getCurrentRolloutPercent())

        // L2a — atomic snapshot เดียว {percent, campaignId} จาก refresh รอบเดียวกันเสมอ (ห้ามอ่าน getter แยก 2
        // ตัว) แล้ว freeze ทั้ง percent/campaign/bucket จาก snapshot นี้พร้อมกันตอนนี้จุดเดียว ไม่คำนวณใหม่กลางสาย
        // เหมือน rollout ข้างบน — chunked=true ต้องได้ legacyObserved=false เสมอไม่ว่า observed config จะเป็นอะไร
        // (fresh legacy call site เดียวเท่านั้นที่ L2a แตะ — chunked path ไม่ผ่านจุดนี้เลยอยู่แล้ว)
        // "ไม่มี campaign_id" ต้องไม่แปลว่า "ทุก campaign" — campaignId ต้อง match ตรงตัวเท่านั้น (null ไม่ match อะไรทั้งนั้น)
        const observedConfig = rolloutConfig.getCurrentLegacyObservedConfig()
        legacyObservedBucket = getLegacyObservedBucket(callSid)
        legacyObservedCampaignMatched = observedConfig.campaignId != null && session.campaign?.id === observedConfig.campaignId
        legacyObservedPercentAtStart = observedConfig.percent
        legacyObserved = !rollout.useChunkedStreaming && legacyObservedCampaignMatched && legacyObservedBucket < observedConfig.percent

        // L2b (design revision 2026-08-21) — safety precedence CONFIRMED (review round 3): chunked ก่อน แล้ว
        // legacyObserved ก่อน แล้วค่อยถึง legacyEarlyTts เป็นลำดับสุดท้าย — เขียนเป็น chain ตรงๆ ในสูตรเดียว ไม่ใช่
        // if/else แยกหลายที่ กัน future edit พลาดลำดับ ทั้งสาม flag mutual exclusive กันโดยสมบูรณ์เสมอ:
        //   chunked=true            → legacyObserved=false, legacyEarlyTts=false
        //   chunked=false, observed=true  → legacyEarlyTts=false
        //   chunked=false, observed=false, campaign match + bucket ผ่าน → legacyEarlyTts=true
        const earlyTtsConfig = rolloutConfig.getCurrentLegacyEarlyTtsConfig()
        legacyEarlyTtsBucket = getLegacyEarlyTtsBucket(callSid)
        legacyEarlyTtsCampaignMatched = isCampaignMatched(earlyTtsConfig.campaignId, session.campaign?.id)
        legacyEarlyTtsPercentAtStart = earlyTtsConfig.percent
        legacyEarlyTts = !rollout.useChunkedStreaming && !legacyObserved && legacyEarlyTtsCampaignMatched && legacyEarlyTtsBucket < earlyTtsConfig.percent

        // STT-A2 (design revision 2026-08-21) — deliberately NOT chained with chunked/legacyObserved/legacyEarlyTts
        // at all (unlike those three which are mutually exclusive with each other). A2 only changes what Google
        // STT is asked for (maxAlternatives) — it can be ON simultaneously with any of the other three, or with
        // none of them. Own bucket namespace ("stt-a2:"), own Sheet keys, own fail-closed atomic snapshot.
        const sttA2Config = rolloutConfig.getCurrentSttA2Config()
        sttA2Bucket = getSttA2Bucket(callSid)
        sttA2CampaignMatched = isCampaignMatched(sttA2Config.campaignId, session.campaign?.id)
        sttA2PercentAtStart = sttA2Config.percent
        sttA2 = sttA2CampaignMatched && sttA2Bucket < sttA2Config.percent

        // A2.1 Shadow (design revision 2026-08-21, Design Gate v2 PASS) — independent fail-closed gate, own
        // Sheet keys (stt_a2_shadow_percent/stt_a2_shadow_campaign_id), own bucket namespace
        // ("stt-a2-shadow:") — deliberately NOT a reuse of A2's gate (Design Review Blocker 1): A2 is already
        // live at 100%/camp10 in production, so reusing its gate would put this shadow-observation code live
        // the instant it deploys, with no deploy→OFF→verify→exposure window. Activation additionally requires
        // sttA2===true (observing alt1/alt2 candidates via a shadow is meaningless without A2 itself asking
        // Google for maxAlternatives>1 in the first place).
        const sttA2ShadowConfig = rolloutConfig.getCurrentSttA2ShadowConfig()
        sttA2ShadowBucket = getSttA2ShadowBucket(callSid)
        sttA2ShadowCampaignMatched = sttA2ShadowConfig.campaignId != null && session.campaign?.id === sttA2ShadowConfig.campaignId
        sttA2ShadowPercentAtStart = sttA2ShadowConfig.percent
        sttA2Shadow = sttA2 === true && sttA2ShadowCampaignMatched && sttA2ShadowBucket < sttA2ShadowConfig.percent

        console.log(`[Rollout] callSid=${callSid} bucket=${rollout.bucket} percent=${rollout.percentAtStart} chunked=${rollout.useChunkedStreaming} legacyObserved=${legacyObserved} legacyObservedBucket=${legacyObservedBucket} legacyObservedPercent=${observedConfig.percent} campaignMatched=${legacyObservedCampaignMatched} legacyEarlyTts=${legacyEarlyTts} legacyEarlyTtsBucket=${legacyEarlyTtsBucket} legacyEarlyTtsPercent=${earlyTtsConfig.percent} earlyTtsCampaignMatched=${legacyEarlyTtsCampaignMatched} sttA2=${sttA2} sttA2Bucket=${sttA2Bucket} sttA2Percent=${sttA2Config.percent} sttA2CampaignMatched=${sttA2CampaignMatched} sttA2Shadow=${sttA2Shadow} sttA2ShadowBucket=${sttA2ShadowBucket} sttA2ShadowPercent=${sttA2ShadowConfig.percent} sttA2ShadowCampaignMatched=${sttA2ShadowCampaignMatched}`)

        // L1a: ต้องคำนวณหลัง rollout freeze แล้วเท่านั้น (ดูหมายเหตุที่ค่าคงที่ด้านบนไฟล์)
        const interimFinalizeMs = rollout.useChunkedStreaming ? STT_INTERIM_FINALIZE_MS_CHUNKED : 900

        console.log(`[WS] Stream started: ${streamSid}`)

        durationTimer = setTimeout(handleMaxDuration, MAX_CALL_DURATION_MS)

        // C6c follow-up (production discovery 2026-08-19): เดิม sttProcessing gate เช็คก่อนถึง isSpeaking/bargeIn()
        // เสมอ ("if (sttProcessing) return" อยู่บนสุดของ callback) — แปลว่าลูกค้าพูดแทรกตอน AI ยัง generate/พูดอยู่
        // จริง (sttProcessing=true) จะถูกทิ้งเงียบๆ ก่อนแม้แต่จะพยายาม barge-in เลย ทั้งที่นี่คือช่วงที่ barge-in
        // สำคัญที่สุด (ตรงกับที่ production Call 2 เจอสองรอบ: interrupt ลงจังหวะหลัง AI พูดจบเทิร์นแล้วเท่านั้น)
        //
        // แก้โดยแยก "interrupt control" (bargeIn ต้องยิงได้ทันทีไม่ว่า sttProcessing จะเป็นอะไร) ออกจาก
        // "transcript processing single-flight gate" (sttProcessing ยังคุม "ห้ามมี Claude/TTS pipeline ซ้อนกัน
        // สองอัน" เหมือนเดิมทุกประการ เป็น single writer เดียว — เทิร์นเจ้าของเท่านั้นที่ reset มันได้ ดู bargeIn())
        //
        // ถ้า barge-in เกิดระหว่างเทิร์นเดิมยัง sttProcessing=true อยู่จริง (ยังไม่ถึง finally ของมันเอง) transcript
        // ที่พูดแทรกจะถูกเก็บไว้ใน pendingTranscript (ช่องเดียว, latest-wins — ไม่ทำ FIFO queue กันหลาย turn เกิด
        // จากพูดครั้งเดียวระหว่าง STT ส่ง final ซ้ำ/fragment ใกล้ๆ กัน) แล้วให้เทิร์นเดิมเป็นคนสั่ง process ต่อเองใน
        // finally ของ processTranscript() หลังปล่อย sttProcessing จริง (ผ่าน generation guard เดิมที่มีอยู่แล้ว
        // ทำให้ async work ที่เหลือของเทิร์นเดิมเป็น no-op ทั้งหมดอยู่แล้วหลัง bumpGeneration ใน bargeIn())
        async function processTranscript(currentSession, transcript) {
          if (sttProcessing) return // defensive — ไม่ควรเกิดจริงเพราะทุกจุดที่เรียกมาเช็คมาก่อนแล้ว
          sttProcessing = true
          currentSession.messages.push({ role: 'user', content: transcript })
          const pipelineId = ++activePipelineId
          isSpeaking = true

          // C2: sync generationId/turnState กับ turn lifecycle จริง — ยัง observational เท่านั้น
          // (ไม่มี isCurrentGeneration gate ใน legacy path ตอนนี้ — activePipelineId/signal.aborted เดิมยังคุม cancellation ทั้งหมด)
          const generationId = bumpGeneration(callState)
          const turnState = createTurnState(generationId)

          // C1: per-turn latency instrumentation (ยังไม่มี chunked path ให้ path='chunked' จริง เพราะ rollout=0%)
          const turnMetrics = createTurnMetrics({
            callSid,
            generationId,
            path: rollout?.useChunkedStreaming ? 'chunked' : 'legacy',
            rolloutBucket: rollout?.bucket ?? null,
            rolloutPercent: rollout?.percentAtStart ?? null,
            legacyObserved,
            legacyObservedBucket,
            legacyObservedPercentAtStart,
            legacyObservedCampaignMatched,
            legacyEarlyTts,
            legacyEarlyTtsBucket,
            legacyEarlyTtsPercentAtStart,
            legacyEarlyTtsCampaignMatched,
          })
          markOnce(turnMetrics, 't1')

          ttsAbortController = new AbortController()
          const signal = ttsAbortController.signal
          let fullText = ''
          let totalSent = 0
          let endCallRequested = false // C3a: signaled by chunkedTurn.js's onControl (end_call tool) — separate channel from legacy's [END_CALL] text marker

          // Safety: ถ้า Claude/TTS ค้างนานผิดปกติ ให้ unlock อัตโนมัติ
          // เช็ค pipelineId ด้วย — ถ้า pipeline นี้ถูก barge-in ทิ้งไปแล้วและมี pipeline ใหม่เริ่มทำงานอยู่
          // timer เก่านี้ต้องไม่ไปตัด isSpeaking/sttProcessing ของ pipeline ใหม่ที่กำลังทำงานปกติอยู่
          const processingGuard = setTimeout(() => {
            if (activePipelineId === pipelineId && sttProcessing) {
              console.error('[AI] sttProcessing stuck >30s — force reset')
              sttProcessing = false
              isSpeaking = false
            }
          }, 30000)

          // Capture prewarm reference — ป้องกัน pipeline เก่าล้าง prewarm ของ pipeline ใหม่
          // Commit A: snapshot prewarmAbort คู่กับ myPrewarm ด้วย (ไม่ใช่แค่ text) — prewarmPromise/prewarmAbort
          // เป็น mutable global ที่ clearPrewarm()/startPrewarm() เปลี่ยน ref ได้ตลอดเวลา ถ้า grace-timeout handler
          // อ้าง prewarmAbort ตรงๆ (แทนที่จะ snapshot ไว้ ณ จุดนี้) อาจไป abort() ตัวที่ไม่ใช่เจ้าของ myPrewarm จริง
          // ถ้ามี prewarm ใหม่เริ่มไปแล้วระหว่างที่ turn นี้ยัง await grace อยู่ (unlikely จาก sttProcessing/isSpeaking
          // guard เดิม แต่ snapshot ตรงตามตัวแปรที่ผูกกันจริงเป็น invariant ที่แข็งแรงกว่า ไม่ต้องพึ่ง guard อื่นค้ำ)
          const myPrewarm = prewarmPromise
          const myPrewarmText = prewarmStartText
          const myPrewarmAbort = prewarmAbort
          const myPrewarmStartedAt = prewarmStartedAt
          // L1b: snapshot เดียวกันเหตุผลเดียวกัน — chunkedPrewarmHandle เป็น mutable global ที่ startChunkedSpeculation()/
          // abortChunkedSpeculation() เปลี่ยน ref ได้ตลอดเวลา ต้อง snapshot ก่อนเข้า branch ไม่ใช่อ่านซ้ำทีหลัง
          const mySpecHandle = chunkedPrewarmHandle
          // Track P — same snapshot-before-branch reasoning as myPrewarm/mySpecHandle above: prewarmDiag is a
          // mutable global that startPrewarm()/settlePrewarmDiag() can move on at any time. Harmless no-op for
          // a chunked call (prewarmDiag stays null all game — startPrewarm() is never called for one), so this
          // line lives here in the shared pre-split code rather than duplicated into just the legacy branch.
          const myPrewarmDiag = prewarmDiag
          const myPrewarmAttempt = myPrewarmDiag?.currentAttempt ?? null
          // Correction #3 (design review รอบ 4): snapshot เวลา "final ถูกยอมรับ" ก่อน grace ใดๆ ทั้งสิ้น — ถ้าคำนวณ
          // prewarmAgeAtFinalMs หลังผ่าน grace (150ms) แล้ว ค่าจะบวก grace เข้าไปผิดๆ ทั้งที่ field นี้ควรวัด "อายุของ
          // speculation ณ ตอน final มาถึงจริง" เพื่อใช้ตัดสินว่า speculation ได้ head-start จริงกี่ ms
          const finalAcceptedAt = performance.now()
          // Track P — state-at-final snapshot, derived from the exact myPrewarmAttempt captured above (never
          // re-read prewarmDiag.currentAttempt later — that could theoretically be a newer attempt by the
          // time we get around to it, even though sttProcessing=true structurally blocks retriggers after
          // this point today; deriving from the frozen reference doesn't need to rely on that guard holding).
          const prewarmStateAtFinal = myPrewarmAttempt?.resultState ?? null
          const prewarmReadyBeforeFinal = prewarmStateAtFinal === 'READY_TEXT'
          const initialPrewarmAgeAtFinalMs = myPrewarmDiag ? Math.round(finalAcceptedAt - myPrewarmDiag.initialTriggerAt) : null
          const lastPrewarmAgeAtFinalMs = myPrewarmAttempt ? Math.round(finalAcceptedAt - myPrewarmAttempt.triggerAt) : null

          // C3a: ตัดสิน branch ก่อน Claude side effect แรกเสมอ — ห้ามมี code เรียก Claude ก่อนจุดนี้ไม่ว่า path ไหน
          // rollout ยัง 0% เสมอตอนนี้ จึงยังไม่มีสายไหนเข้า branch นี้จริงในโปรดักชัน (ดู C0/decideRollout)
          if (rollout.useChunkedStreaming) {
            try {
              // C6c follow-up: legacy มี t2 (Claude request sent) แต่ chunked branch ไม่เคยถูกใส่ไว้เลยตั้งแต่ C1 —
              // เจอจาก production trace จริงที่ t2 เป็น null ทุกเทิร์น ทำให้ claudeTTFT (t2→t3) และ requestToAudio
              // (t2→t7) วัดไม่ได้เลย มาร์กตรงนี้ (จุดเดียวกับที่ legacy มาร์ก คือ "กำลังจะเริ่มขอคำตอบ" ก่อนเรียก
              // Claude จริง) ไม่กระทบ branch decision invariant เพราะเป็นแค่ timestamp ไม่ใช่ side effect
              markOnce(turnMetrics, 't2')

              // L1b — ถ้ามี speculation ที่ interim ตรงกับ final แบบ exact (normalized) พยายาม adopt ก่อนเริ่ม
              // fresh call ใดๆ ทั้งสิ้น — ดู classifyForAdoption()/design lock (รอบ 3-4) สำหรับ state table เต็ม
              let attempt
              let usedSpeculation = false

              if (mySpecHandle && isSpeculationMatch(mySpecHandle.transcript, transcript)) {
                let classification = classifyForAdoption(mySpecHandle)

                if (classification.decision === 'GRACE') {
                  // เคสเดียวที่เหลือให้รอ pre-adoption: ไม่มี progress อะไรเลย (ไม่มี delta แม้แต่ตัวเดียว) — ถ้ามี
                  // delta แล้ว (DELTA_ONLY_HIT) classifyForAdoption คืน ADOPT_NOW ทันทีไปแล้ว ไม่มาถึง branch นี้
                  const graceAttempt = await runAttemptWithWatchdog({
                    signal,
                    timeoutMs: CHUNKED_SPEC_PROGRESS_GRACE_TIMEOUT_MS,
                    reason: 'CHUNKED_SPEC_PROGRESS_GRACE_TIMEOUT',
                    run: (childSignal) => {
                      bridgeAbort(childSignal, mySpecHandle.abortController) // Correction #1: ต้องเช็ค childSignal.aborted ก่อนเรียกด้วย ไม่ใช่ addEventListener เพียวๆ
                      return mySpecHandle.producer.waitForFirstProgress()
                    },
                  })
                  if (graceAttempt.outcome === 'success') {
                    classification = classifyForAdoption(mySpecHandle)
                    if (classification.decision === 'ADOPT_NOW') classification = { ...classification, outcome: 'GRACE_HIT' }
                  } else {
                    classification = {
                      decision: 'DROP',
                      outcome: graceAttempt.outcome === 'timeout' ? 'GRACE_TIMEOUT_FRESH'
                        : graceAttempt.outcome === 'aborted' ? 'ABORTED' : 'ERROR_FRESH',
                    }
                  }
                }

                recordChunkedPrewarmMetrics(turnMetrics, mySpecHandle, finalAcceptedAt, classification.outcome)

                if (classification.decision === 'ADOPT_NOW') {
                  usedSpeculation = true
                  mySpecHandle.adopted = true
                  // Correction #2 upgrade: หลัง adopt แล้ว producer's getIsValid() ต้องอ้างอิง generation guard จริง
                  // ของเทิร์นนี้ (ไม่ใช่ "ยัง current handle อยู่ไหม" อีกต่อไป — handle ถูก clear ทิ้งจาก closure แล้ว)
                  mySpecHandle.generationGuard = () => isCurrentGeneration(callState, generationId)
                  console.log(`[ChunkedPrewarm] ${classification.outcome} — adopting speculative producer`)

                  if (classification.outcome === 'CONTROL_ONLY_HIT') {
                    // ไม่มี text ให้พูดเลย (end_call ล้วนๆ) — ไม่มีอะไรให้ watchdog รอ แต่ยัง mark t3 ถ้าเคยมี delta
                    // จริงมาก่อน (เช่น whitespace เดียวที่ไม่เคยข้าม MIN_CHUNK_LENGTH) เพื่อความถูกต้องของ telemetry
                    mySpecHandle.producer.onFirstDelta(() => markOnce(turnMetrics, 't3'))
                    const result = await adoptChunkedProducer({
                      producer: mySpecHandle.producer, signal, socket, streamSid,
                      voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                      onControl: (control) => { if (control?.type === 'end_call') endCallRequested = true },
                      onChunkAudioStart: (text) => noteActiveSpokenChunk(pipelineId, text),
                    })
                    attempt = { outcome: 'success', result }
                  } else {
                    // Correction #3: เริ่มที่ Watchdog B เสมอ (ไม่ใช่ C) — ถ้ามี chunk พร้อมอยู่แล้ว onFirstChunk
                    // replay ทันที (disarm B ในติ๊กเดียวกัน) แล้ว onFirstTtsRequest/onFirstTtsAudio arm/disarm C ที่
                    // t5/t6 จริงเหมือน fresh path ทุกประการ — ไม่ใช่นับจาก adoption
                    attempt = await runAttemptWithWatchdog({
                      signal,
                      timeoutMs: CHUNK_READY_TIMEOUT_MS,
                      reason: 'CHUNK_READY_TIMEOUT',
                      run: (childSignal, armWatchdog) => {
                        bridgeAbort(childSignal, mySpecHandle.abortController) // Correction #1
                        mySpecHandle.producer.onFirstDelta(() => markOnce(turnMetrics, 't3'))
                        mySpecHandle.producer.onFirstChunk(() => { markOnce(turnMetrics, 't4'); armWatchdog() })
                        return adoptChunkedProducer({
                          producer: mySpecHandle.producer, signal: childSignal, socket, streamSid,
                          voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                          onControl: (control) => { if (control?.type === 'end_call') endCallRequested = true },
                          onFirstTtsRequest: () => armWatchdog(TTS_FIRST_AUDIO_TIMEOUT_MS, 'TTS_FIRST_AUDIO_TIMEOUT'),
                          onFirstTtsAudio: () => armWatchdog(),
                          onChunkAudioStart: (text) => noteActiveSpokenChunk(pipelineId, text),
                        })
                      },
                    })
                  }
                } else {
                  console.log(`[ChunkedPrewarm] ${classification.outcome} — dropping speculation, fresh chunked`)
                }
              } else if (mySpecHandle) {
                recordChunkedPrewarmMetrics(turnMetrics, mySpecHandle, finalAcceptedAt, 'MISMATCH_FRESH')
                console.log(`[ChunkedPrewarm] Mismatch — interim "${mySpecHandle.transcript}" vs final "${transcript}" — dropping, fresh chunked`)
              }

              // handle ที่ไม่ถูก adopt (DROP ทุกแบบ รวม mismatch) ต้อง abort ทันที — ไม่รอให้ network cancellation
              // settle ก่อน (ดู invariant ท้ายบล็อกนี้) และเคลียร์ตัวชี้ออกจาก closure ไม่ว่า adopt หรือไม่ก็ตาม
              if (mySpecHandle && !mySpecHandle.adopted) mySpecHandle.abortController.abort()
              if (chunkedPrewarmHandle === mySpecHandle) chunkedPrewarmHandle = null

              // C4b: race runChunkedTurn ต่อ watchdog สามวงเรียงกัน (Watchdog A → B → C) ผ่าน child AbortController
              // เดียวที่ compose มาจาก outer signal (barge-in) — barge-in ยังฆ่าทั้งคู่ได้เสมอ แต่ watchdog ฆ่าได้แค่
              // chunked attempt นี้เท่านั้น ไม่แตะ outer signal เลย เพื่อให้ fallback ด้านล่าง (ที่ใช้ outer signal)
              // ยังทำงานได้จริง — A (CLAUDE_FIRST_DELTA_TIMEOUT) ตั้งไว้ก่อนเริ่ม, rearm เป็น B (CHUNK_READY_TIMEOUT)
              // ตอน t3, disarm ตอน t4 (ไม่ rearm ทันที — ช่องว่างจนกว่า TTS request แรกจะเริ่มจริงคือ intentional gap),
              // แล้ว rearm เป็น C (TTS_FIRST_AUDIO_TIMEOUT) ตอน t5 (TTS request แรกของทั้งเทิร์นเริ่มจริง ไม่ใช่ตอน
              // chunk ถูก dequeue), disarm ตอน t6 — first-only ทั้งคู่ ไม่ rearm ตาม speech chunk ถัดๆ ไป
              //
              // Correction #4a: ถ้า speculation ไม่ถูก adopt เพราะ grace ถูก barge-in (ABORTED) หรือ turn นี้ stale
              // ไปแล้วด้วยเหตุผลอื่น ห้ามพยายามเริ่ม fresh call เลย (เหมือน barge-in ธรรมดาทุกประการ — ไม่ใช่ "เริ่ม
              // แล้วถูก abort ทันที" ซึ่งจะทำให้ debug/invariant ยุ่งขึ้นโดยไม่จำเป็น)
              if (!usedSpeculation) {
                if (!signal.aborted && callActive && isCurrentGeneration(callState, generationId)) {
                  attempt = await runAttemptWithWatchdog({
                    signal,
                    timeoutMs: CLAUDE_FIRST_DELTA_TIMEOUT_MS,
                    reason: 'CLAUDE_FIRST_DELTA_TIMEOUT',
                    run: (childSignal, armWatchdog) => runChunkedTurn({
                      session: currentSession,
                      signal: childSignal,
                      socket,
                      streamSid,
                      voiceId: currentSession.campaign.voice_id,
                      turnMetrics,
                      turnState,
                      callState,
                      generationId,
                      onControl: (control) => { if (control?.type === 'end_call') endCallRequested = true },
                      onFirstDelta: () => armWatchdog(CHUNK_READY_TIMEOUT_MS, 'CHUNK_READY_TIMEOUT'),
                      onFirstChunk: () => armWatchdog(),
                      onFirstTtsRequest: () => armWatchdog(TTS_FIRST_AUDIO_TIMEOUT_MS, 'TTS_FIRST_AUDIO_TIMEOUT'),
                      onFirstTtsAudio: () => armWatchdog(),
                      onChunkAudioStart: (text) => noteActiveSpokenChunk(pipelineId, text),
                    }),
                  })
                } else {
                  attempt = { outcome: 'aborted' }
                }
              }

              if (attempt.outcome === 'success') {
                fullText = attempt.result.fullText
                totalSent = attempt.result.totalSent

                // C3c/C4a: parity กับ legacy's shouldBlockEndCall guard — ใช้ helper เดียวกันไม่ว่า endCallRequested
                // จะมาจาก chunked path ปกติ (ตรงนี้) หรือจาก legacy fallback ด้านล่าง (normalize มาแล้วเหมือนกัน)
                const guarded = await applyChunkedEndCallGuard({
                  endCallRequested, fullText, totalSent, currentSession, signal, socket, streamSid,
                  voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                  onChunkAudioStart: (text) => noteActiveSpokenChunk(pipelineId, text),
                })
                fullText = guarded.fullText
                endCallRequested = guarded.endCallRequested
                totalSent = guarded.totalSent
              } else if (attempt.outcome === 'aborted') {
                // C4c: barge-in ยกเลิก attempt นี้กลางทาง (ไม่ใช่ watchdog/error) — bargeIn() จัดการ cleanup
                // (clear event, isSpeaking/sttProcessing reset) ไปแล้วแยกต่างหาก ห้าม fallback ต่อเด็ดขาด
                // เพราะลูกค้ากำลังพูดแทรกอยู่ พยายามพูดอะไรตอนนี้ผิดหลักการเดียวกับที่ C3b ทั้งชุดกันไว้
                console.log('[Chunked] Attempt aborted by barge-in — no fallback, turn ends with no audio')
              } else {
                // outcome: 'timeout' (watchdog) หรือ 'error' (Claude/TTS จริง) — ทั้งสองทางวิ่งเข้า fallback gate เดียวกัน
                // runAttemptWithWatchdog abort child ไปให้แล้วก่อน return ในทั้งสองกรณี ไม่ต้อง abort เพิ่มที่นี่
                let triggerReason
                if (attempt.outcome === 'timeout') {
                  triggerReason = attempt.reason // CLAUDE_FIRST_DELTA_TIMEOUT / CHUNK_READY_TIMEOUT / TTS_FIRST_AUDIO_TIMEOUT
                  console.log(`[Watchdog] ${attempt.reason} — chunked attempt aborted, considering fallback`)
                } else {
                  // C4c: แยก CLAUDE_ERROR/TTS_ERROR จาก tag ที่ chunkedTurn.js ใส่ไว้ให้แล้ว แทนป้ายรวมๆ เดิม —
                  // สำคัญกับการวิเคราะห์ตอน rollout จริง เพราะ Claude กับ ElevenLabs พังคนละสาเหตุคนละทางแก้กัน
                  triggerReason = attempt.error.source === 'TTS' ? 'TTS_ERROR' : 'CLAUDE_ERROR'
                  console.error('[AI/TTS error]', attempt.error.message)
                  healthMonitor.reportError('ai_tts', attempt.error.message)
                }

                // C4a/C4b: chunked พังหรือหมดเวลาก่อน commit เสียง → ลอง fallback ไปใช้ legacy Claude/TTS สำหรับ
                // เทิร์นนี้แทน ปล่อยลูกค้าเงียบไปเฉยๆ ไม่ bump generation เพราะนี่คือการกู้ turn เดิม ไม่ใช่ turn ใหม่
                if (isCurrentGeneration(callState, generationId) && claimFallback(turnState)) {
                  turnMetrics.fallbackTriggered = true
                  turnMetrics.fallbackReason = triggerReason
                  turnMetrics.fallbackStartedAt = performance.now()
                  console.log('[Fallback] Falling back to legacy Claude/TTS for this turn')

                  // C4c follow-up: 2-phase commit-aware watchdog — pre-commit terminal timeout, แทนที่ด้วย
                  // rolling post-commit idle timeout ทันทีที่มีก้อนเสียงคอมมิตจริง (ผ่าน onAudioSent ที่ rearm
                  // ทุกครั้ง — ก้อนแรกก็เข้ามาแทนที่ precommit timer ไปในตัวเพราะ arm() clear timer เดิมก่อนตั้งใหม่เสมอ)
                  //
                  // fallbackProgress คือ side-channel นอก race โดยตั้งใจ — runAttemptWithWatchdog ทิ้งผลลัพธ์ของ
                  // ฝ่ายแพ้ race เสมอเมื่อ watchdog ชนะ (ถูกต้องสำหรับ pre-commit ที่ยังไม่มีอะไรคอมมิต) แต่ totalSent
                  // ที่คอมมิตไปแล้วจริงก่อน timeout/error ต้องรอดจากการทิ้งนั้นได้ จึงเก็บนอก return value ของ run()
                  const fallbackProgress = { totalSent: 0 }
                  const fallbackAttempt = await runAttemptWithWatchdog({
                    signal,
                    timeoutMs: FALLBACK_PRECOMMIT_TIMEOUT_MS,
                    reason: 'FALLBACK_TIMEOUT',
                    run: (fallbackSignal, armWatchdog) => runLegacyFallback({
                      session: currentSession, signal: fallbackSignal, socket, streamSid,
                      voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                      onAudioSent: () => {
                        // granularity เดียวกับ totalSent เดิมเป๊ะ — นับก้อนที่ synthesizeAndSend ส่งสำเร็จ ไม่ใช่ raw
                        // ElevenLabs/Twilio byte frame (ถ้าวันหลังอยากนับ frame แยกต่างหาก ใช้ field ใหม่คนละชื่อ)
                        fallbackProgress.totalSent++
                        armWatchdog(FALLBACK_IDLE_TIMEOUT_MS, 'FALLBACK_PARTIAL_TIMEOUT')
                      },
                      onChunkAudioStart: (text) => noteActiveSpokenChunk(pipelineId, text),
                    }),
                  })

                  if (fallbackAttempt.outcome === 'success' || fallbackAttempt.outcome === 'aborted') {
                    const fb = fallbackAttempt.result
                    // เช็คซ้ำหลัง await ยาว (askClaudeStream) — ห้ามเอาผลลัพธ์ของ generation ที่ stale ไปแล้วมาใช้
                    // ต่อ ไม่งั้น [END_CALL] เก่าอาจไปสั่ง hangup ทั้งที่ลูกค้ากำลังคุยกับ generation ใหม่อยู่
                    if (isCurrentGeneration(callState, generationId)) {
                      turnMetrics.fallbackOutcome = 'SPOKEN'
                      const guarded = await applyChunkedEndCallGuard({
                        endCallRequested: fb.endCallRequested, fullText: fb.fullText, totalSent: fb.totalSent,
                        currentSession, signal, socket, streamSid, voiceId: currentSession.campaign.voice_id,
                        turnMetrics, turnState, callState, generationId,
                        onChunkAudioStart: (text) => noteActiveSpokenChunk(pipelineId, text),
                      })
                      fullText = guarded.fullText
                      endCallRequested = guarded.endCallRequested
                      totalSent = guarded.totalSent
                    } else {
                      turnMetrics.fallbackOutcome = 'STALE'
                      console.log('[Fallback] Generation went stale while legacy fallback was in flight — discarding its result')
                    }
                  } else if (fallbackAttempt.outcome === 'timeout' || fallbackAttempt.outcome === 'error') {
                    // production incident 2026-08-19: turnState.audioCommitted (ไม่ใช่ outcome type เฉยๆ) คือตัว
                    // ตัดสินใจเดียวว่านี่คือ "ไม่มีเสียงออกเลย" หรือ "มีเสียงออกไปแล้วบางส่วนก่อนพัง" — ต้องแยกให้ชัด
                    // ไม่งั้น downstream (ai_done mark ไปยัง Twilio, playback-unlock timer) จะเข้าใจผิดว่าไม่มีเสียงออก
                    // ทั้งที่ลูกค้าได้ยินไปแล้วจริง (นี่คือบั๊กที่เจอจาก production trace ของสายทดสอบ barge-in retry)
                    if (turnState.audioCommitted) {
                      totalSent = fallbackProgress.totalSent
                      // ระบบไม่มี text↔audio word-level mapping จริง — ห้ามใส่ spokenText เต็มก้อนเป็น assistant
                      // history เพราะไม่รู้ว่าพูดถึงคำไหนจริง ใส่ marker ตรงไปตรงมาแทน ให้ Claude เทิร์นถัดไปรู้บริบท
                      // โดยไม่ assume ว่าลูกค้าได้ยินครบ
                      fullText = '[ระบบ: คำตอบก่อนหน้าถูกขัดจังหวะหลังเริ่มส่งเสียง ลูกค้าอาจได้ยินเพียงบางส่วน]'
                      // ไม่ไว้ใจ end_call intent จากคำตอบที่อาจไม่สมบูรณ์ — claimFallback เป็น single-fire อยู่แล้ว จึงไม่มีทาง retry fallback ซ้ำอีกในเทิร์นนี้ด้วย
                      endCallRequested = false
                      if (fallbackAttempt.outcome === 'timeout') {
                        turnMetrics.fallbackOutcome = 'FALLBACK_PARTIAL_TIMEOUT'
                        console.error(`[Fallback] FALLBACK_PARTIAL_TIMEOUT — audio stalled ${FALLBACK_IDLE_TIMEOUT_MS}ms after ${fallbackProgress.totalSent} chunk(s) already committed`)
                      } else {
                        turnMetrics.fallbackOutcome = 'FALLBACK_PARTIAL_ERROR'
                        console.error(`[Fallback error after commit] ${fallbackAttempt.error.message} — ${fallbackProgress.totalSent} chunk(s) already committed`)
                        healthMonitor.reportError('ai_tts_fallback', fallbackAttempt.error.message)
                      }
                    } else if (fallbackAttempt.outcome === 'timeout') {
                      turnMetrics.fallbackOutcome = 'FALLBACK_TIMEOUT'
                      console.error('[Fallback] FALLBACK_TIMEOUT — giving up before any audio committed, no further recovery attempted')
                    } else {
                      turnMetrics.fallbackOutcome = 'FALLBACK_ERROR'
                      console.error('[Fallback error]', fallbackAttempt.error.message)
                      healthMonitor.reportError('ai_tts_fallback', fallbackAttempt.error.message)
                    }
                  }
                }
              }
            } catch (err) {
              console.error('[AI/TTS error]', err.message)
              healthMonitor.reportError('ai_tts', err.message)
            } finally {
              clearTimeout(processingGuard)
              if (prewarmPromise === myPrewarm) clearPrewarm()
              if (activePipelineId === pipelineId) {
                ttsAbortController = null
                sttProcessing = false
              }
            }
          } else {
          try {
            // Use pre-warmed Claude response if available and applicable
            let aiText = null
            markOnce(turnMetrics, 't2') // legacy: askClaudeStream yield ข้อความเต็มก้อนเดียว จึงไม่มี t3/t4 ที่มีความหมาย

            // Track P — synthetic record: no lifecycle object exists at all this turn, so it never goes
            // through settlePrewarmDiag() (there's nothing to settle) — emitted directly here instead.
            if (!myPrewarmDiag) {
              emitPrewarmDiag({ prewarmDiagId: null, generationId, outcome: 'NO_PREWARM' })
            }
            // Track P — computed here purely for the diagnostic payload below. This is a SEPARATE call to
            // isPrewarmUsable() from the one the production `if` still makes on its own a few lines down —
            // deliberately not reused/cached, so the original `if (myPrewarm && isPrewarmUsable(...))` line
            // stays byte-for-byte untouched (the more conservative option: isPrewarmUsable() is pure and
            // side-effect-free, so calling it twice with identical args is guaranteed to agree both times —
            // this diagnostic read can never disagree with, or influence, what production actually decided).
            const prewarmUsableResult = myPrewarm ? isPrewarmUsable(myPrewarmText, transcript) : null
            const prewarmTextRelation = myPrewarmDiag ? classifyPrewarmTextRelation(myPrewarmText, transcript) : null
            let prewarmDiagOutcome = myPrewarmDiag ? 'MISMATCH' : null   // default when a lifecycle exists but isn't usable; overwritten below when it is
            let prewarmDiagGraceWaitMs = null
            if (myPrewarm && isPrewarmUsable(myPrewarmText, transcript)) {
              const graceWaitStartedAt = performance.now() // เวลาเริ่ม "รอบรอ grace นี้" คนละตัวกับ myPrewarmStartedAt (เวลาที่ request เริ่มจริงตอน startPrewarm())
              console.log(`[Prewarm] Awaiting pre-warmed response for: "${transcript}" (grace=${PREWARM_GRACE_MS}ms)`)
              const attempt = await runAttemptWithWatchdog({
                signal, // signal เดิมของเทิร์นนี้ — barge-in ยัง abort ได้ทันทีเหมือนเดิม ไม่ต้องเพิ่มโค้ดแยก
                timeoutMs: PREWARM_GRACE_MS,
                reason: 'PREWARM_GRACE_TIMEOUT',
                run: (childSignal) => {
                  // ต้อง abort myPrewarmAbort ที่ snapshot ไว้ (ไม่ใช่ prewarmAbort global) — กัน race ถ้ามี
                  // prewarm รอบใหม่เริ่มไปแล้วระหว่างที่ turn นี้ยัง await grace อยู่พอดี
                  childSignal.addEventListener('abort', () => {
                    if (myPrewarmAbort && !myPrewarmAbort.signal.aborted) myPrewarmAbort.abort()
                  }, { once: true })
                  return myPrewarm
                },
              })
              const waitedMs = Math.round(performance.now() - graceWaitStartedAt)
              prewarmDiagGraceWaitMs = waitedMs

              // prewarm IIFE เองมี try/catch คลุมอยู่แล้ว (คืน null เสมอตอน error ไม่เคย throw ออกมาจริง) จึงต้อง
              // เช็คทั้ง outcome และ result ร่วมกัน — success+null ก็ต้องถือเป็น miss แล้วไป fresh call เหมือนกัน
              //
              // ทุก branch (รวม aborted) ปล่อยให้ไหลลงไปถึง shared tail ท้าย processTranscript() ตามปกติ ห้าม
              // early return ที่นี่เด็ดขาด — ถ้า barge-in ทำให้ transcript ถูก queue ไว้ใน pendingTranscript ไปแล้ว
              // (โดย onTranscript ตั้งแต่ bargeIn() ทำงาน) ต้องให้ drain logic ท้ายฟังก์ชันเป็นคนสั่ง process ต่อ
              // aiText ปล่อยเป็น null แล้วให้ guard เดิมด้านล่าง (!signal.aborted) กัน fresh call/TTS เองตามปกติพอ
              if (attempt.outcome === 'success' && attempt.result) {
                aiText = attempt.result
                prewarmDiagOutcome = 'HIT'
                console.log(`[Prewarm] Grace success waitedMs=${waitedMs} — skipping fresh Claude call`)
              } else if (attempt.outcome === 'success') {
                prewarmDiagOutcome = 'NULL_RESULT'
                console.log(`[Prewarm] Grace success waitedMs=${waitedMs} but null result — falling back to fresh call`)
              } else if (attempt.outcome === 'timeout') {
                prewarmDiagOutcome = 'GRACE_TIMEOUT'
                console.log(`[Prewarm] Grace timeout waitedMs=${waitedMs} prewarmAgeMs=${Math.round(performance.now() - myPrewarmStartedAt)} — falling back to fresh call`)
              } else if (attempt.outcome === 'aborted') {
                prewarmDiagOutcome = 'BARGE_IN'
                console.log('[Prewarm] Grace wait aborted by barge-in — no fresh call, turn ends with no audio')
              } else {
                prewarmDiagOutcome = 'ERROR'
                console.log(`[Prewarm] Grace error waitedMs=${waitedMs} (${attempt.error?.message}) — falling back to fresh call`)
              }
            }

            // Track P — primary settlement, MUST happen before the existing clearPrewarm() line right below
            // (locks the diagnostic outcome in before transport cleanup can race it — same ordering rule
            // applied at bargeIn()/handleMaxDuration()/'stop'/'close'). myPrewarmAttempt.settledAt reflects
            // whatever the identity-checked writer inside startPrewarm()'s IIFE last wrote to THIS exact
            // attempt object — read once here, not re-derived from a live/possibly-newer prewarmDiag.
            if (myPrewarmDiag) {
              const prewarmAttemptSettleRelativeToFinalMs = myPrewarmAttempt?.settledAt != null
                ? Math.round(myPrewarmAttempt.settledAt - finalAcceptedAt)
                : null
              settlePrewarmDiag(myPrewarmDiag, prewarmDiagOutcome, {
                generationId,
                initialPrewarmAgeAtFinalMs,
                lastPrewarmAgeAtFinalMs,
                prewarmStateAtFinal,
                prewarmReadyBeforeFinal,
                prewarmAttemptSettleRelativeToFinalMs,
                prewarmTextRelation,
                prewarmUsable: prewarmUsableResult,
                graceWaitMs: prewarmDiagGraceWaitMs,
              })
            }
            if (prewarmPromise === myPrewarm) clearPrewarm()

            if (!aiText && !signal.aborted && callActive && isSpeaking) {
              if (legacyEarlyTts) {
                // L2b exposure gate (design revision 2026-08-21, review round 3) — TTS happens INSIDE run()
                // itself, interleaved with Claude streaming, unlike CONTROL/OBSERVED below where aiText is
                // set first and the shared TTS block (:1177) speaks it afterward. legacyEarlyTts deliberately
                // never sets aiText, so that shared block naturally no-ops here — audio for this turn is
                // already fully sent by the time this branch resolves.
                //
                // Signal separation (MANDATORY refinement 1) — the single most important invariant of this
                // branch: TTS is bound to the OUTER `signal` (generation/barge-in signal), never to
                // `childSignal` (the Claude-watchdog's own child). A 6000ms Claude-side timeout must only be
                // able to stop MORE Claude generation — it has NO right to abort audio already being sent or
                // already committed for an earlier chunk. Barge-in (outer `signal` aborting) still stops
                // everything, because childSignal is bridged from signal by attachChildAbort() inside
                // runAttemptWithWatchdog() itself (barge-in → signal aborts → child aborts too → Claude AND
                // in-flight speakFixedText() calls both stop, since speakFixedText's own isCurrentGeneration
                // guard also fires independently of which signal it was passed).
                // BLOCKER D fix (design review round 4) — runAttemptWithWatchdog() races [attemptPromise,
                // watchdogPromise] and returns the INSTANT the watchdog wins; it never waits for the loser
                // (attemptPromise) to actually settle, only attaches a late-rejection log handler to it. Since
                // speakFixedText() below is deliberately bound to the OUTER `signal` (not `childSignal`, per
                // the signal-separation invariant above), a watchdog fire does NOT stop an in-flight
                // speakFixedText() call — it keeps running in the background while runAttemptWithWatchdog
                // already returned control here. Without tracking it explicitly, the outcome-branching code
                // right below could read turnState.audioCommitted BEFORE the loser's TTS call has actually
                // committed audio, misclassify as precommit, speak a recovery phrase, and race an unrelated
                // audio stream still being sent from the loser — confirmed against real code, not assumed.
                let inFlightTtsPromise = null

                const freshAttempt = await runAttemptWithWatchdog({
                  signal,
                  timeoutMs: LEGACY_FRESH_CLAUDE_TIMEOUT_MS,
                  reason: 'LEGACY_CLAUDE_TIMEOUT',
                  run: async (childSignal, arm) => {
                    let canonicalFinalText = null
                    let endCallRequestedResult = false
                    const onEarlyTtsMilestone = (key, value) => {
                      const fieldMap = {
                        requestAt: 'legacyEarlyTtsRequestAt',
                        firstDeltaAt: 'legacyEarlyTtsFirstDeltaAt',
                        firstSafeAt: 'legacyEarlyTtsFirstSafeAt',
                        fullAt: 'legacyEarlyTtsFullAt',
                      }
                      const field = fieldMap[key]
                      if (field && turnMetrics[field] == null) turnMetrics[field] = value
                      else if (key === 'mode' && turnMetrics.legacyEarlyTtsMode == null) turnMetrics.legacyEarlyTtsMode = value
                      // MANDATORY refinement 2 — capture only, do NOT push session.messages here. Claude may
                      // complete (fullAt/finalText milestone) while an earlier chunk is still being spoken;
                      // pushing immediately risks a barge-in mid-tail seeing a fabricated complete history
                      // entry for audio the customer never actually heard in full. History is written once,
                      // at the shared tail (:1223-ish), from whatever this run() call returns — same timing
                      // as legacy's existing fullText push, not earlier.
                      else if (key === 'finalText') canonicalFinalText = value
                      else if (key === 'endCallRequested') endCallRequestedResult = value
                      // Track L (diagnostic only, design revision 2026-08-22, Design Gate R3 PASS) — object
                      // payload, handled separately from the scalar fieldMap above. Wrapped in its own
                      // try/catch (second independent guard, alongside the one already in claude.js around
                      // the emission itself) so a malformed payload here can never affect turnMetrics beyond
                      // these 5 fields, let alone propagate back into the Claude streaming loop.
                      else if (key === 'inputStats') {
                        try {
                          if (value && typeof value === 'object') {
                            turnMetrics.l2bSystemPromptCharCount = value.systemPromptCharCount ?? null
                            turnMetrics.l2bPriorHistoryCharCount = value.priorHistoryCharCount ?? null
                            turnMetrics.l2bRequestMessageCount = value.requestMessageCount ?? null
                            turnMetrics.l2bCurrentUserCharCount = value.currentUserCharCount ?? null
                            turnMetrics.l2bApproxInputTextCharCount = value.approxInputTextCharCount ?? null
                            turnMetrics.l2bCampaignPromptCharCount = value.campaignPromptCharCount ?? null
                          }
                        } catch (_) { /* diagnostic only — leave fields at their null default */ }
                      }
                      // Track O0 (diagnostic only, design LOCKED 2026-08-24 — Master Latency Design R3.2;
                      // Review Fix 1, 2026-08-25 — type-validated + atomic) — same pattern as
                      // chunkReasonStats: validate the whole payload BEFORE writing either field, so a
                      // wrong-typed value (string/NaN/negative) rejects the pair atomically instead of one
                      // field silently taking on a garbage value while the other stays correct.
                      else if (key === 'cacheUsage') {
                        try {
                          if (isValidCacheUsage(value)) {
                            turnMetrics.l2bCacheCreationTokens = value.cacheCreationInputTokens
                            turnMetrics.l2bCacheReadTokens = value.cacheReadInputTokens
                          }
                          // invalid payload → both fields stay at their null default, atomic (never partial)
                        } catch (_) { /* diagnostic only — leave fields at their null default */ }
                      }
                      else if (key === 'responseCharCount') {
                        turnMetrics.l2bResponseCharCount = typeof value === 'number' ? value : null
                      }
                      // Track M (diagnostic only, design R3 LOCKED 2026-08-22) — object payload, same pattern
                      // as inputStats above: own try/catch, malformed payload → fields stay/default null,
                      // never affects the rest of turnMetrics or the Claude/TTS streams themselves.
                      else if (key === 'chunkReasonStats') {
                        try {
                          if (isValidChunkReasonStats(value)) {
                            turnMetrics.l2bChunkReason = value.reason
                            turnMetrics.l2bChunkCharCount = value.charCount
                            turnMetrics.l2bChunkDeltaCount = value.deltaCount
                            turnMetrics.l2bChunkFirstCandidateElapsedMs = value.firstCandidateElapsedMs
                            turnMetrics.l2bChunkNumericProtectionBlocked = value.numericProtectionBlocked
                            turnMetrics.l2bChunkPreSafeDeltaGapMs = value.preSafeDeltaGapMs
                            turnMetrics.l2bChunkFirstSafeTrigger = value.firstSafeTrigger
                          }
                          // invalid payload → all 7 fields stay at their null default, atomic (never partial)
                        } catch (_) { /* diagnostic only — leave fields at their null default */ }
                      }
                      // Design review round 4 — Claude finishing (fullAt) must disarm the 6000ms watchdog.
                      // Without this, the SAME window also covers however long the tail TTS chunk(s) take
                      // after Claude is already fully done, so a slow-but-healthy ElevenLabs response could
                      // fire "LEGACY_CLAUDE_TIMEOUT" for a turn where Claude itself answered in 2s — a
                      // misleading reason and outcome for what is actually just TTS latency, matching what
                      // the existing chunked/adoptChunkedProducer path already accepts as its own risk (no
                      // dedicated per-chunk TTS watchdog there either — not a new gap introduced here).
                      //
                      // MUST be a separate unconditional check, not another `else if` in the chain above —
                      // 'fullAt' already matches fieldMap (first branch sets turnMetrics.legacyEarlyTtsFullAt),
                      // so as an else-if this line was dead code and the watchdog was never actually being
                      // disarmed (self-caught via the round-4 fullAt-disarm test failing against real code,
                      // not assumed from reading the diff).
                      if (key === 'fullAt') arm()
                    }
                    // canonical t3/t4 — unlike L2a (always null for legacy), L2b genuinely streams/speaks
                    // early, so these are meaningful the same way chunked path's t3/t4 already are.
                    let t3Marked = false, t4Marked = false
                    const wrappedMilestone = (key, value) => {
                      onEarlyTtsMilestone(key, value)
                      if (key === 'firstDeltaAt' && !t3Marked) { markOnce(turnMetrics, 't3'); t3Marked = true }
                      if (key === 'firstSafeAt' && !t4Marked) { markOnce(turnMetrics, 't4'); t4Marked = true }
                    }
                    for await (const chunk of askClaudeConditionalStream(currentSession, childSignal, wrappedMilestone)) {
                      const ttsPromise = speakFixedText({
                        text: chunk, signal, socket, streamSid,
                        voiceId: currentSession.campaign.voice_id,
                        turnMetrics, turnState, callState, generationId,
                        startingSentCount: totalSent,
                        onChunkAudioStart: (t) => noteActiveSpokenChunk(pipelineId, t),
                      })
                      inFlightTtsPromise = ttsPromise
                      const result = await ttsPromise
                      inFlightTtsPromise = null
                      totalSent += result.sentCount
                    }
                    return { canonicalFinalText, endCallRequestedResult }
                  },
                })

                // BLOCKER D fix, continued — if the watchdog won while a speakFixedText() call was still
                // outstanding (the loser scenario documented above), wait for it to actually settle before
                // touching turnState.audioCommitted or anything shared-tail-adjacent. The loser is bound to
                // the OUTER `signal`, so it either already committed audio or is about to — this must be known
                // before deciding precommit-vs-postcommit, not raced. Swallow the loser's own rejection here —
                // freshAttempt's outcome was already decided by runAttemptWithWatchdog and stands regardless.
                if (inFlightTtsPromise) {
                  try { await inFlightTtsPromise } catch { /* loser's own error is irrelevant to freshAttempt's outcome */ }
                }

                let shouldSpeakRecoveryEarlyTts = false
                if (freshAttempt.outcome === 'success' && freshAttempt.result?.canonicalFinalText?.trim()) {
                  fullText = freshAttempt.result.canonicalFinalText
                  endCallRequested = freshAttempt.result.endCallRequestedResult || endCallRequested
                  turnMetrics.legacyEarlyTtsOutcome = 'COMPLETED'
                } else if (freshAttempt.outcome === 'success') {
                  turnMetrics.legacyEarlyTtsOutcome = 'EMPTY'
                  if (!turnState.audioCommitted) shouldSpeakRecoveryEarlyTts = true
                } else if (freshAttempt.outcome === 'aborted') {
                  turnMetrics.legacyEarlyTtsOutcome = 'ABORTED'
                  // barge-in — เหมือน CONTROL/OBSERVED's aborted branch เป๊ะ ไม่พูด recovery ไม่ commit fullText
                } else if (freshAttempt.outcome === 'timeout') {
                  // MANDATORY refinement 1 — postcommit ต้องไม่พูด recovery/fabricate ทับเสียงที่ commit ไปแล้ว
                  if (turnState.audioCommitted) {
                    turnMetrics.legacyEarlyTtsOutcome = 'TIMEOUT_POSTCOMMIT'
                    console.log('[L2b] Claude tail timed out after audio already committed — no recovery phrase, no replay')
                  } else {
                    turnMetrics.legacyEarlyTtsOutcome = 'TIMEOUT_PRECOMMIT'
                    console.error(`[Watchdog] LEGACY_CLAUDE_TIMEOUT (L2b precommit) — Claude ไม่ตอบภายใน ${LEGACY_FRESH_CLAUDE_TIMEOUT_MS}ms, speaking recovery phrase`)
                    shouldSpeakRecoveryEarlyTts = true
                  }
                } else {
                  if (turnState.audioCommitted) {
                    turnMetrics.legacyEarlyTtsOutcome = 'ERROR_POSTCOMMIT'
                    console.error('[L2b] Tail error after audio already committed — not fabricating recovery/history:', freshAttempt.error.message)
                  } else {
                    turnMetrics.legacyEarlyTtsOutcome = 'ERROR'
                    console.error('[AI/TTS error] (L2b precommit)', freshAttempt.error.message)
                    healthMonitor.reportError('ai_tts', freshAttempt.error.message)
                    shouldSpeakRecoveryEarlyTts = true
                  }
                }

                if (shouldSpeakRecoveryEarlyTts) {
                  console.log('[Recovery] Speaking canned recovery phrase (L2b precommit)')
                  const recoveryResult = await speakFixedText({
                    text: LEGACY_RECOVERY_PHRASE, signal, socket, streamSid,
                    voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                    startingSentCount: totalSent,
                    onChunkAudioStart: (t) => noteActiveSpokenChunk(pipelineId, t),
                  })
                  totalSent += recoveryResult.sentCount
                  if (recoveryResult.sentCount > 0) {
                    const deliveredFully = !signal.aborted && isCurrentGeneration(callState, generationId) && callActive && socket.readyState === socket.OPEN
                    fullText = deliveredFully
                      ? LEGACY_RECOVERY_PHRASE
                      : '[ระบบ: คำตอบก่อนหน้าถูกขัดจังหวะหลังเริ่มส่งเสียง ลูกค้าอาจได้ยินเพียงบางส่วน]'
                  }
                }

                // MANDATORY refinement 3 — premature END_CALL guard, L2b-local equivalent of the legacy-only
                // guard further below (that one only ever sees fullText.includes('[END_CALL]'), which is
                // always false for L2b by design — L2b's own signal is the endCallRequested boolean instead).
                if (endCallRequested && shouldBlockEndCall(currentSession, fullText)) {
                  console.log('[Guard] Premature END_CALL blocked — injecting follow-up question (L2b)')
                  const followUp = 'มีอะไรสอบถามเพิ่มเติมไหมคะ'
                  endCallRequested = false // MUST reset — shared tail (:1247-ish) checks this boolean directly
                  fullText = `${fullText} ${followUp}`.trim()
                  try {
                    markOnce(turnMetrics, 't5')
                    markTtsPending(turnState)
                    for await (const followUpChunk of synthesizeSpeechStream(followUp, currentSession.campaign.voice_id, signal)) {
                      if (socket.readyState !== socket.OPEN || signal.aborted) break
                      markOnce(turnMetrics, 't6')
                      if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                      if (totalSent === 0) noteActiveSpokenChunk(pipelineId, followUp)
                      socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: followUpChunk.toString('base64') } }))
                      markOnce(turnMetrics, 't7')
                      markAudioCommitted(turnState)
                      totalSent++
                    }
                  } catch (err) {
                    if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
                      console.error('[Guard TTS error] (L2b)', err.message)
                      healthMonitor.reportError('tts', err.message)
                    }
                  }
                }
              } else {
              const freshAttempt = await runAttemptWithWatchdog({
                signal,
                timeoutMs: LEGACY_FRESH_CLAUDE_TIMEOUT_MS,
                reason: 'LEGACY_CLAUDE_TIMEOUT',
                run: async (childSignal) => {
                  let text = ''
                  // L2a exposure gate (design revision 2026-08-20) — เฉพาะสายที่ legacyObserved=true เท่านั้นที่
                  // สลับไปใช้ askClaudeObservedFullResponse() ([.stream()) — สายอื่นทั้งหมด (CONTROL) ยังเรียก
                  // askClaudeStream() ([.create()]) เดิมเป๊ะ ไม่มี milestone ใดๆ ถูกเขียนเข้า turnMetrics เลย ทำให้
                  // legacyClaude* fields ทั้งหมดคง null ตามที่ CONTROL ควรเป็น — นี่คือ "เบรก" ที่กัน a982fdf
                  // ไม่ให้เปลี่ยน transport ของ fresh legacy call ทุกสายทันทีที่ push แม้ rollout_percent=0 ก็ตาม
                  // prewarm (:389) และ runLegacyFallback (:205) ยังเรียก askClaudeStream() เหมือนเดิมทุกประการ
                  // ไม่ถูกกระทบเลย ไม่ว่า legacyObserved จะเป็นอะไร (นอกขอบเขตของ gate นี้ทั้งคู่)
                  if (legacyObserved) {
                    const onLegacyClaudeMilestone = (key, value) => {
                      const fieldMap = {
                        requestAt: 'legacyClaudeRequestAt',
                        firstDeltaAt: 'legacyClaudeFirstDeltaAt',
                        firstSafeAt: 'legacyClaudeFirstSafeAt',
                        fullAt: 'legacyClaudeFullAt',
                      }
                      const field = fieldMap[key]
                      if (field && turnMetrics[field] == null) turnMetrics[field] = value
                    }
                    try {
                      for await (const chunk of askClaudeObservedFullResponse(currentSession, childSignal, onLegacyClaudeMilestone)) {
                        if (childSignal.aborted) break
                        text += (text ? ' ' : '') + chunk
                      }
                    } catch (err) {
                      // normalize barge-in/watchdog-driven cancellation: ถ้า childSignal ถูก abort ไปแล้ว ให้ resolve
                      // แบบ graceful แทนที่จะ throw ต่อ — ไม่งั้น runAttemptWithWatchdog จะจัดเป็น outcome:'error' แทน
                      // 'aborted' (เพราะ helper คืน 'aborted' เฉพาะตอน run() resolve หลัง child ถูก abort เท่านั้น)
                      // ทำให้ barge-in ถูกเข้าใจผิดเป็น Claude error แล้วพูด recovery phrase ทับเสียงลูกค้าที่กำลังพูดแทรกอยู่
                      if (childSignal.aborted) return text
                      throw err // error จริงที่ไม่เกี่ยวกับ abort ต้อง propagate ต่อให้ outcome:'error' เหมือนเดิม
                    }
                    return text
                  }

                  // CONTROL — โค้ดเดิมเป๊ะก่อน commit a982fdf (รวม try/catch normalization เดียวกัน — askClaudeStream()
                  // ก็ signal-aware ผ่าน client.messages.create({...}, {signal}) เหมือนกัน อาจ throw AbortError ได้
                  // ไม่ต่างจาก .stream() จึงต้อง normalize เหมือนกันทุกประการ ไม่ใช่แค่ bare loop)
                  try {
                    for await (const chunk of askClaudeStream(currentSession, false, childSignal)) {
                      if (childSignal.aborted) break
                      text += (text ? ' ' : '') + chunk
                    }
                  } catch (err) {
                    if (childSignal.aborted) return text
                    throw err
                  }
                  return text
                },
              })

              let shouldSpeakRecovery = false

              // L2a — legacyClaudeOutcome แยกจาก legacyClaudeFirstSafeAt=null โดยตั้งใจ (ดู comment ที่ turnMetrics.js)
              // เขียนเฉพาะตอน legacyObserved=true เท่านั้น — CONTROL ต้องคง legacyClaudeOutcome=null (เหมือนทุก
              // legacyClaude* field อื่น) ไม่ใช่แค่ timestamp fields เท่านั้นที่ต้อง null การแยกสาขา
              // aiText/shouldSpeakRecovery/console log ทั้งหมดยังทำงานเหมือนกันทุกประการไม่ว่า CONTROL หรือ OBSERVED
              if (freshAttempt.outcome === 'success' && freshAttempt.result?.trim()) {
                aiText = freshAttempt.result
                if (legacyObserved) turnMetrics.legacyClaudeOutcome = 'COMPLETED'
              } else if (freshAttempt.outcome === 'success') {
                // askClaudeStream() yield ข้อความก็ต่อเมื่อยาวอย่างน้อย 3 ตัวอักษรเท่านั้น (ดูตัวมันเอง) — เรียก
                // Claude สำเร็จแต่ไม่มีอะไรให้พูดออกไปเลยก็ยังถือว่า "ไม่ได้คำตอบที่ใช้ได้" เหมือน timeout/error ทุก
                // ประการ ต้องพูด recovery phrase เช่นกัน ไม่งั้นลูกค้าจะเจอความเงียบทั้งที่ API เรียกสำเร็จ
                console.error('[AI/TTS error] Claude ตอบสำเร็จแต่ไม่มีข้อความให้พูดเลย (empty/blank result), speaking recovery phrase')
                shouldSpeakRecovery = true
                if (legacyObserved) turnMetrics.legacyClaudeOutcome = 'EMPTY'
              } else if (freshAttempt.outcome === 'aborted') {
                // barge-in — ไม่พูด recovery phrase, ไม่ commit อะไร, ปล่อยไหลลง shared tail ตามปกติ (เหมือน
                // prewarm's aborted branch ด้านบน) เพื่อให้ pendingTranscript (ถ้ามี) ถูก drain ต่อได้
                console.log('[AI] Fresh Claude call aborted by barge-in — no recovery phrase, turn ends with no audio')
                if (legacyObserved) turnMetrics.legacyClaudeOutcome = 'ABORTED'
              } else if (freshAttempt.outcome === 'timeout') {
                console.error(`[Watchdog] LEGACY_CLAUDE_TIMEOUT — Claude ไม่ตอบภายใน ${LEGACY_FRESH_CLAUDE_TIMEOUT_MS}ms, speaking recovery phrase`)
                shouldSpeakRecovery = true
                if (legacyObserved) turnMetrics.legacyClaudeOutcome = 'TIMEOUT'
              } else {
                console.error('[AI/TTS error]', freshAttempt.error.message)
                healthMonitor.reportError('ai_tts', freshAttempt.error.message)
                shouldSpeakRecovery = true
                if (legacyObserved) turnMetrics.legacyClaudeOutcome = 'ERROR'
              }

              // timeout/error จริง/success-แต่ข้อความว่างเปล่า — ทั้งสามแบบพูด recovery phrase เดียวกัน (จากมุม
              // ลูกค้าคือ "ไม่ได้คำตอบที่ใช้ได้" เหมือนกันหมด)
              if (shouldSpeakRecovery) {
                console.log('[Recovery] Speaking canned recovery phrase')
                const recoveryResult = await speakFixedText({
                  text: LEGACY_RECOVERY_PHRASE, signal, socket, streamSid,
                  voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                  startingSentCount: totalSent,
                  onChunkAudioStart: (t) => noteActiveSpokenChunk(pipelineId, t),
                })
                totalSent += recoveryResult.sentCount
                if (recoveryResult.sentCount > 0) {
                  // ห้าม assume ว่าพูดจบครบเพียงเพราะประโยคสั้น — speakFixedText() หยุดกลางทางได้จริงถ้า generation
                  // stale/signal abort/socket ปิดระหว่างพูด (guard อยู่ใน synthesizeAndSend เอง) sentCount>0 พิสูจน์
                  // แค่ "เริ่มพูดแล้ว" ไม่ใช่ "พูดจบแล้ว" — ต้องเช็คสถานะ ณ ตอนนี้ (หลัง speakFixedText คืนค่าแล้ว) ด้วย
                  const deliveredFully = !signal.aborted && isCurrentGeneration(callState, generationId) && callActive && socket.readyState === socket.OPEN
                  fullText = deliveredFully
                    ? LEGACY_RECOVERY_PHRASE
                    : '[ระบบ: คำตอบก่อนหน้าถูกขัดจังหวะหลังเริ่มส่งเสียง ลูกค้าอาจได้ยินเพียงบางส่วน]'
                }
                // sentCount === 0 → ไม่ commit อะไรเข้า history เลย (fullText คงเป็น '' เหมือนเดิม)
              }
              } // end else (CONTROL/legacyObserved — L2b takes the `if (legacyEarlyTts)` branch above instead)
            }

            if (aiText && !signal.aborted && callActive && isSpeaking) {
              console.log(`[AI] "${aiText}"`)
              fullText = aiText
              const cleanText = aiText.replace(/\[END_CALL\]/g, '').trim()
              if (cleanText) {
                try {
                  markOnce(turnMetrics, 't5')
                  markTtsPending(turnState)
                  for await (const chunk of synthesizeSpeechStream(cleanText, currentSession.campaign.voice_id, signal)) {
                    if (socket.readyState !== socket.OPEN || signal.aborted) break
                    markOnce(turnMetrics, 't6')
                    // จุดวัด "ความเงียบจริง" ที่ลูกค้ารู้สึก — ต่างจาก [AI full] ที่รวมเวลาพูดทั้งประโยคเข้าไปด้วย
                    // (ประโยคยาวก็ใช้เวลาส่งครบนานกว่าเป็นธรรมชาติ ไม่ได้แปลว่าดีเลย์มากขึ้น) ต้องวัดจาก [STT] ถึง log นี้เท่านั้น
                    if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                    if (totalSent === 0) noteActiveSpokenChunk(pipelineId, cleanText)
                    socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                    markOnce(turnMetrics, 't7')
                    markAudioCommitted(turnState) // อยู่หลัง signal.aborted/readyState check (legacy staleness ของลูปนี้) และหลัง socket.send() จริงเท่านั้น
                    totalSent++
                  }
                } catch (err) {
                  if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
                    console.error('[TTS error]', err.message)
                    healthMonitor.reportError('tts', err.message)
                  }
                }
              }
            }

            if (fullText.includes('[END_CALL]') && shouldBlockEndCall(currentSession, fullText)) {
              console.log('[Guard] Premature END_CALL blocked — injecting follow-up question')
              const followUp = 'มีอะไรสอบถามเพิ่มเติมไหมคะ'
              fullText = fullText.replace(/\[END_CALL\]/g, '').trim() + ' ' + followUp
              try {
                markOnce(turnMetrics, 't5')
                markTtsPending(turnState)
                for await (const chunk of synthesizeSpeechStream(followUp, currentSession.campaign.voice_id, signal)) {
                  if (socket.readyState !== socket.OPEN || signal.aborted) break
                  markOnce(turnMetrics, 't6')
                  // เผื่อ cleanText ว่างเปล่า (คำตอบ AI มีแค่ [END_CALL] ล้วนๆ) — ลูปหลักด้านบนไม่ได้ส่งอะไรเลย
                  // ทำให้นี่กลายเป็นก้อนเสียงแรกจริงของเทิร์นนี้ ต้อง log จุดนี้ด้วยกันพลาดข้อมูล
                  if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                  if (totalSent === 0) noteActiveSpokenChunk(pipelineId, followUp)
                  socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                  markOnce(turnMetrics, 't7')
                  markAudioCommitted(turnState) // อยู่หลัง signal.aborted/readyState check (legacy staleness ของลูปนี้) และหลัง socket.send() จริงเท่านั้น
                  totalSent++
                }
              } catch (err) {
                if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
                  console.error('[Guard TTS error]', err.message)
                  healthMonitor.reportError('tts', err.message)
                }
              }
            }
          } catch (err) {
            // Track P — defensive settlement: if an unexpected exception here happened BEFORE the primary
            // settlePrewarmDiag() call above ever ran, prewarmDiag would otherwise be left dangling across the
            // turn boundary. settlePrewarmDiag()'s own identity/settled guard makes this a pure no-op if
            // primary settlement already happened normally — never a double emission. Diagnostic-only, must
            // never alter the existing AI/TTS error handling below it.
            //
            // Review Gate fix — the 4 state-at-final fields (and settle-relative, if the attempt happened to
            // have already settled by the time the exception hit) were already computed at the shared
            // pre-split snapshot boundary above, before this turn ever entered the try block — passing them
            // here means an ERROR record still carries real state-at-final data instead of going all-null
            // just because the exception happened to land before primary settlement got a chance to run.
            if (myPrewarmDiag) {
              const prewarmAttemptSettleRelativeToFinalMs = myPrewarmAttempt?.settledAt != null
                ? Math.round(myPrewarmAttempt.settledAt - finalAcceptedAt)
                : null
              settlePrewarmDiag(myPrewarmDiag, 'ERROR', {
                generationId,
                initialPrewarmAgeAtFinalMs,
                lastPrewarmAgeAtFinalMs,
                prewarmStateAtFinal,
                prewarmReadyBeforeFinal,
                prewarmAttemptSettleRelativeToFinalMs,
              })
            }
            console.error('[AI/TTS error]', err.message)
            healthMonitor.reportError('ai_tts', err.message)
          } finally {
            clearTimeout(processingGuard)
            if (prewarmPromise === myPrewarm) clearPrewarm()
            // เช็ค pipelineId ก่อน null ทิ้ง — ไม่งั้น pipeline เก่าที่เพิ่งโดน barge-in อาจไป null ทับ
            // ttsAbortController ของ pipeline ใหม่ที่เพิ่งสร้างขึ้นมา ทำให้ barge-in ครั้งถัดไปตัดเสียง AI ไม่ขาด
            if (activePipelineId === pipelineId) {
              ttsAbortController = null
              sttProcessing = false
            }
          }
          } // end legacy else branch (C3a)

          if (fullText) {
            currentSession.messages.push({ role: 'assistant', content: fullText })
            console.log(`[AI full] "${fullText}"`)
          }

          markDone(turnState) // ทุก pre-terminal phase ไปจบที่ DONE ได้ตรงๆ (turnState.js ไม่ guard transition นี้) — ครอบคลุมทุกทางจบของ legacy turn
          console.log('[Metrics]', JSON.stringify({ ...turnMetrics, ...computeDerivedMetrics(turnMetrics) }))

          const hasAudio = !signal?.aborted && isSpeaking && socket.readyState === socket.OPEN && totalSent > 0
          if (hasAudio) {
            pendingSpokenText = { pipelineId, text: fullText }
            socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: ownedMarkName('ai_done', pipelineId) } }))
          } else if (totalSent === 0) {
            isSpeaking = false
          }

          const playbackMs = totalSent * 20 + 1500
          setTimeout(() => {
            if (activePipelineId === pipelineId && isSpeaking) {
              console.log('[Audio] Fallback unlock')
              isSpeaking = false
              startSilenceTimer()
            }
          }, playbackMs)

          if (fullText.includes('[END_CALL]') || endCallRequested) {
            pendingEndCall = true
            currentSession.hangupReason = 'ai_ended'
            // Fallback: ปิดสายถ้า mark ไม่มาภายในเวลาที่คาดไว้
            const fallbackDelay = totalSent * 20 + 5000
            setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, fallbackDelay)
          }

          // Design B: no-audio completion — ต้องเรียกหลัง pendingEndCall ถูกกำหนดค่าสุดท้ายของ turn นี้แล้วเท่านั้น
          // (บรรทัดข้างบน) ไม่ใช่ก่อนหน้า ไม่งั้น ack อาจถูก deliver ไปแล้วก่อนที่ turn จะรู้ตัวว่ากลายเป็น terminal
          // จริง — กรณีมี audio (hasAudio=true) ต้องรอ owned mark กลับมาจริง ไม่ deliver ตรงนี้
          if (!hasAudio) await tryDeliverPendingShortAck(pipelineId, currentSession)

          // C6c follow-up: sttProcessing ถูกปล่อยไปแล้วจริง (ใน finally ของ branch ด้านบน) ณ จุดนี้ — ถ้ามี
          // transcript ที่พูดแทรกเข้ามาระหว่างเทิร์นนี้ยังทำงานอยู่ (เก็บไว้ใน pendingTranscript โดย onTranscript)
          // ให้ process ต่อทันทีในฐานะเทิร์นใหม่ปกติ — เช็ค callActive กันไม่ให้ process ต่อหลังสายวางไปแล้ว
          if (pendingTranscript && callActive) {
            const next = pendingTranscript
            pendingTranscript = null
            await processTranscript(next.session, next.text)
          }
        }
        processTranscriptDispatch = processTranscript // Design B: ref สำหรับ mark handler/no-audio sites ที่อยู่นอก scope นี้ (mark handler เป็น sibling if-block, ไม่ hoist ฟังก์ชันใหญ่ทั้งก้อน)

        // STT-A1 (observability only, 2026-08-21): "dumb" emitter — ประกอบ [STT_DIAG] JSON + log เท่านั้น ไม่อ่าน
        // isSpeaking/bargeInCooldown/bargeInPendingFinal หรือ derive คำตัดสินใจใดๆ เอง — disposition/reason ต้องถูก
        // ตัดสินที่ branch เดิมเป๊ะแล้วส่งเข้ามาเป็นค่าสำเร็จรูปเท่านั้น กัน diagnostic drift จาก filter logic จริง
        // "diagnostic failure must never affect call flow" — ห่อ try/catch ทั้งก้อน ไม่มีทางโยน error ออกไปกระทบสายจริง
        //
        // Enum: disposition = DELIVERED | DROPPED | DEFERRED, reason = null | POST_MARK_ECHO |
        // ACTIVE_PLAYBACK_ECHO | TIER2_ACK | PENDING_TRANSCRIPT | BUSY
        // (SHORT_FRAGMENT_ECHO retired by Active-Playback Speech Guard R1 — replaced by ACTIVE_PLAYBACK_ECHO,
        // which requires echo evidence instead of a whitespace-invalid-for-Thai length heuristic)
        // (BARGE_IN_COOLDOWN retired by Final Cooldown Preservation R1.1 — a FINAL arriving during cooldown
        // now flows through to DELIVERED/PENDING_TRANSCRIPT like any other, never silently DROPPED; cooldown
        // still suppresses a repeat physical bargeIn() call when isSpeaking is already false, see the
        // BARGE_COOLDOWN_INTERRUPT_SUPPRESSED console.log for that specific case's observability)
        //
        // STT-A2 (design revision 2026-08-21) — `alternatives` key present only when sttMeta.alternatives is
        // truthy (A2 ON for this utterance). Diagnostic-only: `text`/delivered transcript above always comes
        // from alternatives[0] regardless of A2, this array is never used to select what enters conversation.
        //
        // Review Gate round 2 amendment — DEFERRED/PENDING_TRANSCRIPT คือ disposition ณ ขณะที่ callback exit
        // เท่านั้น ไม่ใช่ final lifecycle outcome ของ transcript นั้น: ถ้า pendingTranscript ถูก latest-wins
        // แทนที่ด้วยตัวใหม่ทีหลัง (ดู 3 จุดที่ set pendingTranscript ด้านล่าง) หรือถูก bargeIn() ล้างทิ้ง ไม่มี
        // event ย้อนกลับมาบอกว่า item เดิมกลายเป็นอะไรต่อ — known limitation เดียวกับที่ยอมรับไว้แล้วสำหรับ
        // TIER2_ACK (pendingShortAck ก็มีชะตากรรมแบบเดียวกันทุกประการ)
        const emitSttDiag = (sttMeta, disposition, reason, text) => {
          if (!sttMeta) return
          try {
            console.log(`[STT_DIAG] ${JSON.stringify({
              callSid,
              streamId: sttMeta.streamId,
              utteranceId: sttMeta.utteranceId,
              source: sttMeta.source,
              disposition,
              reason,
              interimCount: sttMeta.interimCount,
              regressionCount: sttMeta.regressionCount,
              firstInterimAt: sttMeta.firstInterimAt,
              lastInterimAt: sttMeta.lastInterimAt,
              finalAt: sttMeta.finalAt,
              firstInterimToFinalMs: sttMeta.firstInterimToFinalMs,
              lastStability: sttMeta.lastStability,
              maxStability: sttMeta.maxStability,
              finalConfidence: sttMeta.finalConfidence,
              coldMutePackets: sttMeta.coldMutePackets,
              // STT-A2 (diagnostic only): key ปรากฏเฉพาะตอน A2 มีข้อมูลจริง — non-A2 calls ต้องไม่มี key นี้เลย
              ...(sttMeta.alternatives ? { alternatives: sttMeta.alternatives } : {}),
              text,
            })}`)
          } catch (e) {
            console.error('[STT_DIAG] emit failed (non-fatal, ignored):', e.message)
          }
        }

        // A2.1 Shadow Google Final Diagnostics (design revision 2026-08-21, Design Gate v2 PASS) — separate
        // log line from [STT_DIAG] on purpose (design requirement: shadow events must be clearly separated
        // from live-stream events, both in code and in logs). googleSTT.js's settleShadow() already wraps
        // this callback in try/catch (a throwing diagnostic must never affect STT lifecycle) — the try/catch
        // here mirrors emitSttDiag()'s own defensive pattern above for consistency, not because it's required.
        const emitSttShadowDiag = (payload) => {
          try {
            console.log(`[STT_SHADOW_DIAG] ${JSON.stringify({ callSid, ...payload })}`)
          } catch (e) {
            console.error('[STT_SHADOW_DIAG] emit failed (non-fatal, ignored):', e.message)
          }
        }

        // เริ่ม STT stream
        sttStream = transcribeStream(async (transcript, sttMeta) => {
          if (!transcript || !callActive) return
          if (pendingEndCall) return
          if (socket.readyState !== socket.OPEN) return
          // Final Cooldown Preservation R1.1 (design locked) — cooldown must suppress a REPEAT physical
          // interrupt only, never the transcript itself (same "echo evidence required to drop" doctrine as
          // Active-Playback Speech Guard R1 — a blanket time-based drop with no evidence contradicts it).
          // If isSpeaking is already false, bargeIn() further below is a guaranteed no-op anyway (its own
          // top-line guard) — nothing is left to physically interrupt, so this transcript must flow through
          // to the exact same queue/immediate-dispatch path bargeInPendingFinal already uses (sttProcessing
          // check further below), not be silently dropped. If isSpeaking is true, a NEW pipeline started
          // speaking during the old cooldown window — a genuinely new interruption target unrelated to the
          // previous barge-in's trailing-echo grace window — so it must not be gated here at all; let the
          // isSpeaking branch below handle it exactly as if there were no cooldown.
          if (bargeInCooldown && !bargeInPendingFinal && !isSpeaking) {
            // Echo evidence still wins over cooldown-preservation (design section 10 — must remain FIRST).
            // activeSpokenRef of the pipeline that was just interrupted is still valid/readable here —
            // bargeIn() never bumps activePipelineId itself (only the next real speaking pipeline does), so
            // this is the same reference isLikelyPostMarkEcho() would use during isSpeaking=true, just read
            // one step later. Same Tier1-ack exemption as the isSpeaking branch (see its own comment).
            const cooldownRefText = (activeSpokenRef && activeSpokenRef.pipelineId === activePipelineId) ? activeSpokenRef.text : null
            const cooldownAckTier = classifyAck(transcript)
            if (cooldownAckTier !== 'TIER1' && isLikelyPostMarkEcho(transcript, cooldownRefText)) {
              console.log(`[STT] Active-playback echo (post-interrupt cooldown) — ignoring: "${transcript}"`)
              emitSttDiag(sttMeta, 'DROPPED', 'ACTIVE_PLAYBACK_ECHO', transcript)
              return
            }
            console.log(`[STT] BARGE_COOLDOWN_INTERRUPT_SUPPRESSED — no repeat interrupt, transcript preserved: "${transcript.substring(0, 40)}"`)
          }
          // Post-mark echo filter: fragment ภายใน 500ms ของ mark ที่เป็นหางของสิ่ง AI เพิ่งพูดจริง =
          // delayed PSTN echo (Lightweight Post-Mark Echo Guard, design locked — เดิมใช้ whitespace
          // word-count/length ซึ่งผิดกับภาษาไทย ดู isLikelyPostMarkEcho() ด้านบนสำหรับ root cause เต็ม)
          // Design B: คำรับคำสั้นที่รู้จัก (ครับ/ค่ะ/โอเค/ok ฯลฯ ทั้ง 2 tier) ได้รับการยกเว้นจาก filter นี้ — mark
          // ยืนยันแล้วว่า playback คิวเดิมเล่นจบจริง ความเสี่ยง echo ต่ำกว่าตอน isSpeaking=true มาก ไม่ต้องแยก tier
          const msSinceMark = Date.now() - lastMarkTime
          if (msSinceMark < 500 && !classifyAck(transcript)) {
            if (isLikelyPostMarkEcho(transcript, lastMarkedSpokenText)) {
              console.log(`[STT] Echo suppressed (${msSinceMark}ms after mark): "${transcript}"`)
              emitSttDiag(sttMeta, 'DROPPED', 'POST_MARK_ECHO', transcript)
              return
            }
          }

          const currentSession = callSessions.get(callSid)
          if (!currentSession) return

          console.log(`[STT] "${transcript}"`)
          clearSilenceTimer()
          silencePromptCount = 0

          // Barge-in: ต้องเช็คก่อน sttProcessing เสมอ (ดูหมายเหตุที่ processTranscript() ด้านบน)
          if (isSpeaking) {
            // Design B: Tier2 ack (ครับ/ค่ะ/ใช่/ไม่/โอเค/ok เดี่ยวๆ) คือ particle ที่ AI เองพูดลงท้ายประโยคบ่อย
            // ที่สุด เสี่ยง false-echo สูงสุด — ไม่ bargeIn ทันที เก็บไว้รอ owned mark/no-audio completion ของ
            // pipeline นี้ก่อน (ดู tryDeliverPendingShortAck) Tier1 (คำผสม เช่น "โอเคครับ") เสี่ยง false-echo ต่ำกว่า
            // มาก ปล่อยผ่านไป bargeIn() ตามปกติด้านล่างได้เลย
            const ackTier = classifyAck(transcript)
            if (ackTier === 'TIER2') {
              pendingShortAck = { text: transcript, pipelineId: activePipelineId, capturedAt: Date.now() }
              console.log(`[STT] Tier2 ack deferred during AI speech: "${transcript}"`)
              emitSttDiag(sttMeta, 'DEFERRED', 'TIER2_ACK', transcript)
              return
            }
            // Active-Playback Speech Guard R1 (design locked) — replaces the old whitespace wordCount/length
            // heuristic (invalid for Thai — see isLikelyPostMarkEcho's own comment). ECHO EVIDENCE REQUIRED
            // TO DROP: compare against activeSpokenRef (AI text whose audio has actually started being sent,
            // for the CURRENTLY active pipeline only — never a stale one) using the exact same algorithm as
            // POST_MARK_ECHO. No reference available (e.g. very first words of this pipeline's audio, before
            // any chunk committed yet) → ACCEPT by default, per isLikelyPostMarkEcho's own null-safe contract.
            const activeRefText = (activeSpokenRef && activeSpokenRef.pipelineId === activePipelineId) ? activeSpokenRef.text : null
            if (ackTier !== 'TIER1' && isLikelyPostMarkEcho(transcript, activeRefText)) {
              console.log(`[STT] Active-playback echo — ignoring: "${transcript}"`)
              emitSttDiag(sttMeta, 'DROPPED', 'ACTIVE_PLAYBACK_ECHO', transcript)
              return
            }
            bargeIn()
            bargeInCooldown = true
            setTimeout(() => { bargeInCooldown = false }, 400)

            if (sttProcessing) {
              // เทิร์นเดิมยัง await Claude/TTS อยู่จริง (ยังไม่ถึง finally ของมันเอง) — ห้าม process ซ้อนตอนนี้
              // เก็บ transcript นี้ไว้ก่อน แล้วให้เทิร์นเดิมเป็นคนสั่ง process ต่อเอง (ดู processTranscript() ด้านบน)
              pendingTranscript = { session: currentSession, text: transcript }
              console.log('[Barge-in] Old turn still processing — transcript queued (latest-wins)')
              emitSttDiag(sttMeta, 'DEFERRED', 'PENDING_TRANSCRIPT', transcript)
              return
            }
            await new Promise(r => setTimeout(r, 200))
            // Review Gate round 3 (accepted as-is): defensive telemetry ภายใต้ invariant ปัจจุบัน — bargeIn() ตั้ง
            // bargeInCooldown 400ms (นานกว่า 200ms นี้) และ transcript อื่นทุกตัวที่มาระหว่างนี้โดน cooldown ก่อนจะมี
            // โอกาสตั้ง sttProcessing=true ทัน จึง reachability ยัง unproven จาก code review + probe เชิงประจักษ์
            // (ดูรายงานรอบ review) — ห้ามเพิ่ม test-only hook หรือแก้ state machine เพื่อบังคับ coverage ให้ branch นี้
            if (sttProcessing) { emitSttDiag(sttMeta, 'DROPPED', 'BUSY', transcript); return } // เผื่อมี turn อื่นแทรกเข้ามาระหว่าง 200ms นี้
          } else if (bargeInPendingFinal) {
            // C6c follow-up (STT listening): interim ระหว่าง isSpeaking=true เพิ่ง trigger bargeIn() ไปแล้ว (ดู
            // onInterim ด้านล่าง) — isSpeaking ถูก bargeIn() ตั้งเป็น false ไปแล้วตั้งแต่ตอนนั้น final ตัวนี้จึงไม่
            // เข้า `if (isSpeaking)` ด้านบนอีก แต่คือประโยคที่พูดแทรกจริงที่รอมาส่งต่อ ไม่ใช่ transcript ทั่วไปที่ยัง
            // ไม่เคย trigger อะไรเลย — ต้องใช้ path เดียวกับ queueing/pendingTranscript ไม่ใช่ปล่อยผ่านไป busy-drop
            bargeInPendingFinal = false
            if (sttProcessing) {
              pendingTranscript = { session: currentSession, text: transcript }
              console.log('[Barge-in] Final transcript after interim-triggered barge-in — queued (latest-wins)')
              emitSttDiag(sttMeta, 'DEFERRED', 'PENDING_TRANSCRIPT', transcript)
              return
            }
            // sttProcessing ว่างแล้ว (เทิร์นเดิมจบไปแล้วจริงตั้งแต่ก่อน final นี้มาถึง) — ไปต่อด้านล่างได้เลยตามปกติ
          }

          if (sttProcessing) {
            if (pendingTranscript) {
              // ยังอยู่ในช่วงรอเทิร์นเดิมปล่อย sttProcessing หลัง barge-in ไปแล้ว (isSpeaking ถูก bargeIn() ตั้งเป็น
              // false ไปแล้วตั้งแต่รอบก่อนหน้า แต่ pendingTranscript ยังไม่ถูก drain) — STT อาจส่ง final ซ้ำ/fragment
              // ต่อเนื่องระหว่างที่ลูกค้ายังพูดอยู่ ถือเป็นส่วนหนึ่งของการพูดแทรกเดียวกัน ใช้ตัวล่าสุดแทนที่เสมอ
              // (latest-wins ช่องเดียว ไม่ FIFO — กัน AI ตอบหลาย turn จากการพูดแทรกครั้งเดียว)
              pendingTranscript = { session: currentSession, text: transcript }
              console.log('[Barge-in] Additional transcript while still queued — replacing with latest')
              emitSttDiag(sttMeta, 'DEFERRED', 'PENDING_TRANSCRIPT', transcript)
              return
            }
            // non-barge-in overlap (isSpeaking=false แต่ sttProcessing=true) — กันเหมือนเดิมทุกประการ
            //
            // Review Gate round 3 (accepted as-is): เช่นเดียวกับ BUSY อีกจุดด้านบน — defensive telemetry ภายใต้
            // invariant ปัจจุบัน ปกติ isSpeaking=false ระหว่าง sttProcessing=true ยังคง true จะเจอ pendingTranscript
            // ตั้งไว้แล้วเข้า branch latest-wins ด้านบนก่อนถึงตรงนี้เสมอ (bargeIn() คือจุดเดียวที่ตั้ง isSpeaking=false
            // ระหว่างที่ sttProcessing ยัง true และมันตั้ง pendingTranscript ไปพร้อมกันเสมอ) reachability ของ branch
            // นี้เองยัง unproven เช่นกัน — ห้ามเพิ่ม test-only hook หรือแก้ state machine เพื่อบังคับ coverage
            const trimmed = transcript.trim()
            const wc = trimmed ? trimmed.split(/\s+/).length : 0
            console.log(`[STT] Transcript dropped (busy, ${wc} words): "${transcript}"`)
            emitSttDiag(sttMeta, 'DROPPED', 'BUSY', transcript)
            return
          }

          // DELIVERED = ผ่านทุก filter แล้วถูกส่งเข้า processTranscript() จริง — ไม่ได้ยืนยันว่า Claude API ได้รับ
          // คำขอแล้ว (นั่นคือ generation/network layer คนละชั้น ไม่ใช่สิ่งที่ STT-A1 วัด)
          emitSttDiag(sttMeta, 'DELIVERED', null, transcript)
          await processTranscript(currentSession, transcript)
        }, (interimText) => {
          if (!callActive || bargeInCooldown) return

          if (isSpeaking) {
            // C6c follow-up (STT listening): STT ตอนนี้ฟังต่อเนื่องระหว่าง AI พูดจริงแล้ว (googleSTT.js rotate
            // ทันทีหลัง utterance ก่อนหน้า deliver ไม่รอ Twilio mark อีกต่อไป) — interim ระหว่าง isSpeaking=true
            // คือสัญญาณ interrupt-control เท่านั้น ต้อง trigger bargeIn() ทันทีไม่ว่า sttProcessing จะเป็นอะไร
            // (เช็คก่อน sttProcessing เสมอ เหมือนที่แก้ไปแล้วใน onTranscript — sttProcessing ไม่มีสิทธิ์ขวาง
            // interrupt control อีกต่อไป) เร็วกว่ารอ final ซึ่งมี debounce 900ms ในตัว แต่ห้ามส่ง text นี้เข้า
            // Claude ตรงๆ เด็ดขาด — ต้องรอ final ของประโยคเดียวกันเสมอ (ดู bargeInPendingFinal ใน onTranscript ด้านบน)
            // เพื่อไม่ให้ Claude เห็นประโยคที่พูดยังไม่จบ
            //
            // Active-Playback Speech Guard R1 (design locked) — replaces the old whitespace wordCount/length
            // heuristic with two checks, in order:
            //   1. Echo evidence (same isLikelyPostMarkEcho() used everywhere else) — a strong reproduction
            //      of the AI's own in-flight audio never counts as barge-in evidence at all, regardless of
            //      2-signal state.
            //   2. 2-signal confirmation — a single interim is no longer enough on its own. The FIRST
            //      non-echo interim for this pipeline just opens a candidate; only a SECOND, COHERENT interim
            //      (same normalized text, or a forward extension of it — i.e. genuine STT progress on the
            //      same utterance, not a different fragment) within BARGE_CONFIRM_WINDOW_MS actually fires
            //      bargeIn(). This trades a small amount of latency on some true barge-ins (typically one
            //      extra interim tick, ~100-400ms in production) for eliminating false barge-ins on short
            //      fragments that used to fail the old length heuristic — and for short/numeric speech where
            //      the 2nd interim often arrives before the old heuristic's word-count would even have passed,
            //      it can trigger EARLIER than before, not later.
            const activeRefText = (activeSpokenRef && activeSpokenRef.pipelineId === activePipelineId) ? activeSpokenRef.text : null
            if (isLikelyPostMarkEcho(interimText, activeRefText)) return // ACTIVE_PLAYBACK_ECHO — no candidate created/advanced, no diagnostic (interim path is diagnostic-free by design, same as before)

            const normalized = normalizeForEchoCompare(interimText)
            const noCandidate = !bargeCandidate || bargeCandidate.pipelineId !== activePipelineId || (Date.now() - bargeCandidate.firstAt) > BARGE_CONFIRM_WINDOW_MS
            if (noCandidate) {
              bargeCandidate = { pipelineId: activePipelineId, previousText: normalized, firstAt: Date.now() }
              return
            }
            if (bargeCandidate.previousText.startsWith(normalized) && normalized !== bargeCandidate.previousText) {
              // Regression (STT shrank back, e.g. "1697 ค่ะ" → "1697") — not new evidence, but not discarded
              // either; leave the existing candidate exactly as-is per design (a later coherent interim vs.
              // the ORIGINAL previousText can still confirm).
              return
            }
            const coherent = normalized === bargeCandidate.previousText || normalized.startsWith(bargeCandidate.previousText)
            if (!coherent) {
              // Not a continuation of the same utterance (e.g. "คิดถึง" → "ระบบ") — reset to a fresh
              // candidate seeded by THIS interim, don't confirm on unrelated fragments.
              bargeCandidate = { pipelineId: activePipelineId, previousText: normalized, firstAt: Date.now() }
              return
            }
            // Coherent second signal: exact repeat OR a forward extension of the candidate's previous text.
            bargeCandidate = null
            bargeIn()
            bargeInPendingFinal = true // ตั้งเฉพาะตอน trigger จาก interim เท่านั้น — บอก onTranscript ว่า final ตัวถัดไปคือประโยคที่พูดแทรกจริง
            bargeInCooldown = true
            setTimeout(() => { bargeInCooldown = false }, 400)
            return
          }

          if (sttProcessing) return // ไม่ trigger prewarm ระหว่างกำลัง process เทิร์นอื่นอยู่ (isSpeaking=false แต่ sttProcessing=true) — พฤติกรรมเดิม

          clearSilenceTimer()
          silencePromptCount = 0
          // C3c/L1b: prewarm เดิม (Promise<string|null>) ใช้กับ legacy เท่านั้น — สายที่ freeze เป็น chunked bucket
          // ใช้ startChunkedSpeculation() แทน (คนละ mechanism แต่ throttle/guard shape เดียวกัน)
          const session = callSessions.get(callSid)
          if (!session) return
          if (rollout?.useChunkedStreaming) { startChunkedSpeculation(session, interimText); return }
          // L2b Legacy Prewarm Bypass (design locked) — legacyEarlyTts calls never adopt legacy's
          // blocking/full-response prewarm result (see the `if (!aiText)` guard further down), and
          // production PREWARM_DIAG showed 0% HIT / 92.5% GRACE_TIMEOUT for this cohort — starting it here
          // only pays PREWARM_GRACE_MS on the final path for no measured benefit. Skipping the start keeps
          // prewarmPromise null for the whole call (its only other writer is clearPrewarm()), so the
          // existing `if (myPrewarm && isPrewarmUsable(...))` grace block on the final path short-circuits
          // to false by itself — no second guard needed there.
          if (!legacyEarlyTts) startPrewarm(session, interimText)
        }, { interimFinalizeMs, maxAlternatives: sttA2 ? 3 : null, enableShadow: sttA2Shadow, onShadowDiagnostic: emitSttShadowDiag })

        // AI ทักทายก่อนเลย — ใช้ pre-generated audio ถ้ามี (ลด latency)
        const playGreeting = async () => {
          const session = callSessions.get(callSid)
          if (!session || !callActive) return
          isSpeaking = true
          const pipelineId = ++activePipelineId
          try {
            if (session.greetingChunks) {
              // ใช้ audio ที่ pre-generate ไว้แล้ว — ส่งได้ทันที
              console.log(`[Greeting] Using pre-generated audio (${session.greetingChunks.length} chunks)`)
              const chunks = session.greetingChunks
              session.greetingChunks = null  // ใช้แล้วล้างทิ้ง
              greetingAbortController = new AbortController()
              let sent = 0
              for (const chunk of chunks) {
                if (socket.readyState !== socket.OPEN || greetingAbortController?.signal.aborted) break
                if (sent === 0) noteActiveSpokenChunk(pipelineId, session.greetingText)
                socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                sent++
              }
              greetingAbortController = null
              if (!isSpeaking) return  // barge-in happened during greeting
              if (socket.readyState === socket.OPEN) {
                pendingSpokenText = { pipelineId, text: session.greetingText }
                socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: ownedMarkName('greeting_done', pipelineId) } }))
              }
              const playbackMs = sent * 20 + 1500
              setTimeout(() => { if (activePipelineId === pipelineId && isSpeaking) { console.log('[Audio] Fallback unlock (greeting)'); isSpeaking = false; startSilenceTimer() } }, playbackMs)
              console.log(`[Greeting] Sent ${sent} pre-generated chunks`)
            } else {
              // Fallback: generate ใหม่ถ้า pre-gen ไม่สำเร็จ
              console.log(`[Greeting] Pre-gen not ready, generating now...`)
              const greeting = await askClaude(session)
              console.log(`[Greeting] "${greeting.substring(0, 100)}"`)
              session.messages.push({ role: 'assistant', content: greeting })
              pendingSpokenText = { pipelineId, text: greeting }
              await speakAndWait(greeting, session, 'greeting_done', pipelineId)
            }
          } catch (err) {
            console.error('[Greeting error]', err.message)
            isSpeaking = false
          }
        }

        // รอ 300ms แทน 1000ms — แค่ให้ stream stable
        setTimeout(playGreeting, 300)
      }

      if (msg.event === 'media' && sttStream) {
        // ส่งเสียงลูกค้าให้ STT เสมอ (รวมถึงตอน AI พูด เพื่อ barge-in detection)
        // Twilio PSTN handles echo cancellation — ไม่ต้องกังวลเสียง AI ย้อนกลับ
        try {
          const audioData = Buffer.from(msg.media.payload, 'base64')
          sttStream.write(audioData)
        } catch (e) {
          try { sttStream.end() } catch (_) {}
          sttStream = null
        }
      }

      if (msg.event === 'mark') {
        const { kind, ownerId } = parseMarkName(msg.mark?.name)
        console.log(`[WS] Mark received: kind=${kind} ownerId=${ownerId}`)

        // Design B (round 6 correction) — mark ที่ไม่ผ่านการยืนยัน owner (ไม่ตรงกับ activePipelineId ปัจจุบัน หรือ
        // ไม่มี owner encode มาเลย) ต้องไม่ทำ side effect ใดๆ เลยแม้แต่ข้อเดียว รวมถึง pendingEndCall close-scheduling
        // ด้วย (bug ที่พบใน round 5: เดิม unlock ถูกกันแล้วแต่ close ยังหลุดผ่านไปได้ทำให้ stale mark ตัดสายที่ owner
        // ใหม่ยังพูดไม่จบได้จริง) — ต้อง return ก่อนแตะ state ใดๆ ทั้งสิ้น ไม่ใช่กันทีละจุด
        if (ownerId === null || ownerId !== activePipelineId) {
          console.log(`[WS] Mark ignored — no verified current owner (kind=${kind}, ownerId=${ownerId}, active=${activePipelineId})`)
          return
        }

        isSpeaking = false
        lastMarkTime = Date.now()
        // Lightweight Post-Mark Echo Guard fix (Review Blocker 1) — promote only now that ownerId is verified
        // above to actually equal activePipelineId; pendingSpokenText was set with that same pipelineId at
        // speech-start time, so a stale/superseded mark (already rejected by the owner check above) can never
        // reach here and clobber the reference with the wrong pipeline's text.
        if (pendingSpokenText && pendingSpokenText.pipelineId === ownerId) {
          lastMarkedSpokenText = pendingSpokenText.text
        }
        if (pendingEndCall) {
          setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, 1000)
          return
        }
        const markSession = callSessions.get(callSid)
        if (markSession) await tryDeliverPendingShortAck(activePipelineId, markSession)
        // C6c follow-up (STT listening): ไม่ reset() STT stream ที่นี่อีกต่อไป — googleSTT.js rotate stream ให้
        // ฟัง utterance ถัดไปทันทีตั้งแต่ตอน utterance ก่อนหน้า deliver แล้ว (ไม่รอ mark) ถ้า reset() ซ้ำตรงนี้อีก
        // จะเสี่ยงทำลาย stream ที่กำลังฟังลูกค้าพูดอยู่จริง (เช่น ลูกค้าเริ่มพูดพอดีตอน mark มาถึง) โดยไม่จำเป็น
        startSilenceTimer()
      }

      if (msg.event === 'stop') {
        console.log(`[WS] Stream stopped: ${callSid}`)
        callActive = false
        clearSilenceTimer()
        clearDurationTimer()
        if (prewarmDiag) settlePrewarmDiag(prewarmDiag, 'CALL_ENDED') // Track P: settle before transport cleanup below
        clearPrewarm()
        abortChunkedSpeculation()
        endCall(callState)
        pendingTranscript = null // C6c follow-up: กัน pending transcript ของสายที่จบไปแล้วถูก drain ทีหลัง (แม้ processTranscript() จะเช็ค callActive ป้องกันไว้อีกชั้นอยู่แล้วก็ตาม)
        pendingShortAck = null // Design B: mirror pendingTranscript เดียวกัน — กันสายที่จบไปแล้วมี ack ค้างถูก deliver ทีหลัง
        processTranscriptDispatch = null // Design B: กัน stray reference ถูกเรียกหลัง teardown
        bargeInPendingFinal = false
        if (sttStream) { sttStream.end(); sttStream = null }
      }
    })

    socket.on('close', () => {
      console.log(`[WS] Disconnected: ${callSid}`)
      callActive = false
      if (prewarmDiag) settlePrewarmDiag(prewarmDiag, 'CALL_ENDED') // Track P: settle before transport cleanup below
      clearPrewarm()
      abortChunkedSpeculation()
      clearSilenceTimer()
      clearDurationTimer()
      endCall(callState)
      pendingTranscript = null // C6c follow-up: เช่นเดียวกับ 'stop' — กัน reference ค้างถ้าปิดสายจาก close โดยไม่มี stop event มาก่อน
      pendingShortAck = null // Design B: mirror pendingTranscript เดียวกัน
      processTranscriptDispatch = null // Design B: กัน stray reference ถูกเรียกหลัง teardown
      bargeInPendingFinal = false
      if (sttStream) { sttStream.end(); sttStream = null }
      // ถ้ายังไม่มีเหตุผลปิดสายถูก tag ไว้เลย (ไม่ใช่ AI ปิดปกติ/หมดเวลาเงียบ) แปลว่าอีกฝั่งวางสายเอง
      const endedSession = callSessions.get(callSid)
      if (endedSession && !endedSession.hangupReason) endedSession.hangupReason = 'customer_hangup'
    })

    socket.on('error', (err) => {
      console.error(`[WS] Error for ${callSid}:`, err.message)
    })
  })
}

module.exports = { registerWebSocket }
