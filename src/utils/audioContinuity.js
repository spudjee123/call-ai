// Phase A — Audio Continuity Telemetry Only (Implementation Gate #1, design locked). Diagnostic-only: never
// gates behavior, never adds an await/delay to the hot path — every mark below is a synchronous
// performance.now() call or property write, same cost class as turnMetrics.js's markOnce().
//
// Scope: the legacy/CONTROL conversational turn flow in audioStream.js's processTranscript() only. Greeting,
// max-duration goodbye, and silence-prompt speech are separate playback paths outside the customer-interrupt
// race this exists to measure (customer never has an open bargeCandidate before them), and the chunked/L2b
// helper paths carry ~0% production traffic per their own rollout gates — neither is instrumented here.
//
// One [AudioContinuity] summary log is emitted per generation (same point [Metrics] already logs at) —
// never per-frame, to avoid Render log volume/CPU overhead on every 20ms audio chunk.
const { performance } = require('perf_hooks')
const { duration } = require('./turnMetrics')

const CANDIDATE_TEXT_MAX_CHARS = 80

function truncateForLog(text) {
  if (!text) return null
  return text.length > CANDIDATE_TEXT_MAX_CHARS ? text.slice(0, CANDIDATE_TEXT_MAX_CHARS) : text
}

function createAudioContinuity({ callSid, generationId, pipelineId }) {
  return {
    callSid,
    generationId,
    pipelineId,

    candidateFirstAt: null,
    candidateFirstText: null,
    candidateConfirmAt: null,
    candidateConfirmText: null,
    bargeTrigger: null, // 'INTERIM_CONFIRM' | 'FINAL_TIER1' | 'FINAL' | null (null = turn never barged)

    providerFirstAudioAt: null,
    twilioFirstMediaAt: null,
    twilioLastMediaAt: null,

    framesSent: 0,
    bytesSent: 0,

    clearSentAt: null,

    recentBargeCount5s: null,
    recentBargeCount10s: null,

    // internal-only accumulators — stripped by finalizeAudioContinuity() before logging, never appear in [AudioContinuity]
    _lastFrameAt: null,
    _frameGapMax: null,
    _frameGapSamples: [],
  }
}

// เรียกทุกครั้งที่ audio chunk จริงถูก socket.send ออกไป Twilio (event:'media') ภายใน processTranscript() เท่านั้น
// ห้ามเรียกจาก greeting/max-duration/silence-prompt playback (นอก scope ของ Phase A ตามที่ระบุไว้บนไฟล์)
function recordFrameSent(ac, chunkBuffer) {
  // Single performance.now() read reused for every field this call touches — turnMetrics.js's markOnce()
  // takes its own independent reading internally, which would make providerFirstAudioAt/twilioFirstMediaAt
  // drift a few microseconds from twilioLastMediaAt/the gap baseline on frame 1 (and could even make
  // twilioLastMediaAt read as EARLIER than providerFirstAudioAt on that same call) — inlined here instead so
  // every field this call sets shares exactly one clock domain reading.
  const now = performance.now()
  if (ac.providerFirstAudioAt == null) ac.providerFirstAudioAt = now
  if (ac.twilioFirstMediaAt == null) ac.twilioFirstMediaAt = now
  ac.twilioLastMediaAt = now
  ac.framesSent++
  ac.bytesSent += chunkBuffer ? chunkBuffer.length : 0
  if (ac._lastFrameAt != null) {
    const gap = now - ac._lastFrameAt
    ac._frameGapSamples.push(gap)
    if (ac._frameGapMax == null || gap > ac._frameGapMax) ac._frameGapMax = gap
  }
  ac._lastFrameAt = now
}

function percentile(samples, p) {
  if (!samples.length) return null
  const sorted = [...samples].sort((a, b) => a - b)
  const idx = Math.min(sorted.length - 1, Math.floor(p * sorted.length))
  return Math.round(sorted[idx] * 100) / 100
}

// เรียกครั้งเดียวตอนจบ turn (จุดเดียวกับที่ log [Metrics] อยู่แล้ว) — คำนวณ derived fields แล้วตัด internal
// accumulator (_lastFrameAt/_frameGapMax/_frameGapSamples) ทิ้งก่อนคืนค่าที่จะ JSON.stringify ไป log จริง
function finalizeAudioContinuity(ac) {
  const candidateBeforeFirstAudio = ac.candidateFirstAt != null && ac.providerFirstAudioAt != null
    ? ac.candidateFirstAt <= ac.providerFirstAudioAt
    : null

  let outcome
  if (ac.clearSentAt != null) {
    if (ac.providerFirstAudioAt == null) {
      // Barged before any TTS audio was ever sent (e.g. customer speaks while Claude is still generating) —
      // must never fall into POST_AUDIO_BARGE, which specifically means audio HAD started playing. Given the
      // multi-second Claude/TTS TTFT this RCA measured, this precommit case is likely the MOST common barge
      // outcome, not an edge case — misclassifying it here would corrupt PRE_AUDIO_OVERLAP_LEAK_RATE.
      outcome = 'PRECOMMIT_BARGE'
    } else {
      // candidateBeforeFirstAudio is a real true/false here (never null) since providerFirstAudioAt is non-null.
      outcome = candidateBeforeFirstAudio ? 'PRE_AUDIO_OVERLAP' : 'POST_AUDIO_BARGE'
    }
  } else if (ac.providerFirstAudioAt != null) {
    outcome = 'COMPLETED_NO_BARGE'
  } else {
    outcome = 'NO_AUDIO'
  }

  return {
    callSid: ac.callSid,
    generationId: ac.generationId,
    pipelineId: ac.pipelineId,

    candidateFirstAt: ac.candidateFirstAt,
    candidateFirstText: ac.candidateFirstText,
    candidateConfirmAt: ac.candidateConfirmAt,
    candidateConfirmText: ac.candidateConfirmText,
    bargeTrigger: ac.bargeTrigger,

    providerFirstAudioAt: ac.providerFirstAudioAt,
    twilioFirstMediaAt: ac.twilioFirstMediaAt,
    twilioLastMediaAt: ac.twilioLastMediaAt,

    framesSent: ac.framesSent,
    bytesSent: ac.bytesSent,

    clearSentAt: ac.clearSentAt,

    candidateBeforeFirstAudio,
    candidateToFirstAudioMs: duration(ac.candidateFirstAt, ac.providerFirstAudioAt),
    firstAudioToClearMs: duration(ac.providerFirstAudioAt, ac.clearSentAt),

    outboundP95FrameGapMs: percentile(ac._frameGapSamples, 0.95),
    outboundMaxFrameGapMs: ac._frameGapMax,

    recentBargeCount5s: ac.recentBargeCount5s,
    recentBargeCount10s: ac.recentBargeCount10s,

    outcome,
  }
}

module.exports = { createAudioContinuity, recordFrameSent, finalizeAudioContinuity, truncateForLog }
