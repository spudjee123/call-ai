// Dual STT Provider (Google vs Deepgram Nova-3) — design frozen 2026-08-31. Thin selector, mirrors
// conversationAI.js's resolveExplicitProvider()/askConversationConditionalStream() pattern exactly:
// resolve once at session creation, freeze as a scalar, dispatch on the frozen value forever after.
const { transcribeStream: googleTranscribeStream } = require('./googleSTT')
const { transcribeStream: deepgramTranscribeStream, DEEPGRAM_MODEL } = require('./deepgramSTT')

// Label only — mirrors STT_CONFIG.model inside googleSTT.js for logging/turnMetrics purposes. Never fed
// back into googleSTT.js, which remains the single source of truth for its own actual request config.
const GOOGLE_MODEL = 'latest_short'

// Returns null for blank/unrecognized stt_provider — same "null = leave existing routing completely
// untouched" contract resolveExplicitProvider() already established for llm_provider. An unrecognized
// value is treated as blank here (never an error) — campaign data can't 500 a call at read time; the
// REJECT-invalid-values behavior lives at write time in campaign.js's validation instead (mirrors how
// llm_provider validation was split the same way).
function resolveSttProvider(campaign) {
  const raw = (campaign?.stt_provider || '').trim().toLowerCase()
  if (raw === 'google') return { provider: 'google', model: GOOGLE_MODEL }
  if (raw === 'deepgram') return { provider: 'deepgram', model: DEEPGRAM_MODEL }
  return null
}

// createTranscribeStream() — the ONLY call site audioStream.js should use instead of calling
// googleSTT.transcribeStream() directly. Dispatches on session.sttProvider, frozen once at session
// creation (twilio.js/webhook.js) exactly like session.llmProvider — never re-read from session.campaign
// mid-call.
//
// Google path: calls googleSTT.transcribeStream() completely unchanged, every argument passed through
// verbatim — googleSTT.js itself is not modified by this feature at all (git diff on that file must stay
// empty). The only thing this function does to Google's output is annotate the EXISTING sttMeta object
// with `provider`/`model` before forwarding it to the caller — an additive property merge, not a
// restructure.
//
// ACCEPTED DESIGN DEVIATION (explicit Review acceptance, not silently assumed compliant): the locked
// "Common Contract + providerMeta" design is enforced strictly for the NEW Deepgram path (see
// deepgramSTT.js), where every field is fresh and has no existing consumers to break. Google's
// already-shipped sttMeta shape stays backward-compatible/additive-only in this rollout — its existing
// fields are NOT migrated into providerMeta, on the reasoning that doing so would touch exactly the code
// (audioStream.js's [STT_DIAG] emitter, turnMetrics.js) this whole feature was designed to leave
// untouched. This does not abandon the Common Contract concept — it separates the PROVIDER ROUTING
// contract (this file) from LEGACY TELEMETRY MIGRATION (Google's existing field shape), which is
// explicitly out of scope for this rollout. A future normalized-schema migration for Google's shape, if
// ever wanted, should be its own separate piece of work with its own migration tests — never silently
// folded into a provider-selection feature like this one.
function createTranscribeStream(session, onTranscript, onInterim, options = {}) {
  const provider = session?.sttProvider === 'deepgram' ? 'deepgram' : 'google'
  const model = session?.sttModel || (provider === 'deepgram' ? DEEPGRAM_MODEL : GOOGLE_MODEL)

  // Regression caught by test/sttDiagAudioStream.test.js's backward-compat test: `{ ...undefined, x }`
  // evaluates to `{ x }` in JS, NOT undefined — spreading unconditionally would turn a genuinely-missing
  // sttMeta into a truthy object, defeating emitSttDiag()'s `if (!sttMeta) return` guard and emitting
  // [STT_DIAG] when the caller explicitly didn't pass metadata. Only annotate when sttMeta is truthy;
  // pass a falsy sttMeta through completely untouched.
  const wrappedOnTranscript = (transcript, sttMeta) =>
    onTranscript(transcript, sttMeta ? { ...sttMeta, provider, model } : sttMeta)

  if (provider === 'deepgram') {
    return deepgramTranscribeStream(wrappedOnTranscript, onInterim, options)
  }
  return googleTranscribeStream(wrappedOnTranscript, onInterim, options)
}

module.exports = { resolveSttProvider, createTranscribeStream, GOOGLE_MODEL, DEEPGRAM_MODEL }
