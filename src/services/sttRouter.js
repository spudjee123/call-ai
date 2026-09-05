// Dual STT Provider (Google vs Deepgram Nova-3) — design frozen 2026-08-31. Thin selector, mirrors
// conversationAI.js's resolveExplicitProvider()/askConversationConditionalStream() pattern exactly:
// resolve once at session creation, freeze as a scalar, dispatch on the frozen value forever after.
//
// Deepgram-Primary STT Migration (design 2026-09-05) — Deepgram is now the default provider for a call
// with no explicit campaign choice; Google remains fully intact and immediately selectable as an
// emergency fallback (`stt_provider: 'google'` on any campaign) — see resolveSttProviderForSession()
// below for exactly where that default decision is made. googleSTT.js itself is NOT touched by this
// migration at all (git diff on that file stays empty) — same "Google path unmodified" guarantee the
// original Dual STT Provider design already established.
const { transcribeStream: googleTranscribeStream } = require('./googleSTT')
const { transcribeStream: deepgramTranscribeStream, DEEPGRAM_MODEL } = require('./deepgramSTT')

// Label only — mirrors STT_CONFIG.model inside googleSTT.js for logging/turnMetrics purposes. Never fed
// back into googleSTT.js, which remains the single source of truth for its own actual request config.
const GOOGLE_MODEL = 'latest_short'

// Returns null for blank/unrecognized stt_provider — same "null = no explicit campaign choice was made"
// contract resolveExplicitProvider() already established for llm_provider. This contract is UNCHANGED by
// the Deepgram-Primary migration below (design 2026-09-05) — callers (resolveSttProviderForSession(), and
// audioStream.js's [STTRoute] log) still need to tell "explicit" apart from "resolved default" after the
// fact, which a null-vs-object return makes trivial. What DID change is which provider a null resolves to
// downstream — that decision now lives in resolveSttProviderForSession() below, not here.
//
// An unrecognized NON-BLANK value (typo, or a Sheet row edited directly, bypassing campaign.js's own
// VALID_STT_PROVIDERS write-time validation) is still never an error here — campaign data can't 500 a call
// at read time — but is no longer SILENT: logged as CONFIG_ERROR so a typo doesn't quietly and invisibly
// land on whichever provider happens to be the current default, which would be far more surprising now
// that the default is Deepgram (a call quietly landing on Deepgram because someone mistyped "google" would
// be a much worse silent failure than the reverse used to be).
function resolveSttProvider(campaign) {
  const raw = (campaign?.stt_provider || '').trim().toLowerCase()
  if (raw === 'google') return { provider: 'google', model: GOOGLE_MODEL }
  if (raw === 'deepgram') return { provider: 'deepgram', model: DEEPGRAM_MODEL }
  if (raw) {
    console.error(`[STTRoute] CONFIG_ERROR — unrecognized stt_provider="${raw}" (campaignId=${campaign?.id ?? 'unknown'}) — treating as unset, falling back to current default`)
  }
  return null
}

// Deepgram-Primary STT Migration (design 2026-09-05) — this is the ONE place the "what does a call get when
// the campaign didn't ask for anything specific" policy decision lives. Previously that decision was made
// implicitly by createTranscribeStream()'s own dispatch fallback (`=== 'deepgram' ? deepgram : google`),
// which defaulted every unrecognized/absent session.sttProvider straight to Google. That fallback is
// DELIBERATELY left untouched below — this migration instead makes BOTH real session-creation call sites
// (twilio.js's makeOutboundCall, webhook.js's inbound handler) call this function and freeze an explicit,
// always-populated { provider, model, source } onto the session, so createTranscribeStream() never actually
// exercises its own bare fallback for a real call anymore (session.sttProvider is never null/undefined by
// the time it gets there) — while every OTHER caller of createTranscribeStream() that constructs a session
// without going through this function (the entire existing test suite's WS-harness sessions) keeps hitting
// that same old fallback completely unchanged, exactly as before. This is what makes the migration a
// two-call-site change instead of a routing-dispatch change, and is why the full existing suite needs no
// updates for this migration.
//
// source is 'CAMPAIGN_EXPLICIT' | 'DEFAULT_PRIMARY' — consumed by audioStream.js's [STTRoute] log (Phase 4)
// so an operator can always tell whether a given call's provider was an explicit choice or the current
// default, without needing to separately cross-reference the campaign config.
const DEFAULT_STT_PROVIDER = 'deepgram'
const DEFAULT_STT_MODEL = DEEPGRAM_MODEL

function resolveSttProviderForSession(campaign) {
  const explicit = resolveSttProvider(campaign)
  if (explicit) return { provider: explicit.provider, model: explicit.model, source: 'CAMPAIGN_EXPLICIT' }
  return { provider: DEFAULT_STT_PROVIDER, model: DEFAULT_STT_MODEL, source: 'DEFAULT_PRIMARY' }
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

module.exports = { resolveSttProvider, resolveSttProviderForSession, createTranscribeStream, GOOGLE_MODEL, DEEPGRAM_MODEL }
