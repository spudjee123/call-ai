// Dual Conversation Provider A/B (design locked) — thin selector between Claude and Gemini for the
// conditional/L2b conversation path only. Does not touch greeting (always Claude Haiku) or post-call
// summarization (always Claude Sonnet 5) — those stay fixed per the locked design's scope.
const { askClaudeConditionalStream } = require('./claude')
const { askGeminiConditionalStream, GEMINI_MODEL } = require('./gemini')

const CLAUDE_MODEL = 'claude-sonnet-5'

// Resolves a campaign's explicit provider choice into a frozen { provider, model } pair for a session.
// Returns null when the campaign has no explicit choice (blank/missing llm_provider) — callers must treat
// null as "leave existing routing (legacyEarlyTts rollout, etc.) completely untouched," never as "default
// to Claude via this function." An unrecognized value is treated the same as blank (safe default), not as
// an error — campaign data can't 500 a call.
function resolveExplicitProvider(campaign) {
  const raw = (campaign?.llm_provider || '').trim().toLowerCase()
  if (raw === 'claude') return { provider: 'claude', model: CLAUDE_MODEL }
  if (raw === 'gemini') return { provider: 'gemini', model: GEMINI_MODEL }
  return null
}

// Single call site audioStream.js's L2b branch uses instead of calling askClaudeConditionalStream()
// directly — dispatches on session.llmProvider, which is resolved ONCE at session creation (twilio.js /
// webhook.js) and never re-read from session.campaign mid-call, so the provider is frozen for the whole
// call even if the campaign is edited while the call is in progress (session.campaign is a live object
// reference, not a snapshot — freezing had to be a resolved scalar, not an assumption about the object).
async function* askConversationConditionalStream(session, signal = null, onMilestone = null) {
  if (session.llmProvider === 'gemini') {
    yield* askGeminiConditionalStream(session, signal, onMilestone)
    return
  }
  yield* askClaudeConditionalStream(session, signal, onMilestone)
}

module.exports = { resolveExplicitProvider, askConversationConditionalStream, CLAUDE_MODEL, GEMINI_MODEL }
