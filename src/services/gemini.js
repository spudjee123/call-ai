const { GoogleGenAI } = require('@google/genai')
const { performance } = require('perf_hooks')
const { findChunkBoundary, getNumericProtectionRemainingMs, evaluateNumericProtectionDiagnostic, CHUNK_REASON, SOFT_TIMEOUT_MS: CHUNKER_SOFT_TIMEOUT_MS } = require('../utils/speechChunker')
const { buildSystemPrompt, MAX_HISTORY } = require('./claude')

// Dual Conversation Provider A/B (design locked) — Gemini side of the experiment. Deliberately a FULL
// duplicate of claude.js's askClaudeConditionalStream() driver/chunking logic, not a shared/refactored
// engine — this is an explicit design decision (not an oversight): the first round of this experiment must
// be able to say "Claude path never changed" with certainty, so any latency/behavior difference we measure
// can only be attributed to the provider itself, never to a side effect of extracting a generic engine.
// Only the provider-specific seams differ from askClaudeConditionalStream(): the API client/streaming call,
// the delta-extraction condition, the cache/usage milestone (Gemini's caching semantics aren't Anthropic's
// token-accounting concept — see the cacheUsage note below), and the model/thinking config. Everything else
// — the driver/queue shape, CONDITIONAL_GRACE_MS race, findChunkBoundary()/numeric-protection/HARD_MAX
// re-check, and the [END_CALL] marker contract — reuses the SAME shared speechChunker.js utilities Claude
// uses and is line-for-line the same orchestration, on purpose, so neither can drift from the other.
//
// onMilestone key contract is IDENTICAL to askClaudeConditionalStream()'s (requestAt, firstDeltaAt,
// firstSafeAt, fullAt, mode, inputStats, chunkReasonStats, finalText, responseCharCount, endCallRequested)
// so audioStream.js's existing onEarlyTtsMilestone/wrappedMilestone handler needs no provider-specific
// branching at all — it already treats these keys as provider-agnostic. cacheUsage is the one milestone
// this function never emits (see below), leaving l2bCacheCreationTokens/l2bCacheReadTokens correctly null
// for Gemini turns rather than fabricating a Claude-shaped number that doesn't mean the same thing.
//
// IMPORTANT documented SDK limitation (verified against @google/genai's own type definitions, not assumed):
// GenerateContentConfig.abortSignal is explicitly client-only — "Using it to cancel an operation will not
// cancel the request in the service. You will still be charged usage for any applicable operations." So a
// barge-in correctly stops us from reading/speaking any further Gemini output (same UX as Claude), but
// Google's server keeps generating and bills for the full completion regardless of our abort. This is an
// inherent cost difference from Claude's abort behavior that no amount of correct wiring here can avoid —
// flagged for the A/B cost analysis, not something this file can fix.
const CONDITIONAL_GRACE_MS = 150
const GEMINI_MODEL = 'gemini-3.7-flash'

let cachedClient = null
function getClient() {
  if (!cachedClient) cachedClient = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY })
  return cachedClient
}

async function* askGeminiConditionalStream(session, signal = null, onMilestone = null) {
  const { name, campaign, messages } = session
  const systemPrompt = buildSystemPrompt(campaign.script || campaign.system_prompt, name)
  const history = messages.slice(-MAX_HISTORY)

  if (!history.length) { yield 'สวัสดีค่ะ'; return }

  // Gemini's canonical history uses role 'model' where Claude/our internal session.messages use
  // 'assistant' — mapped ONLY here, at the API boundary. session.messages itself is never touched (same
  // invariant askClaudeStreamChunked's tool-based path already keeps for its own transport differences).
  const contents = history.map(m => ({
    role: m.role === 'assistant' ? 'model' : 'user',
    parts: [{ text: m.content }],
  }))

  // Track L equivalent (diagnostic only) — same fields/shape as askClaudeConditionalStream's inputStats so
  // the two providers populate the exact same turnMetrics.l2b* fields for direct comparison.
  try {
    const charLen = (c) => typeof c === 'string' ? c.length : null

    const priorMessages = history.slice(0, -1)
    const priorLens = priorMessages.map(m => charLen(m.content))
    const priorHistoryCharCount = priorLens.some(l => l === null) ? null : priorLens.reduce((a, b) => a + b, 0)

    const currentUserCharCount = charLen(history[history.length - 1].content)

    const approxInputTextCharCount =
      (priorHistoryCharCount === null || currentUserCharCount === null)
        ? null
        : systemPrompt.length + priorHistoryCharCount + currentUserCharCount

    const campaignPromptCharCount = charLen(campaign.script || campaign.system_prompt)

    try {
      onMilestone?.('inputStats', {
        systemPromptCharCount: systemPrompt.length,
        priorHistoryCharCount,
        requestMessageCount: history.length,
        currentUserCharCount,
        approxInputTextCharCount,
        campaignPromptCharCount,
      })
    } catch (_) { /* diagnostic only — must never affect the real request below */ }
  } catch (e) {
    try { onMilestone?.('inputStats', null) } catch (_) { /* diagnostic only */ }
  }

  const requestAt = performance.now()
  onMilestone?.('requestAt', requestAt)

  const stream = await getClient().models.generateContentStream({
    model: GEMINI_MODEL,
    contents,
    config: {
      systemInstruction: { parts: [{ text: systemPrompt }] },
      maxOutputTokens: 200,
      thinkingConfig: { thinkingLevel: 'LOW' },
      abortSignal: signal || undefined,
    },
  })

  // Internal producer/consumer split — identical shape to askClaudeConditionalStream()'s (a plain generator
  // can only yield from its own body, never from a detached setTimeout callback, so the grace-vs-stream-
  // completion race has to live in a separate driver that pushes decided items into a queue this
  // generator's body drains).
  const items = []
  let waiter = null
  function push(item) { items.push(item); if (waiter) { const w = waiter; waiter = null; w() } }
  function waitForItem() {
    if (items.length > 0) return Promise.resolve()
    return new Promise(resolve => { waiter = resolve })
  }

  let rawText = ''
  let firstDeltaAt = null
  let firstSafeAt = null
  let mode = null // null (deciding) | 'SINGLE_SHOT' | 'CHUNKED'

  const driver = (async () => {
    let buffer = ''
    let segmentStartMs = null
    let numericProtectionTimer = null
    let graceTimer = null
    let pendingFirstChunk = null

    function clearNumericProtectionTimer() { if (numericProtectionTimer) { clearTimeout(numericProtectionTimer); numericProtectionTimer = null } }
    function clearGraceTimer() { if (graceTimer) { clearTimeout(graceTimer); graceTimer = null } }

    // never search past the first unmatched '[' — see [END_CALL] bracket-safety note in claude.js
    function safeView() {
      const idx = buffer.indexOf('[')
      return idx === -1 ? buffer : buffer.slice(0, idx)
    }
    function tryFindBoundary(elapsedMs) {
      const view = safeView()
      const result = findChunkBoundary(view, elapsedMs)
      if (!result) return null
      return { chunk: result.chunk, remainder: result.remainder + buffer.slice(view.length), reason: result.reason }
    }
    function numericProtectionRemainingMs(elapsedMs) {
      return getNumericProtectionRemainingMs(safeView(), elapsedMs)
    }

    let deltaCount = 0
    let lastDeltaAt = null
    let firstCandidateAt = null
    let numericProtectionEverBlocked = false

    let hardMaxRecheckTimer = null
    let hardMaxRecheckPromise = null
    let firstNumericCandidateEligibleAt = null

    function drainChunked() {
      clearNumericProtectionTimer()
      while (true) {
        const elapsedMs = performance.now() - segmentStartMs
        const result = tryFindBoundary(elapsedMs)
        if (result) {
          push({ type: 'chunk', text: result.chunk })
          buffer = result.remainder
          segmentStartMs = performance.now()
          continue
        }
        const remainingMs = numericProtectionRemainingMs(elapsedMs)
        if (remainingMs != null) {
          numericProtectionTimer = setTimeout(() => {
            numericProtectionTimer = null
            if (signal?.aborted) return
            drainChunked()
          }, remainingMs)
        }
        return
      }
    }

    function armGrace() {
      return new Promise(resolve => {
        graceTimer = setTimeout(() => resolve('GRACE'), CONDITIONAL_GRACE_MS)
      })
    }

    function clearHardMaxRecheckTimer() {
      if (hardMaxRecheckTimer) { clearTimeout(hardMaxRecheckTimer); hardMaxRecheckTimer = null }
      hardMaxRecheckPromise = null
    }

    function armHardMaxRecheckIfNeeded(elapsedMs) {
      const remainingMs = numericProtectionRemainingMs(elapsedMs)
      if (remainingMs == null) {
        firstNumericCandidateEligibleAt = null
        return
      }
      if (firstNumericCandidateEligibleAt === null) {
        firstNumericCandidateEligibleAt = elapsedMs >= CHUNKER_SOFT_TIMEOUT_MS
          ? (segmentStartMs + elapsedMs)
          : (segmentStartMs + CHUNKER_SOFT_TIMEOUT_MS)
      }
      hardMaxRecheckPromise = new Promise(resolve => {
        hardMaxRecheckTimer = setTimeout(() => { hardMaxRecheckTimer = null; resolve() }, remainingMs)
      })
    }

    function attemptFirstSafeCut(elapsedMs, trigger, deltaGapMs) {
      const result = tryFindBoundary(elapsedMs)
      if (!result) return false
      firstSafeAt = performance.now()
      onMilestone?.('firstSafeAt', firstSafeAt)
      try {
        const isStrongOrSoft = result.reason === CHUNK_REASON.STRONG_BOUNDARY || result.reason === CHUNK_REASON.SOFT_BOUNDARY
        const candidateTimestamps = [firstCandidateAt, firstNumericCandidateEligibleAt].filter(v => v !== null)
        const candidateAt = candidateTimestamps.length ? Math.min(...candidateTimestamps) : null
        const firstCandidateElapsedMs = isStrongOrSoft
          ? (firstSafeAt - firstDeltaAt)
          : (candidateAt !== null ? (candidateAt - firstDeltaAt) : (firstSafeAt - firstDeltaAt))
        const preSafeDeltaGapMs = trigger === 'DELTA' ? deltaGapMs : (firstSafeAt - lastDeltaAt)
        const numericProtectionBlocked = isStrongOrSoft
          ? false
          : trigger === 'HARD_MAX_TIMER'
            ? true
            : numericProtectionEverBlocked
        onMilestone?.('chunkReasonStats', {
          reason: result.reason,
          charCount: result.chunk.length,
          deltaCount,
          firstCandidateElapsedMs,
          numericProtectionBlocked,
          preSafeDeltaGapMs,
          firstSafeTrigger: trigger,
        })
      } catch (_) { /* diagnostic only */ }
      pendingFirstChunk = result.chunk
      buffer = result.remainder
      gracePromise = armGrace()
      return true
    }

    let doneSent = false
    function sendDone() { if (!doneSent) { doneSent = true; push({ type: 'done' }) } }

    const iterator = stream[Symbol.asyncIterator]()
    let pendingNext = iterator.next()
    let gracePromise = null

    try {
      while (true) {
        const racers = [pendingNext.then(r => ({ kind: 'stream', r }))]
        if (gracePromise) racers.push(gracePromise.then(() => ({ kind: 'grace' })))
        if (hardMaxRecheckPromise) racers.push(hardMaxRecheckPromise.then(() => ({ kind: 'hardMaxRecheck' })))
        const winner = await Promise.race(racers)

        if (signal?.aborted) { clearHardMaxRecheckTimer(); sendDone(); return }

        if (winner.kind === 'grace') {
          gracePromise = null
          clearGraceTimer()
          mode = 'CHUNKED'
          onMilestone?.('mode', mode)
          push({ type: 'chunk', text: pendingFirstChunk })
          segmentStartMs = performance.now()
          drainChunked()
          continue
        }

        if (winner.kind === 'hardMaxRecheck') {
          hardMaxRecheckPromise = null
          if (firstSafeAt === null) {
            const elapsedMs = performance.now() - segmentStartMs
            const cut = attemptFirstSafeCut(elapsedMs, 'HARD_MAX_TIMER')
            if (!cut) armHardMaxRecheckIfNeeded(elapsedMs)
          }
          continue
        }

        const { value: chunk, done } = winner.r
        if (done) break
        pendingNext = iterator.next()

        if (signal?.aborted) { clearHardMaxRecheckTimer(); sendDone(); return }

        // No cacheUsage milestone here — Gemini's caching semantics (implicit/explicit context caching) are
        // not the same concept as Anthropic's cache_creation_input_tokens/cache_read_input_tokens, and this
        // experiment's design explicitly rejects forcing them into the same field names (see file header).
        // turnMetrics.l2bCacheCreationTokens/l2bCacheReadTokens correctly stay null for Gemini turns.

        const deltaText = chunk?.text
        if (!deltaText) continue

        const deltaObservedAt = performance.now()

        if (firstDeltaAt === null) { firstDeltaAt = performance.now(); onMilestone?.('firstDeltaAt', firstDeltaAt) }
        rawText += deltaText

        const wasEmpty = buffer.length === 0
        buffer += deltaText
        if (wasEmpty) segmentStartMs = performance.now()

        if (mode === 'CHUNKED') {
          drainChunked()
        } else if (mode === null) {
          if (firstSafeAt === null) {
            clearHardMaxRecheckTimer()

            deltaCount++
            const deltaArrivedAt = performance.now()
            const gapFromPreviousDeltaMs = lastDeltaAt === null ? 0 : (deltaArrivedAt - lastDeltaAt)
            lastDeltaAt = deltaArrivedAt

            if (firstNumericCandidateEligibleAt !== null && deltaObservedAt >= firstNumericCandidateEligibleAt) {
              numericProtectionEverBlocked = true
            }

            const elapsedMs = performance.now() - segmentStartMs

            try {
              const diag = evaluateNumericProtectionDiagnostic(safeView(), elapsedMs)
              if (diag.candidateEligible && firstCandidateAt === null) firstCandidateAt = performance.now()
              if (diag.blockedByNumericProtection) numericProtectionEverBlocked = true
            } catch (_) { /* diagnostic only */ }

            const cut = attemptFirstSafeCut(elapsedMs, 'DELTA', gapFromPreviousDeltaMs)
            if (!cut) armHardMaxRecheckIfNeeded(elapsedMs)
          }
        }
      }

      clearNumericProtectionTimer()
      clearGraceTimer()
      clearHardMaxRecheckTimer()
      if (signal?.aborted) { sendDone(); return }

      const fullAt = performance.now()
      onMilestone?.('fullAt', fullAt)

      // END_CALL contract — same marker convention as Claude (design lock: no Gemini function-calling for
      // END_CALL in this round, see file header).
      const finalText = rawText.replace(/\[END_CALL\]/g, '').trim()
      const endCallRequested = rawText.includes('[END_CALL]')
      onMilestone?.('finalText', finalText)
      try { onMilestone?.('responseCharCount', finalText.length) } catch (_) { /* diagnostic only */ }
      onMilestone?.('endCallRequested', endCallRequested)

      if (mode === 'CHUNKED') {
        const lastSpeechChunk = buffer.replace(/\[END_CALL\]/g, '').trim()
        if (lastSpeechChunk) push({ type: 'chunk', text: lastSpeechChunk })
      } else {
        mode = 'SINGLE_SHOT'
        onMilestone?.('mode', mode)
        if (finalText.length >= 3) push({ type: 'chunk', text: finalText })
      }
      sendDone()
    } catch (err) {
      clearNumericProtectionTimer()
      clearGraceTimer()
      clearHardMaxRecheckTimer()
      if (!signal?.aborted) push({ type: 'error', err })
      else sendDone()
    }
  })()

  try {
    while (true) {
      await waitForItem()
      const item = items.shift()
      if (item.type === 'chunk') yield item.text
      else if (item.type === 'done') break
      else if (item.type === 'error') throw item.err
    }
  } finally {
    await driver.catch(() => {})
  }
}

module.exports = { askGeminiConditionalStream, GEMINI_MODEL }
