// Dual STT Provider (Google vs Deepgram Nova-3) — design frozen 2026-08-31 (Implementation Gate: "Dual
// STT Provider Google vs Deepgram + Campaign STT Schema"). API shape below verified against the actually
// installed @deepgram/sdk@5.9.0 type definitions (node_modules/@deepgram/sdk/dist/cjs/...) — never assumed
// from memory or from the docs URLs quoted in planning, per the explicit instruction not to blind-copy
// option names. Key facts confirmed this way, not guessed:
//   - Constructor option is `apiKey` (HeaderAuthProvider.d.ts: `const PARAM_KEY: "apiKey"`)
//   - Streaming entry point is `client.listen.v1.connect(args): Promise<V1Socket>`
//   - `ListenV1Encoding` includes `Mulaw: "mulaw"` — Deepgram accepts raw μ-law directly. Google Cloud
//     STT ALSO supports a MULAW encoding value (confirmed against Google's own API — not a Deepgram-only
//     capability); the Call AI Google path (googleSTT.js) simply CHOSE to convert μ-law → PCM16 before
//     sending (see mulawBufferToPcm16) — that is our own config choice, not something Google's API forces.
//     This Deepgram adapter sends Twilio's raw μ-law bytes straight through with ZERO conversion.
//   - `ListenV1Language` is typed `unknown` in the SDK (no compile-time enum) — 'th' is Deepgram's
//     documented BCP-47 tag for Thai at design time, but Nova-3 Thai accuracy has NOT been verified
//     against real production audio yet (no DEEPGRAM_API_KEY available in this environment) — flagged
//     explicitly for the production smoke test before this path is trusted for real campaigns.
//   - V1Socket events are 'open'/'message'/'close'/'error' via `.on(event, cb)`; audio goes out via
//     `.sendMedia(ArrayBuffer|Blob|ArrayBufferView)` — a Node Buffer satisfies ArrayBufferView structurally.
//   - CRITICAL, two-stage finding from reading the ACTUAL ws.js AND Socket.js source (not just .d.ts
//     types), across two rounds of Review challenge:
//     (1) The SDK's ReconnectingWebSocket (exposed as the public `V1Socket.socket` field) calls its OWN
//         internal `_connect()` retry SYNCHRONOUSLY inside `_handleClose()`, BEFORE it fires the public
//         'close' event this adapter listens to (ws.js:153-165, and independently confirmed at the
//         V1Socket layer too — Socket.js's own `close()` re-invokes its close handler synchronously/
//         re-entrantly, which is why this adapter's `end()` sets `destroyed = true` BEFORE calling
//         `socket.close()`, not after). Blindly replacing the connection on every 'close' would race a
//         duplicate live connection against the SDK's own in-flight retry.
//     (2) A single-snapshot `retryCount >= ceiling` check is NOT enough to fix that, because
//         ReconnectingWebSocket's `_retryCount` starts at -1 and is incremented on every `_connect()` call
//         including the very first one — so `retryCount === ceiling` is observed on TWO different 'close'
//         events (once right after the last allowed attempt was just launched, once when the next attempt
//         was blocked) with no way to tell them apart from one reading alone. See the full trace and the
//         actual fix (tracking whether retryCount ADVANCED since the previous close on this connection,
//         not comparing it to the ceiling) at the `close` handler below, in createConnection().
//
// Architecture difference from googleSTT.js (deliberate, not an oversight): Google's singleUtterance:true
// design means a stream is created PER UTTERANCE and rotated constantly (see googleSTT.js's
// rotateForNextUtterance/activatePrewarm machinery). Deepgram's streaming API has no equivalent concept —
// ONE WebSocket connection is meant to stay open for the WHOLE CALL, with is_final/speech_final flags on
// each message marking utterance boundaries server-side. This adapter deliberately does NOT replicate
// Google's per-utterance rotation, prewarm, cold-mute, or EOS-recovery state machine — per the locked
// design ("ห้าม copy Google EOS/prewarm state machine... provider lifecycle คนละแบบกัน").
//
// Connection ownership invariant (locked design, Lock B): only the CURRENT connectionId may ever mutate
// adapter/session state (emit interim/final, trigger reconnect). Every event handler below checks
// isCurrentConnection() FIRST, before touching anything else — a stale/superseded connection's late
// events are silently dropped, never forwarded, never allowed to change state, matching the identity-
// scoped pattern googleSTT.js already uses for its own shadow/EOS-recovery (same principle, new lifecycle).

const { DeepgramClient } = require('@deepgram/sdk')

const DEEPGRAM_MODEL = 'nova-3'
const DEEPGRAM_LANGUAGE = 'th'
// Explicit, deliberate override of the SDK's own default (30) — with the SDK's exponential backoff
// (1-5s → 10s cap, ×1.3 growth per attempt), 30 attempts could keep a live call's STT silently retrying
// for minutes. Verified against the actual ws.js source (constructor sets `_retryCount = -1`, so this
// value means "1 initial attempt + 5 retries = 6 total connection attempts", not 5 total) — bounds
// worst-case internal-retry time to roughly 15-25s before this adapter gives up and replaces the
// connection itself. A value chosen for a live phone call, not validated against real production Deepgram
// outages yet (flagged for the production smoke test).
const DEEPGRAM_RECONNECT_ATTEMPTS = 5

let cachedClient = null
function getClient() {
  if (!cachedClient) {
    const apiKey = process.env.DEEPGRAM_API_KEY
    // Fail loud, no silent Google fallback (locked design) — a campaign explicitly configured for
    // Deepgram must never quietly end up talking to Google instead just because the key is missing.
    // code:'DEEPGRAM_MISSING_API_KEY' lets createConnection()'s catch block below tell this apart from a
    // genuinely transient connect failure — a missing key is a CONFIG error, not a network blip; retrying
    // it every 200ms for the rest of the call would just be a silent, infinite retry-storm in the logs
    // (caught by this file's own test suite, not assumed) instead of one clear failure.
    if (!apiKey) {
      const err = new Error('Deepgram STT selected but DEEPGRAM_API_KEY is missing')
      err.code = 'DEEPGRAM_MISSING_API_KEY'
      throw err
    }
    cachedClient = new DeepgramClient({ apiKey })
  }
  return cachedClient
}

// Exposed for tests only — lets a test swap in a fake client without touching the module's real
// require('@deepgram/sdk') wiring, and resets the module-level connection-id counter between tests.
function _resetClientForTest(fakeClient) {
  cachedClient = fakeClient || null
}

let connectionIdCounter = 0

// interimFinalizeMs accepted for call-site signature parity with googleSTT.js's transcribeStream() (so
// sttRouter.js can forward the exact same options object to either provider unchanged) but intentionally
// UNUSED here — this adapter uses Deepgram's own server-side is_final/speech_final signal as the finality
// source, not a client-side silence timer (see file header on why Google's TIMER_FINAL mechanism isn't
// replicated: it exists specifically because Google's model doesn't reliably send a final on its own for
// short utterances — Deepgram's finality signal is a first-class server feature this adapter should use
// as documented, not work around with an artificial client timer that would just be over-engineering a
// vanilla-baseline comparison per the locked "no accuracy hacks in Phase 1" rule).
function transcribeStream(onTranscript, onInterim, { interimFinalizeMs } = {}) {
  let destroyed = false
  let currentConnectionId = null
  let currentSocket = null
  let utteranceIdCounter = 0
  let currentUtteranceId = null
  let interimCount = 0
  let firstInterimAt = null
  let lastInterimAt = null

  function resetUtteranceState() {
    interimCount = 0
    firstInterimAt = null
    lastInterimAt = null
    currentUtteranceId = null
  }

  function isCurrentConnection(connectionId) {
    return !destroyed && connectionId === currentConnectionId
  }

  function handleMessage(connectionId, message) {
    if (!isCurrentConnection(connectionId)) return // stale connection — never mutate current state
    if (message?.type !== 'Results') return // Metadata/UtteranceEnd/SpeechStarted events not consumed by this adapter yet

    const alt = message.channel?.alternatives?.[0]
    const text = alt?.transcript || ''
    if (!text) return

    const isFinal = Boolean(message.is_final)
    if (currentUtteranceId === null) currentUtteranceId = ++utteranceIdCounter
    const nowTs = Date.now()

    if (!isFinal) {
      interimCount++
      if (firstInterimAt === null) firstInterimAt = nowTs
      lastInterimAt = nowTs
      onInterim?.(text)
      return
    }

    const finalAt = nowTs
    const thisUtteranceId = currentUtteranceId
    const finalInterimCount = interimCount
    const finalFirstInterimAt = firstInterimAt
    const finalLastInterimAt = lastInterimAt
    resetUtteranceState()

    onTranscript(text, {
      // Required Common Core (locked design, Lock 2): source, text (1st arg), finalConfidence,
      // alternatives, interimCount, firstInterimAt, finalAt — provider/model added by sttRouter.js.
      source: 'DEEPGRAM_FINAL',
      utteranceId: thisUtteranceId,
      interimCount: finalInterimCount,
      firstInterimAt: finalFirstInterimAt,
      finalAt,
      // Optional Common Telemetry (Review-approved addition, NOT part of Required Common Core) — kept
      // because Google's existing sttMeta already has both and [STT_DIAG]'s emitter already reads both by
      // name; omitting them here would just leave two blank columns for Deepgram-sourced log lines.
      // Locked semantics: lastInterimAt null = this utterance had no interim at all; firstInterimToFinalMs
      // null = uncomputable (no first interim or no valid final timestamp) — NEVER 0 standing in for
      // "no data." When computable it is always finalAt - firstInterimAt (>= 0 by construction).
      lastInterimAt: finalLastInterimAt,
      firstInterimToFinalMs: finalFirstInterimAt !== null ? finalAt - finalFirstInterimAt : null,
      // Nullable field semantics (locked design, Lock A): finalConfidence null = provider gave no
      // confidence value or it was non-positive/unreliable — never fabricated as 0.
      finalConfidence: (typeof alt?.confidence === 'number' && alt.confidence > 0) ? alt.confidence : null,
      // alternatives is always an array (never null) — empty array means the provider returned no
      // alternatives for this result, distinct from "not requested/not applicable."
      alternatives: (message.channel?.alternatives || []).map((a, i) => ({
        index: i,
        text: a.transcript,
        confidence: (typeof a.confidence === 'number' && a.confidence > 0) ? a.confidence : null,
        selected: i === 0,
      })),
      // providerMeta is always an object (never null) — Deepgram-specific data that has no Google
      // equivalent lives here, never forced into a Google-shaped top-level field name.
      providerMeta: {
        speechFinal: Boolean(message.speech_final),
        requestId: message.metadata?.request_id ?? null,
      },
    })
  }

  async function createConnection() {
    if (destroyed) return
    const connectionId = ++connectionIdCounter
    currentConnectionId = connectionId
    resetUtteranceState()

    let socket
    try {
      socket = await getClient().listen.v1.connect({
        model: DEEPGRAM_MODEL,
        language: DEEPGRAM_LANGUAGE,
        encoding: 'mulaw',
        sample_rate: 8000,
        channels: 1,
        interim_results: true,
        punctuate: true,
        smart_format: false, // Phase 1 vanilla baseline — no formatting/accuracy hacks (locked design)
        reconnectAttempts: DEEPGRAM_RECONNECT_ATTEMPTS,
      })
    } catch (err) {
      if (!isCurrentConnection(connectionId)) return // superseded while connecting — this failure is moot
      if (err.code === 'DEEPGRAM_MISSING_API_KEY') {
        console.error('[Deepgram] fatal config error, not retrying:', err.message)
        return // retrying can never fix a missing key — one clear log line, not an infinite retry-storm
      }
      console.error('[Deepgram] connect error, retrying:', err.message)
      setTimeout(() => { if (isCurrentConnection(connectionId)) createConnection() }, 200)
      return
    }

    if (!isCurrentConnection(connectionId)) { try { socket.close() } catch (_) {} return } // superseded mid-connect

    currentSocket = socket

    socket.on('message', (message) => handleMessage(connectionId, message))

    socket.on('error', (err) => {
      if (!isCurrentConnection(connectionId)) return
      console.error('[Deepgram error]', err.message)
    })

    // SDK-internal-reconnect race guard (see file header) — SECOND finding from a Review challenge on the
    // exact retry-counting semantics: ReconnectingWebSocket's `_retryCount` starts at -1 (verified in
    // ws.js's constructor) and is incremented EVERY time `_connect()` proceeds past its budget check,
    // including the very first connection. Tracing concretely with reconnectAttempts=5: attempts 1-6 each
    // advance retryCount to 0,1,2,3,4,5 respectively (6 TOTAL attempts execute — the initial one PLUS 5
    // retries, not 5 total), and the 7th attempt is blocked with retryCount staying at 5, unchanged. That
    // means retryCount===5 is observed on TWO different 'close' events: once right after attempt #6 was
    // just launched (SDK still actively retrying — must NOT replace), and once when attempt #7 was blocked
    // (SDK truly gave up — safe to replace). A single-snapshot `retryCount >= ceiling` check (this file's
    // first version of this guard) cannot tell those apart — it isn't just an off-by-one in the constant,
    // it's a genuine ambiguity in what one retryCount reading means. Fixed by tracking whether retryCount
    // ADVANCED since the previous 'close' we saw on this same connection: advancing means _connect() just
    // launched a new attempt (trust it); staying the same means this _connect() call was blocked (truly
    // exhausted, replace now). This needs no knowledge of the exact ceiling value to make the call — the
    // ceiling is still passed to the SDK (reconnectAttempts, above) purely to bound worst-case retry time.
    let lastCloseRetryCount // undefined = "no close observed yet on this connection"
    socket.on('close', () => {
      if (!isCurrentConnection(connectionId)) return // already superseded — expected, not a failure
      if (destroyed) return

      const rawRetryCount = socket.socket?.retryCount
      if (typeof rawRetryCount !== 'number') {
        // Can't observe the SDK's internal retry state at all (unexpected shape/older SDK version) — don't
        // trust an internal retry we can't verify is happening; replace immediately (fail safe toward
        // replacing rather than silently going deaf for an unbounded time).
        console.warn('[Deepgram] connection closed, retry state unreadable — creating replacement immediately (fail-safe)')
        currentSocket = null
        createConnection()
        return
      }

      if (lastCloseRetryCount !== rawRetryCount) {
        lastCloseRetryCount = rawRetryCount
        console.warn(`[Deepgram] connection dropped, SDK retrying internally (retryCount=${rawRetryCount}) — not creating a replacement`)
        return
      }

      console.warn(`[Deepgram] SDK internal reconnect attempts exhausted (retryCount=${rawRetryCount} unchanged since last close) — creating replacement connection`)
      currentSocket = null
      createConnection()
    })
  }

  createConnection()

  return {
    write(mulawBuffer) {
      if (destroyed || !currentSocket) return // dropped during (re)connect window — same accepted trade-off googleSTT.js already makes during its own cold-mute/rotation windows
      try {
        currentSocket.sendMedia(mulawBuffer)
      } catch (e) {
        console.error('[Deepgram] write error:', e.message)
      }
    },
    end() {
      if (destroyed) return
      // LIFECYCLE INVARIANT (Review-confirmed, not incidental ordering — do not reorder these two
      // statements): `destroyed = true` MUST be set BEFORE calling `currentSocket.close()`. V1Socket's own
      // close() (Socket.js) invokes this adapter's registered 'close' handler SYNCHRONOUSLY and
      // re-entrantly as part of the same call — not just later via an async event — so that handler's own
      // `if (destroyed) return` guard must already see `destroyed === true` the FIRST time it runs, or it
      // would treat our own intentional shutdown as an unexpected drop and try to create a replacement
      // connection after end() was already called.
      destroyed = true
      currentConnectionId = null
      try { currentSocket?.close() } catch (_) {}
      currentSocket = null
    },
  }
}

module.exports = { transcribeStream, DEEPGRAM_MODEL, DEEPGRAM_LANGUAGE, DEEPGRAM_RECONNECT_ATTEMPTS, _resetClientForTest }
