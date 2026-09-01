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
//
// Socket readiness (Implementation Gate "Deepgram Live Socket Readiness + Handshake Diagnostics",
// 2026-08-31) — production proved a real gap: `currentSocket = socket` becomes truthy the instant
// `client.listen.v1.connect()` resolves, but that only means a V1Socket OBJECT exists, not that the
// underlying WebSocket has finished its handshake. write() was calling sendMedia() on a socket that could
// still be CONNECTING, producing a continuous "Socket is not open." error on every Twilio frame with zero
// transcript ever produced (confirmed live: callSid CA29e95e15e63c8c3944d2d6334278d490, 2026-08-31, 1000+
// consecutive write errors over ~19s with no open/error/close event ever observed in between).
//
// A hypothesis raised during triage — that this adapter needed to additionally call the V1Socket instance
// method `.connect()` (which internally calls `.reconnect()`) because `client.listen.v1.connect()` never
// starts a real attempt on its own — was checked against the actual installed source and is FALSE for this
// SDK version: `V1Client.connect()` (Client.js:102-114) constructs a `new core.ReconnectingWebSocket(...)`,
// and that class's OWN constructor calls `this._connect()` unconditionally at the end (ws.js:179) unless
// `options.startClosed` is passed (it isn't, here) — the handshake already starts automatically. Calling
// the V1Socket's separate `.connect()` method on top of that would call `.reconnect()` (ws.js:271-282),
// which ABORTS whatever attempt is already in flight and restarts from scratch — actively harmful, not a
// fix. Do not add that call.
//
// SUPERSEDED 2026-09-02 — the paragraph above is real, verified analysis, but of the WRONG class: it
// traces the SDK's GENERATED `V1Client` (api/resources/listen/resources/v1/client/Client.js), which this
// adapter does NOT actually use. `require('@deepgram/sdk')` re-exports its public `DeepgramClient` as
// `CustomClient.js`'s `CustomDeepgramClient` (verified directly: dist/cjs/index.js:50 —
// `Object.defineProperty(exports, "DeepgramClient", { get: () => CustomClient_js_1.CustomDeepgramClient })`
// — the generated client is separately exported there as `DefaultDeepgramClient`, not what this file
// imports). `CustomDeepgramClient.listen.v1` returns a `WrappedListenV1Client` (also in CustomClient.js),
// whose OWN `connect()` override calls `createWebSocketConnection()` with `startClosed: true` explicitly
// (CustomClient.js:912,928) — on THIS runtime path, `client.listen.v1.connect()` deliberately returns an
// UNSTARTED socket; the SDK's own JSDoc on the sibling `createConnection()` method says so outright:
// "the returned socket is not connected until you call socket.connect()". This is not a contradiction of
// the paragraph above — `startClosed: true` is exactly the condition that paragraph already identified as
// the one case where auto-connect does NOT happen; the miss was not checking which class this app's own
// `require()` actually resolves to before concluding "it isn't, here" for that option.
//
// `startClosed: true` makes `ReconnectingWebSocket`'s constructor set `_shouldReconnect = false`, so the
// unconditional `this._connect()` call at its end returns immediately on `!this._shouldReconnect` without
// creating `_ws`, without incrementing `_retryCount` (stays at its initial -1 forever). This fully explains
// every piece of live evidence with no residual gap: `readyState` reads 3/CLOSED (the getter's fallback
// path when `_ws` doesn't exist and `startClosed` is true — ws.js's readyState getter); the public
// `retryCount` getter is `Math.max(this._retryCount, 0)`, so the log line `retryCount=0` is that -1 clamped
// to zero, not "one attempt happened"; and no open/error/close ever fires because nothing downstream of
// `_ws` can fire without `_ws` existing. Confirmed live: callSid CAac0792417318fa71504aaf8b736f8cd5,
// 2026-09-01, `readyState=3 retryCount=0` unchanged for the entire ~28s call, zero open/error/close.
//
// Fix: the wrapper ships its own `WrappedListenV1Socket.connect()` (CustomClient.js:1096-1111) — a public
// method, not a private/internal one, and specifically NOT the same call this file's superseded analysis
// above warned against (that was `V1Socket.connect()` on the GENERATED socket, calling `.reconnect()` on an
// ALREADY-auto-connecting transport — actively harmful for that class; here it's the ONLY thing that ever
// flips `_shouldReconnect` back to true via `this.socket.reconnect()`, which is required exactly once,
// since nothing else on this runtime path ever will). Called exactly once per connectionId, immediately
// after all of message/open/error/close are registered (see createConnection() below) — a synchronous
// `open` during `.connect()` (real in the wrapper: it calls `super.connect()` → `V1Socket.connect()` →
// `this.socket.reconnect()` → `_connect()`, none of which are guaranteed async by this file's own reading)
// must never be missed.
//
// `waitForOpen()` (Socket.js:127-142) was evaluated too: it's confirmed passive/side-effect-free (just
// reads current readyState, else registers one-shot 'open'/'error' listeners on the underlying socket) —
// but it is a ONE-SHOT promise. A single connectionId here can span several of the SDK's OWN internal
// reconnect attempts (see the retryCount-advance guard below); if the FIRST attempt errors, waitForOpen()'s
// promise already rejects and settles — a LATER successful internal retry resolves nothing anyone is still
// awaiting. `socket.on('open', ...)` (registered once, like message/error/close below) is a persistent
// listener that correctly fires on whichever internal attempt actually succeeds, so that — plus a readyState
// snapshot taken immediately AFTER calling socket.connect() (not before — before that call, on this runtime
// path, readyState is unconditionally CLOSED and tells us nothing) — is what marks a connection ready below.

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

// SDK-internal retryCount RESET point (Review-identified, 2026-09-01, verified against ws.js's
// ReconnectingWebSocket): _handleOpen() arms `setTimeout(() => this._acceptOpen(), minUptime)`, and
// _acceptOpen() sets `this._retryCount = 0` — silently, no event fired. DEFAULT_OPTIONS.minUptime is 5000ms
// and V1Client.connect() does NOT override it (only debug/maxRetries/connectionTimeout are set), so this is
// the real value in effect here. This matters for the retryCount-advance guard below: a `lastCloseRetryCount`
// recorded BEFORE a stable-enough open is stale once that reset has silently happened, and comparing a
// fresh post-reset retryCount against it produces a false "unchanged = exhausted" reading purely by numeric
// coincidence, even though the SDK is actually retrying normally again after a brand new drop.
const DEEPGRAM_SDK_MIN_UPTIME_MS = 5000

// Twilio Media Streams sends 8-bit μ-law @ 8000 Hz mono — 1 byte/sample, so bytes/ms = sample rate / 1000.
// Named here so the pre-open buffer cap below is derived from the same real audio-rate math the adapter
// already sends to Deepgram (sample_rate below), not a made-up byte count.
const SAMPLE_RATE_HZ = 8000
const BYTES_PER_MS = SAMPLE_RATE_HZ / 1000 // 8

// Bounded pre-open audio buffer (locked design) — the handshake window is normally sub-second, but
// production proved it can stall far longer. Buffering the customer's speech during that window (instead
// of write()'s old behavior of throwing it away silently on every frame) avoids reintroducing the exact
// short-utterance-loss failure mode this whole investigation started from. Capped so a genuinely stuck
// connection can't grow this unbounded in memory for the rest of the call.
const PRE_OPEN_BUFFER_MS = 1500
const PRE_OPEN_BUFFER_MAX_BYTES = PRE_OPEN_BUFFER_MS * BYTES_PER_MS // 12000 bytes

// Log-throttling rate for the pre-open-buffer-full warning (Review-identified secondary issue, 2026-09-02)
// — a stuck connection drops on every Twilio frame (~50/sec), so logging every drop reproduces the exact
// per-frame log storm this whole fix removed from write()'s old direct-sendMedia() path. Frame #1 always
// logs immediately; after that, only every Nth. Counters (droppedFrameCount/droppedBytes) still advance on
// every dropped frame regardless — only the console.warn call itself is rate-limited.
const DROP_LOG_INTERVAL_FRAMES = 50

// Application-level readiness DIAGNOSTIC boundary only — locked design explicitly forbids using this to
// drop buffered audio a second time or to spin up a parallel connection (that stays owned by the SDK's own
// reconnect plus this file's existing retryCount-advance guard below, which already race-safely decides
// when a replacement connection is warranted). This exists purely so a stuck handshake produces a clear,
// searchable log line instead of the silence production just proved is possible even with the SDK's own
// internal connectionTimeout (4000ms, DEFAULT_OPTIONS in ws.js) which apparently did not fire audibly.
const DEEPGRAM_READINESS_TIMEOUT_MS = 6000

// Verified against the installed SDK (ws.js:477-487): ReconnectingWebSocket.ReadyState.OPEN === 1, the
// standard WebSocket readyState value. Not re-exported from the package's public surface, so used here as
// a documented literal rather than reaching into an internal module path.
const WS_READY_STATE_OPEN = 1

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

  // Readiness/buffer state — scoped per connection (reset at the top of every createConnection() call, same
  // as resetUtteranceState()) so a replacement connection never inherits a stale connection's buffered audio
  // or ready flag.
  let socketReady = false
  let audioBuffer = []
  let audioBufferBytes = 0
  // Overflow policy (Review-requested clarification, locked): drop NEWEST, keep OLDEST — once the cap is
  // hit, further incoming frames are rejected outright rather than evicting already-buffered audio. This
  // preserves the start of whatever the customer already said (the earliest audio is what a late-opening
  // socket needs most) at the cost of losing only the tail beyond ~1.5s of stall, which is itself already a
  // degraded scenario. Counters below make the actual loss visible in the diagnostics rather than leaving
  // "dropping frame" as a vague, uncounted warning.
  let droppedFrameCount = 0
  let droppedBytes = 0
  let hasLoggedBufferingStart = false
  let currentReadinessTimeout = null
  let currentConnectionStartedAt = null // set at the top of createConnection(), read by markSocketReady() for elapsedMs

  function resetUtteranceState() {
    interimCount = 0
    firstInterimAt = null
    lastInterimAt = null
    currentUtteranceId = null
  }

  function resetConnectionBufferState() {
    socketReady = false
    audioBuffer = []
    audioBufferBytes = 0
    droppedFrameCount = 0
    droppedBytes = 0
    hasLoggedBufferingStart = false
    // Timer hygiene: a connection that gets superseded (close handler creates a replacement) while its own
    // readiness timeout is still pending never reaches markSocketReady() or end() — nothing else would ever
    // clear that dangling timer otherwise. isCurrentConnection() already makes it a behavioral no-op if left
    // to fire on its own, but explicitly clearing it here (at the start of every new connection, which is
    // exactly when a previous one's timer would go stale) avoids accumulating orphaned timers across a call
    // with several reconnects.
    if (currentReadinessTimeout) { clearTimeout(currentReadinessTimeout); currentReadinessTimeout = null }
  }

  // Marks the connectionId's socket ready-to-send and flushes whatever audio accumulated during the
  // handshake window, in arrival order, exactly once. Safe to call from either the synchronous
  // already-open snapshot or the persistent 'open' listener — idempotent per connection, and a stale
  // connectionId (superseded, or destroyed via end()) is a silent no-op via isCurrentConnection() — this is
  // also what guarantees a stale connection's late 'open' can never flush a buffer, clear the CURRENT
  // connection's readiness timer, or mutate socketReady/audioBuffer belonging to whichever connection is
  // actually current: every one of those four things happens only past the isCurrentConnection() check
  // below, never before it. A readiness timeout having already fired for this SAME (still current)
  // connectionId does not "poison" it either — the timeout is diagnostic-only (see
  // DEEPGRAM_READINESS_TIMEOUT_MS above) and never sets any flag this function checks; a late, genuinely
  // successful open still marks ready and flushes normally.
  function markSocketReady(connectionId, source) {
    if (!isCurrentConnection(connectionId)) return
    if (socketReady) return
    socketReady = true
    if (currentReadinessTimeout) { clearTimeout(currentReadinessTimeout); currentReadinessTimeout = null }
    const toFlush = audioBuffer
    const flushedBytes = audioBufferBytes
    const droppedFramesTotal = droppedFrameCount
    const droppedBytesTotal = droppedBytes
    const elapsedMs = currentConnectionStartedAt !== null ? Date.now() - currentConnectionStartedAt : null
    audioBuffer = []
    audioBufferBytes = 0
    console.log(`[Deepgram] socket ready connectionId=${connectionId} source=${source} elapsedMs=${elapsedMs} bufferedFrames=${toFlush.length} bufferedBytes=${flushedBytes} droppedFrames=${droppedFramesTotal} droppedBytes=${droppedBytesTotal}`)
    for (const buf of toFlush) {
      try { currentSocket.sendMedia(buf) } catch (e) { console.error(`[Deepgram] flush write error connectionId=${connectionId}:`, e.message) }
    }
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
    resetConnectionBufferState()
    currentConnectionStartedAt = Date.now()

    let socket
    try {
      socket = await getClient().listen.v1.connect({
        model: DEEPGRAM_MODEL,
        language: DEEPGRAM_LANGUAGE,
        encoding: 'mulaw',
        sample_rate: SAMPLE_RATE_HZ,
        channels: 1,
        interim_results: true,
        punctuate: true,
        smart_format: false, // Phase 1 vanilla baseline — no formatting/accuracy hacks (locked design)
        reconnectAttempts: DEEPGRAM_RECONNECT_ATTEMPTS,
      })
    } catch (err) {
      if (!isCurrentConnection(connectionId)) return // superseded while connecting — this failure is moot
      if (err.code === 'DEEPGRAM_MISSING_API_KEY') {
        console.error(`[Deepgram] fatal config error connectionId=${connectionId}, not retrying:`, err.message)
        return // retrying can never fix a missing key — one clear log line, not an infinite retry-storm
      }
      console.error(`[Deepgram] connect error connectionId=${connectionId}, retrying:`, err.message)
      setTimeout(() => { if (isCurrentConnection(connectionId)) createConnection() }, 200)
      return
    }

    if (!isCurrentConnection(connectionId)) { try { socket.close() } catch (_) {} return } // superseded mid-connect

    currentSocket = socket

    // retryCount-tracking state (see the reconnect-race-guard comment at the close handler below for the
    // ADVANCE-detection logic, and DEEPGRAM_SDK_MIN_UPTIME_MS above for the reset-timer mirror) — declared
    // here, before anything can fire 'open' or 'close', so both handlers close over the same per-connection
    // state consistently.
    let lastCloseRetryCount // undefined = "no close observed yet on this connection"
    // Review-identified race (2026-09-02): a wall-clock `Date.now()` diff cannot tell "minUptime elapsed AND
    // the SDK's _acceptOpen() actually ran" apart from "minUptime elapsed, but _handleClose()'s
    // _clearTimeouts() cancelled the SDK's _uptimeTimeout before it ever got to fire" — those look identical
    // to a timestamp read, but only the first one means the SDK's retryCount was really reset. Mirroring the
    // SDK's OWN timer with our own setTimeout (armed moments after the SDK arms its, inside the same 'open'
    // handling) sidesteps this: our mirror timer is subject to the exact same event-loop scheduling behavior
    // the SDK's timer is, so "our callback actually fired" is real evidence the SDK's earlier-registered
    // timer fired too — not a guess. If a close arrives before our mirror timer's callback runs (including
    // via the identical clear-before-fire race), retryBudgetResetExpected correctly stays false, matching
    // the SDK's own true (not-yet-reset) state — erring toward preserving exhaustion detection, never toward
    // a false "reset" that would abandon a genuinely exhausted connection.
    let retryBudgetResetExpected = false
    let minUptimeMirrorTimer = null

    // Diagnostic (see file header, "Explicit socket startup" 2026-09-02): on this SDK's public
    // DeepgramClient (CustomDeepgramClient), the socket returned here is ALWAYS startClosed — readyState=3
    // and retryCount=0 at this exact point are the expected, unstarted state, not evidence of a problem.
    // Logged separately from the post-startup snapshot below so a log reader can tell "not started yet"
    // apart from "started but still connecting/stuck".
    console.log(`[Deepgram] connection object created connectionId=${connectionId} readyState=${socket.readyState} retryCount=${socket.socket?.retryCount}`)

    socket.on('message', (message) => handleMessage(connectionId, message))

    socket.on('open', () => {
      // Re-arm the mirror timer on every open (including internal-SDK-retry re-opens), mirroring the SDK's
      // own _handleOpen() re-arming _uptimeTimeout every time. Not needed for the very first open of a
      // connectionId (lastCloseRetryCount is still undefined then, so the invalidation check below is moot
      // regardless) but arming it unconditionally here is simplest and harmless.
      retryBudgetResetExpected = false
      if (minUptimeMirrorTimer) clearTimeout(minUptimeMirrorTimer)
      minUptimeMirrorTimer = setTimeout(() => {
        if (!isCurrentConnection(connectionId)) return
        retryBudgetResetExpected = true
      }, DEEPGRAM_SDK_MIN_UPTIME_MS)
      markSocketReady(connectionId, 'open-event')
    })

    socket.on('error', (err) => {
      if (!isCurrentConnection(connectionId)) return
      console.error(`[Deepgram error] connectionId=${connectionId}`, err.message)
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
    socket.on('close', () => {
      if (!isCurrentConnection(connectionId)) return // already superseded — expected, not a failure
      if (destroyed) return

      // Mirror the SDK's own _handleClose() → _clearTimeouts() ordering: cancel our mirror timer FIRST,
      // before reading/using its flag, so a connection that's about to transition never leaves a dangling
      // timer that could set retryBudgetResetExpected on a stale connectionId later (isCurrentConnection()
      // inside the timer callback already guards that, but clearing here matches the timer-hygiene pattern
      // used elsewhere in this file and avoids an unnecessary pending timer).
      if (minUptimeMirrorTimer) { clearTimeout(minUptimeMirrorTimer); minUptimeMirrorTimer = null }

      const rawRetryCount = socket.socket?.retryCount
      if (typeof rawRetryCount !== 'number') {
        // Can't observe the SDK's internal retry state at all (unexpected shape/older SDK version) — don't
        // trust an internal retry we can't verify is happening; replace immediately (fail safe toward
        // replacing rather than silently going deaf for an unbounded time).
        console.warn(`[Deepgram] connection closed connectionId=${connectionId}, retry state unreadable — creating replacement immediately (fail-safe)`)
        currentSocket = null
        createConnection()
        return
      }

      // Review-identified guard (2026-09-01, refined 2026-09-02): if our mirror timer's callback has
      // actually fired since the most recent open on this connectionId, that's real evidence the SDK's own
      // _acceptOpen() (ws.js) fired too and silently reset its internal retryCount to 0 — see the
      // retryBudgetResetExpected declaration above for why this is timer-completion-based, not a wall-clock
      // comparison. A lastCloseRetryCount recorded before that reset is stale — comparing it against a fresh
      // post-reset value would read as "unchanged" by sheer numeric coincidence, not because the SDK is
      // actually blocked, wrongly triggering exhaustion and abandoning a healthy internal retry. A
      // connection that never actually opened (still failing its very first handshake attempts) never has
      // retryBudgetResetExpected set true — entirely unaffected, matching existing exhaustion-detection
      // behavior for that case exactly.
      if (retryBudgetResetExpected) {
        lastCloseRetryCount = undefined
      }
      retryBudgetResetExpected = false

      if (lastCloseRetryCount !== rawRetryCount) {
        lastCloseRetryCount = rawRetryCount
        console.warn(`[Deepgram] connection dropped connectionId=${connectionId}, SDK retrying internally (retryCount=${rawRetryCount}) — not creating a replacement`)
        // Review-identified gap: this branch keeps the SAME connectionId/V1Socket (the SDK is retrying
        // internally on it), but the underlying transport genuinely went back to not-open. socketReady must
        // track that, or write() keeps taking the direct sendMedia() path into a socket that just dropped —
        // the exact same failure mode this whole fix started from, just mid-call instead of at handshake.
        // Only fires on the ready→not-ready transition (guarded by `if (socketReady)`) so a connection that
        // was never ready in the first place (still on its original handshake attempt) is unaffected — its
        // original readiness timeout from createConnection() is still the one ticking.
        if (socketReady) {
          socketReady = false
          hasLoggedBufferingStart = false
          console.warn(`[Deepgram] connection dropped connectionId=${connectionId} while already ready — audio buffers again until the SDK's internal retry re-opens it`)
          currentReadinessTimeout = setTimeout(() => {
            if (!isCurrentConnection(connectionId) || socketReady) return
            console.warn(`[Deepgram] readiness timeout (post-drop) connectionId=${connectionId} elapsedMs=${Date.now() - currentConnectionStartedAt} readyState=${currentSocket?.readyState} retryCount=${currentSocket?.socket?.retryCount} bufferedBytes=${audioBufferBytes}`)
          }, DEEPGRAM_READINESS_TIMEOUT_MS)
        }
        return
      }

      console.warn(`[Deepgram] SDK internal reconnect attempts exhausted connectionId=${connectionId} (retryCount=${rawRetryCount} unchanged since last close) — creating replacement connection`)
      currentSocket = null
      createConnection()
    })

    // Explicit socket startup (locked design, 2026-09-02 — see "SUPERSEDED" file header note for the full
    // trace). All of message/open/error/close are registered above BEFORE this call, deliberately — a
    // synchronous 'open' during socket.connect() (real risk here: the wrapper's connect() calls
    // super.connect() → this.socket.reconnect() → _connect(), none of which this file's own reading found
    // guaranteed to defer to a later tick) must never be missed. Called exactly once per connectionId; the
    // SDK's own internal retry (the reconnect-race guard above) owns every attempt after this one.
    try {
      socket.connect()
    } catch (err) {
      if (!isCurrentConnection(connectionId)) return
      // A synchronous throw here is a startup/config-level failure (bad URL, invalid WebSocket
      // implementation, etc.), not a transient network blip — retrying the exact same call would very
      // likely throw again. Log once and stop, matching the missing-API-key precedent above: one clear
      // diagnostic line, not a synchronous throw-loop from immediately calling createConnection() again.
      console.error(`[Deepgram] socket.connect() threw synchronously connectionId=${connectionId}, not retrying:`, err.message)
      return
    }

    // Snapshot AFTER startup, not before: pre-startup readyState is unconditionally CLOSED on this runtime
    // path (see the "connection object created" log above) and tells us nothing about this specific
    // attempt. This is the race-safe check — OPEN can legitimately already be true by the time this line
    // runs (fast network, or a synchronous-open fake/test socket), and markSocketReady() is idempotent
    // (`if (socketReady) return`) so a synchronous 'open' during connect() calling it once via the listener
    // above, then again here, is a safe no-op — no double-arm of the readiness timeout, no double-flush.
    const postStartReadyState = socket.readyState
    console.log(`[Deepgram] socket startup invoked connectionId=${connectionId} postStartReadyState=${postStartReadyState}`)

    if (postStartReadyState === WS_READY_STATE_OPEN) {
      markSocketReady(connectionId, 'synchronous-snapshot')
    } else {
      // Diagnostic-only boundary (locked design — see DEEPGRAM_READINESS_TIMEOUT_MS above): never drops the
      // buffer, never spins up a replacement connection on its own. Cleared by markSocketReady() the moment
      // this connectionId actually becomes ready.
      currentReadinessTimeout = setTimeout(() => {
        if (!isCurrentConnection(connectionId) || socketReady) return
        console.warn(`[Deepgram] readiness timeout connectionId=${connectionId} elapsedMs=${Date.now() - currentConnectionStartedAt} readyState=${currentSocket?.readyState} retryCount=${currentSocket?.socket?.retryCount} bufferedBytes=${audioBufferBytes}`)
      }, DEEPGRAM_READINESS_TIMEOUT_MS)
    }
  }

  createConnection()

  return {
    write(mulawBuffer) {
      if (destroyed || !currentSocket) return // dropped during (re)connect window — same accepted trade-off googleSTT.js already makes during its own cold-mute/rotation windows
      if (!socketReady) {
        // Bounded pre-open buffer (locked design) — hold the audio instead of throwing it away, so a slow
        // (but eventually successful) handshake doesn't silently lose the customer's first utterance.
        // Overflow policy: drop NEWEST, keep OLDEST (see droppedFrameCount comment above) — once full, this
        // frame is rejected outright, never evicts what's already buffered.
        if (audioBufferBytes + mulawBuffer.length <= PRE_OPEN_BUFFER_MAX_BYTES) {
          if (!hasLoggedBufferingStart) {
            // One-time marker per connection (not per-frame — a frame arrives every ~20ms, logging every one
            // would be 50 lines/sec of noise) so a smoke-test log reader can see buffering genuinely started
            // during the handshake window, distinct from silence.
            hasLoggedBufferingStart = true
            console.log(`[Deepgram] pre-open buffering started connectionId=${currentConnectionId}`)
          }
          audioBuffer.push(mulawBuffer)
          audioBufferBytes += mulawBuffer.length
        } else {
          droppedFrameCount++
          droppedBytes += mulawBuffer.length
          // Log throttling (Review-identified secondary issue, 2026-09-02): production proved a genuinely
          // stuck connection generates a drop on every Twilio frame (~50/sec) for the rest of the call —
          // logging every one turned the exact storm this fix removed from write() into a new one here.
          // The counters above still advance on EVERY frame regardless (accuracy never suffers); only the
          // logging is rate-limited — first drop always logs (so a smoke-test reader sees it start
          // immediately), then every DROP_LOG_INTERVAL_FRAMES-th after that. The running total in each
          // logged line still reflects the true cumulative count, not just the frames since the last log.
          if (droppedFrameCount === 1 || droppedFrameCount % DROP_LOG_INTERVAL_FRAMES === 0) {
            console.warn(`[Deepgram] pre-open buffer full connectionId=${currentConnectionId} capBytes=${PRE_OPEN_BUFFER_MAX_BYTES} — dropping frame #${droppedFrameCount} (${mulawBuffer.length} bytes, ${droppedBytes} bytes dropped total this connection)`)
          }
        }
        return
      }
      try {
        currentSocket.sendMedia(mulawBuffer)
      } catch (e) {
        console.error(`[Deepgram] write error connectionId=${currentConnectionId}:`, e.message)
      }
    },
    end() {
      if (destroyed) return
      // Final drop summary (Review-identified secondary issue, 2026-09-02; wording corrected 2026-09-02
      // after a second Review pass) — markSocketReady() already logs a droppedFrames/droppedBytes summary
      // when a connection DOES become ready, but ending while `!socketReady` never reaches that log line —
      // the only trace would otherwise be the throttled per-frame warnings above, easy to miss in a long
      // log. One line here closes that gap unconditionally. IMPORTANT: `!socketReady` here does NOT mean
      // "this connectionId never opened" — the mid-call reconnect branch above (see the close handler's
      // `if (socketReady) { socketReady = false; ... }`) resets this exact same flag back to false after a
      // connection that WAS open drops mid-call and starts re-buffering while the SDK retries internally.
      // So this branch covers BOTH "never opened at all" and "was open, dropped, still reconnecting when
      // end() was called" — the log wording must not claim it's the former specifically.
      if (!socketReady && droppedFrameCount > 0) {
        console.warn(`[Deepgram] connection ended while not ready connectionId=${currentConnectionId} droppedFrames=${droppedFrameCount} droppedBytes=${droppedBytes} bufferedBytes=${audioBufferBytes}`)
      }
      // LIFECYCLE INVARIANT (Review-confirmed, not incidental ordering — do not reorder these two
      // statements): `destroyed = true` MUST be set BEFORE calling `currentSocket.close()`. V1Socket's own
      // close() (Socket.js) invokes this adapter's registered 'close' handler SYNCHRONOUSLY and
      // re-entrantly as part of the same call — not just later via an async event — so that handler's own
      // `if (destroyed) return` guard must already see `destroyed === true` the FIRST time it runs, or it
      // would treat our own intentional shutdown as an unexpected drop and try to create a replacement
      // connection after end() was already called.
      destroyed = true
      currentConnectionId = null
      if (currentReadinessTimeout) { clearTimeout(currentReadinessTimeout); currentReadinessTimeout = null }
      try { currentSocket?.close() } catch (_) {}
      currentSocket = null
    },
  }
}

module.exports = { transcribeStream, DEEPGRAM_MODEL, DEEPGRAM_LANGUAGE, DEEPGRAM_RECONNECT_ATTEMPTS, _resetClientForTest }
