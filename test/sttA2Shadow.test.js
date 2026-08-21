// A2.1 Shadow Google Final Diagnostics (design revision 2026-08-21, Design Gate v2 PASS) — unit tests for
// the shadow-observation lifecycle in src/services/googleSTT.js: an already-rotated-away stream is watched
// for a late native final (or timeout/stream-end/error) purely for diagnostics, and must NEVER call
// onTranscript() or otherwise affect the live conversation path.
const { test } = require('node:test')
const assert = require('node:assert/strict')
const stt = require('./_googleSttHarness')

function waitFor(conditionFn, { timeout = 500, interval = 5 } = {}) {
  return new Promise((resolve, reject) => {
    const start = Date.now()
    const tick = () => {
      if (conditionFn()) return resolve()
      if (Date.now() - start > timeout) return reject(new Error('waitFor timeout'))
      setTimeout(tick, interval)
    }
    tick()
  })
}

test('shadow FINAL: late native final on shadowed (old, rotated-away) stream inside the observation window → onShadowDiagnostic fires FINAL, onTranscript is never called a second time', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'ทดสอบ')
  await waitFor(() => calls.length === 1) // TIMER_FINAL delivered, shadow now pending on oldStream
  assert.equal(calls[0].meta.source, 'TIMER_FINAL')

  stt.emitFinal(oldStream, 'ทดสอบครับ', 0.9, [{ transcript: 'อื่น' }])
  await waitFor(() => shadowCalls.length === 1)

  assert.equal(shadowCalls[0].shadowOutcome, 'FINAL')
  assert.equal(calls.length, 1, 'onTranscript ต้องไม่ถูกเรียกซ้ำจาก shadow final เด็ดขาด')
  assert.equal(shadowCalls[0].timerFinalText, 'ทดสอบ')
  assert.equal(shadowCalls[0].shadowAlternatives[0].text, 'ทดสอบครับ')
  assert.equal(shadowCalls[0].shadowAlternatives[0].selected, true)
  assert.ok(typeof shadowCalls[0].shadowFinalDelayMs === 'number' && shadowCalls[0].shadowFinalDelayMs >= 0)
  assert.equal(shadowCalls[0].shadowFinalAt - shadowCalls[0].timerFinalAt, shadowCalls[0].shadowFinalDelayMs, 'shadowFinalAt กับ shadowFinalDelayMs ต้องอ้าง Date.now() ครั้งเดียวกันเป๊ะ')

  handle.end()
})

test('shadow TIMEOUT: no late final within SHADOW_TIMEOUT_MS (2000ms) → onShadowDiagnostic TIMEOUT, all final-related fields null', { timeout: 5000 }, async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const shadowCalls = []
  const handle = transcribeStream(() => {}, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'ทดสอบ')
  await waitFor(() => shadowCalls.length === 1, { timeout: 2600 })

  assert.equal(shadowCalls[0].shadowOutcome, 'TIMEOUT')
  assert.equal(shadowCalls[0].shadowFinalAt, null)
  assert.equal(shadowCalls[0].shadowFinalDelayMs, null)
  assert.equal(shadowCalls[0].shadowAlternatives, null)

  handle.end()
})

test('shadow STREAM_END: shadowed stream ends before timeout, no final ever arrived → onShadowDiagnostic STREAM_END', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'ทดสอบ')
  await waitFor(() => calls.length === 1)

  stt.emitStreamEnd(oldStream)
  await waitFor(() => shadowCalls.length === 1)

  assert.equal(shadowCalls[0].shadowOutcome, 'STREAM_END')

  handle.end()
})

test('shadow ERROR: error on shadowed stream → onShadowDiagnostic ERROR, does not touch errorRetryCount/recreation logic (live/rotated-to stream still works normally for the next utterance)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'ทดสอบ')
  await waitFor(() => calls.length === 1)

  stt.emitError(oldStream, new Error('simulated STT error on shadowed stream'))
  await waitFor(() => shadowCalls.length === 1)
  assert.equal(shadowCalls[0].shadowOutcome, 'ERROR')

  // live stream (rotated-to via prewarm during the interim above) must still work completely normally —
  // proves the shadow error never fell through into the errorRetryCount/stream-recreation logic meant only
  // for currentStream/nextStream
  const liveStream = stt.streams[1]
  stt.emitInterim(liveStream, 'เทิร์นถัดไป')
  await waitFor(() => calls.length === 2)
  assert.equal(calls[1].text, 'เทิร์นถัดไป')
  assert.equal(calls[1].meta.source, 'TIMER_FINAL')

  handle.end()
})

test('shadow SUPERSEDED: a second TIMER_FINAL while a shadow is still pending → prior shadow settles SUPERSEDED, new shadow starts cleanly, never two active shadows at once', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const stream0 = stt.streams[0]

  stt.emitInterim(stream0, 'เทิร์นแรก')
  await waitFor(() => calls.length === 1) // shadow #1 now pending on stream0

  const stream1 = stt.streams[1] // prewarm-created during the interim above, now currentStream after rotation
  stt.emitInterim(stream1, 'เทิร์นสอง')
  await waitFor(() => calls.length === 2) // TIMER_FINAL #2 → shadow #1 forced SUPERSEDED, shadow #2 starts on stream1

  await waitFor(() => shadowCalls.length === 1)
  assert.equal(shadowCalls[0].shadowOutcome, 'SUPERSEDED')
  assert.equal(shadowCalls[0].timerFinalText, 'เทิร์นแรก')

  // shadow #2 must have started cleanly and independently — settle it too, via a late final on stream1
  stt.emitFinal(stream1, 'เทิร์นสองครับ', 0.9)
  await waitFor(() => shadowCalls.length === 2)
  assert.equal(shadowCalls[1].shadowOutcome, 'FINAL')
  assert.equal(shadowCalls[1].timerFinalText, 'เทิร์นสอง')

  handle.end()
})

test('shadow duplicate finals: two isFinal events on the same shadowed stream → only the first settles, the second is a no-op', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'ทดสอบ')
  await waitFor(() => calls.length === 1)

  stt.emitFinal(oldStream, 'ตัวแรก', 0.9)
  await waitFor(() => shadowCalls.length === 1)
  stt.emitFinal(oldStream, 'ตัวสอง', 0.9) // duplicate final on the already-settled shadow — must be ignored
  await stt.delay(20)

  assert.equal(shadowCalls.length, 1, 'ต้องไม่มี diagnostic ที่สองจาก final ซ้ำ')
  assert.equal(shadowCalls[0].shadowAlternatives[0].text, 'ตัวแรก')

  handle.end()
})

test('GOOGLE_FINAL delivery never creates a shadow — only TIMER_FINAL does', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const stream0 = stt.streams[0]

  stt.emitFinal(stream0, 'ผลลัพธ์จริงจาก Google', 0.9) // isFinal directly, no interim first
  await waitFor(() => calls.length === 1)
  assert.equal(calls[0].meta.source, 'GOOGLE_FINAL')

  await stt.delay(50)
  assert.equal(shadowCalls.length, 0, 'GOOGLE_FINAL ต้องไม่สร้าง shadow เลยไม่ว่ากรณีใด')

  handle.end()
})

test('shadow structurally OFF via maxAlternatives: enableShadow=true but maxAlternatives not >1 (A2 itself OFF) → shadow never created (googleSTT.js defense-in-depth, not just trusting the caller)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, enableShadow: true, // maxAlternatives omitted → null → A2 itself is OFF
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const stream0 = stt.streams[0]

  stt.emitInterim(stream0, 'ทดสอบ')
  await waitFor(() => calls.length === 1)
  await stt.delay(100)

  assert.equal(shadowCalls.length, 0, 'A2 OFF ต้องทำให้ A2.1 OFF structurally เสมอ ไม่ว่า enableShadow จะเป็นอะไร')

  handle.end()
})

test('shadow structurally OFF via enableShadow: A2 ON (maxAlternatives:3) but the independent shadow gate is OFF (enableShadow=false) → shadow never created — proves the two gates are genuinely independent', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: false,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const stream0 = stt.streams[0]

  stt.emitInterim(stream0, 'ทดสอบ')
  await waitFor(() => calls.length === 1)
  assert.notEqual(calls[0].meta.alternatives, null, 'A2 เองยัง ON จริง (alternatives ยังถูกส่งใน [STT_DIAG] ตามปกติ ไม่ใช่ null)')
  await stt.delay(100)

  assert.equal(shadowCalls.length, 0, 'A2 ON เพียงอย่างเดียวไม่พอ — shadow gate ของตัวเองต้อง ON ด้วย')

  handle.end()
})

test('onShadowDiagnostic throws: exception is caught inside settleShadow, never escapes the stream event handler, live stream continues completely normally for the next utterance', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  let shadowCallCount = 0
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: () => { shadowCallCount++; throw new Error('diagnostic sink exploded') },
  })
  const stream0 = stt.streams[0]

  stt.emitInterim(stream0, 'เทิร์นแรก')
  await waitFor(() => calls.length === 1)

  assert.doesNotThrow(() => stt.emitFinal(stream0, 'สายเก่า', 0.9), 'onShadowDiagnostic throw ต้องไม่หลุดออกมาถึง emit() เลย')
  await waitFor(() => shadowCallCount === 1)

  const liveStream = stt.streams[1]
  stt.emitInterim(liveStream, 'เทิร์นถัดไป')
  await waitFor(() => calls.length === 2)
  assert.equal(calls[1].text, 'เทิร์นถัดไป')

  handle.end()
})

test('shadow non-final data event: a non-final event on the shadowed (old) stream must not settle the shadow, must not call onTranscript, and must not affect the live stream\'s own interim/TIMER_FINAL text — errorRetryCount isolation is covered by the ERROR test above (settleShadow returns before that line is ever reached)', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'เทิร์นแรก')
  await waitFor(() => calls.length === 1) // shadow now pending on oldStream

  const liveStream = stt.streams[1]
  stt.emitInterim(oldStream, 'ผีเดิมพูดต่อ') // non-final event on the OLD shadowed stream — must be fully ignored
  await stt.delay(30)
  assert.equal(shadowCalls.length, 0, 'non-final event บน shadowed stream ต้องไม่ settle shadow')
  assert.equal(calls.length, 1, 'ต้องไม่มี onTranscript เพิ่มจาก shadowed stream เด็ดขาด')

  // live stream's own interim/TIMER_FINAL must be completely unaffected by the phantom shadow interim above
  stt.emitInterim(liveStream, 'เทิร์นถัดไปจริง')
  await waitFor(() => calls.length === 2)
  assert.equal(calls[1].text, 'เทิร์นถัดไปจริง', 'live stream ต้อง deliver ข้อความของตัวเองเท่านั้น ไม่ปนกับ shadow')

  handle.end()
})

test('end() with pending shadow: CALL_ENDED settles exactly once, timer is cleared, and any later event on the old shadowed stream never emits a second diagnostic — end() itself stays idempotent', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []
  const handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
    interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
    onShadowDiagnostic: (p) => shadowCalls.push(p),
  })
  const oldStream = stt.streams[0]

  stt.emitInterim(oldStream, 'ทดสอบ')
  await waitFor(() => calls.length === 1)

  handle.end()
  assert.equal(shadowCalls.length, 1)
  assert.equal(shadowCalls[0].shadowOutcome, 'CALL_ENDED')

  // stray late events after end() must never produce a second diagnostic
  stt.emitFinal(oldStream, 'สาย', 0.9)
  stt.emitStreamEnd(oldStream)
  stt.emitError(oldStream, new Error('late, after call ended'))
  await stt.delay(20)
  assert.equal(shadowCalls.length, 1, 'ต้องไม่มี diagnostic ซ้ำหลัง CALL_ENDED')

  assert.doesNotThrow(() => handle.end(), 'end() ต้อง idempotent — เรียกซ้ำได้โดยไม่ throw/ไม่ settle ซ้ำ')
  assert.equal(shadowCalls.length, 1)
})

test('Implementation Review regression: stale timeout callback from a SUPERSEDED shadow must never settle a newer active shadow — settleShadow() must reject on shadow identity, not rely on clearTimeout() timing alone', async () => {
  const { transcribeStream } = stt.ensureStubbed()
  const calls = []
  const shadowCalls = []

  // Intercept setTimeout calls specifically at SHADOW_TIMEOUT_MS (2000ms) to capture the shadow timeout
  // callbacks by reference — still delegates to the real setTimeout underneath (so normal clearTimeout()
  // cleanup keeps working exactly as in production), this only lets the test fire a stale callback
  // deterministically instead of waiting a real 2000ms. No other timer in this module uses a 2000ms delay
  // (interimFinalizeMs here is 20ms, retry/rotation timers are 50/100/300ms), so matching on the exact
  // delay value is safe and doesn't touch any other timer path.
  const originalSetTimeout = global.setTimeout
  const capturedShadowTimeouts = []
  global.setTimeout = function (fn, delayMs, ...args) {
    if (delayMs === 2000) capturedShadowTimeouts.push(fn)
    return originalSetTimeout(fn, delayMs, ...args)
  }

  let handle
  try {
    handle = transcribeStream((text, meta) => { calls.push({ text, meta }) }, () => {}, {
      interimFinalizeMs: 20, maxAlternatives: 3, enableShadow: true,
      onShadowDiagnostic: (p) => shadowCalls.push(p),
    })
    const stream0 = stt.streams[0]

    stt.emitInterim(stream0, 'เทิร์นแรก')
    await waitFor(() => calls.length === 1) // shadow #1 created — its 2000ms timeout callback captured

    const stream1 = stt.streams[1]
    stt.emitInterim(stream1, 'เทิร์นสอง')
    await waitFor(() => calls.length === 2) // shadow #1 → SUPERSEDED, shadow #2 created — its own timeout captured too

    await waitFor(() => shadowCalls.length === 1)
    assert.equal(shadowCalls[0].shadowOutcome, 'SUPERSEDED')
    assert.equal(capturedShadowTimeouts.length, 2, 'ต้องจับ setTimeout(...,2000) ได้ครบทั้งสอง shadow')

    // fire shadow #1's stale timeout callback manually — shadow #2 is currently active; this must be a
    // complete no-op (no new diagnostic at all), proven via identity check (activeShadow !== shadow1), not
    // via clearTimeout() having already cancelled it
    capturedShadowTimeouts[0]()
    assert.equal(shadowCalls.length, 1, 'stale timeout callback ของ shadow #1 ต้องไม่ settle อะไรเพิ่มเลย (shadow #2 ยัง active อยู่)')

    // shadow #2 must still be able to settle completely normally afterward
    stt.emitFinal(stream1, 'เทิร์นสองครับ', 0.9)
    await waitFor(() => shadowCalls.length === 2)
    assert.equal(shadowCalls[1].shadowOutcome, 'FINAL')
    assert.equal(shadowCalls[1].timerFinalText, 'เทิร์นสอง')

    // and even shadow #2's OWN real timeout firing late (after it already settled FINAL) must also be a no-op
    capturedShadowTimeouts[1]()
    assert.equal(shadowCalls.length, 2, 'shadow #2 settled แล้ว ห้ามมี diagnostic เพิ่มอีกแม้ timeout ของตัวเองจะยิงซ้ำทีหลัง')
  } finally {
    global.setTimeout = originalSetTimeout
    if (handle) handle.end()
  }
})
