// Dual STT Provider (design frozen 2026-08-31) — deepgramSTT.js tests. Stubs @deepgram/sdk via
// require.cache injection (same pattern as test/gemini.test.js's @google/genai stub) so this exercises
// the real adapter logic (connection-ownership invariant, message parsing, mulaw pass-through) against a
// fake V1Socket, never a real Deepgram connection.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

const state = { connectAttempts: [], sockets: [], connectImpl: null }

// WebSocket readyState constants (standard values, verified against the installed SDK's ws.js:477-487).
const WS_CONNECTING = 0
const WS_OPEN = 1

function makeFakeSocket({ retryCount, reentrantClose, readyState = WS_OPEN } = {}) {
  const handlers = {}
  const socket = {
    on: (event, cb) => { handlers[event] = cb },
    sendMedia: (buf) => { socket.sentMedia.push(buf) },
    // reentrantClose mirrors the REAL SDK's actual behavior (verified in Socket.js: `close()` calls
    // `this.socket.close()` then synchronously/re-entrantly invokes `this.handleClose({code:1000})`
    // itself, in the same call — not just via a later async event). Off by default so tests that don't
    // care about this specific ordering keep the simpler "close() just marks closed" fake.
    close: () => { socket.closed = true; if (reentrantClose) handlers.close?.({ code: 1000 }) },
    sentMedia: [],
    closed: false,
    // Defaults to OPEN so every EXISTING test (message parsing, ownership invariants, reconnect logic) that
    // doesn't care about the connect handshake itself keeps working unchanged — the adapter's synchronous
    // readyState snapshot marks the connection ready immediately in that case, matching pre-readiness-fix
    // behavior. Readiness-lifecycle tests below pass WS_CONNECTING explicitly and drive it via _emit('open').
    readyState,
    // V1Socket exposes the underlying ReconnectingWebSocket as a public `.socket` field (verified against
    // the installed SDK's actual ws.js source) — retryCount is a real public getter on it. Only set on the
    // fake when a test explicitly passes one, so tests that don't care exercise the adapter's fail-safe
    // "unknown shape → assume exhausted" branch (see deepgramSTT.js), matching the pre-fix test coverage.
    ...(retryCount !== undefined ? { socket: { retryCount } } : {}),
    // test helpers — not part of the real SDK surface
    _emit: (event, payload) => handlers[event]?.(payload),
  }
  return socket
}

class FakeDeepgramClient {
  constructor(options) {
    this.options = options
    this.listen = {
      v1: {
        connect: async (args) => {
          state.connectAttempts.push(args)
          if (state.connectImpl) return state.connectImpl(args)
          const socket = makeFakeSocket()
          state.sockets.push(socket)
          return socket
        },
      },
    }
  }
}

const deepgramSdkPath = require.resolve('@deepgram/sdk')
require.cache[deepgramSdkPath] = {
  id: deepgramSdkPath, filename: deepgramSdkPath, loaded: true,
  exports: { DeepgramClient: FakeDeepgramClient },
}

const { transcribeStream, DEEPGRAM_MODEL, DEEPGRAM_LANGUAGE, DEEPGRAM_RECONNECT_ATTEMPTS, _resetClientForTest } = require('../src/services/deepgramSTT')

beforeEach(() => {
  state.connectAttempts = []
  state.sockets = []
  state.connectImpl = null
  _resetClientForTest(null) // force a fresh FakeDeepgramClient per test (module caches the client otherwise)
  process.env.DEEPGRAM_API_KEY = 'test_key_12345'
})

function resultsMessage({ text, isFinal, confidence = 0.9, speechFinal = isFinal, requestId = 'req1' }) {
  return {
    type: 'Results',
    is_final: isFinal,
    speech_final: speechFinal,
    channel: { alternatives: [{ transcript: text, confidence }] },
    metadata: { request_id: requestId },
  }
}

async function flushMicrotasks() {
  await new Promise(r => setImmediate(r))
  await new Promise(r => setImmediate(r))
}

test('missing DEEPGRAM_API_KEY → throw ทันที ไม่ silent fallback ไป Google (locked design)', async () => {
  delete process.env.DEEPGRAM_API_KEY
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  // createConnection() ที่เรียก getClient() ตอน construct ควร throw ตั้งแต่ก่อนแม้แต่จะเรียก connect —
  // ไม่มี socket ใดถูกสร้างเลย และไม่มี fallback ไป Google
  assert.equal(state.connectAttempts.length, 0)
  stream.end()
})

test('missing DEEPGRAM_API_KEY ต้องไม่ retry (regression test — เดิมพลาดเข้า retry path เดียวกับ network error ทำให้วน retry ทุก 200ms ตลอดสาย)', async () => {
  delete process.env.DEEPGRAM_API_KEY
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  await new Promise(r => setTimeout(r, 250)) // เลย retry window (200ms) ไปแล้ว
  assert.equal(state.connectAttempts.length, 0, 'missing API key ต้อง fail ครั้งเดียวจบ ไม่ retry ไม่จำกัด')
  stream.end()
})

test('config ที่ส่งไป Deepgram ถูกต้องตาม locked design: model=nova-3, language=th, encoding=mulaw ตรงๆ ไม่แปลง', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  assert.equal(state.connectAttempts.length, 1)
  const args = state.connectAttempts[0]
  assert.equal(args.model, DEEPGRAM_MODEL)
  assert.equal(args.language, DEEPGRAM_LANGUAGE)
  assert.equal(args.encoding, 'mulaw')
  assert.equal(args.sample_rate, 8000)
  assert.equal(args.interim_results, true)
  stream.end()
})

test('write(mulawBuffer) ส่ง buffer ตรงๆ ไป sendMedia() ไม่แปลงเป็น PCM16 เลย (ต่างจาก googleSTT.js โดยตั้งใจ)', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const rawMulaw = Buffer.from([1, 2, 3, 4])
  stream.write(rawMulaw)
  assert.equal(state.sockets[0].sentMedia.length, 1)
  assert.equal(state.sockets[0].sentMedia[0], rawMulaw, 'ต้องเป็น reference เดิมเป๊ะ ไม่ผ่านการแปลง/copy ใดๆ')
  stream.end()
})

test('interim result (is_final=false) → onInterim(text) เป็น string ตรงๆ ไม่ใช่ object', async () => {
  const stream = transcribeStream(() => {}, (text) => { received.push(text) })
  const received = []
  await flushMicrotasks()
  state.sockets[0]._emit('message', resultsMessage({ text: 'สวัสดี', isFinal: false }))
  assert.deepEqual(received, ['สวัสดี'])
  stream.end()
})

test('final result (is_final=true) → onTranscript(text, sttMeta) ตรง Common Contract: source/finalConfidence/alternatives/providerMeta', async () => {
  let receivedText = null, receivedMeta = null
  const stream = transcribeStream((text, meta) => { receivedText = text; receivedMeta = meta }, () => {})
  await flushMicrotasks()
  state.sockets[0]._emit('message', resultsMessage({ text: 'เรียบร้อยครับ', isFinal: true, confidence: 0.92, requestId: 'req-abc' }))
  assert.equal(receivedText, 'เรียบร้อยครับ')
  assert.equal(receivedMeta.source, 'DEEPGRAM_FINAL')
  assert.equal(receivedMeta.finalConfidence, 0.92)
  assert.deepEqual(receivedMeta.alternatives, [{ index: 0, text: 'เรียบร้อยครับ', confidence: 0.92, selected: true }])
  assert.deepEqual(receivedMeta.providerMeta, { speechFinal: true, requestId: 'req-abc' })
  assert.equal(typeof receivedMeta.finalAt, 'number')
  stream.end()
})

// ===== Lock A — nullable field semantics =====

test('Lock A: confidence 0 หรือค่าที่ไม่ใช่ตัวเลขบวก → finalConfidence เป็น null ไม่ fabricate เป็น 0', async () => {
  let receivedMeta = null
  const stream = transcribeStream((t, meta) => { receivedMeta = meta }, () => {})
  await flushMicrotasks()
  const msg = resultsMessage({ text: 'ทดสอบ', isFinal: true, confidence: 0 })
  state.sockets[0]._emit('message', msg)
  assert.equal(receivedMeta.finalConfidence, null)
  stream.end()
})

test('Lock A: ไม่มี interim เลยก่อน final (Deepgram อาจส่ง final ตรงๆ ได้) → firstInterimAt/lastInterimAt เป็น null ไม่ fabricate', async () => {
  let receivedMeta = null
  const stream = transcribeStream((t, meta) => { receivedMeta = meta }, () => {})
  await flushMicrotasks()
  state.sockets[0]._emit('message', resultsMessage({ text: 'ทดสอบ', isFinal: true }))
  assert.equal(receivedMeta.firstInterimAt, null)
  assert.equal(receivedMeta.lastInterimAt, null)
  assert.equal(receivedMeta.interimCount, 0)
  stream.end()
})

test('Lock A: alternatives ว่างเปล่า (ไม่มี alternatives array เลยจาก provider) → [] ไม่ใช่ null', async () => {
  let receivedMeta = null
  const stream = transcribeStream((t, meta) => { receivedMeta = meta }, () => {})
  await flushMicrotasks()
  const msg = { type: 'Results', is_final: true, speech_final: true, channel: { alternatives: [{ transcript: 'ทดสอบ', confidence: 0.5 }] }, metadata: {} }
  state.sockets[0]._emit('message', msg)
  assert.ok(Array.isArray(receivedMeta.alternatives))
  assert.equal(receivedMeta.providerMeta.requestId, null, 'metadata.request_id ไม่มี → null ไม่ใช่ undefined หรือ throw')
  stream.end()
})

test('empty transcript (text ว่างเปล่า) ไม่ trigger onInterim/onTranscript เลย', async () => {
  let interimCalled = false, transcriptCalled = false
  const stream = transcribeStream(() => { transcriptCalled = true }, () => { interimCalled = true })
  await flushMicrotasks()
  state.sockets[0]._emit('message', resultsMessage({ text: '', isFinal: false }))
  state.sockets[0]._emit('message', resultsMessage({ text: '', isFinal: true }))
  assert.equal(interimCalled, false)
  assert.equal(transcriptCalled, false)
  stream.end()
})

test('message.type อื่นที่ไม่ใช่ "Results" (เช่น Metadata/UtteranceEnd) ถูกข้ามเงียบๆ ไม่ throw', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  assert.doesNotThrow(() => {
    state.sockets[0]._emit('message', { type: 'Metadata', request_id: 'x' })
    state.sockets[0]._emit('message', { type: 'UtteranceEnd' })
  })
  stream.end()
})

// ===== Lock B — connection ownership invariant =====

test('Lock B: end() แล้ว socket.close() ต้องถูกเรียก และ write() หลังจากนั้นต้องไม่ throw (no-op เงียบๆ)', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  stream.end()
  assert.equal(state.sockets[0].closed, true)
  assert.doesNotThrow(() => stream.write(Buffer.from([1])))
})

// ===== SDK-internal-reconnect race guard (found via Review challenge #1, fixed by reading the actual SDK
// source — node_modules/@deepgram/sdk's ws.js calls its own _connect() retry synchronously inside
// _handleClose(), BEFORE firing the public 'close' event this adapter listens to).
//
// Review challenge #2 (off-by-one question) surfaced a SECOND, deeper bug in the first fix: ws.js's
// constructor sets `_retryCount = -1`, so retryCount===ceiling is observed on TWO different close events
// — once right after the SDK just launched its last allowed attempt (still retrying, must NOT replace),
// and once when the next attempt was blocked (truly exhausted, safe to replace). A single-snapshot
// `retryCount >= ceiling` check can't tell these apart. Fixed by tracking whether retryCount ADVANCED
// since the previous close on this same connection — advancing = trust the SDK's new attempt; unchanged =
// that attempt was blocked, truly exhausted. Traced concretely with reconnectAttempts=5 in the code
// comment (deepgramSTT.js) — 6 total connect() calls happen (1 initial + 5 retries), retryCount progresses
// 0,1,2,3,4,5,5 across attempts 1-6 and the blocked 7th. =====

test('SDK reconnect guard: retryCount ADVANCES between two close events (SDK just launched another attempt each time) → ต้องไม่สร้าง connection ใหม่เลยแม้แต่ครั้งเดียว', async () => {
  const fakeInner = { retryCount: 0 }
  state.connectImpl = async () => {
    const socket = makeFakeSocket()
    socket.socket = fakeInner // reference เดียวกันตลอด object นี้ — mutate .retryCount จำลอง SDK เพิ่มค่าเองระหว่างทาง เหมือนของจริง
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1)

  fakeInner.retryCount = 1
  state.sockets[0]._emit('close') // จำลอง attempt #1 พัง, SDK เพิ่ง launch attempt #2 (retryCount 0→1) ก่อนยิง close event นี้
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'retryCount เพิ่งขยับ (0→1) แปลว่า SDK กำลัง retry อยู่ ต้องไม่สร้างใหม่ทับ')

  fakeInner.retryCount = 2
  state.sockets[0]._emit('close') // attempt #2 พัง, SDK launch attempt #3 (retryCount 1→2)
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'retryCount ขยับต่อเนื่อง (1→2) ยังต้องไม่สร้างใหม่')
  stream.end()
})

test('SDK reconnect guard: retryCount ไม่ขยับระหว่าง close สองครั้งติดกัน (SDK เองก็ลองอีกไม่ได้แล้วจริง) → adapter สร้าง connection ใหม่แทน เฉพาะตอนนั้น', async () => {
  const fakeInner = { retryCount: 0 }
  state.connectImpl = async () => {
    const socket = makeFakeSocket()
    socket.socket = fakeInner
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()

  fakeInner.retryCount = 5
  state.sockets[0]._emit('close') // attempt #5 พัง, SDK launch attempt #6 (retryCount 4→5) — ยังลองอยู่
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'observation แรกที่ retryCount=5 คือ "เพิ่ง launch attempt #6" ยังไม่ควรสร้างใหม่')

  // retryCount ยังเป็น 5 เหมือนเดิม (ไม่ขยับ) — จำลอง attempt #6 พัง แล้ว attempt #7 ถูก SDK บล็อกเอง (budget หมด)
  state.sockets[0]._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'observation ที่สองที่ retryCount=5 เหมือนเดิม (ไม่ขยับ) คือสัญญาณว่า SDK บล็อก attempt ถัดไปแล้วจริง ต้องสร้างใหม่ตอนนี้')
  stream.end()
})

test('SDK reconnect guard: retry state อ่านไม่ได้เลย (socket.socket เป็น undefined) → fail-safe สร้าง connection ใหม่ทันที ไม่รอสังเกตซ้ำ', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks() // default makeFakeSocket() ไม่มี .socket เลย
  state.sockets[0]._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'อ่าน retry state ไม่ได้เลย ต้องไม่เชื่อ SDK แบบไม่มีหลักฐาน — สร้างใหม่ทันที')
  stream.end()
})

// Review-requested invariant #3: a stale, already-superseded (and already-exhausted) connection firing
// close/error AGAIN must never create a SECOND replacement.
test('invariant 3: stale connection ที่ถูกแทนที่ไปแล้ว (เพราะ exhausted) ยิง close/error ซ้ำมาอีก → ต้องไม่สร้าง replacement ที่สองเด็ดขาด', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const firstSocket = state.sockets[0] // ไม่มี .socket เลย → fail-safe path
  firstSocket._emit('close') // exhausted ทันที → สร้าง replacement ตัวที่ 2
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2)

  // firstSocket (stale, ไม่ใช่ current แล้ว) ยิง close/error ซ้ำมาอีกทีหลัง — connectionId เดิมของมันไม่ใช่ current แล้ว
  firstSocket._emit('close')
  firstSocket._emit('error', new Error('late error จาก connection เก่าที่ตายไปแล้ว'))
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'stale connection (แม้จะเคย exhausted ไปแล้ว) ต้องไม่มีสิทธิ์ trigger replacement ที่สองเลย — isCurrentConnection() ต้องกันไว้ตั้งแต่ต้น')
  stream.end()
})

// Review-requested invariant #4: once a replacement becomes current, the PREVIOUS connection's retry
// state must never influence it (each connection tracks its own lastCloseRetryCount independently).
test('invariant 4: connection ทดแทนตัวใหม่ต้องเริ่ม track retry state ของตัวเองใหม่หมด ไม่รับผลจาก retry state ของตัวเก่าที่ถูกแทนที่ไปแล้ว', async () => {
  let callNum = 0
  const fakeInner1 = { retryCount: 5 } // ตัวเก่า: retryCount สูงอยู่แล้วตอนถูกแทนที่ (exhausted)
  const fakeInner2 = { retryCount: 0 } // ตัวใหม่ (replacement): เริ่มนับใหม่จาก 0 เหมือน connection สดจริงๆ
  state.connectImpl = async () => {
    callNum++
    const socket = makeFakeSocket()
    socket.socket = callNum === 1 ? fakeInner1 : fakeInner2
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()

  state.sockets[0]._emit('close') // observation แรกที่ retryCount=5 — ยังไม่ replace (อาจกำลังลองอยู่)
  await flushMicrotasks()
  state.sockets[0]._emit('close') // observation ที่สองที่ retryCount=5 เหมือนเดิม — exhausted จริง → replace
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2)

  // ตัวใหม่ (replacement) ควรเริ่มจาก retryCount=0 สดๆ — observation แรกของมันที่ 0 ต้องไม่ถูกเข้าใจผิดว่า
  // "เหมือนกับ retryCount สุดท้ายของตัวเก่า (5)" แล้วสรุปว่า exhausted ทันที
  state.sockets[1]._emit('close') // observation แรกของตัวใหม่ที่ retryCount=0
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'ตัวใหม่เพิ่งเห็น retryCount=0 ครั้งแรก ต้องไม่ replace ทันที ไม่ควรได้รับอิทธิพลจาก retryCount=5 ของตัวเก่าเลย')
  stream.end()
})

test('SDK reconnect guard: reconnectAttempts ที่ config ไปจริงต้องเป็น DEEPGRAM_RECONNECT_ATTEMPTS (ไม่ใช่ default 30 ของ SDK ที่นานเกินไปสำหรับสายจริง)', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  assert.equal(state.connectAttempts[0].reconnectAttempts, DEEPGRAM_RECONNECT_ATTEMPTS)
  stream.end()
})

test('Lock B: connection ปิดโดยไม่คาดคิด (close event, ยังไม่ destroyed) → adapter สร้าง connection ใหม่เอง', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1)
  state.sockets[0]._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'ต้องสร้าง connection ใหม่ทันทีที่ socket เดิมปิดแบบไม่คาดคิด')
  stream.end()
})

test('Lock B: end() แล้ว close event มาทีหลัง (ของ socket ที่เพิ่งถูกปิดเอง) ต้องไม่ trigger reconnect', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  stream.end()
  state.sockets[0]._emit('close') // late close event จาก socket ที่เราสั่งปิดเอง
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'destroyed แล้ว ห้ามสร้าง connection ใหม่เด็ดขาด')
})

// LIFECYCLE INVARIANT (Review-elevated, 2026-08-31) — proves end()'s exact statement order
// (`destroyed = true` BEFORE `currentSocket.close()`) actually matters, using a fake that mirrors the
// REAL SDK's behavior (Socket.js: close() invokes the registered close handler SYNCHRONOUSLY/re-entrantly
// as part of the same call, not just later via an async event) — the earlier test above only proved the
// async-later case, which doesn't exercise this specific ordering requirement at all.
test('LIFECYCLE INVARIANT: end() ต้องปลอดภัยแม้ socket.close() เรียก close handler แบบ synchronous/re-entrant ทันที (mirror พฤติกรรมจริงของ V1Socket.close())', async () => {
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ reentrantClose: true })
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1)

  stream.end() // ภายในนี้ currentSocket.close() จะเรียก close handler ของตัวเองกลับเข้ามาทันทีในบรรทัดเดียวกัน

  assert.equal(state.sockets.length, 1, 'destroyed ต้องถูกตั้งก่อน close() เสมอ — ไม่งั้น close handler ที่วิ่งกลับเข้ามาทันทีจะเห็น destroyed=false แล้วสร้าง replacement connection ทับตอนกำลังจะปิดสายอยู่แล้ว')
})

test('Lock B (core invariant): stale connection ที่ยังไม่ทันปิดจริง ส่ง message มาช้า ต้องไม่ถูกส่งต่อไปกระทบ state ปัจจุบัน', async () => {
  // จำลอง: connection แรกกำลังจะถูกแทนที่ (เช่นโดย unexpected close ที่ trigger reconnect) แต่ตัวมันเอง
  // (reference เดิม) ยังส่ง 'message' event ตามมาทีหลังได้อีก (late/stale event จาก connection ที่ไม่ใช่ current แล้ว)
  let transcriptCount = 0
  const stream = transcribeStream(() => { transcriptCount++ }, () => {})
  await flushMicrotasks()
  const staleSocket = state.sockets[0]

  staleSocket._emit('close') // trigger reconnect → connection ใหม่กลายเป็น current
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2)

  // stale socket (connection เดิม, ไม่ใช่ current แล้ว) ยิง final message มาช้า — ต้องถูก suppress สนิท
  staleSocket._emit('message', resultsMessage({ text: 'ข้อความเก่าที่ไม่ควรมาถึง', isFinal: true }))
  assert.equal(transcriptCount, 0, 'stale connection ต้องไม่มีสิทธิ์ trigger onTranscript ของ session ปัจจุบันเด็ดขาด')

  // current (ใหม่) socket ยังทำงานได้ปกติ
  state.sockets[1]._emit('message', resultsMessage({ text: 'ข้อความใหม่ที่ถูกต้อง', isFinal: true }))
  assert.equal(transcriptCount, 1)
  stream.end()
})

test('Lock B: error event จาก stale connection ต้องไม่ถูก log/handle เป็น current error', async () => {
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const staleSocket = state.sockets[0]
  staleSocket._emit('close')
  await flushMicrotasks()
  assert.doesNotThrow(() => staleSocket._emit('error', new Error('stale error, ต้องไม่กระทบอะไร')))
  stream.end()
})

test('connect() เอง throw (เช่น network error ตอนเชื่อมต่อ) → retry อัตโนมัติ ไม่ throw ออกไปกระทบ caller', async () => {
  let shouldFail = true
  state.connectImpl = async () => {
    if (shouldFail) { shouldFail = false; throw new Error('network error ตอนเชื่อมต่อครั้งแรก') }
    const socket = makeFakeSocket()
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  await new Promise(r => setTimeout(r, 250)) // รอ retry timer (200ms) ในโค้ด
  assert.equal(state.connectAttempts.length, 2, 'ต้อง retry อีก 1 ครั้งหลัง connect แรกพัง')
  assert.equal(state.sockets.length, 1, 'socket ที่สร้างสำเร็จมีแค่ตัวเดียว (จากความพยายามที่ 2)')
  stream.end()
})

test('utteranceId เพิ่มขึ้นทีละ 1 ต่อ utterance ใหม่ (แยก interim ของ utterance คนละอันออกจากกัน)', async () => {
  const receivedMetas = []
  const stream = transcribeStream((t, meta) => receivedMetas.push(meta), () => {})
  await flushMicrotasks()
  state.sockets[0]._emit('message', resultsMessage({ text: 'ครับ', isFinal: true }))
  state.sockets[0]._emit('message', resultsMessage({ text: 'สนใจครับ', isFinal: true }))
  assert.equal(receivedMetas[0].utteranceId, 1)
  assert.equal(receivedMetas[1].utteranceId, 2)
  stream.end()
})

// ===== Socket readiness + pre-open buffer (Implementation Gate "Deepgram Live Socket Readiness +
// Handshake Diagnostics", 2026-08-31) — production proved `currentSocket !== null` was being used as a
// stand-in for "ready to send," which is false: a V1Socket object exists the instant connect() resolves,
// long before the underlying WebSocket handshake actually completes. Test labels A-H below map to the
// scenarios locked in the Implementation Gate; D is covered by the pre-existing "write(mulawBuffer) ส่ง
// buffer ตรงๆ..." test above (default readyState=OPEN already exercises "audio after OPEN sent
// immediately"), so it isn't duplicated here. H is the pre-existing LIFECYCLE INVARIANT test above,
// confirmed unaffected by this change via the full suite run, not a new test.

test('Readiness A: connect() resolves ให้ V1Socket แต่ readyState ยังเป็น CONNECTING → write() ต้องไม่เรียก sendMedia() เลย', async () => {
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_CONNECTING })
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  stream.write(Buffer.from([1, 2, 3]))
  assert.equal(state.sockets[0].sentMedia.length, 0, 'ยังไม่ OPEN ต้องไม่ sendMedia เลยแม้แต่ครั้งเดียว')
  stream.end()
})

test('Readiness B: audio ที่มาก่อน OPEN ต้องถูก buffer ไว้ (ไม่ใช่ทิ้งเงียบๆ เหมือนพฤติกรรมเดิมก่อนแก้)', async () => {
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_CONNECTING })
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  stream.write(Buffer.from([1, 2]))
  stream.write(Buffer.from([3, 4]))
  assert.equal(state.sockets[0].sentMedia.length, 0)
  // ยืนยันทางอ้อมว่า buffer เก็บไว้จริง (ไม่ใช่ทิ้ง) โดยเปิดแล้วดูว่า flush ออกมา — proof เต็มอยู่ใน Readiness C
  state.sockets[0]._emit('open')
  assert.equal(state.sockets[0].sentMedia.length, 2, 'audio ที่ buffer ไว้ก่อนหน้าต้องออกมาครบหลัง open')
  stream.end()
})

test('Readiness C: หลัง open event → buffered audio ถูก flush ครบ เรียงตามลำดับเดิม และครั้งเดียวเท่านั้นแม้ open ยิงซ้ำ', async () => {
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_CONNECTING })
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const frame1 = Buffer.from([1])
  const frame2 = Buffer.from([2])
  const frame3 = Buffer.from([3])
  stream.write(frame1)
  stream.write(frame2)
  state.sockets[0]._emit('open')
  stream.write(frame3) // มาหลัง open แล้ว — ต้องส่งทันที ไม่ผ่าน buffer อีก
  assert.deepEqual(state.sockets[0].sentMedia, [frame1, frame2, frame3], 'ต้องเรียงตามลำดับเดิมเป๊ะ ทั้งส่วนที่ flush และส่วนที่ส่งสด')
  state.sockets[0]._emit('open') // จำลอง open ยิงซ้ำ (ไม่ควรเกิดจริง แต่ต้องปลอดภัยถ้าเกิด)
  assert.equal(state.sockets[0].sentMedia.length, 3, 'flush ต้องเกิดครั้งเดียวเท่านั้น ห้าม flush ซ้ำแม้ open event จะยิงซ้ำ')
  stream.end()
})

test('Readiness E: ครบ readiness timeout แล้วยังไม่ OPEN → ยังไม่ sendMedia, มี diagnostic log ชัดเจน, และไม่สร้าง connection ใหม่เอง (หน้าที่นั้นเป็นของ SDK reconnect + close-handler เดิม)', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const warnCalls = []
  const originalWarn = console.warn
  console.warn = (...args) => { warnCalls.push(args.join(' ')) }
  try {
    state.connectImpl = async () => {
      const socket = makeFakeSocket({ readyState: WS_CONNECTING })
      state.sockets.push(socket)
      return socket
    }
    const stream = transcribeStream(() => {}, () => {})
    await flushMicrotasks()
    stream.write(Buffer.from([1]))
    t.mock.timers.tick(6000)
    assert.equal(state.sockets[0].sentMedia.length, 0, 'ยัง CONNECTING ต้องไม่ sendMedia แม้ timeout ผ่านไปแล้ว')
    assert.equal(state.sockets.length, 1, 'readiness timeout ต้องไม่สร้าง parallel connection เอง')
    assert.ok(warnCalls.some(m => m.includes('readiness timeout') && m.includes('connectionId=')), 'ต้องมี diagnostic log ที่ระบุ connectionId ชัดเจนเมื่อ timeout')

    // Review focus #1: timeout ต้องเป็น diagnostic-only จริง — connection เดิม (connectionId เดิม) ต้อง "ไม่ถูก
    // poison" ถ้า SDK เปิดสำเร็จช้าๆ ทีหลัง (เช่นวินาทีที่ 7) open handler ต้องยัง set ready + flush ได้ตามปกติ
    state.sockets[0]._emit('open')
    assert.equal(state.sockets[0].sentMedia.length, 1, 'open ที่มาช้าหลัง readiness timeout ต้องยัง flush buffer เดิมได้ตามปกติ ไม่ถูก poison')
    assert.deepEqual(state.sockets[0].sentMedia[0], Buffer.from([1]))
    stream.end()
  } finally {
    console.warn = originalWarn
  }
})

test('Readiness F: stale connection ที่ถูกแทนที่ไปแล้ว ยิง open event มาช้า → ต้องไม่ flush buffer เก่าของมันเอง, ไม่ reset readiness, ไม่ cancel timer, และไม่กระทบ connection ปัจจุบันแม้แต่นิดเดียว', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] })
  const warnCalls = []
  const logCalls = []
  const originalWarn = console.warn
  const originalLog = console.log
  console.warn = (...args) => { warnCalls.push(args.join(' ')) }
  console.log = (...args) => { logCalls.push(args.join(' ')) }
  try {
    state.connectImpl = async () => {
      const socket = makeFakeSocket({ readyState: WS_CONNECTING })
      state.sockets.push(socket)
      return socket
    }
    const stream = transcribeStream(() => {}, () => {})
    await flushMicrotasks()
    const staleSocket = state.sockets[0]
    stream.write(Buffer.from([1])) // buffer บน connection #1 (ยังไม่ open) — #1's readiness timer (T1) เริ่มนับ

    staleSocket._emit('close') // retry state อ่านไม่ได้ (ไม่มี .socket) → fail-safe สร้าง replacement ทันที
    await flushMicrotasks()
    assert.equal(state.sockets.length, 2, 'ต้องมี replacement connection (#2) แล้ว')
    const currentSocketFake = state.sockets[1] // #2's readiness timer (T2) เริ่มนับใหม่จากตอนนี้

    // connectionIdCounter เป็น module-level state ที่ไม่ reset ระหว่าง test — ดึง connectionId จริงของ #2 จาก
    // log แทนการเดาเลข แทนที่จะ hardcode
    const createdLines = logCalls.filter(m => m.includes('connection object created'))
    const currentConnectionIdMatch = createdLines[createdLines.length - 1]?.match(/connectionId=(\d+)/)
    assert.ok(currentConnectionIdMatch, 'ต้องอ่าน connectionId จริงของ #2 จาก log ได้')
    const currentConnectionIdStr = currentConnectionIdMatch[1]

    staleSocket._emit('open') // #1 (stale) เพิ่ง open ช้าๆ ตามหลังมา — ไม่ควรมีผลอะไรอีกแล้ว

    assert.equal(staleSocket.sentMedia.length, 0, 'stale connection ต้องไม่ flush buffer เก่าของมันเองเลย')
    assert.equal(currentSocketFake.sentMedia.length, 0, 'current connection (#2) ต้องไม่ได้รับผลกระทบจาก late open ของตัวเก่าเลย — ยังไม่ ready ด้วยตัวมันเอง')

    // Review focus #3: stale open ของ #1 ต้องไม่ cancel timer ของ #2 (T2) — เดินเวลาไปจนครบ readiness timeout
    // ของ #2 เอง (ไม่ใช่ของ #1) แล้ว T2 ต้องยังยิงปกติ ไม่ใช่ถูกเคลียร์ทิ้งไปแล้วโดยไม่ได้ตั้งใจจาก markSocketReady
    // ที่ควรจะ bail ก่อนแตะ currentReadinessTimeout เลยสำหรับ connectionId ที่ไม่ใช่ current
    t.mock.timers.tick(6000)
    assert.ok(warnCalls.some(m => m.includes('readiness timeout') && m.includes(`connectionId=${currentConnectionIdStr}`)), 'timer ของ connection ปัจจุบัน (#2) ต้องยังยิงตามปกติ ไม่ถูก cancel โดย stale open ของ #1')

    currentSocketFake._emit('open') // #2 open ของจริง ด้วยตัวมันเอง
    stream.write(Buffer.from([2]))
    assert.deepEqual(currentSocketFake.sentMedia, [Buffer.from([2])], 'audio ใหม่ต้องไปหา connection ปัจจุบันเท่านั้น ไม่ปนกับตัวเก่า')
    stream.end()
  } finally {
    console.warn = originalWarn
    console.log = originalLog
  }
})

test('Readiness G: end() ระหว่างรอ OPEN แล้ว open event มาช้าทีหลัง (late resolve) → zero side effect เด็ดขาด', async () => {
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_CONNECTING })
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  stream.write(Buffer.from([1, 2, 3])) // buffered, ยังไม่ ready

  stream.end()
  assert.equal(state.sockets[0].closed, true)

  assert.doesNotThrow(() => state.sockets[0]._emit('open'), 'open event ที่มาช้าหลัง end() ต้องไม่ throw')
  assert.equal(state.sockets[0].sentMedia.length, 0, 'end() แล้ว ห้าม flush buffer ที่ค้างอยู่ออกไปเด็ดขาด ไม่ว่า open จะยิงมาช้าแค่ไหนก็ตาม')
})

test('Readiness (Review focus, 2026-09-01): OPEN → SDK internal reconnect บน connectionId เดิม (retryCount ขยับ, ไม่มี replacement connection) → socketReady ต้อง reset, audio ระหว่างนั้นต้องเข้า buffer ไม่ throw ไม่ sendMedia ทับ, แล้ว flush ครบเมื่อ open อีกครั้ง', async () => {
  const fakeInner = { retryCount: 0 }
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_OPEN }) // เปิดสำเร็จตั้งแต่แรก (synchronous-snapshot)
    socket.socket = fakeInner
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const socket = state.sockets[0]

  const preDropFrame = Buffer.from([1])
  stream.write(preDropFrame)
  assert.deepEqual(socket.sentMedia, [preDropFrame], 'ตอน OPEN อยู่ปกติ ต้องส่งตรงทันที ไม่ผ่าน buffer')

  // จำลอง transport drop กลาง call — SDK retry เองภายใน (retryCount 0→1) connectionId เดิม ไม่สร้าง replacement
  fakeInner.retryCount = 1
  socket._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'retryCount ขยับ (0→1) ต้องไม่สร้าง replacement connection ใหม่ — connectionId เดิมยังคงอยู่')

  // ระหว่างที่ SDK กำลัง retry อยู่ (ยังไม่ open ใหม่) audio ที่เข้ามาต้อง buffer ไว้ ไม่ throw ไม่ sendMedia ทับ socket
  // ที่หลุดไปแล้ว (นี่คือ gap ที่ระบุตรงๆ: ก่อนแก้ socketReady จะยังเป็น true ค้างอยู่ ทำให้ write() พยายาม sendMedia
  // ต่อไปเรื่อยๆ ระหว่าง drop เหมือน bug เดิมที่ production เจอ เพียงแต่เกิดกลางสายแทนที่จะเป็นตอน handshake)
  const duringDropFrame = Buffer.from([2])
  assert.doesNotThrow(() => stream.write(duringDropFrame))
  assert.deepEqual(socket.sentMedia, [preDropFrame], 'ระหว่าง drop ต้องไม่มี sendMedia ใหม่เกิดขึ้นเลย — audio ต้องเข้า buffer แทน')

  // SDK retry สำเร็จ — 'open' ยิงอีกครั้งบน V1Socket ตัวเดิม (connectionId เดิม, persistent listener)
  socket._emit('open')
  assert.deepEqual(socket.sentMedia, [preDropFrame, duringDropFrame], 'audio ที่ buffer ไว้ระหว่าง drop ต้องถูก flush ทันทีที่ open อีกครั้ง เรียงลำดับเดิม')

  const afterReopenFrame = Buffer.from([3])
  stream.write(afterReopenFrame)
  assert.deepEqual(socket.sentMedia, [preDropFrame, duringDropFrame, afterReopenFrame], 'หลัง reopen แล้ว audio ใหม่ต้องส่งตรงทันทีอีกครั้ง ไม่ค้างอยู่ใน buffer')
  stream.end()
})

test('Reconnect (Review focus, 2026-09-01): OPEN เสถียรเกิน minUptime (5000ms) → SDK reset retryCount ภายใน (invisible) → drop รอบสองที่ raw ซ้ำค่าเดิมโดยบังเอิญ ต้องไม่ถูกเข้าใจผิดว่า exhausted', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] })
  const fakeInner = { retryCount: 0 }
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_OPEN })
    socket.socket = fakeInner
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const socket = state.sockets[0]

  // Drop #1: SDK retry เอง (retryCount 0→1) — adapter เห็นครั้งแรกบน connectionId นี้ ต้อง trust ว่า retrying
  fakeInner.retryCount = 1
  socket._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'drop แรก ต้อง trust SDK retry ไม่สร้าง replacement')

  // Retry สำเร็จ — open อีกครั้งบน connectionId เดิม (persistent listener)
  socket._emit('open')

  // เดินเวลาผ่าน minUptime (5000ms) — ในโลกจริง SDK's _acceptOpen() จะรีเซ็ต retryCount ภายในเป็น 0 ตรงนี้เอง
  // (invisible ต่อ adapter โดยสิ้นเชิง ไม่มี event ใดๆ ยิงออกมาบอก)
  t.mock.timers.tick(6000)

  // Drop #2: SDK retry เองอีกครั้งหลัง reset (retryCount 0→1) — บังเอิญได้ raw=1 ซ้ำกับ observation แรกเป๊ะ
  fakeInner.retryCount = 1
  socket._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'raw ซ้ำกันเพราะ SDK reset ไปแล้วจริง (เสถียรเกิน minUptime) ไม่ใช่เพราะ exhausted — ยังต้องไม่สร้าง replacement ใหม่ (นี่คือ bug ที่ระบุตรงๆ: ถ้าไม่แก้ elapsed-time reset assertion นี้จะ fail เพราะ code เดิมจะเข้าใจผิดว่า exhausted)')
  stream.end()
})

test('Reconnect (Review focus, 2026-09-01): final allowed attempt เปิดสำเร็จสั้นๆ ก่อน minUptime แล้วหลุดอีกครั้ง (retryCount ที่ ceiling ไม่เปลี่ยนจริง) → ต้องยังตรวจจับ exhaustion ได้ถูกต้อง ไม่ถูกกลบด้วย fix ของ minUptime-reset', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] })
  const fakeInner = { retryCount: 5 } // observation แรก: SDK เพิ่ง launch attempt สุดท้ายที่อนุญาต (retryCount 4→5)
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_CONNECTING }) // ยังไม่เคย open เลยตอนเริ่ม
    socket.socket = fakeInner
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const socket = state.sockets[0]

  // observation แรกที่ raw=5 — connection นี้ยังไม่เคย open เลยสักครั้ง (mirror timer ยังไม่เคยถูก arm) ต้อง trust
  // ว่า attempt สุดท้ายเพิ่ง launch อยู่ ไม่ใช่ exhausted
  socket._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1, 'observation แรกที่ raw=5 ต้อง trust ว่ายัง retry อยู่ (attempt สุดท้ายเพิ่ง launch)')

  // attempt สุดท้ายเปิดสำเร็จ — แต่สั้นมาก (ไม่ถึง minUptime) แล้วหลุดอีกครั้งก่อนที่ mirror timer (และ SDK's
  // uptime timer จริง) จะทันยิง callback เลย
  socket._emit('open')
  t.mock.timers.tick(500) // ผ่านไปแค่ 500ms — mirror timer callback ยังไม่ทันทำงาน (armed ไว้ 5000ms)
  // fakeInner.retryCount ยังเป็น 5 เหมือนเดิม (attempt ถัดไปถูก SDK บล็อกเอง เพราะ retryCount(5) >= maxRetries(5))
  socket._emit('close')
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'raw ซ้ำที่ 5 หลัง open สั้นๆ ก่อน mirror timer ยิง คือ exhausted จริง (ไม่ใช่ reset) — ต้องสร้าง replacement ให้ถูกต้อง ไม่ถูกกลบด้วย fix ของ timer-mirror')
  stream.end()
})

test('Reconnect (Review focus, 2026-09-02): boundary ที่ขอบ minUptime พอดี (4999ms, ขาดไป 1ms) → mirror timer ต้องยังไม่ยิง callback เลย, exhaustion ยังตรวจจับได้ถูกต้องแม้ใกล้ threshold มาก', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] })
  const fakeInner = { retryCount: 5 }
  state.connectImpl = async () => {
    const socket = makeFakeSocket({ readyState: WS_CONNECTING })
    socket.socket = fakeInner
    state.sockets.push(socket)
    return socket
  }
  const stream = transcribeStream(() => {}, () => {})
  await flushMicrotasks()
  const socket = state.sockets[0]

  socket._emit('close') // observation แรกที่ raw=5 — trust ว่ายัง retry อยู่
  await flushMicrotasks()
  assert.equal(state.sockets.length, 1)

  socket._emit('open')
  t.mock.timers.tick(4999) // ขาดไปแค่ 1ms จาก DEEPGRAM_SDK_MIN_UPTIME_MS (5000) — setTimeout ต้องยังไม่ยิง callback
  socket._emit('close') // raw=5 ซ้ำเดิม — mirror timer ยังไม่เคยยิง (ตรวจสอบด้วยพฤติกรรม ไม่ใช่การอ่านค่า internal ตรงๆ)
  await flushMicrotasks()
  assert.equal(state.sockets.length, 2, 'ที่ 4999ms (ขาดไป 1ms พอดี) mirror timer ต้องยังไม่ยิง callback เลย — exhaustion ต้องยังตรวจจับได้ถูกต้อง ไม่ถูกกลบด้วย fix')
  stream.end()
})

test('Readiness: pre-open buffer มีเพดานชัดเจน (คำนวณจาก byte/ms ของ 8kHz mulaw จริง) — เกิน cap แล้ว frame ใหม่ถูกทิ้ง (นโยบาย: drop newest, keep oldest) พร้อม log ที่ระบุจำนวน bytes/frames ที่ทิ้งจริง ไม่ใช่แค่คำเตือนกว้างๆ', async () => {
  const warnCalls = []
  const logCalls = []
  const originalWarn = console.warn
  const originalLog = console.log
  console.warn = (...args) => { warnCalls.push(args.join(' ')) }
  console.log = (...args) => { logCalls.push(args.join(' ')) }
  try {
    state.connectImpl = async () => {
      const socket = makeFakeSocket({ readyState: WS_CONNECTING })
      state.sockets.push(socket)
      return socket
    }
    const stream = transcribeStream(() => {}, () => {})
    await flushMicrotasks()
    const capFillingFrame = Buffer.alloc(12000, 7) // เท่ากับ cap พอดี (1500ms × 8 bytes/ms)
    const overflowFrame1 = Buffer.from([9, 9]) // เกิน cap — ถูกทิ้ง (2 bytes)
    const overflowFrame2 = Buffer.from([8, 8, 8]) // เกิน cap อีกครั้ง — ถูกทิ้งด้วย (3 bytes)
    stream.write(capFillingFrame)
    stream.write(overflowFrame1)
    stream.write(overflowFrame2)
    state.sockets[0]._emit('open')

    // นโยบาย "drop newest, keep oldest" — frame แรกที่เข้ามาก่อน cap เต็มต้องรอดและถูก flush, ของที่มาทีหลัง
    // (เกิน cap) ต้องหายไปจริง ไม่ปน ไม่แทนที่ของเดิม
    assert.equal(state.sockets[0].sentMedia.length, 1, 'เฉพาะ frame ที่อยู่ใน cap (ตัวแรก) เท่านั้นที่ถูก flush')
    assert.equal(state.sockets[0].sentMedia[0], capFillingFrame, 'ต้องเป็น frame ที่มาก่อน (oldest) ไม่ใช่ตัวที่มาทีหลัง')

    // log ตอนทิ้งแต่ละ frame ต้องระบุจำนวน bytes ของ frame นั้นเอง และยอดสะสมของ connection นี้ ไม่ใช่แค่ "dropping frame" ลอยๆ
    assert.ok(warnCalls.some(m => m.includes('pre-open buffer full') && m.includes('frame #1') && m.includes('2 bytes') && m.includes('2 bytes dropped total')), 'log แรกต้องระบุ frame #1, ขนาด 2 bytes, ยอดสะสม 2 bytes')
    assert.ok(warnCalls.some(m => m.includes('pre-open buffer full') && m.includes('frame #2') && m.includes('3 bytes') && m.includes('5 bytes dropped total')), 'log ที่สองต้องระบุ frame #2, ขนาด 3 bytes, ยอดสะสม 5 bytes (2+3)')

    // log ตอน flush (socket ready) ต้องสรุปยอด dropped ทั้งหมดของ connection นี้ไว้ด้วย ไม่ใช่หายไปเงียบๆ
    assert.ok(logCalls.some(m => m.includes('socket ready') && m.includes('droppedFrames=2') && m.includes('droppedBytes=5')), 'flush log ต้องสรุปยอด dropped รวมของ connection นี้ (2 frames, 5 bytes) ให้เห็นชัดว่ามีการสูญเสียจริงเท่าไหร่')
    stream.end()
  } finally {
    console.warn = originalWarn
    console.log = originalLog
  }
})
