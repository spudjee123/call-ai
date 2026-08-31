// Dual STT Provider (design frozen 2026-08-31) — deepgramSTT.js tests. Stubs @deepgram/sdk via
// require.cache injection (same pattern as test/gemini.test.js's @google/genai stub) so this exercises
// the real adapter logic (connection-ownership invariant, message parsing, mulaw pass-through) against a
// fake V1Socket, never a real Deepgram connection.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

const state = { connectAttempts: [], sockets: [], connectImpl: null }

function makeFakeSocket({ retryCount, reentrantClose } = {}) {
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
