// Dual STT Provider (design frozen 2026-08-31) — sttRouter.js tests. Stubs googleSTT.js and
// deepgramSTT.js via require.cache injection (same pattern used throughout this repo, e.g.
// test/gemini.test.js) so this exercises ONLY the router's own dispatch/annotation logic, never a real
// Google or Deepgram connection.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

const state = { googleCalls: [], deepgramCalls: [] }

const googleSttPath = require.resolve('../src/services/googleSTT')
require.cache[googleSttPath] = {
  id: googleSttPath, filename: googleSttPath, loaded: true,
  exports: {
    transcribeStream: (onTranscript, onInterim, options) => {
      state.googleCalls.push({ onTranscript, onInterim, options })
      return { write: () => {}, end: () => {}, _fake: 'google' }
    },
  },
}

const deepgramSttPath = require.resolve('../src/services/deepgramSTT')
require.cache[deepgramSttPath] = {
  id: deepgramSttPath, filename: deepgramSttPath, loaded: true,
  exports: {
    transcribeStream: (onTranscript, onInterim, options) => {
      state.deepgramCalls.push({ onTranscript, onInterim, options })
      return { write: () => {}, end: () => {}, _fake: 'deepgram' }
    },
    DEEPGRAM_MODEL: 'nova-3',
  },
}

const { resolveSttProvider, createTranscribeStream, GOOGLE_MODEL, DEEPGRAM_MODEL } = require('../src/services/sttRouter')

beforeEach(() => {
  state.googleCalls = []
  state.deepgramCalls = []
})

// ===== resolveSttProvider — mirrors resolveExplicitProvider's own test coverage in spirit =====

test('resolveSttProvider: blank/missing stt_provider → null (ไม่ default เป็น google เอง — ผู้เรียกต้องปล่อย routing เดิมไว้)', () => {
  assert.equal(resolveSttProvider({}), null)
  assert.equal(resolveSttProvider({ stt_provider: '' }), null)
  assert.equal(resolveSttProvider(null), null)
})

test('resolveSttProvider: "google" (case/whitespace-insensitive) → { provider: "google", model: GOOGLE_MODEL }', () => {
  assert.deepEqual(resolveSttProvider({ stt_provider: 'google' }), { provider: 'google', model: GOOGLE_MODEL })
  assert.deepEqual(resolveSttProvider({ stt_provider: ' Google ' }), { provider: 'google', model: GOOGLE_MODEL })
})

test('resolveSttProvider: "deepgram" (case/whitespace-insensitive) → { provider: "deepgram", model: DEEPGRAM_MODEL }', () => {
  assert.deepEqual(resolveSttProvider({ stt_provider: 'deepgram' }), { provider: 'deepgram', model: DEEPGRAM_MODEL })
  assert.deepEqual(resolveSttProvider({ stt_provider: 'DEEPGRAM' }), { provider: 'deepgram', model: DEEPGRAM_MODEL })
})

test('resolveSttProvider: ค่าที่ไม่รู้จัก (typo) → null เหมือน blank (ไม่ throw, ไม่ error — validation อยู่ที่ campaign.js แยกต่างหาก)', () => {
  assert.equal(resolveSttProvider({ stt_provider: 'deepgrm' }), null)
})

// ===== createTranscribeStream — dispatch + annotation =====

test('createTranscribeStream: session.sttProvider="deepgram" → เรียก deepgramSTT ไม่เรียก googleSTT เลย', () => {
  const session = { sttProvider: 'deepgram', sttModel: 'nova-3' }
  const stream = createTranscribeStream(session, () => {}, () => {}, {})
  assert.equal(state.deepgramCalls.length, 1)
  assert.equal(state.googleCalls.length, 0)
  assert.equal(stream._fake, 'deepgram')
})

test('createTranscribeStream: session.sttProvider="google" → เรียก googleSTT ไม่เรียก deepgramSTT เลย', () => {
  const session = { sttProvider: 'google', sttModel: GOOGLE_MODEL }
  const stream = createTranscribeStream(session, () => {}, () => {}, {})
  assert.equal(state.googleCalls.length, 1)
  assert.equal(state.deepgramCalls.length, 0)
  assert.equal(stream._fake, 'google')
})

test('createTranscribeStream: session.sttProvider=null/undefined (default/blank) → เรียก googleSTT (default routing เดิม)', () => {
  const session = {}
  createTranscribeStream(session, () => {}, () => {}, {})
  assert.equal(state.googleCalls.length, 1)
  assert.equal(state.deepgramCalls.length, 0)
})

test('createTranscribeStream: options (interimFinalizeMs ฯลฯ) ถูกส่งผ่านไปยัง provider ตรงๆ ไม่ถูกแก้', () => {
  const session = { sttProvider: 'google' }
  const options = { interimFinalizeMs: 900, maxAlternatives: 3 }
  createTranscribeStream(session, () => {}, () => {}, options)
  assert.equal(state.googleCalls[0].options, options)
})

test('createTranscribeStream: onInterim ถูกส่งผ่านตรงๆ ไม่ถูกห่อ (คงเป็น string callback เดิม — Lock 2)', () => {
  const session = { sttProvider: 'deepgram' }
  const onInterim = (text) => text
  createTranscribeStream(session, () => {}, onInterim, {})
  assert.equal(state.deepgramCalls[0].onInterim, onInterim)
})

test('createTranscribeStream: sttMeta ที่ onTranscript ได้รับ ต้องมี provider/model แปะเพิ่มเข้ามา โดยไม่ทำลาย field เดิมของ provider นั้นเลย', () => {
  const session = { sttProvider: 'google', sttModel: 'latest_short' }
  let received = null
  createTranscribeStream(session, (text, sttMeta) => { received = sttMeta }, () => {}, {})
  // จำลอง googleSTT.js เรียก onTranscript กลับมาจริง ด้วย sttMeta แบบ Google native (มี field เฉพาะของ Google)
  state.googleCalls[0].onTranscript('ทดสอบ', { source: 'GOOGLE_FINAL', streamId: 1, coldMutePackets: 0, regressionCount: 0 })
  assert.equal(received.provider, 'google')
  assert.equal(received.model, 'latest_short')
  // field เดิมของ Google ต้องยังอยู่ครบ ไม่ถูกย้ายเข้า providerMeta หรือหายไป (sttRouter ไม่ restructure shape ของ Google)
  assert.equal(received.source, 'GOOGLE_FINAL')
  assert.equal(received.streamId, 1)
  assert.equal(received.coldMutePackets, 0)
  assert.equal(received.regressionCount, 0)
})

test('createTranscribeStream: sttMeta undefined (caller เก่าที่ไม่ส่ง metadata) ต้องยังคง undefined ไม่ถูก fabricate เป็น {provider,model} (regression: `{...undefined, x}` = `{x}` ใน JS, truthy — เคยพังเทส backward-compat ของ [STT_DIAG])', () => {
  const session = { sttProvider: 'google' }
  let received = 'not-called'
  createTranscribeStream(session, (text, sttMeta) => { received = sttMeta }, () => {}, {})
  state.googleCalls[0].onTranscript('ทดสอบ') // จำลอง caller เก่าที่ไม่ส่ง sttMeta เลย (ไม่ใช่แค่ undefined ตรงๆ)
  assert.equal(received, undefined, 'ต้องยัง undefined ไม่ถูกห่อเป็น object')
})

test('createTranscribeStream: model fallback ถ้า session.sttModel ไม่ได้ตั้งไว้ (ใช้ GOOGLE_MODEL/DEEPGRAM_MODEL default ตาม provider)', () => {
  let receivedGoogle = null, receivedDeepgram = null
  createTranscribeStream({ sttProvider: 'google' }, (t, m) => { receivedGoogle = m }, () => {}, {})
  state.googleCalls[0].onTranscript('x', {})
  assert.equal(receivedGoogle.model, GOOGLE_MODEL)

  createTranscribeStream({ sttProvider: 'deepgram' }, (t, m) => { receivedDeepgram = m }, () => {}, {})
  state.deepgramCalls[0].onTranscript('x', {})
  assert.equal(receivedDeepgram.model, DEEPGRAM_MODEL)
})
