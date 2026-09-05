// Checkpoint C6a — harness สำหรับทดสอบ registerWebSocket() ใน audioStream.js โดยตรง ไม่ผ่าน Fastify/WS จริง
// เลย (ไม่มี real network/port) — เรียก handler ภายในตรงๆ ด้วย fake connection object พร้อม stub ทุก external
// dependency (Claude/TTS/STT/rolloutConfig) ผ่าน require.cache — pattern เดียวกับที่ใช้ทั้ง repo มาตลอด
//
// ไฟล์นี้ไม่ใช่ test file เอง (ไม่มี test() เรียกตรงนี้เลย) แต่ยังอยู่ใต้ test/ ซึ่ง Node test runner จะ auto-discover
// และ require เฉยๆ ด้วย — module-level ของไฟล์นี้จึงต้องไม่มี side effect ใดๆ ทั้งสิ้น (ทุกอย่างอยู่ใต้ฟังก์ชันที่ export
// ถูกเรียกจากเทสจริงเท่านั้น) เพื่อไม่ให้ปลอดภัยแม้ถูก require เฉยๆ โดยไม่มีใครเรียกอะไรต่อจากมันเลย
const EventEmitter = require('events')

let state = null
let audioStreamModule = null
let cachedHandler = null

// stub dependencies ครั้งเดียว (idempotent — เรียกซ้ำได้ปลอดภัย) แล้ว require audioStream.js จริงหลังจาก stub
// เข้าที่แล้วเท่านั้น เพราะ audioStream.js อ่าน rolloutConfig ตอน module load (rolloutConfig.start() top-level)
function ensureStubbed() {
  if (audioStreamModule) return

  state = {
    claudeStreamImpl: async function* () { yield 'default legacy response.' }, // askClaudeStream(session, isGreeting, signal)
    claudeStreamChunkedImpl: async function* () {}, // askClaudeStreamChunked(session, signal, onControl)
    askClaudeImpl: async () => 'สวัสดีค่ะ',
    // L2a — askClaudeObservedFullResponse(session, signal, onMilestone) ที่ fresh legacy call site ใช้จริงตอนนี้
    // (แทน askClaudeStream() เฉพาะจุดนั้น) ค่า default เป็น null: ให้ stub ด้านล่าง delegate ไปที่ claudeStreamImpl
    // ตัวเดียวกับที่เทสเดิมทั้งหมดใช้คุม legacy fresh-call behavior อยู่แล้ว (ยิง synthetic milestone ประกอบไปด้วย
    // เทสเดิมไม่สนใจ field พวกนี้จึงไม่กระทบ) — เทสที่ต้องการคุม milestone/behavior ของ L2a โดยเฉพาะค่อย set
    // claudeObservedImpl ตรงๆ แทน
    claudeObservedImpl: null,
    // L2b — askClaudeConditionalStream(session, signal, onMilestone) ค่า default เป็น null: stub ด้านล่าง
    // delegate ไปที่ claudeStreamImpl เหมือนกัน ยิง milestone ครบชุด (รวม finalText/endCallRequested/mode) ตาม
    // contract จริง แต่เป็น SINGLE_SHOT เสมอ (chunk เดียว) — เทสที่ต้องการคุม CHUNKED mode/milestone ละเอียดค่อย
    // set claudeConditionalImpl ตรงๆ แทน
    claudeConditionalImpl: null,
    // Dual Conversation Provider A/B — askGeminiConditionalStream(session, signal, onMilestone) ค่า default
    // เป็น null: stub ด้านล่าง delegate ไปที่ claudeStreamImpl เหมือนกัน (SINGLE_SHOT เสมอ) ยิง milestone
    // contract เดียวกับ claudeConditionalImpl's default ทุกประการ ยกเว้นไม่มี mode 'CHUNKED' — เทสที่ต้องการคุม
    // Gemini behavior/milestone ละเอียดกว่านี้ค่อย set geminiConditionalImpl ตรงๆ แทน
    geminiConditionalImpl: null,
    ttsImpl: async function* () { yield Buffer.from('audio') }, // synthesizeSpeechStream(text, voiceId, signal)
    rolloutPercent: 0,
    // L2a production exposure gate — default fail-closed {percent:0, campaignId:null} เหมือน production cold
    // start เป๊ะ (ดู rolloutConfig.js cachedObservedConfig) เทสที่ต้องการ observed=true ต้อง set ค่าทั้งคู่เอง
    legacyObservedConfig: { percent: 0, campaignId: null },
    // L2b production exposure gate — default fail-closed เหมือนกันเป๊ะ
    legacyEarlyTtsConfig: { percent: 0, campaignId: null },
    // STT-A2 diagnostic gate — default fail-closed เหมือนกันเป๊ะ
    sttA2Config: { percent: 0, campaignId: null },
    // A2.1 Shadow gate — default fail-closed เหมือนกันเป๊ะ, independent จาก sttA2Config เอง (คนละ Sheet keys)
    sttA2ShadowConfig: { percent: 0, campaignId: null },
    // Opening Hello Guard — default fail-closed เหมือนกันเป๊ะ, independent จากทุก gate อื่น (คนละ Sheet keys)
    openingHelloGuardConfig: { percent: 0, campaignId: null },
    lastSttCallbacks: null, // { onTranscript, onInterim } — set สดทุกครั้งที่มี connection ใหม่เปิด sttStream
  }

  const claudePath = require.resolve('../src/services/claude')
  require.cache[claudePath] = {
    id: claudePath, filename: claudePath, loaded: true,
    exports: {
      askClaude: (...args) => state.askClaudeImpl(...args),
      askClaudeStream: (session, isGreeting, signal) => state.claudeStreamImpl(session, isGreeting, signal),
      askClaudeStreamChunked: (session, signal, onControl) => state.claudeStreamChunkedImpl(session, signal, onControl),
      askClaudeObservedFullResponse: (session, signal, onMilestone) => {
        if (state.claudeObservedImpl) return state.claudeObservedImpl(session, signal, onMilestone)
        return (async function* () {
          onMilestone?.('requestAt', Date.now())
          let first = true
          for await (const chunk of state.claudeStreamImpl(session, false, signal)) {
            if (first) { onMilestone?.('firstDeltaAt', Date.now()); first = false }
            yield chunk
          }
          // mirror guard เดียวกับ askClaudeObservedFullResponse() จริงที่ claude.js (ก่อน commit gate เจอ gap:
          // stub เดิมไม่มี guard นี้ ทำให้ turn ที่ barge-in/abort ระหว่างรอ state.claudeStreamImpl ค้างอยู่ กลับยิง
          // fullAt ทั้งที่ signal อาจ abort ไปแล้วก่อนหน้า — ไม่ตรงกับพฤติกรรมจริงที่มี explicit signal.aborted check
          // ก่อน record fullAt เสมอ) ต้อง mirror ให้ตรงเพื่อให้เทสที่ผ่าน stub นี้สะท้อนพฤติกรรมจริงได้ถูกต้อง
          if (signal?.aborted) return
          onMilestone?.('fullAt', Date.now())
        })()
      },
      // L2b — default stub เป็น SINGLE_SHOT เสมอ (chunk เดียว, delegate ไป claudeStreamImpl เหมือน L2a's stub)
      // ยิง milestone ครบตาม contract จริงของ askClaudeConditionalStream() (requestAt/firstDeltaAt/fullAt/mode/
      // finalText/endCallRequested) — เทสที่ต้องการ CHUNKED mode หรือคุม milestone ละเอียดกว่านี้ค่อย set
      // claudeConditionalImpl ตรงๆ แทน
      askClaudeConditionalStream: (session, signal, onMilestone) => {
        if (state.claudeConditionalImpl) return state.claudeConditionalImpl(session, signal, onMilestone)
        return (async function* () {
          onMilestone?.('requestAt', Date.now())
          let text = ''
          let first = true
          for await (const chunk of state.claudeStreamImpl(session, false, signal)) {
            if (first) { onMilestone?.('firstDeltaAt', Date.now()); first = false }
            text += (text ? ' ' : '') + chunk
          }
          if (signal?.aborted) return
          onMilestone?.('fullAt', Date.now())
          onMilestone?.('mode', 'SINGLE_SHOT')
          const finalText = text.replace(/\[END_CALL\]/g, '').trim()
          onMilestone?.('finalText', finalText)
          onMilestone?.('endCallRequested', text.includes('[END_CALL]'))
          if (finalText.length >= 3) yield finalText
        })()
      },
      // Dual Conversation Provider A/B (design locked) — real claude.js exports these for gemini.js to reuse
      // (prompt parity requirement); the stub needs the same two exports so conversationAI.js's require('./
      // claude') resolves them the same way in tests as in production, or gemini.js/conversationAI.js would
      // throw calling undefined(). Simplified (real templating/instructions stripped) — tests here only need
      // A value, never the exact production wording.
      buildSystemPrompt: (campaignPrompt, customerName) => `${campaignPrompt}\n\nชื่อลูกค้า: ${customerName}`,
      MAX_HISTORY: 20,
    },
  }

  // Dual Conversation Provider A/B (design locked) — stubbed the same way as claude.js above, own
  // geminiConditionalImpl hook so a test can control Gemini's streamed response/milestones independently of
  // Claude's. audioStream.js never imports this module directly — conversationAI.js's askConversationConditional
  // Stream() does, dispatching on session.llmProvider — so this stub also proves the router picks the right one.
  const geminiPath = require.resolve('../src/services/gemini')
  require.cache[geminiPath] = {
    id: geminiPath, filename: geminiPath, loaded: true,
    exports: {
      GEMINI_MODEL: 'gemini-3.7-flash',
      askGeminiConditionalStream: (session, signal, onMilestone) => {
        if (state.geminiConditionalImpl) return state.geminiConditionalImpl(session, signal, onMilestone)
        return (async function* () {
          onMilestone?.('requestAt', Date.now())
          let text = ''
          let first = true
          for await (const chunk of state.claudeStreamImpl(session, false, signal)) {
            if (first) { onMilestone?.('firstDeltaAt', Date.now()); first = false }
            text += (text ? ' ' : '') + chunk
          }
          if (signal?.aborted) return
          onMilestone?.('fullAt', Date.now())
          onMilestone?.('mode', 'SINGLE_SHOT')
          const finalText = text.replace(/\[END_CALL\]/g, '').trim()
          onMilestone?.('finalText', finalText)
          onMilestone?.('endCallRequested', text.includes('[END_CALL]'))
          if (finalText.length >= 3) yield finalText
        })()
      },
    },
  }

  const ttsPath = require.resolve('../src/services/tts')
  require.cache[ttsPath] = {
    id: ttsPath, filename: ttsPath, loaded: true,
    exports: { synthesizeSpeechStream: (text, voiceId, signal, previousText) => state.ttsImpl(text, voiceId, signal, previousText) },
  }

  const sttPath = require.resolve('../src/services/googleSTT')
  require.cache[sttPath] = {
    id: sttPath, filename: sttPath, loaded: true,
    exports: {
      transcribeStream: (onTranscript, onInterim, options) => {
        state.lastSttCallbacks = { onTranscript, onInterim }
        state.lastSttOptions = options // L1a: ให้เทสตรวจได้ว่า interimFinalizeMs ที่ audioStream.js ส่งเข้ามาตรงกับ rollout ของสายนั้นจริง
        return { write: () => {}, end: () => {} }
      },
    },
  }

  const rolloutConfigPath = require.resolve('../src/utils/rolloutConfig')
  require.cache[rolloutConfigPath] = {
    id: rolloutConfigPath, filename: rolloutConfigPath, loaded: true,
    exports: {
      createRolloutConfig: () => ({
        start: () => {},
        stop: () => {},
        getCurrentRolloutPercent: () => state.rolloutPercent,
        getCurrentLegacyObservedConfig: () => state.legacyObservedConfig,
        getCurrentLegacyEarlyTtsConfig: () => state.legacyEarlyTtsConfig,
        getCurrentSttA2Config: () => state.sttA2Config,
        getCurrentSttA2ShadowConfig: () => state.sttA2ShadowConfig,
        getCurrentOpeningHelloGuardConfig: () => state.openingHelloGuardConfig,
      }),
    },
  }

  audioStreamModule = require('../src/websocket/audioStream')
  audioStreamModule.registerWebSocket({ get: (path, opts, handler) => { cachedHandler = handler } })
}

function makeFakeSocket() {
  const socket = new EventEmitter()
  socket.sent = []
  socket.readyState = 1
  socket.OPEN = 1
  socket.send = (msg) => socket.sent.push(JSON.parse(msg))
  // audioStream.js เรียก socket.close() ในหลายจุด รวมถึงผ่าน setTimeout ที่ยิงช้า (เช่น fallbackDelay 5s+ ของ
  // end_call hangup) ซึ่งอาจ fire หลังเทสจบไปแล้ว — ถ้าไม่มี .close() ตรงนี้จะกลายเป็น uncaught exception ทีหลัง
  socket.close = () => { socket.readyState = 3; socket.closed = true }
  return socket
}

// เปิด "การเชื่อมต่อ" ใหม่หนึ่งสาย — callSid ต้องถูก set ไว้ใน callSessions ก่อนเรียก (caller เป็นคนทำ)
// คืน socket (fake connection) กับ helper ส่ง event ทั่วไปที่ใช้บ่อย
function connect({ callSid }) {
  ensureStubbed()
  const socket = makeFakeSocket()
  socket.url = `/stream?callSid=${callSid}`
  cachedHandler(socket, {})
  return socket
}

function sendStart(socket, { streamSid = 'SS1' } = {}) {
  socket.emit('message', JSON.stringify({ event: 'start', start: { streamSid } }))
}

function sendStop(socket) {
  socket.emit('message', JSON.stringify({ event: 'stop' }))
}

// เทสทุกตัวต้องเรียกนี้ตอนจบ ไม่งั้น durationTimer (default 300s, ไม่ได้ unref) และ timer อื่นๆ ที่ 'stop'/'close'
// เป็นคน clear จะค้าง Node process ไว้จนกว่าจะครบเวลาจริง ทำให้ทั้ง test run แขวนเป็นนาทีโดยไม่มี error ให้เห็นเลย
function disconnect(socket) {
  sendStop(socket)
  socket.emit('close')
}

// จำลอง final transcript หนึ่งเทิร์น — คืน Promise ที่ resolve เมื่อ turn processing ทั้งหมดจบ (รวม fallback/guard
// ถ้ามี) เพราะ transcribeStream ของจริงไม่ await callback นี้เลย แต่ในเทสเราต้อง await เพื่อ assert ผลลัพธ์ได้
async function sendFinalTranscript(text, sttMeta) {
  await state.lastSttCallbacks.onTranscript(text, sttMeta)
}

function sendInterim(text) {
  state.lastSttCallbacks.onInterim(text)
}

// Active-Playback Speech Guard R1 — a single interim no longer confirms barge-in during isSpeaking=true (see
// audioStream.js's 2-signal bargeCandidate logic); it takes a second COHERENT interim within
// BARGE_CONFIRM_WINDOW_MS. Exact repeat counts as coherent, so sending the same text twice is the simplest
// way to simulate "STT confirmed the same partial text again" and reach a real bargeIn() for tests whose
// intent is "this interim actually interrupts the AI", without each test having to know the confirmation
// mechanics itself.
function sendInterimConfirmed(text) {
  state.lastSttCallbacks.onInterim(text)
  state.lastSttCallbacks.onInterim(text)
}

function getState() { return state }

module.exports = { ensureStubbed, connect, sendStart, sendStop, sendFinalTranscript, sendInterim, sendInterimConfirmed, disconnect, getState }
