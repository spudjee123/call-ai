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
    ttsImpl: async function* () { yield Buffer.from('audio') }, // synthesizeSpeechStream(text, voiceId, signal)
    rolloutPercent: 0,
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
async function sendFinalTranscript(text) {
  await state.lastSttCallbacks.onTranscript(text)
}

function sendInterim(text) {
  state.lastSttCallbacks.onInterim(text)
}

function getState() { return state }

module.exports = { ensureStubbed, connect, sendStart, sendStop, sendFinalTranscript, sendInterim, disconnect, getState }
