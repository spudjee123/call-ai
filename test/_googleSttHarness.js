// STT-A1 test harness — stub @google-cloud/speech ทั้งโมดูลก่อน require googleSTT.js จริง เพื่อขับ event
// (interim/final/error/end) แบบควบคุมได้ในเทส โดยไม่แตะ network/credentials จริงเลย
const { EventEmitter } = require('events')

const speechPath = require.resolve('@google-cloud/speech')
const credsPath = require.resolve('../src/utils/googleCredentials')
const sttPath = require.resolve('../src/services/googleSTT')

let createdStreams = []
let capturedOptions = []

function ensureStubbed() {
  createdStreams = []
  capturedOptions = []

  require.cache[speechPath] = {
    id: speechPath, filename: speechPath, loaded: true,
    exports: {
      SpeechClient: class {
        streamingRecognize(options) {
          capturedOptions.push(options) // STT-A1: ใช้ตรวจว่า request config ที่ส่งจริงไม่เปลี่ยนไปจาก A1
          const stream = new EventEmitter()
          stream.write = () => {}
          stream.end = () => {}
          createdStreams.push(stream)
          return stream
        }
      },
    },
  }

  require.cache[credsPath] = {
    id: credsPath, filename: credsPath, loaded: true,
    exports: { getGoogleClientOptions: () => ({}) },
  }

  delete require.cache[sttPath] // บังคับ re-require ให้ผูกกับ SpeechClient stub ด้านบนเสมอ
  return require('../src/services/googleSTT')
}

// STT-A2: optional 4th param `otherAlternatives` — array of extra alternative objects (e.g. { transcript, confidence })
// appended after alt0 in result.alternatives, to simulate Google returning maxAlternatives>1 candidates.
function emitInterim(stream, text, stability, otherAlternatives) {
  const alt0 = { transcript: text }
  const result = { isFinal: false, alternatives: [alt0, ...(otherAlternatives || [])] }
  if (stability !== undefined) result.stability = stability
  stream.emit('data', { results: [result] })
}

function emitFinal(stream, text, confidence, otherAlternatives) {
  const alt0 = { transcript: text }
  if (confidence !== undefined) alt0.confidence = confidence
  stream.emit('data', { results: [{ isFinal: true, alternatives: [alt0, ...(otherAlternatives || [])] }] })
}

// A2.1 Shadow: fire 'error'/'end' directly on any stream reference (including an already-rotated-away old
// stream) — streams here are plain EventEmitters so this works identically for current/next/shadowed streams,
// same as emitInterim/emitFinal above already do for 'data'.
function emitError(stream, err) {
  stream.emit('error', err || new Error('simulated STT error'))
}

function emitStreamEnd(stream) {
  stream.emit('end')
}

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

module.exports = {
  ensureStubbed,
  get streams() { return createdStreams },
  get capturedOptions() { return capturedOptions },
  emitInterim,
  emitFinal,
  emitError,
  emitStreamEnd,
  delay,
}
