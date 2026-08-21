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

function emitInterim(stream, text, stability) {
  const result = { isFinal: false, alternatives: [{ transcript: text }] }
  if (stability !== undefined) result.stability = stability
  stream.emit('data', { results: [result] })
}

function emitFinal(stream, text, confidence) {
  const alt = { transcript: text }
  if (confidence !== undefined) alt.confidence = confidence
  stream.emit('data', { results: [{ isFinal: true, alternatives: [alt] }] })
}

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

module.exports = {
  ensureStubbed,
  get streams() { return createdStreams },
  get capturedOptions() { return capturedOptions },
  emitInterim,
  emitFinal,
  delay,
}
