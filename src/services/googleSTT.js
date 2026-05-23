const speech = require('@google-cloud/speech')
const { mulawBufferToPcm16 } = require('../utils/audioConverter')

const clientOptions = {}
if (process.env.GOOGLE_CREDENTIALS_JSON) {
  clientOptions.credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON)
}
const client = new speech.SpeechClient(clientOptions)

const STT_CONFIG = {
  encoding: 'LINEAR16',
  sampleRateHertz: 8000,
  languageCode: 'th-TH',
  model: 'latest_short',
  useEnhanced: true,
  speechContexts: [{
    phrases: ['สวัสดี', 'ครับ', 'ค่ะ', 'สนใจ', 'ราคา', 'โปรโมชั่น', 'ไม่สนใจ', 'ขอบคุณ',
              'PGDOG', 'พีจีด็อก', 'แอดไลน์', 'พอยต์', 'ฝาก', 'สมัคร', 'โบนัส'],
    boost: 15
  }],
  enableAutomaticPunctuation: true,
}

function transcribeStream(onTranscript) {
  let destroyed = false
  let currentStream = null

  function createStream() {
    if (destroyed) return

    const stream = client.streamingRecognize({
      config: STT_CONFIG,
      interimResults: false,
      // singleUtterance: true → Google fires result เร็วกว่า หลังหยุดพูด
      // แล้ว stream จบ (end event) → recreate ทันทีเพื่อรับ utterance ถัดไป
      singleUtterance: true,
    })
    .on('error', (err) => {
      if (destroyed) return
      if (err.code !== 11) console.error('[STT error]', err.message)
      // Recreate stream หลัง error (รวม code 11 = deadline exceeded)
      currentStream = null
      setTimeout(createStream, 100)
    })
    .on('data', (data) => {
      const result = data.results[0]
      if (result?.isFinal) {
        const transcript = result.alternatives[0].transcript.trim()
        if (transcript) onTranscript(transcript)
      }
    })
    .on('end', () => {
      // singleUtterance fired → stream จบ → recreate สำหรับ utterance ถัดไป
      if (!destroyed) {
        currentStream = null
        setTimeout(createStream, 50)
      }
    })

    currentStream = stream
  }

  createStream()

  return {
    write(mulawBuffer) {
      if (destroyed || !currentStream) return
      try {
        const pcm = mulawBufferToPcm16(mulawBuffer)
        currentStream.write(pcm)
      } catch (e) {
        currentStream = null
      }
    },
    end() {
      if (destroyed) return
      destroyed = true
      try { currentStream?.end() } catch (e) {}
      currentStream = null
    }
  }
}

module.exports = { transcribeStream }
