const speech = require('@google-cloud/speech')
const { mulawBufferToPcm16 } = require('../utils/audioConverter')

const clientOptions = {}
if (process.env.GOOGLE_CREDENTIALS_JSON) {
  clientOptions.credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON)
}
const client = new speech.SpeechClient(clientOptions)

function transcribeStream(onTranscript) {
  let destroyed = false

  const request = {
    config: {
      encoding: 'LINEAR16',
      sampleRateHertz: 8000,
      languageCode: 'th-TH',
      model: 'latest_short',
      useEnhanced: true,
      speechContexts: [{
        phrases: ['สวัสดี', 'ครับ', 'ค่ะ', 'สนใจ', 'ราคา', 'โปรโมชั่น', 'ไม่สนใจ', 'ขอบคุณ']
      }],
      enableAutomaticPunctuation: true,
    },
    interimResults: false,
    singleUtterance: false,
  }

  const recognizeStream = client
    .streamingRecognize(request)
    .on('error', (err) => {
      destroyed = true
      if (err.code !== 11) console.error('[STT error]', err.message)
    })
    .on('data', (data) => {
      const result = data.results[0]
      if (result && result.isFinal) {
        const transcript = result.alternatives[0].transcript.trim()
        if (transcript) onTranscript(transcript)
      }
    })

  return {
    write(mulawBuffer) {
      if (destroyed) return
      const pcm = mulawBufferToPcm16(mulawBuffer)
      recognizeStream.write(pcm)
    },
    end() {
      if (destroyed) return
      destroyed = true
      try { recognizeStream.end() } catch (e) {}
    }
  }
}

module.exports = { transcribeStream }
