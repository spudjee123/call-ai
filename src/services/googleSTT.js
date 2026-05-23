const speech = require('@google-cloud/speech')
const { mulawBufferToPcm16 } = require('../utils/audioConverter')

const client = new speech.SpeechClient()

function transcribeStream(onTranscript) {
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
      // stream หมดอายุหลัง 5 นาที เป็นเรื่องปกติ
      if (err.code !== 11) console.error('[STT error]', err.message)
    })
    .on('data', (data) => {
      const result = data.results[0]
      if (result && result.isFinal) {
        const transcript = result.alternatives[0].transcript.trim()
        if (transcript) onTranscript(transcript)
      }
    })

  // wrapper ที่รับ mulaw buffer จาก Twilio แล้วแปลงก่อนส่ง STT
  return {
    write(mulawBuffer) {
      const pcm = mulawBufferToPcm16(mulawBuffer)
      recognizeStream.write(pcm)
    },
    end() {
      recognizeStream.end()
    }
  }
}

module.exports = { transcribeStream }
