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

function transcribeStream(onTranscript, onInterim) {
  let destroyed = false
  let currentStream = null
  let errorRetryCount = 0

  let writeCount = 0
  let code11Count = 0
  let interimText = ''
  let interimTimer = null
  const INTERIM_FINALIZE_MS = 1500  // หยุดพูด 1.5s = treat interim เป็น final

  function createStream() {
    if (destroyed || currentStream) return  // ป้องกัน double-creation
    console.log('[STT] Creating new stream')

    const stream = client.streamingRecognize({
      config: STT_CONFIG,
      interimResults: true,
    })
    .on('error', (err) => {
      if (destroyed) return
      if (currentStream !== stream) return
      if (err.code === 11) {
        code11Count++
        if (code11Count % 5 === 0) console.log(`[STT] Stream reset (code 11) ×${code11Count}`)
      } else {
        console.error('[STT error]', err.message)
      }
      currentStream = null
      errorRetryCount++
      if (errorRetryCount >= 10) {
        console.error('[STT] Too many consecutive errors, stopping recreation')
        return
      }
      setTimeout(createStream, 100)
    })
    .on('data', (data) => {
      if (currentStream !== stream) return
      errorRetryCount = 0
      const result = data.results[0]
      if (!result) return
      const text = result.alternatives?.[0]?.transcript || ''
      if (!result.isFinal) {
        if (text) {
          console.log(`[STT interim] "${text}"`)
          interimText = text
          onInterim?.()
          clearTimeout(interimTimer)
          interimTimer = setTimeout(() => {
            if (interimText && !destroyed) {
              console.log(`[STT] Interim→Final (1.5s silence): "${interimText}"`)
              onTranscript(interimText)
              interimText = ''
            }
          }, INTERIM_FINALIZE_MS)
        }
        return
      }
      clearTimeout(interimTimer)
      interimTimer = null
      interimText = ''
      if (text.trim()) {
        onTranscript(text.trim())
      } else {
        console.log('[STT] Final result but empty transcript')
      }
    })
    .on('end', () => {
      if (!destroyed && currentStream === stream) {
        console.log('[STT] Stream ended — recreating')
        currentStream = null
        interimText = ''
        clearTimeout(interimTimer)
        setTimeout(createStream, 50)
      }
    })

    currentStream = stream
  }

  createStream()

  return {
    write(mulawBuffer) {
      if (destroyed || !currentStream) {
        if (!destroyed) console.log('[STT] write skipped — no stream')
        return
      }
      try {
        const pcm = mulawBufferToPcm16(mulawBuffer)
        currentStream.write(pcm)
        if (++writeCount % 100 === 0) console.log(`[STT] Audio flowing: ${writeCount} packets sent`)
      } catch (e) {
        console.error('[STT] write error, recreating stream:', e.message)
        currentStream = null
        setTimeout(createStream, 100)
      }
    },
    end() {
      if (destroyed) return
      destroyed = true
      clearTimeout(interimTimer)
      interimTimer = null
      try { currentStream?.end() } catch (e) {}
      currentStream = null
    }
  }
}

module.exports = { transcribeStream }
