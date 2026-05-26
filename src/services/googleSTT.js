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
    phrases: [
      'สวัสดี', 'ครับ', 'ค่ะ', 'สนใจ', 'ราคา', 'โปรโมชั่น', 'ไม่สนใจ', 'ขอบคุณ',
      'PGDOG', 'พีจีด็อก', 'แอดไลน์', 'พอยต์', 'ฝาก', 'สมัคร', 'โบนัส',
      'รับ', 'อยากรับ', 'สมัครรับ', 'ต้องทำยังไง',
    ],
    boost: 15
  }],
  enableAutomaticPunctuation: true,
}

function transcribeStream(onTranscript, onInterim) {
  let destroyed = false
  let currentStream = null
  let nextStream = null
  let errorRetryCount = 0

  let writeCount = 0
  let code11Count = 0

  let interimText = ''
  let interimTimer = null
  let utteranceClosed = false  // true after timer delivers transcript — blocks trailing interims and isFinal duplicate
  const INTERIM_FINALIZE_MS = 1500

  function resetUtteranceState() {
    clearTimeout(interimTimer)
    interimTimer = null
    interimText = ''
    utteranceClosed = false
  }

  function activatePrewarm() {
    resetUtteranceState()
    currentStream = nextStream
    nextStream = null
  }

  function createStream(isPrewarm = false) {
    if (destroyed) return
    if (!isPrewarm && currentStream) return

    console.log(isPrewarm ? '[STT] Pre-warming next stream' : '[STT] Creating new stream')

    const stream = client.streamingRecognize({
      config: STT_CONFIG,
      interimResults: true,
      singleUtterance: true,
    })
    .on('error', (err) => {
      if (destroyed) return
      if (stream !== currentStream && stream !== nextStream) return

      if (err.code === 11) {
        code11Count++
        if (code11Count % 5 === 0) console.log(`[STT] Stream reset (code 11) ×${code11Count}`)
      } else {
        console.error('[STT error]', err.message)
      }

      if (stream === currentStream) {
        currentStream = null
        errorRetryCount++
        if (errorRetryCount >= 10) {
          console.error('[STT] Too many consecutive errors, stopping recreation')
          return
        }
        if (nextStream) {
          activatePrewarm()
          console.log('[STT] Error recovery: switched to pre-warmed stream')
        } else {
          resetUtteranceState()
          setTimeout(() => createStream(false), 100)
        }
      } else {
        nextStream = null
      }
    })
    .on('data', (data) => {
      if (stream !== currentStream) return
      errorRetryCount = 0

      const result = data.results[0]
      if (!result) {
        if (data.speechEventType) console.log(`[STT] Event: ${data.speechEventType}`)
        return
      }
      const text = result.alternatives?.[0]?.transcript || ''

      if (!result.isFinal) {
        if (!text || utteranceClosed) return

        console.log(`[STT interim] "${text}"`)
        interimText = text
        onInterim?.()

        if (!nextStream && !destroyed) createStream(true)

        clearTimeout(interimTimer)
        interimTimer = setTimeout(() => {
          if (interimText && !destroyed) {
            console.log(`[STT] Interim→Final (1.5s silence): "${interimText}"`)
            onTranscript(interimText)
            interimText = ''
            utteranceClosed = true
          }
        }, INTERIM_FINALIZE_MS)
        return
      }

      // isFinal
      clearTimeout(interimTimer)
      interimTimer = null
      const finalText = text.trim()
      if (!utteranceClosed) {
        if (finalText) {
          onTranscript(finalText)
        } else {
          console.log('[STT] Final result but empty transcript')
        }
      }
      interimText = ''
      utteranceClosed = false
    })
    .on('end', () => {
      if (destroyed) return
      if (stream === currentStream) {
        currentStream = null
        if (nextStream) {
          console.log('[STT] Switched to pre-warmed stream ✓')
          activatePrewarm()
        } else {
          console.log('[STT] No pre-warm ready — cold start fallback')
          resetUtteranceState()
          setTimeout(() => createStream(false), 50)
        }
      } else if (stream === nextStream) {
        nextStream = null
        if (!destroyed && currentStream) {
          console.log('[STT] Pre-warm ended early — recreating')
          setTimeout(() => { if (!nextStream && !destroyed && currentStream) createStream(true) }, 300)
        }
      }
    })

    if (isPrewarm) {
      nextStream = stream
    } else {
      currentStream = stream
    }
  }

  createStream(false)

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
        if (nextStream) {
          activatePrewarm()
          console.log('[STT] Write error recovery: switched to pre-warmed stream')
        } else {
          resetUtteranceState()
          setTimeout(() => createStream(false), 100)
        }
      }
    },
    reset() {
      if (destroyed) return
      errorRetryCount = 0
      console.log('[STT] Resetting stream (AI done)')
      try { currentStream?.end() } catch (_) {}
      try { nextStream?.end() } catch (_) {}
      currentStream = null
      nextStream = null
      resetUtteranceState()
      createStream(false)
    },
    end() {
      if (destroyed) return
      destroyed = true
      clearTimeout(interimTimer)
      interimTimer = null
      try { currentStream?.end() } catch (_) {}
      try { nextStream?.end() } catch (_) {}
      currentStream = null
      nextStream = null
    }
  }
}

module.exports = { transcribeStream }
