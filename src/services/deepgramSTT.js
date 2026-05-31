const { createClient, LiveTranscriptionEvents } = require('@deepgram/sdk')

const client = createClient(process.env.DEEPGRAM_API_KEY)

const STT_CONFIG = {
  model: 'nova-3',
  language: 'th',
  encoding: 'mulaw',
  sample_rate: 8000,
  channels: 1,
  interim_results: true,
  endpointing: 400,       // ms silence → fires speech_final (แทน timer 1500ms)
  utterance_end_ms: 1000, // fallback ถ้า endpointing ไม่ยิง
  smart_format: false,
}

function transcribeStream(onTranscript, onInterim) {
  let destroyed = false
  let connection = null
  let reconnectTimer = null
  let errorCount = 0

  let interimText = ''
  let utteranceClosed = false

  function resetUtteranceState() {
    interimText = ''
    utteranceClosed = false
  }

  function connect() {
    if (destroyed) return
    console.log('[STT] Connecting to Deepgram...')

    const conn = client.listen.live(STT_CONFIG)

    conn.on(LiveTranscriptionEvents.Open, () => {
      console.log('[STT] Deepgram connected ✓')
      errorCount = 0
      connection = conn
    })

    conn.on(LiveTranscriptionEvents.Transcript, (data) => {
      if (destroyed) return
      const result = data.channel?.alternatives?.[0]
      if (!result) return

      const text = result.transcript?.trim()
      if (!text) return

      const isFinal = data.is_final
      const speechFinal = data.speech_final

      if (!isFinal) {
        // Interim result
        if (utteranceClosed) return
        console.log(`[STT interim] "${text}"`)
        interimText = text
        onInterim?.(text)
        return
      }

      // isFinal = true
      if (utteranceClosed) return

      if (speechFinal) {
        // speech_final = endpointing ตรวจจับว่าพูดจบแล้ว → ยิง transcript ทันที
        console.log(`[STT] speech_final: "${text}"`)
        onTranscript(text)
        interimText = ''
        utteranceClosed = true
      } else {
        // isFinal แต่ยังไม่ speech_final = mid-utterance update
        interimText = text
        onInterim?.(text)
      }
    })

    conn.on(LiveTranscriptionEvents.UtteranceEnd, () => {
      // Fallback: utterance_end_ms ยิงถ้า endpointing ไม่ทำงาน
      if (destroyed || utteranceClosed) return
      if (interimText) {
        console.log(`[STT] UtteranceEnd fallback: "${interimText}"`)
        onTranscript(interimText)
        interimText = ''
        utteranceClosed = true
      }
    })

    conn.on(LiveTranscriptionEvents.Error, (err) => {
      if (destroyed) return
      console.error('[STT error]', err.message || err)
      errorCount++
      connection = null
      if (errorCount >= 10) {
        console.error('[STT] Too many errors, stopping')
        return
      }
      scheduleReconnect(500)
    })

    conn.on(LiveTranscriptionEvents.Close, () => {
      if (destroyed) return
      console.log('[STT] Deepgram disconnected — reconnecting...')
      connection = null
      resetUtteranceState()
      scheduleReconnect(100)
    })
  }

  function scheduleReconnect(ms) {
    if (destroyed || reconnectTimer) return
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null
      connect()
    }, ms)
  }

  connect()

  return {
    write(mulawBuffer) {
      if (destroyed || !connection) return
      try {
        connection.send(mulawBuffer)
      } catch (e) {
        console.error('[STT] write error:', e.message)
      }
    },
    reset() {
      if (destroyed) return
      console.log('[STT] Resetting (AI done)')
      resetUtteranceState()
    },
    end() {
      if (destroyed) return
      destroyed = true
      clearTimeout(reconnectTimer)
      reconnectTimer = null
      try { connection?.finish() } catch (_) {}
      connection = null
    }
  }
}

module.exports = { transcribeStream }
