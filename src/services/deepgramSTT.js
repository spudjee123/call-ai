const { DeepgramClient } = require('@deepgram/sdk')

const client = new DeepgramClient({ apiKey: process.env.DEEPGRAM_API_KEY })

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
  let audioBuffer = []   // buffer audio ที่มาก่อน connection ready
  let errorCount = 0
  let reconnectTimer = null
  let interimText = ''
  let utteranceClosed = false

  function resetUtteranceState() {
    interimText = ''
    utteranceClosed = false
  }

  async function connect() {
    if (destroyed) return
    console.log('[STT] Connecting to Deepgram...')

    try {
      const conn = await client.listen.v1.connect(STT_CONFIG)
      if (destroyed) { try { conn.finish() } catch (_) {}; return }

      conn.on('open', () => {
        console.log('[STT] Deepgram connected ✓')
        errorCount = 0
        connection = conn
        // flush audio ที่ค้างไว้
        for (const buf of audioBuffer) {
          try { conn.sendMedia(buf) } catch (_) {}
        }
        audioBuffer = []
      })

      conn.on('message', (data) => {
        if (destroyed) return

        // UtteranceEnd fallback
        if (data?.type === 'UtteranceEnd') {
          if (!utteranceClosed && interimText) {
            console.log(`[STT] UtteranceEnd fallback: "${interimText}"`)
            onTranscript(interimText)
            interimText = ''
            utteranceClosed = true
          }
          return
        }

        const result = data?.channel?.alternatives?.[0]
        if (!result) return
        const text = result.transcript?.trim()
        if (!text) return

        const isFinal = data.is_final
        const speechFinal = data.speech_final

        if (!isFinal) {
          if (utteranceClosed) return
          console.log(`[STT interim] "${text}"`)
          interimText = text
          onInterim?.(text)
          return
        }

        if (utteranceClosed) return

        if (speechFinal) {
          // endpointing ตรวจว่าพูดจบ → ยิง transcript ทันที
          console.log(`[STT] speech_final: "${text}"`)
          onTranscript(text)
          interimText = ''
          utteranceClosed = true
        } else {
          interimText = text
          onInterim?.(text)
        }
      })

      conn.on('error', (err) => {
        if (destroyed) return
        console.error('[STT error]', err?.message || err)
        errorCount++
        connection = null
        if (errorCount >= 10) { console.error('[STT] Too many errors, stopping'); return }
        scheduleReconnect(500)
      })

      conn.on('close', () => {
        if (destroyed) return
        console.log('[STT] Deepgram disconnected — reconnecting...')
        connection = null
        resetUtteranceState()
        scheduleReconnect(100)
      })

    } catch (err) {
      if (destroyed) return
      console.error('[STT] Connect failed:', err.message)
      errorCount++
      scheduleReconnect(500)
    }
  }

  function scheduleReconnect(ms) {
    if (destroyed || reconnectTimer) return
    reconnectTimer = setTimeout(() => { reconnectTimer = null; connect() }, ms)
  }

  connect()

  return {
    write(mulawBuffer) {
      if (destroyed) return
      if (!connection) {
        audioBuffer.push(mulawBuffer)
        return
      }
      try {
        connection.sendMedia(mulawBuffer)
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
      audioBuffer = []
      try { connection?.finish() } catch (_) {}
      connection = null
    }
  }
}

module.exports = { transcribeStream }
