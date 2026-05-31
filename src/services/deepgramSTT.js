const WebSocket = require('ws')

const _params = new URLSearchParams({
  model: 'nova-3',
  language: 'th',
  encoding: 'mulaw',
  sample_rate: '8000',
  channels: '1',
  interim_results: 'true',
  endpointing: '400',
  utterance_end_ms: '1000',
  smart_format: 'false',
})
;['PGDOG', 'แอดไลน์', 'พีจีด็อก', 'พอยต์', 'โบนัส', 'สมัคร', 'ฝาก'].forEach(k => _params.append('keyterm', k))
const DG_URL = 'wss://api.deepgram.com/v1/listen?' + _params.toString()

function transcribeStream(onTranscript, onInterim) {
  let destroyed = false
  let ws = null
  let audioBuffer = []
  let errorCount = 0
  let reconnectTimer = null
  let interimText = ''
  let utteranceClosed = false

  function resetUtteranceState() {
    interimText = ''
    utteranceClosed = false
  }

  function connect() {
    if (destroyed) return
    console.log('[STT] Connecting to Deepgram...')

    ws = new WebSocket(DG_URL, {
      headers: { Authorization: `Token ${process.env.DEEPGRAM_API_KEY}` }
    })

    ws.on('open', () => {
      console.log('[STT] Deepgram connected ✓')
      errorCount = 0
      for (const buf of audioBuffer) {
        try { ws.send(buf) } catch (_) {}
      }
      audioBuffer = []
    })

    ws.on('message', (raw) => {
      if (destroyed) return
      let data
      try { data = JSON.parse(raw) } catch { return }

      // UtteranceEnd fallback
      if (data.type === 'UtteranceEnd') {
        if (!utteranceClosed && interimText) {
          console.log(`[STT] UtteranceEnd fallback: "${interimText}"`)
          onTranscript(interimText)
          interimText = ''
          utteranceClosed = true
        }
        return
      }

      const text = data?.channel?.alternatives?.[0]?.transcript?.trim()
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
        console.log(`[STT] speech_final: "${text}"`)
        onTranscript(text)
        interimText = ''
        utteranceClosed = true
      } else {
        interimText = text
        onInterim?.(text)
      }
    })

    ws.on('error', (err) => {
      if (destroyed) return
      console.error('[STT error]', err.message)
      errorCount++
      ws = null
      if (errorCount >= 10) { console.error('[STT] Too many errors, stopping'); return }
      scheduleReconnect(500)
    })

    ws.on('close', (code) => {
      if (destroyed) return
      console.log(`[STT] Deepgram closed (${code}) — reconnecting...`)
      ws = null
      resetUtteranceState()
      scheduleReconnect(100)
    })
  }

  function scheduleReconnect(ms) {
    if (destroyed || reconnectTimer) return
    reconnectTimer = setTimeout(() => { reconnectTimer = null; connect() }, ms)
  }

  connect()

  return {
    write(mulawBuffer) {
      if (destroyed || errorCount >= 10) return
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        audioBuffer.push(mulawBuffer)
        return
      }
      try { ws.send(mulawBuffer) } catch (e) {
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
      try { ws?.close() } catch (_) {}
      ws = null
    }
  }
}

module.exports = { transcribeStream }
