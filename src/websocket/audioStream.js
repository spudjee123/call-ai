const callSessions = require('../utils/callSessions')
const { mulawBufferToPcm16, pcm16BufferToMulaw } = require('../utils/audioConverter')
const { transcribeStream } = require('../services/googleSTT')
const { askClaude } = require('../services/claude')
const { synthesizeSpeech } = require('../services/elevenlabs')

function registerWebSocket(fastify) {
  fastify.get('/stream', { websocket: true }, (socket, req) => {
    const rawUrl = req.url || ''
    const qs = rawUrl.includes('?') ? rawUrl.split('?')[1] : ''
    let callSid = new URLSearchParams(qs).get('callSid')
    let streamSid = null
    let sttStream = null
    let isSpeaking = false

    console.log(`[WS] Connected: ${callSid}`)

    socket.on('message', async (rawMsg) => {
      let msg
      try { msg = JSON.parse(rawMsg) } catch { return }

      if (msg.event === 'start') {
        streamSid = msg.start.streamSid
        if (!callSid) callSid = msg.start.callSid
        const session = callSessions.get(callSid)
        if (!session) return

        console.log(`[WS] Stream started: ${streamSid}`)

        // เริ่ม STT stream
        sttStream = transcribeStream(async (transcript) => {
          if (!transcript || isSpeaking) return
          console.log(`[STT] "${transcript}"`)

          session.messages.push({ role: 'user', content: transcript })

          // ส่งให้ Claude
          isSpeaking = true
          try {
            const aiText = await askClaude(session)
            console.log(`[AI] "${aiText}"`)

            session.messages.push({ role: 'assistant', content: aiText })

            // ส่งให้ ElevenLabs แปลงเป็นเสียง
            const audioChunks = await synthesizeSpeech(aiText, session.campaign.voice_id)

            // ส่งเสียงกลับ Twilio
            for (const chunk of audioChunks) {
              const mulawChunk = pcm16BufferToMulaw(chunk)
              const payload = {
                event: 'media',
                streamSid,
                media: { payload: mulawChunk.toString('base64') }
              }
              if (socket.readyState === socket.OPEN) {
                socket.send(JSON.stringify(payload))
              }
            }

            // ส่ง mark เมื่อพูดจบ
            if (socket.readyState === socket.OPEN) {
              socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'ai_done' } }))
            }

            // เช็คว่า AI ต้องการจบสาย
            if (aiText.includes('[END_CALL]')) {
              setTimeout(() => {
                if (socket.readyState === socket.OPEN) socket.close()
              }, 3000)
            }

          } catch (err) {
            console.error('[AI/TTS error]', err.message)
          } finally {
            isSpeaking = false
          }
        })

        // AI ทักทายก่อนเลย
        setTimeout(async () => {
          const session = callSessions.get(callSid)
          if (!session) return
          isSpeaking = true
          try {
            const greeting = await askClaude(session, true)
            session.messages.push({ role: 'assistant', content: greeting })

            const audioChunks = await synthesizeSpeech(greeting, session.campaign.voice_id)
            for (const chunk of audioChunks) {
              const mulawChunk = pcm16BufferToMulaw(chunk)
              if (socket.readyState === socket.OPEN) {
                socket.send(JSON.stringify({
                  event: 'media',
                  streamSid,
                  media: { payload: mulawChunk.toString('base64') }
                }))
              }
            }
          } catch (err) {
            console.error('[Greeting error]', err.message)
          } finally {
            isSpeaking = false
          }
        }, 1000)
      }

      if (msg.event === 'media' && sttStream) {
        const audioData = Buffer.from(msg.media.payload, 'base64')
        sttStream.write(audioData)
      }

      if (msg.event === 'mark') {
        // ลูกค้าได้ยิน AI พูดจบแล้ว
        isSpeaking = false
      }

      if (msg.event === 'stop') {
        console.log(`[WS] Stream stopped: ${callSid}`)
        if (sttStream) sttStream.end()
      }
    })

    socket.on('close', () => {
      console.log(`[WS] Disconnected: ${callSid}`)
      if (sttStream) sttStream.end()
    })

    socket.on('error', (err) => {
      console.error(`[WS] Error for ${callSid}:`, err.message)
    })
  })
}

module.exports = { registerWebSocket }
