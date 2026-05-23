const callSessions = require('../utils/callSessions')
const { transcribeStream } = require('../services/googleSTT')
const { askClaude } = require('../services/claude')
const { synthesizeSpeech } = require('../services/tts')

function registerWebSocket(fastify) {
  fastify.get('/stream', { websocket: true }, (connection, req) => {
    const socket = (typeof connection.send === 'function') ? connection
      : (typeof req?.send === 'function') ? req
      : (connection.socket || connection)

    const rawUrl = (connection?.url || req?.url || '')
    const qs = rawUrl.includes('?') ? rawUrl.split('?')[1] : ''
    let callSid = new URLSearchParams(qs).get('callSid')
    let streamSid = null
    let sttStream = null
    let isSpeaking = false
    let callActive = true

    console.log(`[WS] Connected callSid=${callSid}`)

    // ส่งเสียง AI กลับ Twilio แล้ว unlock isSpeaking หลัง playback จบ
    async function speakAndWait(text, session, markName) {
      if (!callActive || socket.readyState !== socket.OPEN) return

      const audioChunks = await synthesizeSpeech(text, session.campaign.voice_id)
      console.log(`[Audio] Sending ${audioChunks.length} chunks for mark=${markName}`)

      let sent = 0
      for (const chunk of audioChunks) {
        if (socket.readyState === socket.OPEN) {
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          sent++
        }
      }
      console.log(`[Audio] Sent ${sent}/${audioChunks.length} chunks`)

      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: markName } }))
      }

      // Fallback: unlock isSpeaking หลัง expected playback duration
      // Twilio ไม่ส่ง mark กลับมาเสมอ → ใช้ timer เป็น safety net
      // 160 bytes/chunk × 8000 bytes/sec = 20ms/chunk
      const playbackMs = sent * 20 + 1500  // actual duration + 1.5s buffer
      setTimeout(() => {
        if (isSpeaking) {
          console.log(`[Audio] Fallback unlock after ${playbackMs}ms (mark not received)`)
          isSpeaking = false
        }
      }, playbackMs)
    }

    socket.on('message', async (rawMsg) => {
      let msg
      try {
        msg = JSON.parse(rawMsg)
      } catch (e) {
        console.error('[WS] Parse error:', e.message)
        return
      }
      console.log(`[WS] Event received: ${msg.event}`)

      if (msg.event === 'start') {
        streamSid = msg.start.streamSid
        if (!callSid) callSid = msg.start.callSid
        console.log(`[WS] callSid resolved: ${callSid}`)
        const session = callSessions.get(callSid)
        if (!session) return

        console.log(`[WS] Stream started: ${streamSid}`)

        // เริ่ม STT stream
        sttStream = transcribeStream(async (transcript) => {
          // Block ถ้า: กำลังพูดอยู่, สายตัดแล้ว, socket ปิดแล้ว
          if (!transcript || isSpeaking || !callActive) return
          if (socket.readyState !== socket.OPEN) return
          const currentSession = callSessions.get(callSid)
          if (!currentSession) return

          console.log(`[STT] "${transcript}"`)
          currentSession.messages.push({ role: 'user', content: transcript })

          isSpeaking = true
          try {
            const aiText = await askClaude(currentSession)
            console.log(`[AI] "${aiText}"`)
            currentSession.messages.push({ role: 'assistant', content: aiText })

            await speakAndWait(aiText, currentSession, 'ai_done')

            if (aiText.includes('[END_CALL]')) {
              setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, 3000)
            }
          } catch (err) {
            console.error('[AI/TTS error]', err.message)
            isSpeaking = false
          }
          // ไม่ใส่ finally { isSpeaking = false } — รอ mark event จาก Twilio แทน
        })

        // AI ทักทายก่อนเลย — ใช้ pre-generated audio ถ้ามี (ลด latency)
        const playGreeting = async () => {
          const session = callSessions.get(callSid)
          if (!session || !callActive) return
          isSpeaking = true
          try {
            if (session.greetingChunks) {
              // ใช้ audio ที่ pre-generate ไว้แล้ว — ส่งได้ทันที
              console.log(`[Greeting] Using pre-generated audio (${session.greetingChunks.length} chunks)`)
              const chunks = session.greetingChunks
              session.greetingChunks = null  // ใช้แล้วล้างทิ้ง
              let sent = 0
              for (const chunk of chunks) {
                if (socket.readyState === socket.OPEN) {
                  socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                  sent++
                }
              }
              if (socket.readyState === socket.OPEN) {
                socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'greeting_done' } }))
              }
              const playbackMs = sent * 20 + 1500
              setTimeout(() => { if (isSpeaking) { console.log('[Audio] Fallback unlock (greeting)'); isSpeaking = false } }, playbackMs)
              console.log(`[Greeting] Sent ${sent} pre-generated chunks`)
            } else {
              // Fallback: generate ใหม่ถ้า pre-gen ไม่สำเร็จ
              console.log(`[Greeting] Pre-gen not ready, generating now...`)
              const greeting = await askClaude(session, true)
              console.log(`[Greeting] "${greeting.substring(0, 100)}"`)
              session.messages.push({ role: 'assistant', content: greeting })
              await speakAndWait(greeting, session, 'greeting_done')
            }
          } catch (err) {
            console.error('[Greeting error]', err.message)
            isSpeaking = false
          }
        }

        // รอ 300ms แทน 1000ms — แค่ให้ stream stable
        setTimeout(playGreeting, 300)
      }

      if (msg.event === 'media' && sttStream) {
        // ไม่ส่งเสียงให้ STT ขณะ AI กำลังพูดอยู่ (ป้องกัน AI ได้ยินตัวเอง)
        if (!isSpeaking) {
          try {
            const audioData = Buffer.from(msg.media.payload, 'base64')
            sttStream.write(audioData)
          } catch (e) {
            sttStream = null
          }
        }
      }

      if (msg.event === 'mark') {
        console.log(`[WS] Mark received: ${msg.mark?.name}`)
        // Twilio ยืนยันว่าเล่นเสียง AI จบแล้ว → unlock รับคำพูดลูกค้าได้
        isSpeaking = false
      }

      if (msg.event === 'stop') {
        console.log(`[WS] Stream stopped: ${callSid}`)
        callActive = false
        if (sttStream) { sttStream.end(); sttStream = null }
      }
    })

    socket.on('close', () => {
      console.log(`[WS] Disconnected: ${callSid}`)
      callActive = false
      if (sttStream) { sttStream.end(); sttStream = null }
    })

    socket.on('error', (err) => {
      console.error(`[WS] Error for ${callSid}:`, err.message)
    })
  })
}

module.exports = { registerWebSocket }
