const callSessions = require('../utils/callSessions')
const { transcribeStream } = require('../services/googleSTT')
const { askClaude, askClaudeStream } = require('../services/claude')
const { synthesizeSpeechStream } = require('../services/tts')

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
    let greetingAbortController = null  // barge-in: cancel greeting audio
    let ttsAbortController = null       // barge-in: cancel STT→TTS pipeline
    let sttProcessing = false     // mutex: ป้องกัน concurrent Claude calls
    let bargeInCooldown = false   // cooldown หลัง barge-in ป้องกัน echo false-trigger
    let silenceTimer = null
    let silencePromptCount = 0
    let lastMarkTime = 0
    let pendingEndCall = false

    console.log(`[WS] Connected callSid=${callSid}`)

    function clearSilenceTimer() {
      if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null }
    }

    function startSilenceTimer() {
      clearSilenceTimer()
      if (!callActive || isSpeaking || sttProcessing) return
      silenceTimer = setTimeout(handleSilence, 8000)
    }

    async function handleSilence() {
      silenceTimer = null
      if (!callActive || isSpeaking || sttProcessing) return
      const currentSession = callSessions.get(callSid)
      if (!currentSession) return

      silencePromptCount++
      console.log(`[Silence] Timeout #${silencePromptCount}`)
      isSpeaking = true
      sttProcessing = true
      ttsAbortController = new AbortController()
      const signal = ttsAbortController.signal
      let totalSent = 0

      const promptText = silencePromptCount >= 2
        ? 'ไม่ได้ยินเสียงค่ะ ขอบคุณที่รับสายนะคะ'
        : 'ได้ยินอยู่ไหมคะ'

      try {
        for await (const chunk of synthesizeSpeechStream(promptText, currentSession.campaign.voice_id, signal)) {
          if (socket.readyState !== socket.OPEN || signal.aborted) break
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          totalSent++
        }
      } catch (err) {
        if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
          console.error('[Silence TTS error]', err.message)
        }
      } finally {
        ttsAbortController = null
        sttProcessing = false
      }

      if (totalSent > 0 && isSpeaking && socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'silence_done' } }))
      } else {
        isSpeaking = false
        if (silencePromptCount < 2) startSilenceTimer()
      }

      if (silencePromptCount >= 2) {
        const closeDelay = totalSent * 20 + 4000
        setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, closeDelay)
      }
    }

    // หยุด AI พูดทันที เมื่อลูกค้าพูดแทรก
    function bargeIn() {
      if (!isSpeaking) return
      console.log('[Barge-in] Customer interrupted — stopping AI audio')
      clearSilenceTimer()
      silencePromptCount = 0
      if (greetingAbortController) { greetingAbortController.abort(); greetingAbortController = null }
      if (ttsAbortController) { ttsAbortController.abort(); ttsAbortController = null }
      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({ event: 'clear', streamSid }))
      }
      isSpeaking = false
      sttProcessing = false  // unlock ให้รับ utterance ใหม่ได้ทันที
    }

    // Streaming TTS — ส่ง chunk ไป Twilio ทันทีที่ ElevenLabs generate
    // ไม่ต้องรอ audio ทั้งหมดก่อน → ลด latency 2-3 วินาที
    async function speakAndWait(text, session, markName) {
      if (!callActive || socket.readyState !== socket.OPEN) return

      greetingAbortController = new AbortController()
      const signal = greetingAbortController.signal
      let sent = 0

      try {
        for await (const chunk of synthesizeSpeechStream(text, session.campaign.voice_id, signal)) {
          if (socket.readyState !== socket.OPEN || signal.aborted) break
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          sent++
        }
      } catch (err) {
        if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
          console.error('[Audio Stream error]', err.message)
        }
      } finally {
        greetingAbortController = null
      }

      console.log(`[Audio] Streamed ${sent} chunks for mark=${markName}`)

      // ถ้า barge-in เกิดขึ้นระหว่างส่ง → ไม่ส่ง mark (isSpeaking=false แล้ว)
      if (!isSpeaking) return
      if (sent === 0) { isSpeaking = false; return }

      if (socket.readyState === socket.OPEN) {
        socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: markName } }))
      }

      const playbackMs = sent * 20 + 1500
      setTimeout(() => {
        if (isSpeaking) {
          console.log(`[Audio] Fallback unlock after ${playbackMs}ms`)
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
          if (!transcript || !callActive) return
          if (socket.readyState !== socket.OPEN) return
          if (sttProcessing) {
            console.log(`[STT] Transcript dropped (busy): "${transcript.substring(0, 40)}"`)
            return
          }
          if (bargeInCooldown) {
            console.log(`[STT] Transcript dropped (barge-in cooldown): "${transcript.substring(0, 40)}"`)
            return
          }
          // Post-mark echo filter: short fragment ภายใน 500ms ของ mark = delayed PSTN echo
          const msSinceMark = Date.now() - lastMarkTime
          if (msSinceMark < 500) {
            const wc = transcript.trim().split(/\s+/).length
            if (wc < 3 && transcript.length < 10) {
              console.log(`[STT] Echo suppressed (${msSinceMark}ms after mark): "${transcript}"`)
              return
            }
          }

          const currentSession = callSessions.get(callSid)
          if (!currentSession) return

          console.log(`[STT] "${transcript}"`)
          clearSilenceTimer()
          silencePromptCount = 0

          // Barge-in: ตรวจสอบว่าเป็นเสียงจริง ไม่ใช่ echo ของ AI
          if (isSpeaking) {
            const wordCount = transcript.trim().split(/\s+/).length
            if (wordCount < 2 && transcript.length < 8) {
              // Fragment สั้น = echo หรือ noise → ไม่ barge-in
              console.log(`[STT] Short fragment during AI speech — ignoring echo: "${transcript}"`)
              return
            }
            bargeIn()
            bargeInCooldown = true
            setTimeout(() => { bargeInCooldown = false }, 400)
            await new Promise(r => setTimeout(r, 200))
          }

          if (sttProcessing) return  // double-check หลัง await
          sttProcessing = true
          currentSession.messages.push({ role: 'user', content: transcript })
          isSpeaking = true

          ttsAbortController = new AbortController()
          const signal = ttsAbortController.signal
          let fullText = ''
          let totalSent = 0

          // Safety: ถ้า Claude/TTS ค้างนานผิดปกติ ให้ unlock อัตโนมัติ
          const processingGuard = setTimeout(() => {
            if (sttProcessing) {
              console.error('[AI] sttProcessing stuck >30s — force reset')
              sttProcessing = false
              isSpeaking = false
            }
          }, 30000)

          try {
            // LLM Streaming → TTS Pipeline
            // Claude yield ประโยค → ElevenLabs เริ่มทันที → ไม่ต้องรอ Claude เสร็จ
            for await (const sentence of askClaudeStream(currentSession, false, signal)) {
              if (signal.aborted || !callActive || !isSpeaking) break

              console.log(`[AI] "${sentence}"`)
              fullText += (fullText ? ' ' : '') + sentence

              // Strip [END_CALL] ก่อนส่ง TTS ป้องกันพูดออกเสียงตัว marker
              const cleanSentence = sentence.replace(/\[END_CALL\]/g, '').trim()

              // Stream ประโยคนี้ไป TTS และส่ง Twilio ทันที
              try {
                if (!cleanSentence) { if (sentence.includes('[END_CALL]')) break; continue }
                for await (const chunk of synthesizeSpeechStream(cleanSentence, currentSession.campaign.voice_id, signal)) {
                  if (socket.readyState !== socket.OPEN || signal.aborted) break
                  socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                  totalSent++
                }
              } catch (err) {
                if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
                  console.error('[TTS error]', err.message)
                }
                break
              }

              if (sentence.includes('[END_CALL]')) break
            }
          } catch (err) {
            console.error('[AI/TTS error]', err.message)
          } finally {
            clearTimeout(processingGuard)
            ttsAbortController = null
            sttProcessing = false
          }

          if (fullText) {
            currentSession.messages.push({ role: 'assistant', content: fullText })
            console.log(`[AI full] "${fullText}"`)
          }

          if (!signal?.aborted && isSpeaking && socket.readyState === socket.OPEN && totalSent > 0) {
            socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'ai_done' } }))
          } else if (totalSent === 0) {
            isSpeaking = false
          }

          const playbackMs = totalSent * 20 + 1500
          setTimeout(() => {
            if (isSpeaking) { console.log('[Audio] Fallback unlock'); isSpeaking = false }
          }, playbackMs)

          if (fullText.includes('[END_CALL]')) {
            pendingEndCall = true
            // Fallback: ปิดสายถ้า mark ไม่มาภายในเวลาที่คาดไว้
            const fallbackDelay = totalSent * 20 + 5000
            setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, fallbackDelay)
          }
        }, () => {
          // Interim result = ลูกค้ากำลังพูดอยู่ → reset silence timer ทันที
          // ป้องกัน "ได้ยินอยู่ไหมคะ" ไฟร์ระหว่างที่ลูกค้าพูด
          if (callActive && !isSpeaking && !sttProcessing) {
            clearSilenceTimer()
            silencePromptCount = 0
          }
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
              greetingAbortController = new AbortController()
              let sent = 0
              for (const chunk of chunks) {
                if (socket.readyState !== socket.OPEN || greetingAbortController?.signal.aborted) break
                socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                sent++
              }
              greetingAbortController = null
              if (!isSpeaking) return  // barge-in happened during greeting
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
        // ส่งเสียงลูกค้าให้ STT เสมอ (รวมถึงตอน AI พูด เพื่อ barge-in detection)
        // Twilio PSTN handles echo cancellation — ไม่ต้องกังวลเสียง AI ย้อนกลับ
        try {
          const audioData = Buffer.from(msg.media.payload, 'base64')
          sttStream.write(audioData)
        } catch (e) {
          try { sttStream.end() } catch (_) {}
          sttStream = null
        }
      }

      if (msg.event === 'mark') {
        console.log(`[WS] Mark received: ${msg.mark?.name}`)
        isSpeaking = false
        lastMarkTime = Date.now()
        if (pendingEndCall) {
          setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, 1000)
          return
        }
        startSilenceTimer()
      }

      if (msg.event === 'stop') {
        console.log(`[WS] Stream stopped: ${callSid}`)
        callActive = false
        clearSilenceTimer()
        if (sttStream) { sttStream.end(); sttStream = null }
      }
    })

    socket.on('close', () => {
      console.log(`[WS] Disconnected: ${callSid}`)
      callActive = false
      clearSilenceTimer()
      if (sttStream) { sttStream.end(); sttStream = null }
    })

    socket.on('error', (err) => {
      console.error(`[WS] Error for ${callSid}:`, err.message)
    })
  })
}

module.exports = { registerWebSocket }
