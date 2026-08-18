const callSessions = require('../utils/callSessions')
const { transcribeStream } = require('../services/googleSTT')
const { askClaude, askClaudeStream } = require('../services/claude')
const { synthesizeSpeechStream } = require('../services/tts')
const healthMonitor = require('../utils/healthMonitor')
const { createCallState, endCall } = require('../utils/generationGuard')
const { decideRollout } = require('../utils/rolloutBucket')
const { createTurnMetrics, markOnce, computeDerivedMetrics } = require('../utils/turnMetrics')

const MAX_CALL_DURATION_MS = (parseInt(process.env.MAX_CALL_DURATION_SECONDS) || 300) * 1000

// Checkpoint C0: หาร้อยเปอร์เซ็นต์ chunked-streaming path ใหม่ตายตัวที่ 0% ก่อน — ยังไม่ให้สายไหนเข้าเลย
// จะย้ายไปอ่านค่าจาก Google Sheets ใน Checkpoint C5 (พร้อม cache + last-known-good fallback)
const CHUNKED_ROLLOUT_PERCENT = 0

function shouldBlockEndCall(session, aiResponse) {
  const userMessages = session.messages.filter(m => m.role === 'user')
  const lastUserMsg = userMessages.at(-1)?.content ?? ''
  const hasNegation = lastUserMsg.includes('ไม่') || lastUserMsg.includes('ยังไม่')
  const hasInterest = ['สนใจ', 'อยากลอง', 'อยากสมัคร', 'สมัครเลย'].some(k => lastUserMsg.includes(k))
  if (!hasInterest || hasNegation) return false
  return !aiResponse.includes('เพิ่มเติม')
}

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
    let durationTimer = null
    let lastMarkTime = 0
    let pendingEndCall = false
    let activePipelineId = 0
    let prewarmPromise = null    // pre-warmed Claude response Promise<string|null>
    let prewarmStartText = null  // interim text that triggered prewarm
    let prewarmAbort = null      // AbortController for prewarm call
    let prewarmRetriggerAt = 0   // เวลาต่ำสุดที่อนุญาตเดาใหม่รอบถัดไป (throttle กันยิง Claude ถี่เกิน)

    // Checkpoint C0: safety infrastructure ของ B.5 ผูกไว้กับสายนี้แล้ว แต่ยังไม่มีจุดไหนอ่าน/ใช้งานจริง
    // (ไม่มี chunked path ให้ guard ในไฟล์นี้เลยตอนนี้) — legacy path ทั้งหมดด้านล่างยังทำงานเหมือนเดิมทุกบรรทัด
    const callState = createCallState()
    let rollout = null // freeze ตอน 'start' event เพราะ callSid อาจยังไม่ resolve ตอน connection เปิด

    console.log(`[WS] Connected callSid=${callSid}`)

    function clearSilenceTimer() {
      if (silenceTimer) { clearTimeout(silenceTimer); silenceTimer = null }
    }

    function startSilenceTimer() {
      clearSilenceTimer()
      if (!callActive || isSpeaking || sttProcessing) return
      silenceTimer = setTimeout(handleSilence, 8000)
    }

    function clearDurationTimer() {
      if (durationTimer) { clearTimeout(durationTimer); durationTimer = null }
    }

    // ตัดจบสายอัตโนมัติเมื่อคุยนานเกิน MAX_CALL_DURATION_SECONDS กันสายค้าง/ค่าใช้จ่ายบานปลาย
    async function handleMaxDuration() {
      durationTimer = null
      if (!callActive || pendingEndCall) return
      console.log(`[MaxDuration] Call exceeded ${MAX_CALL_DURATION_MS / 1000}s — closing`)

      const currentSession = callSessions.get(callSid)
      pendingEndCall = true
      if (currentSession) currentSession.hangupReason = 'max_duration'

      clearSilenceTimer()
      clearPrewarm()
      if (greetingAbortController) { greetingAbortController.abort(); greetingAbortController = null }
      if (ttsAbortController) { ttsAbortController.abort(); ttsAbortController = null }

      if (!currentSession || socket.readyState !== socket.OPEN) {
        if (socket.readyState === socket.OPEN) socket.close()
        return
      }

      isSpeaking = true
      sttProcessing = true
      const closeAbort = new AbortController()
      let sent = 0
      try {
        const closingText = 'ขอบคุณมากนะคะที่สละเวลาคุยกับหนู หนูขอตัวก่อนนะคะ'
        for await (const chunk of synthesizeSpeechStream(closingText, currentSession.campaign.voice_id, closeAbort.signal)) {
          if (socket.readyState !== socket.OPEN) break
          socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
          sent++
        }
      } catch (err) {
        console.error('[MaxDuration TTS error]', err.message)
        healthMonitor.reportError('tts', err.message)
      } finally {
        sttProcessing = false
      }

      const closeDelay = sent * 20 + 4000
      setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, closeDelay)
    }

    function isPrewarmUsable(interimText, finalText) {
      if (!interimText || !finalText) return false
      const a = interimText.trim(), b = finalText.trim()
      if (a.length >= 2 && (b.includes(a) || a.includes(b))) return true
      const n = Math.min(4, a.length, b.length)
      return n >= 2 && a.substring(0, n) === b.substring(0, n)
    }

    // Adaptive re-trigger: ถ้าลูกค้าพูดยาวกว่าที่เดาไว้พอสมควร ยกเลิกคำเดาเก่าแล้วเดาใหม่จากข้อความล่าสุด
    // เดาแค่ครั้งเดียวตอนแรกมักพลาดเวลาลูกค้าพูดยาวกว่านั้น (isPrewarmUsable ปฏิเสธ ต้องเริ่มนับ latency ใหม่ทั้งหมด)
    //
    // เคยลองเปลี่ยนเป็น debounce (รอ interim นิ่ง 350ms ก่อนค่อยยิง) เมื่อ 18 ส.ค. แล้ว rollback กลับมาใช้แบบนี้
    // เพราะวัดผลจริงจาก log พบว่า debounce แย่ลง (median ช่วงรอคำตอบพร้อมเพิ่มจาก 1.8s เป็น 3.0s) — สาเหตุคือระหว่าง
    // ลูกค้าพูดต่อเนื่อง STT ส่ง interim ใหม่มาเรื่อยๆ แทบไม่หยุดนิ่งเลยจนพูดจบจริง debounce เลย "นิ่ง" แทบไม่ทัน
    // และเสียข้อดีเดิมที่การเดาตัวแรกมักไม่โดนยกเลิก (ขยับทีละน้อยกว่า 4 ตัวอักษร) จึงสะสม head start มาตลอดทั้งประโยคได้
    function shouldRetriggerPrewarm(oldText, newText) {
      const oldLen = (oldText || '').trim().length
      const newLen = (newText || '').trim().length
      if (newLen - oldLen < 4) return false // เพิ่มขึ้นน้อยไป ไม่คุ้มยิงใหม่ (แค่ STT ขยับคำเล็กน้อย)
      if (Date.now() < prewarmRetriggerAt) return false // กันยิง Claude รัวๆ ระหว่างลูกค้าพูดยาว
      return true
    }

    function startPrewarm(session, interimText) {
      if (!callActive || isSpeaking || sttProcessing) return
      if (prewarmPromise) {
        if (!shouldRetriggerPrewarm(prewarmStartText, interimText)) return
        console.log(`[Prewarm] Re-trigger — interim grew: "${prewarmStartText}" → "${interimText}"`)
        clearPrewarm()
      }
      prewarmRetriggerAt = Date.now() + 700
      prewarmStartText = interimText
      prewarmAbort = new AbortController()
      const signal = prewarmAbort.signal
      const snap = { ...session, messages: [...session.messages, { role: 'user', content: interimText }] }
      console.log(`[Prewarm] Starting for: "${interimText}"`)
      prewarmPromise = (async () => {
        try {
          let text = ''
          for await (const chunk of askClaudeStream(snap, false, signal)) {
            if (signal.aborted) return null
            text += (text ? ' ' : '') + chunk
          }
          if (text) console.log(`[Prewarm] Ready: "${text.substring(0, 60)}"`)
          return text || null
        } catch (err) {
          if (err.name !== 'AbortError') console.error('[Prewarm] Error:', err.message)
          return null
        }
      })()
    }

    function clearPrewarm() {
      if (prewarmAbort) { prewarmAbort.abort(); prewarmAbort = null }
      prewarmPromise = null
      prewarmStartText = null
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
          healthMonitor.reportError('tts', err.message)
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
        pendingEndCall = true
        currentSession.hangupReason = 'silence_timeout'
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
      clearPrewarm()
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
    async function speakAndWait(text, session, markName, pipelineId = -1) {
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
          healthMonitor.reportError('tts', err.message)
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
        if (activePipelineId === pipelineId && isSpeaking) {
          console.log(`[Audio] Fallback unlock after ${playbackMs}ms`)
          isSpeaking = false
          startSilenceTimer()
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

        // Freeze ครั้งเดียวต่อสาย ไม่คำนวณใหม่ทุกเทิร์น — ที่ 0% ปัจจุบัน useChunkedStreaming จะเป็น false เสมอ
        rollout = decideRollout(callSid, CHUNKED_ROLLOUT_PERCENT)
        console.log(`[Rollout] callSid=${callSid} bucket=${rollout.bucket} percent=${rollout.percentAtStart} chunked=${rollout.useChunkedStreaming}`)

        console.log(`[WS] Stream started: ${streamSid}`)

        durationTimer = setTimeout(handleMaxDuration, MAX_CALL_DURATION_MS)

        // เริ่ม STT stream
        sttStream = transcribeStream(async (transcript) => {
          if (!transcript || !callActive) return
          if (pendingEndCall) return
          if (socket.readyState !== socket.OPEN) return
          if (sttProcessing) {
            // log เต็มข้อความ + จำนวนคำ ไว้เช็คความถี่/เนื้อหาจริงที่หายไป ก่อนตัดสินใจว่าคุ้มแก้เป็น queue ไหม (ยังไม่เปลี่ยนพฤติกรรม แค่วัดผล)
            const trimmed = transcript.trim()
            const wc = trimmed ? trimmed.split(/\s+/).length : 0
            console.log(`[STT] Transcript dropped (busy, ${wc} words): "${transcript}"`)
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
          const pipelineId = ++activePipelineId
          isSpeaking = true

          // C1: per-turn latency instrumentation (ยังไม่มี chunked path ให้ path='chunked' จริง เพราะ rollout=0%)
          const turnMetrics = createTurnMetrics({
            callSid,
            generationId: callState.generationId,
            path: rollout?.useChunkedStreaming ? 'chunked' : 'legacy',
            rolloutBucket: rollout?.bucket ?? null,
            rolloutPercent: rollout?.percentAtStart ?? null,
          })
          markOnce(turnMetrics, 't1')

          ttsAbortController = new AbortController()
          const signal = ttsAbortController.signal
          let fullText = ''
          let totalSent = 0

          // Safety: ถ้า Claude/TTS ค้างนานผิดปกติ ให้ unlock อัตโนมัติ
          // เช็ค pipelineId ด้วย — ถ้า pipeline นี้ถูก barge-in ทิ้งไปแล้วและมี pipeline ใหม่เริ่มทำงานอยู่
          // timer เก่านี้ต้องไม่ไปตัด isSpeaking/sttProcessing ของ pipeline ใหม่ที่กำลังทำงานปกติอยู่
          const processingGuard = setTimeout(() => {
            if (activePipelineId === pipelineId && sttProcessing) {
              console.error('[AI] sttProcessing stuck >30s — force reset')
              sttProcessing = false
              isSpeaking = false
            }
          }, 30000)

          // Capture prewarm reference — ป้องกัน pipeline เก่าล้าง prewarm ของ pipeline ใหม่
          const myPrewarm = prewarmPromise
          const myPrewarmText = prewarmStartText

          try {
            // Use pre-warmed Claude response if available and applicable
            let aiText = null
            markOnce(turnMetrics, 't2') // legacy: askClaudeStream yield ข้อความเต็มก้อนเดียว จึงไม่มี t3/t4 ที่มีความหมาย
            if (myPrewarm && isPrewarmUsable(myPrewarmText, transcript)) {
              console.log(`[Prewarm] Awaiting pre-warmed response for: "${transcript}"`)
              aiText = await myPrewarm
              if (aiText) console.log(`[Prewarm] Hit — skipping fresh Claude call`)
              else console.log(`[Prewarm] Null result — falling back to fresh call`)
            }
            if (prewarmPromise === myPrewarm) clearPrewarm()

            if (!aiText && !signal.aborted && callActive && isSpeaking) {
              for await (const chunk of askClaudeStream(currentSession, false, signal)) {
                if (signal.aborted || !callActive || !isSpeaking) break
                aiText = (aiText ? aiText + ' ' : '') + chunk
              }
            }

            if (aiText && !signal.aborted && callActive && isSpeaking) {
              console.log(`[AI] "${aiText}"`)
              fullText = aiText
              const cleanText = aiText.replace(/\[END_CALL\]/g, '').trim()
              if (cleanText) {
                try {
                  markOnce(turnMetrics, 't5')
                  for await (const chunk of synthesizeSpeechStream(cleanText, currentSession.campaign.voice_id, signal)) {
                    if (socket.readyState !== socket.OPEN || signal.aborted) break
                    markOnce(turnMetrics, 't6')
                    // จุดวัด "ความเงียบจริง" ที่ลูกค้ารู้สึก — ต่างจาก [AI full] ที่รวมเวลาพูดทั้งประโยคเข้าไปด้วย
                    // (ประโยคยาวก็ใช้เวลาส่งครบนานกว่าเป็นธรรมชาติ ไม่ได้แปลว่าดีเลย์มากขึ้น) ต้องวัดจาก [STT] ถึง log นี้เท่านั้น
                    if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                    socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                    markOnce(turnMetrics, 't7')
                    totalSent++
                  }
                } catch (err) {
                  if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
                    console.error('[TTS error]', err.message)
                    healthMonitor.reportError('tts', err.message)
                  }
                }
              }
            }

            if (fullText.includes('[END_CALL]') && shouldBlockEndCall(currentSession, fullText)) {
              console.log('[Guard] Premature END_CALL blocked — injecting follow-up question')
              const followUp = 'มีอะไรสอบถามเพิ่มเติมไหมคะ'
              fullText = fullText.replace(/\[END_CALL\]/g, '').trim() + ' ' + followUp
              try {
                markOnce(turnMetrics, 't5')
                for await (const chunk of synthesizeSpeechStream(followUp, currentSession.campaign.voice_id, signal)) {
                  if (socket.readyState !== socket.OPEN || signal.aborted) break
                  markOnce(turnMetrics, 't6')
                  // เผื่อ cleanText ว่างเปล่า (คำตอบ AI มีแค่ [END_CALL] ล้วนๆ) — ลูปหลักด้านบนไม่ได้ส่งอะไรเลย
                  // ทำให้นี่กลายเป็นก้อนเสียงแรกจริงของเทิร์นนี้ ต้อง log จุดนี้ด้วยกันพลาดข้อมูล
                  if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                  socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                  markOnce(turnMetrics, 't7')
                  totalSent++
                }
              } catch (err) {
                if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
                  console.error('[Guard TTS error]', err.message)
                  healthMonitor.reportError('tts', err.message)
                }
              }
            }
          } catch (err) {
            console.error('[AI/TTS error]', err.message)
            healthMonitor.reportError('ai_tts', err.message)
          } finally {
            clearTimeout(processingGuard)
            if (prewarmPromise === myPrewarm) clearPrewarm()
            // เช็ค pipelineId ก่อน null ทิ้ง — ไม่งั้น pipeline เก่าที่เพิ่งโดน barge-in อาจไป null ทับ
            // ttsAbortController ของ pipeline ใหม่ที่เพิ่งสร้างขึ้นมา ทำให้ barge-in ครั้งถัดไปตัดเสียง AI ไม่ขาด
            if (activePipelineId === pipelineId) {
              ttsAbortController = null
              sttProcessing = false
            }
          }

          if (fullText) {
            currentSession.messages.push({ role: 'assistant', content: fullText })
            console.log(`[AI full] "${fullText}"`)
          }

          console.log('[Metrics]', JSON.stringify({ ...turnMetrics, ...computeDerivedMetrics(turnMetrics) }))

          if (!signal?.aborted && isSpeaking && socket.readyState === socket.OPEN && totalSent > 0) {
            socket.send(JSON.stringify({ event: 'mark', streamSid, mark: { name: 'ai_done' } }))
          } else if (totalSent === 0) {
            isSpeaking = false
          }

          const playbackMs = totalSent * 20 + 1500
          setTimeout(() => {
            if (activePipelineId === pipelineId && isSpeaking) {
              console.log('[Audio] Fallback unlock')
              isSpeaking = false
              startSilenceTimer()
            }
          }, playbackMs)

          if (fullText.includes('[END_CALL]')) {
            pendingEndCall = true
            currentSession.hangupReason = 'ai_ended'
            // Fallback: ปิดสายถ้า mark ไม่มาภายในเวลาที่คาดไว้
            const fallbackDelay = totalSent * 20 + 5000
            setTimeout(() => { if (socket.readyState === socket.OPEN) socket.close() }, fallbackDelay)
          }
        }, (interimText) => {
          if (!callActive || isSpeaking || sttProcessing || bargeInCooldown) return
          clearSilenceTimer()
          silencePromptCount = 0
          const session = callSessions.get(callSid)
          if (session) startPrewarm(session, interimText)
        })

        // AI ทักทายก่อนเลย — ใช้ pre-generated audio ถ้ามี (ลด latency)
        const playGreeting = async () => {
          const session = callSessions.get(callSid)
          if (!session || !callActive) return
          isSpeaking = true
          const pipelineId = ++activePipelineId
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
              setTimeout(() => { if (activePipelineId === pipelineId && isSpeaking) { console.log('[Audio] Fallback unlock (greeting)'); isSpeaking = false; startSilenceTimer() } }, playbackMs)
              console.log(`[Greeting] Sent ${sent} pre-generated chunks`)
            } else {
              // Fallback: generate ใหม่ถ้า pre-gen ไม่สำเร็จ
              console.log(`[Greeting] Pre-gen not ready, generating now...`)
              const greeting = await askClaude(session)
              console.log(`[Greeting] "${greeting.substring(0, 100)}"`)
              session.messages.push({ role: 'assistant', content: greeting })
              await speakAndWait(greeting, session, 'greeting_done', pipelineId)
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
        if (!bargeInCooldown && !sttProcessing) sttStream?.reset()
        startSilenceTimer()
      }

      if (msg.event === 'stop') {
        console.log(`[WS] Stream stopped: ${callSid}`)
        callActive = false
        clearSilenceTimer()
        clearDurationTimer()
        clearPrewarm()
        endCall(callState)
        if (sttStream) { sttStream.end(); sttStream = null }
      }
    })

    socket.on('close', () => {
      console.log(`[WS] Disconnected: ${callSid}`)
      callActive = false
      clearPrewarm()
      clearSilenceTimer()
      clearDurationTimer()
      endCall(callState)
      if (sttStream) { sttStream.end(); sttStream = null }
      // ถ้ายังไม่มีเหตุผลปิดสายถูก tag ไว้เลย (ไม่ใช่ AI ปิดปกติ/หมดเวลาเงียบ) แปลว่าอีกฝั่งวางสายเอง
      const endedSession = callSessions.get(callSid)
      if (endedSession && !endedSession.hangupReason) endedSession.hangupReason = 'customer_hangup'
    })

    socket.on('error', (err) => {
      console.error(`[WS] Error for ${callSid}:`, err.message)
    })
  })
}

module.exports = { registerWebSocket }
