const callSessions = require('../utils/callSessions')
const { transcribeStream } = require('../services/googleSTT')
const { askClaude, askClaudeStream } = require('../services/claude')
const { synthesizeSpeechStream } = require('../services/tts')
const healthMonitor = require('../utils/healthMonitor')
const { createCallState, bumpGeneration, isCurrentGeneration, endCall } = require('../utils/generationGuard')
const { decideRollout } = require('../utils/rolloutBucket')
const { createTurnMetrics, markOnce, computeDerivedMetrics } = require('../utils/turnMetrics')
const { createTurnState, markTtsPending, markAudioCommitted, markDone, claimFallback } = require('../utils/turnState')
const { runChunkedTurn, speakFixedText } = require('./chunkedTurn')
const { runAttemptWithWatchdog } = require('../utils/attemptWithWatchdog')
const { createRolloutConfig } = require('../utils/rolloutConfig')
const { performance } = require('perf_hooks')

const MAX_CALL_DURATION_MS = (parseInt(process.env.MAX_CALL_DURATION_SECONDS) || 300) * 1000

// Checkpoint C5: % rollout ของ chunked-streaming path มาจาก Google Sheets แล้ว (แท็บ "Streaming Config")
// ผ่าน background polling ของตัวเอง ไม่ใช่ hardcoded คงที่แบบ C0 อีกต่อไป — start() ครั้งเดียวตอน module load
// (เท่ากับตอน process bootstrap เพราะไฟล์นี้ถูก require ครั้งเดียวตอนเริ่ม server) ให้เริ่ม poll ทันที ไม่ต้องรอ
// สายแรกเข้ามาก่อนถึงจะ trigger fetch ครั้งแรก — cold start ก่อน fetch สำเร็จครั้งแรกยังคง 0% เสมอ (ปลอดภัยไว้ก่อน)
const rolloutConfig = createRolloutConfig()
rolloutConfig.start()

// Checkpoint C4b — เวลาสูงสุดที่ยอมรอ Claude ส่ง delta แรกของ chunked path มา ก่อนจะเลิกรอแล้ว fallback ไป
// legacy แทน ตั้งไว้เผื่อเหนือ TTFT ปกติของ Claude พอสมควร (ปกติต่ำกว่า 1-2s มาก) แต่ไม่นานจนลูกค้ารอเงียบเกินไป
const CLAUDE_FIRST_DELTA_TIMEOUT_MS = 3000

// Watchdog B — เวลาสูงสุดที่ยอมรอ "safe chunk" ก้อนแรกจาก speechChunker หลังจาก delta แรกมาถึงแล้ว (t3→t4)
// ต้องเผื่อ margin เหนือ speechChunker เอง (HARD_MAX_MS=800 ในนั้น) พอสมควร ไม่งั้น watchdog ภายนอกจะฆ่า turn
// ก่อนที่ chunker จะได้โอกาสทำ fallback boundary ของตัวเองครบตามสัญญา — เผื่อ margin ~2.5 เท่า
const CHUNK_READY_TIMEOUT_MS = 2000

// Watchdog C — เวลาสูงสุดที่ยอมรอ ElevenLabs ส่ง audio ก้อนแรกกลับมาหลังจาก TTS request แรกของทั้งเทิร์นเริ่ม
// (t5→t6) ยังไม่มี baseline ttsTTFB จริงจาก production (rollout ยัง 0%) ใช้ค่าตาม roadmap เดิมไปก่อน — ควร tune
// ด้วยข้อมูลจริงจาก [Metrics] log ก่อนเปิด rollout เกิน 0%
const TTS_FIRST_AUDIO_TIMEOUT_MS = 2000

// Checkpoint C4c follow-up (production incident 2026-08-19, C6c barge-in retry call) — fallback watchdog เดิม
// เป็น total-duration timeout ครอบ "ความพยายาม fallback ทั้งก้อน" เดียว ซึ่งพิสูจน์แล้วจาก production ว่าอันตราย:
// ถ้า audio ก้อนแรกคอมมิตไปถึงลูกค้าแล้วก่อน 8s แต่ ElevenLabs stream ที่เหลือช้า/ค้าง watchdog เดิมจะ abort()
// signal ที่ synthesizeAndSend ใช้อยู่ ตัดเสียงกลางประโยคทั้งที่ลูกค้าได้ยินไปบางส่วนแล้ว แถม runAttemptWithWatchdog
// ทิ้งผลลัพธ์ของฝ่ายแพ้ race เสมอ (by design ถูกต้องสำหรับ watchdog A/B/C ที่ไม่มีอะไรคอมมิตมาก่อน) ทำให้
// totalSent/fullText ของ audio ที่ส่งไปแล้วจริงหายไปเป็น 0/'' ด้วย — turn ถัดไปไม่รู้ว่า AI เพิ่งพูดอะไรไป
//
// แก้เป็น 2 phase แยกจากกันชัดเจน ผ่าน armWatchdog rearm (กลไกเดียวกับ Watchdog A→B→C):
//   Phase 1 (pre-commit)  — FALLBACK_PRECOMMIT_TIMEOUT_MS ตั้งแต่ fallback เริ่ม จนกว่าจะมี audio commit ก้อนแรก
//                           ถ้าหมดเวลาก่อน commit: abort ได้เต็มที่เหมือนเดิม (audioCommitted ยังเป็น false แน่นอน)
//   Phase 2 (post-commit) — FALLBACK_IDLE_TIMEOUT_MS แบบ rolling (rearm ใหม่ทุกครั้งที่มีก้อนเสียงส่งสำเร็จ ผ่าน
//                           onAudioSent hook ใน chunkedTurn.js) วัด "หยุดนิ่งไปนานแค่ไหน" ไม่ใช่ "รวมนานแค่ไหน"
//                           เพราะเทิร์นที่พูดยาวโดยธรรมชาติ (>8s ทั้งเทิร์น) ต้องไม่ถูกตัดกลางคันทั้งที่ progress ปกติ
//
// commit (turnState.audioCommitted) คือ source of truth เดียวที่บอกว่าอยู่ phase ไหน — ไม่ใช่ timestamp/ตัวนับเอง
const FALLBACK_PRECOMMIT_TIMEOUT_MS = 8000
const FALLBACK_IDLE_TIMEOUT_MS = 6000

function shouldBlockEndCall(session, aiResponse) {
  const userMessages = session.messages.filter(m => m.role === 'user')
  const lastUserMsg = userMessages.at(-1)?.content ?? ''
  const hasNegation = lastUserMsg.includes('ไม่') || lastUserMsg.includes('ยังไม่')
  const hasInterest = ['สนใจ', 'อยากลอง', 'อยากสมัคร', 'สมัครเลย'].some(k => lastUserMsg.includes(k))
  if (!hasInterest || hasNegation) return false
  return !aiResponse.includes('เพิ่มเติม')
}

// Checkpoint C4a — เมื่อ chunked path พังก่อน commit เสียง ลอง fallback ไปใช้ Claude/TTS เดิม (legacy)
// สำหรับเทิร์นเดียวกันนี้ แทนที่จะปล่อยให้ลูกค้าเงียบไปเฉยๆ ไม่ bump generation เพราะนี่คือการกู้ turn เดิม
// ไม่ใช่ turn ใหม่ — เป็น adapter ระหว่าง legacy transport ([END_CALL] string marker) กับรูปแบบที่ chunked
// branch ใช้อยู่แล้ว (endCallRequested boolean) ก่อน TTS เสมอ กัน marker หลุดเข้าไปให้ speakFixedText() พูดออกไปจริง
async function runLegacyFallback({ session, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onAudioSent }) {
  let rawText = null
  for await (const chunk of askClaudeStream(session, false, signal)) {
    if (signal.aborted) break
    rawText = (rawText ? rawText + ' ' : '') + chunk
  }
  if (!rawText || signal.aborted) return { fullText: '', endCallRequested: false, totalSent: 0 }

  const legacyEndCallRequested = rawText.includes('[END_CALL]')
  const spokenText = rawText.replace(/\[END_CALL\]/g, '').trim()

  if (!spokenText || !isCurrentGeneration(callState, generationId)) {
    return { fullText: spokenText, endCallRequested: legacyEndCallRequested, totalSent: 0 }
  }

  const result = await speakFixedText({ text: spokenText, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, startingSentCount: 0, onAudioSent })
  return { fullText: spokenText, endCallRequested: legacyEndCallRequested, totalSent: result.sentCount }
}

// ใช้ policy เดียวกันไม่ว่า end_call intent จะมาจาก tool call ปกติของ chunked path หรือจาก [END_CALL] marker
// ที่ normalize มาจาก legacy fallback แล้ว — ทั้งสองทางเข้าที่นี่ในรูป endCallRequested boolean เดียวกันเสมอ
async function applyChunkedEndCallGuard({ endCallRequested, fullText, totalSent, currentSession, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId }) {
  if (!endCallRequested || !shouldBlockEndCall(currentSession, fullText)) {
    return { fullText, endCallRequested, totalSent }
  }
  console.log('[Guard] Premature END_CALL blocked — injecting follow-up question (chunked)')
  const followUp = 'มีอะไรสอบถามเพิ่มเติมไหมคะ'
  let newTotalSent = totalSent
  try {
    const followUpResult = await speakFixedText({ text: followUp, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, startingSentCount: totalSent })
    newTotalSent += followUpResult.sentCount
  } catch (err) {
    if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
      console.error('[Guard TTS error]', err.message)
      healthMonitor.reportError('tts', err.message)
    }
  }
  return { fullText: fullText.trim() + ' ' + followUp, endCallRequested: false, totalSent: newTotalSent }
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
      bumpGeneration(callState) // C2: invalidate ก่อนทุกอย่าง — ยัง observational, ไม่ได้ใช้ gate การ abort จริงที่อยู่ถัดไป
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

        // Freeze ครั้งเดียวต่อสาย ไม่คำนวณใหม่ทุกเทิร์น — อ่าน % ปัจจุบันจาก memory cache (rolloutConfig.js)
        // แค่ตอนนี้จุดเดียว ไม่อ่านซ้ำอีกเลยตลอดสาย แม้ % ใน Sheets จะเปลี่ยนกลางสายก็ไม่กระทบสายที่ freeze ไปแล้ว
        rollout = decideRollout(callSid, rolloutConfig.getCurrentRolloutPercent())
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

          // C2: sync generationId/turnState กับ turn lifecycle จริง — ยัง observational เท่านั้น
          // (ไม่มี isCurrentGeneration gate ใน legacy path ตอนนี้ — activePipelineId/signal.aborted เดิมยังคุม cancellation ทั้งหมด)
          const generationId = bumpGeneration(callState)
          const turnState = createTurnState(generationId)

          // C1: per-turn latency instrumentation (ยังไม่มี chunked path ให้ path='chunked' จริง เพราะ rollout=0%)
          const turnMetrics = createTurnMetrics({
            callSid,
            generationId,
            path: rollout?.useChunkedStreaming ? 'chunked' : 'legacy',
            rolloutBucket: rollout?.bucket ?? null,
            rolloutPercent: rollout?.percentAtStart ?? null,
          })
          markOnce(turnMetrics, 't1')

          ttsAbortController = new AbortController()
          const signal = ttsAbortController.signal
          let fullText = ''
          let totalSent = 0
          let endCallRequested = false // C3a: signaled by chunkedTurn.js's onControl (end_call tool) — separate channel from legacy's [END_CALL] text marker

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

          // C3a: ตัดสิน branch ก่อน Claude side effect แรกเสมอ — ห้ามมี code เรียก Claude ก่อนจุดนี้ไม่ว่า path ไหน
          // rollout ยัง 0% เสมอตอนนี้ จึงยังไม่มีสายไหนเข้า branch นี้จริงในโปรดักชัน (ดู C0/decideRollout)
          if (rollout.useChunkedStreaming) {
            try {
              // C6c follow-up: legacy มี t2 (Claude request sent) แต่ chunked branch ไม่เคยถูกใส่ไว้เลยตั้งแต่ C1 —
              // เจอจาก production trace จริงที่ t2 เป็น null ทุกเทิร์น ทำให้ claudeTTFT (t2→t3) และ requestToAudio
              // (t2→t7) วัดไม่ได้เลย มาร์กตรงนี้ (จุดเดียวกับที่ legacy มาร์ก คือ "กำลังจะเริ่มขอคำตอบ" ก่อนเรียก
              // Claude จริง) ไม่กระทบ branch decision invariant เพราะเป็นแค่ timestamp ไม่ใช่ side effect
              markOnce(turnMetrics, 't2')
              // C4b: race runChunkedTurn ต่อ watchdog สามวงเรียงกัน (Watchdog A → B → C) ผ่าน child AbortController
              // เดียวที่ compose มาจาก outer signal (barge-in) — barge-in ยังฆ่าทั้งคู่ได้เสมอ แต่ watchdog ฆ่าได้แค่
              // chunked attempt นี้เท่านั้น ไม่แตะ outer signal เลย เพื่อให้ fallback ด้านล่าง (ที่ใช้ outer signal)
              // ยังทำงานได้จริง — A (CLAUDE_FIRST_DELTA_TIMEOUT) ตั้งไว้ก่อนเริ่ม, rearm เป็น B (CHUNK_READY_TIMEOUT)
              // ตอน t3, disarm ตอน t4 (ไม่ rearm ทันที — ช่องว่างจนกว่า TTS request แรกจะเริ่มจริงคือ intentional gap),
              // แล้ว rearm เป็น C (TTS_FIRST_AUDIO_TIMEOUT) ตอน t5 (TTS request แรกของทั้งเทิร์นเริ่มจริง ไม่ใช่ตอน
              // chunk ถูก dequeue), disarm ตอน t6 — first-only ทั้งคู่ ไม่ rearm ตาม speech chunk ถัดๆ ไป
              const attempt = await runAttemptWithWatchdog({
                signal,
                timeoutMs: CLAUDE_FIRST_DELTA_TIMEOUT_MS,
                reason: 'CLAUDE_FIRST_DELTA_TIMEOUT',
                run: (childSignal, armWatchdog) => runChunkedTurn({
                  session: currentSession,
                  signal: childSignal,
                  socket,
                  streamSid,
                  voiceId: currentSession.campaign.voice_id,
                  turnMetrics,
                  turnState,
                  callState,
                  generationId,
                  onControl: (control) => { if (control?.type === 'end_call') endCallRequested = true },
                  onFirstDelta: () => armWatchdog(CHUNK_READY_TIMEOUT_MS, 'CHUNK_READY_TIMEOUT'),
                  onFirstChunk: () => armWatchdog(),
                  onFirstTtsRequest: () => armWatchdog(TTS_FIRST_AUDIO_TIMEOUT_MS, 'TTS_FIRST_AUDIO_TIMEOUT'),
                  onFirstTtsAudio: () => armWatchdog(),
                }),
              })

              if (attempt.outcome === 'success') {
                fullText = attempt.result.fullText
                totalSent = attempt.result.totalSent

                // C3c/C4a: parity กับ legacy's shouldBlockEndCall guard — ใช้ helper เดียวกันไม่ว่า endCallRequested
                // จะมาจาก chunked path ปกติ (ตรงนี้) หรือจาก legacy fallback ด้านล่าง (normalize มาแล้วเหมือนกัน)
                const guarded = await applyChunkedEndCallGuard({
                  endCallRequested, fullText, totalSent, currentSession, signal, socket, streamSid,
                  voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                })
                fullText = guarded.fullText
                endCallRequested = guarded.endCallRequested
                totalSent = guarded.totalSent
              } else if (attempt.outcome === 'aborted') {
                // C4c: barge-in ยกเลิก attempt นี้กลางทาง (ไม่ใช่ watchdog/error) — bargeIn() จัดการ cleanup
                // (clear event, isSpeaking/sttProcessing reset) ไปแล้วแยกต่างหาก ห้าม fallback ต่อเด็ดขาด
                // เพราะลูกค้ากำลังพูดแทรกอยู่ พยายามพูดอะไรตอนนี้ผิดหลักการเดียวกับที่ C3b ทั้งชุดกันไว้
                console.log('[Chunked] Attempt aborted by barge-in — no fallback, turn ends with no audio')
              } else {
                // outcome: 'timeout' (watchdog) หรือ 'error' (Claude/TTS จริง) — ทั้งสองทางวิ่งเข้า fallback gate เดียวกัน
                // runAttemptWithWatchdog abort child ไปให้แล้วก่อน return ในทั้งสองกรณี ไม่ต้อง abort เพิ่มที่นี่
                let triggerReason
                if (attempt.outcome === 'timeout') {
                  triggerReason = attempt.reason // CLAUDE_FIRST_DELTA_TIMEOUT / CHUNK_READY_TIMEOUT / TTS_FIRST_AUDIO_TIMEOUT
                  console.log(`[Watchdog] ${attempt.reason} — chunked attempt aborted, considering fallback`)
                } else {
                  // C4c: แยก CLAUDE_ERROR/TTS_ERROR จาก tag ที่ chunkedTurn.js ใส่ไว้ให้แล้ว แทนป้ายรวมๆ เดิม —
                  // สำคัญกับการวิเคราะห์ตอน rollout จริง เพราะ Claude กับ ElevenLabs พังคนละสาเหตุคนละทางแก้กัน
                  triggerReason = attempt.error.source === 'TTS' ? 'TTS_ERROR' : 'CLAUDE_ERROR'
                  console.error('[AI/TTS error]', attempt.error.message)
                  healthMonitor.reportError('ai_tts', attempt.error.message)
                }

                // C4a/C4b: chunked พังหรือหมดเวลาก่อน commit เสียง → ลอง fallback ไปใช้ legacy Claude/TTS สำหรับ
                // เทิร์นนี้แทน ปล่อยลูกค้าเงียบไปเฉยๆ ไม่ bump generation เพราะนี่คือการกู้ turn เดิม ไม่ใช่ turn ใหม่
                if (isCurrentGeneration(callState, generationId) && claimFallback(turnState)) {
                  turnMetrics.fallbackTriggered = true
                  turnMetrics.fallbackReason = triggerReason
                  turnMetrics.fallbackStartedAt = performance.now()
                  console.log('[Fallback] Falling back to legacy Claude/TTS for this turn')

                  // C4c follow-up: 2-phase commit-aware watchdog — pre-commit terminal timeout, แทนที่ด้วย
                  // rolling post-commit idle timeout ทันทีที่มีก้อนเสียงคอมมิตจริง (ผ่าน onAudioSent ที่ rearm
                  // ทุกครั้ง — ก้อนแรกก็เข้ามาแทนที่ precommit timer ไปในตัวเพราะ arm() clear timer เดิมก่อนตั้งใหม่เสมอ)
                  //
                  // fallbackProgress คือ side-channel นอก race โดยตั้งใจ — runAttemptWithWatchdog ทิ้งผลลัพธ์ของ
                  // ฝ่ายแพ้ race เสมอเมื่อ watchdog ชนะ (ถูกต้องสำหรับ pre-commit ที่ยังไม่มีอะไรคอมมิต) แต่ totalSent
                  // ที่คอมมิตไปแล้วจริงก่อน timeout/error ต้องรอดจากการทิ้งนั้นได้ จึงเก็บนอก return value ของ run()
                  const fallbackProgress = { totalSent: 0 }
                  const fallbackAttempt = await runAttemptWithWatchdog({
                    signal,
                    timeoutMs: FALLBACK_PRECOMMIT_TIMEOUT_MS,
                    reason: 'FALLBACK_TIMEOUT',
                    run: (fallbackSignal, armWatchdog) => runLegacyFallback({
                      session: currentSession, signal: fallbackSignal, socket, streamSid,
                      voiceId: currentSession.campaign.voice_id, turnMetrics, turnState, callState, generationId,
                      onAudioSent: () => {
                        // granularity เดียวกับ totalSent เดิมเป๊ะ — นับก้อนที่ synthesizeAndSend ส่งสำเร็จ ไม่ใช่ raw
                        // ElevenLabs/Twilio byte frame (ถ้าวันหลังอยากนับ frame แยกต่างหาก ใช้ field ใหม่คนละชื่อ)
                        fallbackProgress.totalSent++
                        armWatchdog(FALLBACK_IDLE_TIMEOUT_MS, 'FALLBACK_PARTIAL_TIMEOUT')
                      },
                    }),
                  })

                  if (fallbackAttempt.outcome === 'success' || fallbackAttempt.outcome === 'aborted') {
                    const fb = fallbackAttempt.result
                    // เช็คซ้ำหลัง await ยาว (askClaudeStream) — ห้ามเอาผลลัพธ์ของ generation ที่ stale ไปแล้วมาใช้
                    // ต่อ ไม่งั้น [END_CALL] เก่าอาจไปสั่ง hangup ทั้งที่ลูกค้ากำลังคุยกับ generation ใหม่อยู่
                    if (isCurrentGeneration(callState, generationId)) {
                      turnMetrics.fallbackOutcome = 'SPOKEN'
                      const guarded = await applyChunkedEndCallGuard({
                        endCallRequested: fb.endCallRequested, fullText: fb.fullText, totalSent: fb.totalSent,
                        currentSession, signal, socket, streamSid, voiceId: currentSession.campaign.voice_id,
                        turnMetrics, turnState, callState, generationId,
                      })
                      fullText = guarded.fullText
                      endCallRequested = guarded.endCallRequested
                      totalSent = guarded.totalSent
                    } else {
                      turnMetrics.fallbackOutcome = 'STALE'
                      console.log('[Fallback] Generation went stale while legacy fallback was in flight — discarding its result')
                    }
                  } else if (fallbackAttempt.outcome === 'timeout' || fallbackAttempt.outcome === 'error') {
                    // production incident 2026-08-19: turnState.audioCommitted (ไม่ใช่ outcome type เฉยๆ) คือตัว
                    // ตัดสินใจเดียวว่านี่คือ "ไม่มีเสียงออกเลย" หรือ "มีเสียงออกไปแล้วบางส่วนก่อนพัง" — ต้องแยกให้ชัด
                    // ไม่งั้น downstream (ai_done mark ไปยัง Twilio, playback-unlock timer) จะเข้าใจผิดว่าไม่มีเสียงออก
                    // ทั้งที่ลูกค้าได้ยินไปแล้วจริง (นี่คือบั๊กที่เจอจาก production trace ของสายทดสอบ barge-in retry)
                    if (turnState.audioCommitted) {
                      totalSent = fallbackProgress.totalSent
                      // ระบบไม่มี text↔audio word-level mapping จริง — ห้ามใส่ spokenText เต็มก้อนเป็น assistant
                      // history เพราะไม่รู้ว่าพูดถึงคำไหนจริง ใส่ marker ตรงไปตรงมาแทน ให้ Claude เทิร์นถัดไปรู้บริบท
                      // โดยไม่ assume ว่าลูกค้าได้ยินครบ
                      fullText = '[ระบบ: คำตอบก่อนหน้าถูกขัดจังหวะหลังเริ่มส่งเสียง ลูกค้าอาจได้ยินเพียงบางส่วน]'
                      // ไม่ไว้ใจ end_call intent จากคำตอบที่อาจไม่สมบูรณ์ — claimFallback เป็น single-fire อยู่แล้ว จึงไม่มีทาง retry fallback ซ้ำอีกในเทิร์นนี้ด้วย
                      endCallRequested = false
                      if (fallbackAttempt.outcome === 'timeout') {
                        turnMetrics.fallbackOutcome = 'FALLBACK_PARTIAL_TIMEOUT'
                        console.error(`[Fallback] FALLBACK_PARTIAL_TIMEOUT — audio stalled ${FALLBACK_IDLE_TIMEOUT_MS}ms after ${fallbackProgress.totalSent} chunk(s) already committed`)
                      } else {
                        turnMetrics.fallbackOutcome = 'FALLBACK_PARTIAL_ERROR'
                        console.error(`[Fallback error after commit] ${fallbackAttempt.error.message} — ${fallbackProgress.totalSent} chunk(s) already committed`)
                        healthMonitor.reportError('ai_tts_fallback', fallbackAttempt.error.message)
                      }
                    } else if (fallbackAttempt.outcome === 'timeout') {
                      turnMetrics.fallbackOutcome = 'FALLBACK_TIMEOUT'
                      console.error('[Fallback] FALLBACK_TIMEOUT — giving up before any audio committed, no further recovery attempted')
                    } else {
                      turnMetrics.fallbackOutcome = 'FALLBACK_ERROR'
                      console.error('[Fallback error]', fallbackAttempt.error.message)
                      healthMonitor.reportError('ai_tts_fallback', fallbackAttempt.error.message)
                    }
                  }
                }
              }
            } catch (err) {
              console.error('[AI/TTS error]', err.message)
              healthMonitor.reportError('ai_tts', err.message)
            } finally {
              clearTimeout(processingGuard)
              if (prewarmPromise === myPrewarm) clearPrewarm()
              if (activePipelineId === pipelineId) {
                ttsAbortController = null
                sttProcessing = false
              }
            }
          } else {
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
                  markTtsPending(turnState)
                  for await (const chunk of synthesizeSpeechStream(cleanText, currentSession.campaign.voice_id, signal)) {
                    if (socket.readyState !== socket.OPEN || signal.aborted) break
                    markOnce(turnMetrics, 't6')
                    // จุดวัด "ความเงียบจริง" ที่ลูกค้ารู้สึก — ต่างจาก [AI full] ที่รวมเวลาพูดทั้งประโยคเข้าไปด้วย
                    // (ประโยคยาวก็ใช้เวลาส่งครบนานกว่าเป็นธรรมชาติ ไม่ได้แปลว่าดีเลย์มากขึ้น) ต้องวัดจาก [STT] ถึง log นี้เท่านั้น
                    if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                    socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                    markOnce(turnMetrics, 't7')
                    markAudioCommitted(turnState) // อยู่หลัง signal.aborted/readyState check (legacy staleness ของลูปนี้) และหลัง socket.send() จริงเท่านั้น
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
                markTtsPending(turnState)
                for await (const chunk of synthesizeSpeechStream(followUp, currentSession.campaign.voice_id, signal)) {
                  if (socket.readyState !== socket.OPEN || signal.aborted) break
                  markOnce(turnMetrics, 't6')
                  // เผื่อ cleanText ว่างเปล่า (คำตอบ AI มีแค่ [END_CALL] ล้วนๆ) — ลูปหลักด้านบนไม่ได้ส่งอะไรเลย
                  // ทำให้นี่กลายเป็นก้อนเสียงแรกจริงของเทิร์นนี้ ต้อง log จุดนี้ด้วยกันพลาดข้อมูล
                  if (totalSent === 0) console.log('[TTS] First audio chunk sent')
                  socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: chunk.toString('base64') } }))
                  markOnce(turnMetrics, 't7')
                  markAudioCommitted(turnState) // อยู่หลัง signal.aborted/readyState check (legacy staleness ของลูปนี้) และหลัง socket.send() จริงเท่านั้น
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
          } // end legacy else branch (C3a)

          if (fullText) {
            currentSession.messages.push({ role: 'assistant', content: fullText })
            console.log(`[AI full] "${fullText}"`)
          }

          markDone(turnState) // ทุก pre-terminal phase ไปจบที่ DONE ได้ตรงๆ (turnState.js ไม่ guard transition นี้) — ครอบคลุมทุกทางจบของ legacy turn
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

          if (fullText.includes('[END_CALL]') || endCallRequested) {
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
          // C3c: prewarm ยิง askClaudeStream เดิมเสมอ ไม่เกี่ยวกับ chunked path เลย — สายที่ freeze เป็น chunked
          // bucket ไม่ควรยิง Claude request ที่ไม่มีทางถูกใช้นี้ (เสียเงิน/เพิ่ม concurrency โดยเปล่าประโยชน์)
          if (rollout?.useChunkedStreaming) return
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
