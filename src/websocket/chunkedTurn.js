// Checkpoint C3a — orchestrates one assistant turn on the new chunked-streaming path.
//
// Producer (Claude delta → speechChunker) and consumer (TTS → Twilio) run concurrently over a FIFO
// queue, so a slow/blocking TTS call never stalls consumption of the Claude stream — the TTS
// consumer still processes exactly one chunk at a time (no parallel TTS) to keep audio order intact.
//
// Ownership split (locked before writing this file, per C3a review):
//   chunkedTurn.js  — askClaudeStreamChunked, delta accumulation, speech chunking, TTS FIFO queue,
//                      synthesizeSpeechStream, Twilio send, t3-t7, markTtsPending/markAudioCommitted,
//                      forwarding control events (end_call) via onControl
//   audioStream.js  — rollout decision, generationId, turnState creation, barge-in lifecycle,
//                      markDone, end-call policy, legacy-vs-chunked branch selection
//
// Checkpoint C3b — isCurrentGeneration() guards at 5 boundaries, locked before writing:
//   1. Claude delta received        (producer, top of for-await body)
//   2. chunker output / enqueue     (producer, right before enqueue())
//   3. before TTS request           (consumer, right before synthesizeSpeechStream call)
//   4+5. ElevenLabs audio arrival + before Twilio send — combined into one check, since nothing
//        async separates "audio chunk arrived" from "about to send" in this loop body
// A stale check must block BEFORE any markOnce()/turnState transition too, not just before the
// side effect (send/enqueue) — a Gen 12 callback arriving after Gen 13 started must not touch t6,
// t7, or turnState at all, not merely be denied socket.send(). Guard-then-mark-then-act everywhere.
//
// This is defense in depth on top of `signal` — in the real call path, bumpGeneration() always runs
// immediately before abort() (barge-in's invalidate-before-abort ordering, C2), so in practice both
// become stale together. isCurrentGeneration() covers the edge cases signal.aborted alone would miss.
//
// Error contract: does NOT swallow errors — a real Claude or TTS error rejects the promise this
// function returns, left for the caller (audioStream.js) to catch/log/report, same as the legacy
// block already does for its own errors. The one exception is abort-driven cancellation (barge-in):
// detected via signal.aborted at the moment of catch — not by error class/name, since that's robust
// regardless of which underlying HTTP client threw it — which ends the turn cleanly with no error.
const { performance } = require('perf_hooks')
const { askClaudeStreamChunked } = require('../services/claude')
const { synthesizeSpeechStream } = require('../services/tts')
const { findChunkBoundary } = require('../utils/speechChunker')
const { markOnce } = require('../utils/turnMetrics')
const { markTtsPending, markAudioCommitted } = require('../utils/turnState')
const { isCurrentGeneration } = require('../utils/generationGuard')

// Checkpoint C3c — logic ของ "พูด text หนึ่งก้อนผ่าน TTS พร้อม guard เต็มชุด" ถูกแยกมาไว้ที่เดียว ใช้ร่วมกัน
// ทั้งจาก consumer loop ปกติ (ทีละ chunk จาก Claude) และจาก speakFixedText() (ข้อความคงที่ เช่น follow-up
// question ตอน blocked end_call) — กัน guard สองชุดที่ทำหน้าที่เดียวกัน drift ออกจากกันในอนาคต
//
// startingSentCount: จำนวน audio chunk ที่ส่งไปแล้วของทั้งเทิร์น (ก่อนเรียกฟังก์ชันนี้) — ใช้ตัดสิน "นี่คือก้อน
// แรกของทั้งเทิร์นจริงไหม" (สำหรับ [TTS] First audio chunk sent log และ onFirstAudioSent) ให้ตรงกับความหมาย
// เดียวกับที่ legacy ใช้ totalSent ตัวเดียวร่วมกันระหว่าง primary block กับ guard/follow-up block
//
// onFirstTtsRequest/onFirstTtsAudio (C4b Watchdog C) เป็น first-only ของทั้งเทิร์น เหมือน t5/t6 เอง — ถ้า chunk
// #1 คอมมิตเสียงไปแล้วและ chunk #2 เริ่ม TTS ใหม่ ฮุคเหล่านี้จะไม่ยิงซ้ำ (เช็คจาก turnMetrics.t5/t6 == null ก่อน
// markOnce เสมอ ไม่ใช่ per-call) — Watchdog C วัดแค่ "ได้ audio ก้อนแรกของทั้งเทิร์นไหม" ไม่ใช่ทุก TTS chunk
async function synthesizeAndSend({ text, signal, socket, streamSid, voiceId, turnMetrics, turnState, isCurrent, startingSentCount, onFirstAudioSent, onFirstTtsRequest, onFirstTtsAudio }) {
  if (!isCurrent() || signal?.aborted) return 0

  const isFirstTtsRequest = turnMetrics.t5 == null
  markOnce(turnMetrics, 't5')
  markTtsPending(turnState) // no-op ถ้า phase ไม่ใช่ GENERATING แล้ว (เช่น AUDIO_COMMITTED ไปแล้วจากข้อความก่อนหน้าในเทิร์นเดียวกัน) — monotonic โดย turnState.js เอง ไม่มีทาง regress
  if (isFirstTtsRequest) onFirstTtsRequest?.() // arm Watchdog C ตอน TTS request เริ่มจริง ไม่ใช่ตอน chunk ถูก dequeue จาก queue

  let sentCount = 0
  for await (const audioChunk of synthesizeSpeechStream(text, voiceId, signal)) {
    // boundary 4+5 รวมกัน (ElevenLabs audio arrival + ก่อน socket.send) — ไม่มี async gap คั่นสองจุดนี้จริง
    // นี่คือ "ด่านสุดท้าย" ที่สำคัญที่สุด: ต้องเช็คก่อนแม้แต่ markOnce(t6)/markAudioCommitted ไม่ใช่แค่ก่อน send
    if (!isCurrent()) break
    if (signal?.aborted) break
    if (socket.readyState !== socket.OPEN) break

    const isFirstTtsAudio = turnMetrics.t6 == null
    markOnce(turnMetrics, 't6')
    if (isFirstTtsAudio) onFirstTtsAudio?.() // disarm Watchdog C — first-only เช่นกัน
    if (startingSentCount + sentCount === 0) console.log('[TTS] First audio chunk sent')
    socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: audioChunk.toString('base64') } }))
    markOnce(turnMetrics, 't7')
    markAudioCommitted(turnState)
    if (startingSentCount + sentCount === 0) onFirstAudioSent?.()
    sentCount++
  }
  return sentCount
}

// พูดข้อความคงที่หนึ่งก้อน (ไม่ผ่าน Claude/chunker) ด้วย guard เดียวกับ consumer loop ปกติทุกประการ — ใช้กับ
// follow-up question ตอน shouldBlockEndCall() บล็อกการวางสายก่อนเวลาอันควร (audioStream.js เป็นเจ้าของ policy
// ว่าเมื่อไหร่ควรเรียก ฟังก์ชันนี้แค่รับผิดชอบพูดออกไปอย่างปลอดภัยเท่านั้น)
//
// หมายเหตุ: ไม่รับ onFirstTtsRequest/onFirstTtsAudio ตอนนี้ — Watchdog C ยังไม่ครอบคลุมการเรียกผ่านทางนี้
// (follow-up หลัง blocked end_call, หรือ TTS ของ legacy fallback ใน audioStream.js) เป็น gap ที่รู้ไว้ก่อน
async function speakFixedText({ text, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onFirstAudioSent, startingSentCount = 0 }) {
  const isCurrent = () => isCurrentGeneration(callState, generationId)
  const sentCount = await synthesizeAndSend({ text, signal, socket, streamSid, voiceId, turnMetrics, turnState, isCurrent, startingSentCount, onFirstAudioSent })
  return { sentCount }
}

async function runChunkedTurn({ session, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onControl, onFirstAudioSent, onFirstDelta, onFirstChunk, onFirstTtsRequest, onFirstTtsAudio }) {
  const isCurrent = () => isCurrentGeneration(callState, generationId)
  const queue = []
  let producerDone = false
  let producerError = null
  let waiter = null

  function enqueue(chunk) {
    queue.push(chunk)
    if (waiter) { waiter(); waiter = null }
  }

  // resolve เมื่อมีงานให้ทำ (queue ไม่ว่าง) หรือ producer จบแล้ว (ไม่มีงานเพิ่มอีก) — สองเงื่อนไขนี้
  // ครอบคลุมทุก state ที่ consumer ต้องตื่นมาตัดสินใจต่อ
  function waitForWork() {
    if (queue.length > 0 || producerDone) return Promise.resolve()
    return new Promise(resolve => { waiter = resolve })
  }

  let fullTextAccum = '' // raw concatenation ของทุก delta ทั้งเทิร์น (ไม่ trim/แทรกอะไรเอง) — ให้ caller เก็บ history/log ต่อได้

  const producer = (async () => {
    let buffer = ''
    let segmentStartMs = null
    try {
      for await (const delta of askClaudeStreamChunked(session, signal, onControl)) {
        if (signal?.aborted) break
        if (!isCurrent()) break // boundary 1: Claude delta — stale generation ต้องไม่แม้แต่ markOnce(t3)
        if (!delta) continue // empty delta → ไม่มีอะไรให้พูด ข้ามไปเฉยๆ

        const isFirstDelta = turnMetrics.t3 == null
        markOnce(turnMetrics, 't3')
        if (isFirstDelta) onFirstDelta?.() // C4b: hook ให้ watchdog ภายนอก (CLAUDE_FIRST_DELTA_TIMEOUT) เคลียร์ timer ของมันเอง
        fullTextAccum += delta

        const wasEmpty = buffer.length === 0
        buffer += delta
        if (wasEmpty) segmentStartMs = performance.now() // elapsedMs นับจากตัวอักษรแรกของ buffer นี้ ตามสัญญาของ speechChunker

        // แยกไว้ต่างหากจาก loop ด้านบนที่รับ delta ทีละก้อน: หนึ่ง delta อาจมีมากกว่าหนึ่งประโยคสมบูรณ์ปนกันมาได้
        // (ไม่บ่อยแต่เกิดได้จริง) — findChunkBoundary คืนทีละจุดตัดตามสัญญาของมันเอง ต้องวนเรียกจนกว่าจะ null
        // เพื่อ drain ทุกก้อนที่พร้อมพูดออกจาก buffer นี้ให้หมดก่อนรอ delta ถัดไป ไม่งั้นประโยคที่สองจะติดอยู่ในนี้
        // เฉยๆ โดยไม่มีเหตุผล จนกว่า delta ถัดไปมาถึง (หรือ stream จบ) ทั้งที่พร้อมพูดตั้งแต่ตอนนี้แล้ว
        while (true) {
          const elapsedMs = performance.now() - segmentStartMs
          const result = findChunkBoundary(buffer, elapsedMs)
          if (!result) break
          if (!isCurrent()) break // boundary 2: chunker output/enqueue — stale ต้องไม่ markOnce(t4)/enqueue
          const isFirstChunk = turnMetrics.t4 == null
          markOnce(turnMetrics, 't4')
          if (isFirstChunk) onFirstChunk?.() // C4b: hook ให้ Watchdog B (CHUNK_READY_TIMEOUT) เคลียร์ timer ของมันเอง
          enqueue(result.chunk)
          buffer = result.remainder
          // remainder ไม่ว่าง = ตัวอักษรพวกนี้ค้างมาตั้งแต่ก่อนหน้านี้แล้ว ไม่ใช่ "เพิ่งมาถึง" — ห้าม reset segmentStartMs
          // ที่นี่ รอบหน้าจะ reset ใหม่เองตอน buffer ว่าง→ไม่ว่างอีกครั้ง (ผ่าน wasEmpty check ด้านบน)
        }
      }

      // Claude stream จบแล้ว (ปกติ หรือเพราะเรียก end_call tool) — flush ก้อนสุดท้ายที่ค้างอยู่ แม้ไม่มี punctuation
      // ปิดท้าย (ล็อกไว้ตั้งแต่ B1/B4: ประโยคจริงอาจจบโดยไม่มี . ? ! ตัวสุดท้ายเลย)
      const finalText = buffer.trim()
      if (finalText && !signal?.aborted && isCurrent()) {
        const isFirstChunk = turnMetrics.t4 == null
        markOnce(turnMetrics, 't4')
        if (isFirstChunk) onFirstChunk?.()
        enqueue(finalText)
      }
    } catch (err) {
      if (!signal?.aborted) producerError = err
    } finally {
      producerDone = true
      if (waiter) { waiter(); waiter = null }
    }
  })()

  let totalSent = 0
  let consumerError = null

  try {
    while (true) {
      await waitForWork()
      if (signal?.aborted) break
      if (queue.length === 0) break // waitForWork รับประกันว่าถ้าไม่มีงาน แปลว่า producer จบแล้วแน่นอน
      if (!isCurrent()) break // boundary 3: ก่อน TTS request — stale ต้องไม่ markOnce(t5)/markTtsPending

      const chunk = queue.shift()
      totalSent += await synthesizeAndSend({ text: chunk, signal, socket, streamSid, voiceId, turnMetrics, turnState, isCurrent, startingSentCount: totalSent, onFirstAudioSent, onFirstTtsRequest, onFirstTtsAudio })
    }
  } catch (err) {
    if (!signal?.aborted) consumerError = err
  }

  await producer

  const err = consumerError || producerError
  if (err) throw err

  return { totalSent, fullText: fullTextAccum }
}

module.exports = { runChunkedTurn, speakFixedText }
