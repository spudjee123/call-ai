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
//
// L1b (chunked speculative prewarm) — producer/consumer decomposed further into two reusable pieces
// (createChunkedProducer + adoptChunkedProducer) so audioStream.js can start the producer early, from
// an interim transcript, before any turn/generationId exists — then attach the consumer only after the
// final transcript confirms the speculation is usable ("adoption"). runChunkedTurn() itself is just
// these two wired together immediately, so its exported behavior/tests are unchanged.
//
// Deliberate decoupling: createChunkedProducer() never touches turnMetrics/turnState/generationId —
// those don't exist yet during speculation. It exposes onFirstDelta()/onFirstChunk() as *attachable*
// (replay-if-already-happened) observers instead, and buffers control events (emitControl) instead of
// forwarding them directly — the caller decides when/whether to mark real telemetry or forward control,
// which is what makes pre-adoption speculation side-effect-free by construction, not just by convention.
const { performance } = require('perf_hooks')
const { askClaudeStreamChunked } = require('../services/claude')
const { synthesizeSpeechStream } = require('../services/tts')
const { findChunkBoundary, getNumericProtectionRemainingMs } = require('../utils/speechChunker')
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
//
// onAudioSent (C4c follow-up: commit-aware fallback watchdog) ตรงข้ามกับสองตัวบน — ยิงทุกก้อนที่ส่งสำเร็จ
// จริง ไม่ใช่ first-only เสมอมาหลัง markAudioCommitted() แล้วเท่านั้น ให้ caller (เช่น fallback's post-commit
// idle watchdog ใน audioStream.js) ใช้ commit เป็น source of truth ในการ rearm ทุกครั้งที่มี progress จริง
async function synthesizeAndSend({ text, signal, socket, streamSid, voiceId, turnMetrics, turnState, isCurrent, startingSentCount, onFirstAudioSent, onAudioSent, onFirstTtsRequest, onFirstTtsAudio }) {
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
    markAudioCommitted(turnState) // ต้องมาก่อน onAudioSent เสมอ — commit คือ source of truth ที่ caller (เช่น fallback's idle watchdog) ใช้ตัดสินใจ ไม่ใช่แค่ "ส่งไปแล้ว"
    if (startingSentCount + sentCount === 0) onFirstAudioSent?.()
    onAudioSent?.() // ทุกก้อน (ไม่ใช่ first-only เหมือน onFirstAudioSent) — granularity เดียวกับ sentCount/totalSent ที่ caller ใช้อยู่แล้ว ไม่ใช่ raw ElevenLabs byte frame
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
async function speakFixedText({ text, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onFirstAudioSent, onAudioSent, startingSentCount = 0 }) {
  const isCurrent = () => isCurrentGeneration(callState, generationId)
  const sentCount = await synthesizeAndSend({ text, signal, socket, streamSid, voiceId, turnMetrics, turnState, isCurrent, startingSentCount, onFirstAudioSent, onAudioSent })
  return { sentCount }
}

// L1b — producer stage: askClaudeStreamChunked → speechChunker → buffered queue. ไม่รู้จัก turnMetrics/
// turnState/generationId/onControl ของ "เทิร์นจริง" เลย — เพื่อให้เรียกได้ทั้งก่อนมีเทิร์นจริง (speculation
// จาก interim) และตอนมีเทิร์นจริงแล้ว (fresh, ผ่าน runChunkedTurn ด้านล่าง) โดยไม่ต้องมี code path แยก
//
// getIsValid(): เทียบเท่า isCurrentGeneration() เดิมของ C3b แต่ injectable — fresh path ส่ง
// isCurrentGeneration(callState, generationId) ตรงๆ (behavior เดิม 100%) ส่วน speculative path ส่ง validity
// predicate ของตัวเอง (ดู audioStream.js) ที่ "upgrade" เป็น generation guard จริงได้หลัง adoption — ไม่ใช่การ
// ถอด generation guard ออก แค่ทำให้ inject ได้ (C3b boundary 1/2 ยังคงอยู่ครบ)
//
// onFirstDelta/onFirstChunk (attachable, replay-if-already-happened — เหมือน attachForwarder ด้านล่าง):
// ให้ caller subscribe "ตอนไหนก็ได้" ไม่ใช่แค่ตอน create — จำเป็นสำหรับ adoption ที่อาจเกิดหลัง delta/chunk
// แรกมาแล้วจริง (ต้อง mark canonical t3/t4 "ตอนนี้" ไม่ใช่ backdate ไปตอน speculation) หรือเกิดก่อน (ต้องรอ
// เหตุการณ์จริงหลัง adopt แล้วค่อย mark)
function createChunkedProducer({ session, signal, getIsValid = () => true }) {
  const queue = []
  let producerDone = false
  let producerError = null
  let waiter = null
  let progressWaiter = null
  let controlEvent = null
  let controlForwarder = null
  let fullTextAccum = ''
  let firstDeltaAt = null
  let firstChunkAt = null
  let deltaListeners = []
  let chunkListeners = []

  function enqueue(chunk) {
    queue.push(chunk)
    if (waiter) { waiter(); waiter = null }
  }

  // resolve เมื่อมีงานให้ทำ (queue ไม่ว่าง) หรือ producer จบแล้ว (ไม่มีงานเพิ่มอีก) — ใช้โดย consumer จริง
  // (adoptChunkedProducer) เท่านั้น หลัง adopt แล้ว
  function waitForWork() {
    if (queue.length > 0 || producerDone) return Promise.resolve()
    return new Promise(resolve => { waiter = resolve })
  }

  // L1b — resolve เมื่อ "มี progress อะไรก็ได้" (delta แรก หรือจบไปแล้วแม้ไม่มี delta เลย) — คนละ predicate กับ
  // waitForWork() โดยตั้งใจ: ใช้เฉพาะช่วง pre-adoption zero-progress grace (150ms) เท่านั้น ต้องตื่นทันทีที่มี
  // delta แรกมาจริง ไม่ใช่รอจน chunk พร้อม (ไม่งั้น grace 150ms จะไม่มีทางตื่นทันเวลาตามที่ design ต้องการ)
  function resolveProgress() { if (progressWaiter) { progressWaiter(); progressWaiter = null } }
  function waitForFirstProgress() {
    if (firstDeltaAt != null || producerDone) return Promise.resolve()
    return new Promise(resolve => { progressWaiter = resolve })
  }

  function notifyDelta() {
    if (firstDeltaAt != null) return
    firstDeltaAt = performance.now()
    deltaListeners.splice(0).forEach(fn => fn())
    resolveProgress()
  }
  function notifyChunk() {
    if (firstChunkAt != null) return
    firstChunkAt = performance.now()
    chunkListeners.splice(0).forEach(fn => fn())
  }
  // replay-if-already-happened: ถ้า delta/chunk แรกเกิดไปแล้วจริง (speculative time) ยิง fn() ทันทีตอน
  // subscribe — นี่คือกลไกที่ทำให้ canonical t3/t4 mark ที่ "เวลา adopt" (accepted-path availability) ไม่ใช่
  // backdate ไปตอน speculation จริง โดยไม่ต้องมี code path แยกระหว่าง "มาแล้ว" กับ "ยังไม่มา"
  function onFirstDelta(fn) { firstDeltaAt != null ? fn() : deltaListeners.push(fn) }
  function onFirstChunk(fn) { firstChunkAt != null ? fn() : chunkListeners.push(fn) }

  // buffer เสมอ, forward ก็ต่อเมื่อมี forwarder ถูก attach แล้วเท่านั้น (attachForwarder) — นี่คือ "quarantine"
  // ของ speculative end_call ทั้งหมด: ก่อน adopt ไม่มี forwarder เลย จึงไม่มีทาง execute control ใดๆ ได้
  //
  // guard signal/getIsValid ตรงนี้ด้วย (ไม่ใช่แค่ที่ delta-loop boundary) เพราะ emitControl ถูกส่งเป็น onControl
  // callback ตรงเข้า askClaudeStreamChunked() — เรียกจาก content_block_stop โดยตรง ไม่ไหลผ่าน boundary check
  // ของ for-await loop ด้านล่างเลย ถ้าไม่ guard ที่นี่เอง end_call ที่มาช้าหลัง producer ถูก invalidate จะหลุด
  // ผ่านไปได้ (gap จริงที่พบระหว่าง design review รอบ 3)
  function emitControl(ev) {
    if (signal?.aborted || !getIsValid()) return
    controlEvent = ev
    controlForwarder?.(ev)
  }
  // revalidate ซ้ำตรงนี้ด้วย (ไม่ใช่แค่ตอน emitControl) — เพราะ controlEvent อาจถูก buffer ไว้ตอน producer
  // ยัง valid จริง แล้ว "เวลา attachForwarder ถูกเรียก" (ตอน adopt) generation อาจ stale ไปแล้ว/signal อาจถูก
  // abort ไปแล้วก็ได้ (เช่น final มาถึงตอน generation เปลี่ยนไปแล้วพอดี) — ห้าม replay end_call ที่ stale ออกไป
  function attachForwarder(fn) {
    if (signal?.aborted || !getIsValid()) return
    controlForwarder = fn
    if (controlEvent && !signal?.aborted && getIsValid()) fn(controlEvent)
  }

  const done = (async () => {
    let buffer = ''
    let segmentStartMs = null
    let numericProtectionTimer = null

    function clearNumericProtectionTimer() {
      if (numericProtectionTimer) { clearTimeout(numericProtectionTimer); numericProtectionTimer = null }
    }

    // L1c1 follow-up (commit-gate review) — findChunkBoundary() ไม่มี wall-clock polling ในตัวเอง เรียกเฉพาะตอน
    // มี delta ใหม่มาถึงเท่านั้น ถ้า buffer กำลังถูก numeric protection กันอยู่ (ตัวเลข+หน่วยนับที่ยังมาไม่ครบ) แล้ว
    // Claude เงียบเกินไป (ไม่มี delta ใหม่มาปลุกเลย) ไม่มีใครมาตรวจว่า protection หมดอายุแล้วจริงที่ HARD_MAX_MS —
    // เสี่ยงชน CHUNK_READY_TIMEOUT (Watchdog B, 2000ms) ทั้งที่สั้นกว่ามาก จึง arm wall-clock timer เองตรงนี้ทุก
    // ครั้งที่ไม่มี chunk พร้อมตัด แต่ buffer ยังถูก protect อยู่ — เรียกซ้ำจากทั้ง delta-arrival path (ผ่าน loop
    // ด้านล่าง) และจาก timer callback เอง (recursive) ด้วย logic เดียวกันเป๊ะ กัน drift ระหว่างสอง path
    function drainReadyChunks() {
      clearNumericProtectionTimer()
      while (true) {
        const elapsedMs = performance.now() - segmentStartMs
        const result = findChunkBoundary(buffer, elapsedMs)
        if (result) {
          if (!getIsValid()) return // boundary 2 (C3b, injectable) — stale ต้องไม่ notifyChunk()/enqueue()
          notifyChunk()
          enqueue(result.chunk)
          buffer = result.remainder
          continue // buffer อาจมี chunk พร้อมมากกว่าหนึ่งก้อน ต้องวนต่อจนกว่าจะหมด
        }
        const remainingMs = getNumericProtectionRemainingMs(buffer, elapsedMs)
        if (remainingMs != null) {
          numericProtectionTimer = setTimeout(() => {
            numericProtectionTimer = null
            if (signal?.aborted || !getIsValid()) return // ถูก abort/invalidate ไปแล้วระหว่างรอ — ไม่ทำอะไรต่อ
            drainReadyChunks()
          }, remainingMs)
        }
        return
      }
    }

    try {
      for await (const delta of askClaudeStreamChunked(session, signal, emitControl)) {
        if (signal?.aborted) break
        if (!getIsValid()) break // boundary 1 (C3b, injectable)
        if (!delta) continue

        notifyDelta()
        fullTextAccum += delta

        const wasEmpty = buffer.length === 0
        buffer += delta
        if (wasEmpty) segmentStartMs = performance.now()

        drainReadyChunks()
      }

      clearNumericProtectionTimer() // stream จบแล้ว (ปกติหรือ error) — ไม่ต้องรอ expiry timer อีกต่อไป final flush ด้านล่างจัดการเอง

      const finalText = buffer.trim()
      if (finalText && !signal?.aborted && getIsValid()) {
        notifyChunk()
        enqueue(finalText)
      }
    } catch (err) {
      if (!signal?.aborted) producerError = err
    } finally {
      clearNumericProtectionTimer()
      producerDone = true
      if (waiter) { waiter(); waiter = null }
      resolveProgress()
    }
  })()

  return {
    queue, waitForWork, waitForFirstProgress, attachForwarder, onFirstDelta, onFirstChunk,
    get producerDone() { return producerDone },
    get producerError() { return producerError },
    get fullTextAccum() { return fullTextAccum },
    get controlEvent() { return controlEvent },
    get firstDeltaAt() { return firstDeltaAt },
    get firstChunkAt() { return firstChunkAt },
    done,
  }
}

// L1b — consumer stage: attach ให้ producer ที่มีอยู่แล้ว (สร้างสดหรือ speculative ที่ adopt มา) แล้ว dequeue →
// TTS → Twilio เหมือน consumer loop เดิมทุกประการ (C3b boundary 3/4+5 ยังอยู่ครบ ผ่าน isCurrent ที่ผูกกับ
// generationId/callState จริงเสมอ — จุดนี้ไม่เคยเสีย generation guard เลยตั้งแต่แรก เพราะ generationId/callState
// มีให้ใช้แค่ตอน adopt เท่านั้นอยู่แล้ว)
async function adoptChunkedProducer({ producer, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onControl, onFirstAudioSent, onAudioSent, onFirstTtsRequest, onFirstTtsAudio }) {
  const isCurrent = () => isCurrentGeneration(callState, generationId)
  producer.attachForwarder(control => { if (control?.type === 'end_call') onControl?.(control) })

  let totalSent = 0
  let consumerError = null
  try {
    while (true) {
      await producer.waitForWork()
      if (signal?.aborted) break
      if (producer.queue.length === 0) break // waitForWork รับประกันว่าถ้าไม่มีงาน แปลว่า producer จบแล้วแน่นอน
      if (!isCurrent()) break // boundary 3

      const chunk = producer.queue.shift()
      totalSent += await synthesizeAndSend({ text: chunk, signal, socket, streamSid, voiceId, turnMetrics, turnState, isCurrent, startingSentCount: totalSent, onFirstAudioSent, onAudioSent, onFirstTtsRequest, onFirstTtsAudio })
    }
  } catch (err) {
    if (!signal?.aborted) consumerError = err
  }

  await producer.done

  // C4c: tag ว่า error มาจาก Claude (producer) หรือ TTS (consumer) — ให้ caller (audioStream.js) จัดหมวด
  // fallbackReason เป็น CLAUDE_ERROR/TTS_ERROR แทนป้ายรวมๆ อย่างเดียวได้ ไม่ต้องเดาจาก error.message
  const err = consumerError || producer.producerError
  if (err && !signal?.aborted) {
    err.source = consumerError ? 'TTS' : 'CLAUDE'
    throw err
  }

  return { totalSent, fullText: producer.fullTextAccum }
}

// runChunkedTurn — export/behavior เดิม 100% ไม่เปลี่ยน (regression bar ของ L1b refactor คือ chunkedTurn.test.js
// เดิมทั้งชุดต้องผ่านโดยไม่แก้แม้แต่บรรทัดเดียว) เป็นแค่ createChunkedProducer + adoptChunkedProducer ผูกกันทันที
// ด้วย generation guard จริง — markOnce(t3/t4) ย้ายมาไว้ตรงนี้ (แทนที่จะอยู่ใน producer loop ตรงๆ แบบเดิม)
// เพราะ producer ที่ decouple ออกมาไม่รู้จัก turnMetrics แล้ว
async function runChunkedTurn({ session, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onControl, onFirstAudioSent, onAudioSent, onFirstDelta, onFirstChunk, onFirstTtsRequest, onFirstTtsAudio }) {
  const producer = createChunkedProducer({
    session, signal,
    getIsValid: () => isCurrentGeneration(callState, generationId),
  })
  producer.onFirstDelta(() => { markOnce(turnMetrics, 't3'); onFirstDelta?.() })
  producer.onFirstChunk(() => { markOnce(turnMetrics, 't4'); onFirstChunk?.() })
  return adoptChunkedProducer({ producer, signal, socket, streamSid, voiceId, turnMetrics, turnState, callState, generationId, onControl, onFirstAudioSent, onAudioSent, onFirstTtsRequest, onFirstTtsAudio })
}

module.exports = { runChunkedTurn, speakFixedText, createChunkedProducer, adoptChunkedProducer }
