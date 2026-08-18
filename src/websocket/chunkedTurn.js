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
// No isCurrentGeneration() guard here yet — that's Checkpoint C3b, added at the same boundaries
// this file already marks (Claude delta / chunker output / before TTS / TTS audio / before Twilio
// send). This file is not called from the live path yet (rollout stays 0% through C3a).
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

async function runChunkedTurn({ session, signal, socket, streamSid, voiceId, turnMetrics, turnState, onControl, onFirstAudioSent }) {
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
        if (!delta) continue // empty delta → ไม่มีอะไรให้พูด ข้ามไปเฉยๆ

        markOnce(turnMetrics, 't3')
        fullTextAccum += delta

        const wasEmpty = buffer.length === 0
        buffer += delta
        if (wasEmpty) segmentStartMs = performance.now() // elapsedMs นับจากตัวอักษรแรกของ buffer นี้ ตามสัญญาของ speechChunker
        const elapsedMs = performance.now() - segmentStartMs

        const result = findChunkBoundary(buffer, elapsedMs)
        if (result) {
          markOnce(turnMetrics, 't4')
          enqueue(result.chunk)
          buffer = result.remainder
          // remainder ไม่ว่าง = ตัวอักษรพวกนี้ค้างมาตั้งแต่ก่อนหน้านี้แล้ว ไม่ใช่ "เพิ่งมาถึง" — ห้าม reset segmentStartMs
          // ที่นี่ รอบหน้าจะ reset ใหม่เองตอน buffer ว่าง→ไม่ว่างอีกครั้ง (ผ่าน wasEmpty check ด้านบน)
        }
      }

      // Claude stream จบแล้ว (ปกติ หรือเพราะเรียก end_call tool) — flush ก้อนสุดท้ายที่ค้างอยู่ แม้ไม่มี punctuation
      // ปิดท้าย (ล็อกไว้ตั้งแต่ B1/B4: ประโยคจริงอาจจบโดยไม่มี . ? ! ตัวสุดท้ายเลย)
      const finalText = buffer.trim()
      if (finalText && !signal?.aborted) {
        markOnce(turnMetrics, 't4')
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

      const chunk = queue.shift()
      markOnce(turnMetrics, 't5')
      markTtsPending(turnState)

      for await (const audioChunk of synthesizeSpeechStream(chunk, voiceId, signal)) {
        if (socket.readyState !== socket.OPEN || signal?.aborted) break
        markOnce(turnMetrics, 't6')
        if (totalSent === 0) console.log('[TTS] First audio chunk sent')
        socket.send(JSON.stringify({ event: 'media', streamSid, media: { payload: audioChunk.toString('base64') } }))
        markOnce(turnMetrics, 't7')
        markAudioCommitted(turnState)
        if (totalSent === 0) onFirstAudioSent?.()
        totalSent++
      }
    }
  } catch (err) {
    if (!signal?.aborted) consumerError = err
  }

  await producer

  const err = consumerError || producerError
  if (err) throw err

  return { totalSent, fullText: fullTextAccum }
}

module.exports = { runChunkedTurn }
