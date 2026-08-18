// Checkpoint C4b — races an attempt (e.g. the chunked path) against a milestone timeout, using a
// child AbortController composed from the caller's outer signal:
//   outer signal aborts  → child aborts too (barge-in must always be able to kill everything)
//   watchdog times out   → only the child aborts; the outer signal is left untouched, so a
//                           caller-driven fallback that still needs the outer signal (e.g. legacy
//                           Claude for the same turn) can still run after the loser is killed
//
// Deliberately generic — knows nothing about turns, generationId, Claude, or TTS. That's what makes
// it unit-testable without any of the streaming machinery, and reusable for future milestone
// watchdogs (chunk-ready, first-TTS-audio) without re-deriving this composition each time.

function attachChildAbort(signal) {
  const child = new AbortController()
  const propagate = () => { if (!child.signal.aborted) child.abort() }
  if (signal.aborted) child.abort()
  else signal.addEventListener('abort', propagate, { once: true })
  return { child, detach: () => signal.removeEventListener('abort', propagate) }
}

// run(childSignal, clearWatchdog) — the caller's attempt. It MUST call clearWatchdog() itself once
// its own milestone has passed (e.g. chunkedTurn.js's onFirstDelta hook calling straight through to
// this), or the watchdog fires regardless of how far the attempt actually progressed.
//
// คืนค่าอย่างใดอย่างหนึ่ง:
//   { outcome: 'success', result }   — run() ชนะ race ก่อน watchdog จะ timeout
//   { outcome: 'timeout', reason }   — watchdog ชนะ; child ถูก abort ไปแล้วก่อน return
//   { outcome: 'error', error }      — run() reject ด้วย error จริง (ไม่ใช่ watchdog); child ถูก abort ไปแล้วก่อน return
async function runAttemptWithWatchdog({ signal, timeoutMs, reason, run }) {
  const { child, detach } = attachChildAbort(signal)

  let clearWatchdogTimer = () => {}
  const watchdogPromise = new Promise((_, reject) => {
    const timer = setTimeout(() => {
      reject(Object.assign(new Error(reason), { code: 'WATCHDOG_TIMEOUT', reason }))
    }, timeoutMs)
    clearWatchdogTimer = () => clearTimeout(timer)
  })

  const attemptPromise = run(child.signal, clearWatchdogTimer)
  // ติด handler ทันที กันไม่ให้เกิด unhandled rejection ถ้า watchdog ชนะ race ไปแล้ว attemptPromise มา reject
  // ทีหลังด้วยเหตุผลอื่นที่ไม่เกี่ยวกับ abort เลย
  //
  // ใช้ flag `settled` แทนการเช็ค child.signal.aborted อย่างเดียว: ถ้า attemptPromise เองเป็นตัวที่ reject
  // จน Promise.race ตัดสินผล error (ไม่ใช่ timeout) handler นี้จะถูกเรียกจาก JS engine ก่อน catch หลักด้านล่าง
  // เสมอ (FIFO ตามลำดับ attach — attach ก่อน Promise.race) ทั้งที่ error นั้นไม่ใช่ "late" เลยแต่เป็นตัวตัดสินผลเอง
  // ต้องรอให้ catch หลักตัดสินผล race เสร็จ (settled=true) ก่อน ถึงจะถือว่า rejection ถัดมาเป็น "late" จริง
  let settled = false
  attemptPromise.catch(err => {
    if (!settled) return // error นี้คือตัวตัดสินผล race เอง ปล่อยให้ catch หลักจัดการ ไม่ใช่ late error แยกต่างหาก
    if (child.signal.aborted) return // ถูก abort ไปแล้ว (จาก watchdog หรือ outer) → คาดหวังได้อยู่แล้ว ไม่ต้อง log ซ้ำ
    console.error('[AttemptWithWatchdog] Late error after losing the race:', err.message)
  })

  try {
    const result = await Promise.race([attemptPromise, watchdogPromise])
    clearWatchdogTimer()
    settled = true
    return { outcome: 'success', result }
  } catch (err) {
    clearWatchdogTimer()
    child.abort() // ฆ่า loser ก่อนเสมอ ไม่ว่าจะแพ้ด้วย watchdog หรือ error จริง — ก่อน caller จะไปทำอะไรต่อ (เช่น claimFallback)
    settled = true
    if (err?.code === 'WATCHDOG_TIMEOUT') {
      return { outcome: 'timeout', reason: err.reason }
    }
    return { outcome: 'error', error: err }
  } finally {
    detach()
  }
}

module.exports = { runAttemptWithWatchdog }
