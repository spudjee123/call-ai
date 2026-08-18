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

// run(childSignal, arm) — the caller's attempt. `arm(ms, reason)` (re)arms the watchdog with a new
// window and reason, cancelling whatever window was running before — this lets a caller chain
// sequential milestone timeouts over a single attempt (e.g. "Claude first delta" window handing off
// to "chunk ready" window the moment the first delta lands, per C4b's Watchdog A → B design). Calling
// arm() with no arguments just disarms — the original single-milestone contract still works unchanged.
// If nothing ever re-arms, the attempt runs to completion with no watchdog once the first arm() fires.
//
// คืนค่าอย่างใดอย่างหนึ่ง:
//   { outcome: 'success', result }   — run() ชนะ race จริง ก่อน watchdog (วงไหนก็ตามที่กำลัง arm อยู่) จะ timeout
//                                       และ child ไม่เคยถูก abort เลยตลอดทาง (completion ปกติแท้ๆ)
//   { outcome: 'aborted', result }   — run() resolve สำเร็จก็จริง (ไม่ throw) แต่ child ถูก abort ไปแล้วระหว่างทาง
//                                       (จาก outer signal เท่านั้น — watchdog เองไม่มีทางทำให้ resolve สำเร็จได้
//                                       เพราะมันแพ้ race ด้วยการ reject) เช่น barge-in กลาง run() ที่จัดการ
//                                       signal.aborted แบบ graceful-return แทนที่จะ throw — ไม่ควรถูกนับเป็น
//                                       "completion สำเร็จ" ตอนอ่าน telemetry เพราะ turn ถูกยกเลิกจริง ไม่ใช่จบปกติ
//   { outcome: 'timeout', reason }   — watchdog ชนะ (reason ของวงที่กำลัง arm อยู่ ณ ตอนนั้น); child ถูก abort ไปแล้วก่อน return
//   { outcome: 'error', error }      — run() reject ด้วย error จริง (ไม่ใช่ watchdog); child ถูก abort ไปแล้วก่อน return
async function runAttemptWithWatchdog({ signal, timeoutMs, reason, run }) {
  const { child, detach } = attachChildAbort(signal)

  let currentTimer = null
  let rejectWatchdog = null
  const watchdogPromise = new Promise((_, reject) => { rejectWatchdog = reject })

  // arm(ms, r): เคลียร์ timer เดิม (ถ้ามี) แล้วตั้งวงใหม่ด้วย ms/r ที่ให้มา — เรียกแบบไม่มี argument (arm())
  // เพื่อแค่เคลียร์เฉยๆ ไม่ตั้งวงใหม่ (คือ contract เดิมของ clearWatchdog ก่อนหน้านี้ ไม่มีอะไรเปลี่ยนสำหรับ caller เดิม)
  function arm(ms, r) {
    if (currentTimer) { clearTimeout(currentTimer); currentTimer = null }
    if (ms == null) return
    currentTimer = setTimeout(() => {
      rejectWatchdog(Object.assign(new Error(r), { code: 'WATCHDOG_TIMEOUT', reason: r }))
    }, ms)
  }
  arm(timeoutMs, reason) // arm วงแรกก่อนเริ่ม run()

  const attemptPromise = run(child.signal, arm)
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
    arm() // เคลียร์วงที่เหลืออยู่ (ถ้ามี) ให้หมด
    settled = true
    // child ถูก abort ได้จากทางเดียวเท่านั้นตอนที่ยัง resolve สำเร็จ (ไม่ reject): outer signal propagate ลงมา —
    // ถ้าเป็นเช่นนั้นจริง turn นี้ถูกยกเลิกกลางทาง ไม่ใช่ completion ปกติ ต้องแยกให้ telemetry อ่านไม่หลอกว่า success
    return { outcome: child.signal.aborted ? 'aborted' : 'success', result }
  } catch (err) {
    arm()
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
