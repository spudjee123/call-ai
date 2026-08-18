const { test } = require('node:test')
const assert = require('node:assert/strict')
const { runAttemptWithWatchdog } = require('../src/utils/attemptWithWatchdog')

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

test('run() ชนะ race (เสร็จก่อน timeout) → outcome success, ได้ result กลับมา', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 50,
    reason: 'TEST_TIMEOUT',
    run: async () => 'done',
  })
  assert.deepEqual(outcome, { outcome: 'success', result: 'done' })
})

test('watchdog ชนะ race (run() ช้าเกิน timeoutMs) → outcome timeout พร้อม reason, child signal ถูก abort', async () => {
  let childSignalSeen = null
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 20,
    reason: 'CLAUDE_FIRST_DELTA_TIMEOUT',
    run: async (childSignal) => {
      childSignalSeen = childSignal
      await delay(200) // ช้ากว่า watchdog แน่นอน
      return 'too late'
    },
  })
  assert.equal(outcome.outcome, 'timeout')
  assert.equal(outcome.reason, 'CLAUDE_FIRST_DELTA_TIMEOUT')
  assert.equal(childSignalSeen.aborted, true, 'child signal ต้องถูก abort ไปแล้วก่อน return')
})

test('run() reject ด้วย error จริง (ไม่ใช่ timeout) → outcome error, child signal ถูก abort ด้วยเช่นกัน', async () => {
  let childSignalSeen = null
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 50,
    reason: 'TEST_TIMEOUT',
    run: async (childSignal) => {
      childSignalSeen = childSignal
      throw new Error('boom')
    },
  })
  assert.equal(outcome.outcome, 'error')
  assert.equal(outcome.error.message, 'boom')
  assert.equal(childSignalSeen.aborted, true)
})

test('clearWatchdog() ที่ run() เรียกเอง ก่อน timeoutMs → watchdog ไม่มีสิทธิ์ชนะแม้ run() จะใช้เวลารวมนานกว่า timeoutMs', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 20,
    reason: 'TEST_TIMEOUT',
    run: async (childSignal, clearWatchdog) => {
      await delay(5) // เร็วกว่า timeoutMs — milestone มาทันเวลา
      clearWatchdog()
      await delay(100) // ช่วงหลังนี้ยาวกว่า timeoutMs รวมกันแล้ว แต่ไม่ควรกระทบเพราะ clear ไปแล้ว
      return 'finished after clearing'
    },
  })
  assert.deepEqual(outcome, { outcome: 'success', result: 'finished after clearing' })
})

test('outer signal ถูก abort ระหว่าง run() กำลังทำงานอยู่ → child signal ต้อง abort ตามทันที (propagate)', async () => {
  const outer = new AbortController()
  let childSignalSeen = null
  const runPromise = runAttemptWithWatchdog({
    signal: outer.signal,
    timeoutMs: 500,
    reason: 'TEST_TIMEOUT',
    run: async (childSignal) => {
      childSignalSeen = childSignal
      await delay(30)
      return childSignal.aborted ? 'saw-abort' : 'no-abort'
    },
  })
  await delay(5)
  outer.abort() // barge-in จำลอง — เกิดระหว่าง run() ยังไม่เสร็จ
  const outcome = await runPromise
  assert.equal(childSignalSeen.aborted, true, 'child ต้อง abort ทันทีที่ outer abort ไม่ต้องรอ run() เสร็จ')
  assert.equal(outcome.result, 'saw-abort')
})

test('outer signal abort ไปแล้วตั้งแต่ก่อนเรียก runAttemptWithWatchdog เลย → child ต้อง abort ทันทีตั้งแต่เริ่ม ไม่พลาด', async () => {
  const outer = new AbortController()
  outer.abort() // abort ไปก่อนแล้ว
  let childSignalSeen = null
  await runAttemptWithWatchdog({
    signal: outer.signal,
    timeoutMs: 500,
    reason: 'TEST_TIMEOUT',
    run: async (childSignal) => { childSignalSeen = childSignal; return 'x' },
  })
  assert.equal(childSignalSeen.aborted, true)
})

test('run() reject ทันที (เป็นตัวตัดสินผล race เอง ไม่ใช่ timeout) ต้องไม่ถูก log เป็น "late error" ผิดๆ', async () => {
  // เคยเป็นบั๊กจริง: handler ของ "late error" ถูก attach ก่อน Promise.race ตาม FIFO เลยทำงานก่อน catch หลัก
  // ตัดสินผล race เสร็จ ทำให้ error ที่เป็นตัวตัดสินผลเองถูก log เป็น "late" อย่างผิดๆ ทั้งที่ไม่ late เลย
  const originalError = console.error
  const logs = []
  console.error = (...args) => { logs.push(args.join(' ')) }
  try {
    const outcome = await runAttemptWithWatchdog({
      signal: new AbortController().signal,
      timeoutMs: 50,
      reason: 'TEST_TIMEOUT',
      run: async () => { throw new Error('immediate boom') },
    })
    assert.equal(outcome.outcome, 'error')
  } finally {
    console.error = originalError
  }
  assert.ok(!logs.some(l => l.includes('Late error')), `ไม่ควรมี log "Late error" สำหรับ error ที่ตัดสินผล race เอง — ได้ log: ${JSON.stringify(logs)}`)
})

test('late rejection ของ run() หลังแพ้ race (timeout ชนะ) ไม่ทำให้เกิด unhandled rejection', async () => {
  let unhandled = null
  const onUnhandled = (err) => { unhandled = err }
  process.on('unhandledRejection', onUnhandled)
  try {
    const outcome = await runAttemptWithWatchdog({
      signal: new AbortController().signal,
      timeoutMs: 10,
      reason: 'TEST_TIMEOUT',
      run: async () => {
        await delay(30)
        throw new Error('late failure unrelated to abort') // มาช้าหลัง watchdog ชนะไปแล้ว
      },
    })
    assert.equal(outcome.outcome, 'timeout')
    await delay(50) // รอให้ late rejection ของ run() เกิดขึ้นจริงและถูก handler ข้างในจับ
  } finally {
    process.removeListener('unhandledRejection', onUnhandled)
  }
  assert.equal(unhandled, null, 'ต้องไม่มี unhandledRejection หลุดออกมาแม้ run() จะ reject ช้าหลังแพ้ race')
})

// ---------------------------------------------------------------------------
// rearm — วง watchdog หลายขั้นต่อเนื่องบน attempt เดียว (Watchdog A → B ของ C4b)
// ---------------------------------------------------------------------------

test('arm(ms, reason) เรียกซ้ำจาก run() → ยกเลิกวงเดิม ตั้งวงใหม่ด้วย reason ใหม่ ถ้าวงใหม่ timeout ก่อน run() เสร็จ', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 200, // วงแรกยาวมาก ไม่มีทาง timeout ก่อน rearm
    reason: 'STAGE_A_TIMEOUT',
    run: async (childSignal, arm) => {
      await delay(5)
      arm(15, 'STAGE_B_TIMEOUT') // ตั้งวงใหม่สั้นกว่าเดิมมาก
      await delay(100) // ช้ากว่าวง B แน่นอน
      return 'too late'
    },
  })
  assert.equal(outcome.outcome, 'timeout')
  assert.equal(outcome.reason, 'STAGE_B_TIMEOUT', 'ต้องได้ reason ของวงที่ timeout จริง (วง B) ไม่ใช่วง A เดิม')
})

test('arm(ms, reason) rearm แล้ว run() เสร็จก่อนวงใหม่ timeout → success ปกติ ไม่ถูกวงเดิม (ที่ถูกยกเลิกไปแล้ว) รบกวน', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 5, // วงแรกสั้นมาก ถ้าไม่ถูกยกเลิกจะ timeout ก่อน run() เสร็จแน่ๆ
    reason: 'STAGE_A_TIMEOUT',
    run: async (childSignal, arm) => {
      arm(200, 'STAGE_B_TIMEOUT') // rearm เป็นวงยาวทันที ก่อนวง A จะทัน timeout
      await delay(10) // นานกว่าวง A เดิม (5ms) แต่สั้นกว่าวง B (200ms) มาก
      return 'done in time'
    },
  })
  assert.deepEqual(outcome, { outcome: 'success', result: 'done in time' })
})

test('arm() ไม่มี argument (contract เดิม) ยังทำงานเหมือนเดิมทุกประการหลัง redesign', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 15,
    reason: 'TEST_TIMEOUT',
    run: async (childSignal, arm) => {
      await delay(5)
      arm() // แค่เคลียร์ ไม่ตั้งวงใหม่ — เหมือนพฤติกรรม clearWatchdog เดิมทุกประการ
      await delay(100)
      return 'finished after clearing'
    },
  })
  assert.deepEqual(outcome, { outcome: 'success', result: 'finished after clearing' })
})

test('watchdog สองวงเรียงกัน จำลอง Watchdog A → B จริง: A ไม่ทัน timeout, B ทัน timeout → outcome timeout ของ B, child ถูก abort', async () => {
  let childSignalAtFailure = null
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 100, // "CLAUDE_FIRST_DELTA_TIMEOUT" จำลอง
    reason: 'CLAUDE_FIRST_DELTA_TIMEOUT',
    run: async (childSignal, arm) => {
      await delay(5) // "t3" มาเร็ว ทันวง A แน่นอน
      arm(15, 'CHUNK_READY_TIMEOUT') // "t4" ไม่เคยมา — ไม่มีการ clear ครั้งที่สอง
      childSignalAtFailure = childSignal
      await delay(200) // ไม่มีทางถึง — ควรถูก race แพ้ไปก่อนโดยวง B
      return 'unreachable'
    },
  })
  assert.equal(outcome.outcome, 'timeout')
  assert.equal(outcome.reason, 'CHUNK_READY_TIMEOUT')
  assert.equal(childSignalAtFailure.aborted, true)
})

test('watchdog สามวงเรียงกัน จำลอง Watchdog A → B → C จริง (t3, t4 มาทัน, t5 มาทัน แต่ t6 ไม่มาเลย) → outcome timeout ของ C', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 100, // CLAUDE_FIRST_DELTA_TIMEOUT จำลอง
    reason: 'CLAUDE_FIRST_DELTA_TIMEOUT',
    run: async (childSignal, arm) => {
      await delay(5) // t3 มาทัน A
      arm(100, 'CHUNK_READY_TIMEOUT') // arm B
      await delay(5) // t4 มาทัน B
      arm(15, 'TTS_FIRST_AUDIO_TIMEOUT') // arm C (จำลองเริ่ม TTS request จริง)
      await delay(200) // t6 ไม่เคยมา — ควรแพ้ race ให้ C ก่อนถึงตรงนี้
      return 'unreachable'
    },
  })
  assert.equal(outcome.outcome, 'timeout')
  assert.equal(outcome.reason, 'TTS_FIRST_AUDIO_TIMEOUT')
})

test('watchdog สามวงเรียงกัน ทุกวงทันเวลาหมด (t3, t4, t6 มาครบก่อน timeout ของแต่ละวง) → outcome success', async () => {
  const outcome = await runAttemptWithWatchdog({
    signal: new AbortController().signal,
    timeoutMs: 100,
    reason: 'CLAUDE_FIRST_DELTA_TIMEOUT',
    run: async (childSignal, arm) => {
      await delay(5)
      arm(100, 'CHUNK_READY_TIMEOUT')
      await delay(5)
      arm(100, 'TTS_FIRST_AUDIO_TIMEOUT')
      await delay(5)
      arm() // t6 มาทัน — disarm สนิท ไม่เหลือวงไหนค้างอยู่
      return 'all milestones on time'
    },
  })
  assert.deepEqual(outcome, { outcome: 'success', result: 'all milestones on time' })
})

test('claimFallback-style caller: หลัง timeout ต้อง detach listener แล้ว ไม่ throw ถ้า outer abort ซ้ำอีกทีหลังจบไปแล้ว', async () => {
  const outer = new AbortController()
  await runAttemptWithWatchdog({
    signal: outer.signal,
    timeoutMs: 10,
    reason: 'TEST_TIMEOUT',
    run: async () => { await delay(50); return 'x' },
  })
  assert.doesNotThrow(() => outer.abort())
})
