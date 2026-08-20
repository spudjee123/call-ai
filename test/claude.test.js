const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')

// L2a — stub @anthropic-ai/sdk ทั้งโมดูล (pattern เดียวกับ test/askClaudeStreamChunked.test.js) คุม event ที่
// stream ส่งกลับมาเองได้ ไม่ยิง API จริง — เทสชุดนี้พิสูจน์ askClaudeObservedFullResponse() เท่านั้น ไม่แตะ
// askClaudeStream()/askClaudeStreamChunked() เดิม (มีเทสของตัวเองอยู่แล้วแยกต่างหาก ไม่ต้องพิสูจน์ซ้ำที่นี่)
const state = { events: [], signalCheckDelayMs: 0 }

function makeFakeStream(events, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < events.length; i++) {
        if (signal?.aborted) {
          const err = new Error('Request was aborted.')
          err.name = 'AbortError'
          throw err
        }
        yield events[i]
      }
    },
  }
}

// สำหรับเทสที่ต้องรอ real delay ระหว่าง event (จำลอง Claude เงียบไปพักหนึ่งก่อน delta ถัดไป หรือไม่มี delta ถัดไปเลย)
function makeSlowFakeStream(events, delaysMs, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < events.length; i++) {
        if (delaysMs[i]) await new Promise(r => setTimeout(r, delaysMs[i]))
        if (signal?.aborted) {
          const err = new Error('Request was aborted.')
          err.name = 'AbortError'
          throw err
        }
        yield events[i]
      }
    },
  }
}

class FakeAnthropic {
  constructor() {
    this.messages = {
      stream: (params, options) => state.streamImpl(state.events, options?.signal),
    }
  }
}

const anthropicSdkPath = require.resolve('@anthropic-ai/sdk')
require.cache[anthropicSdkPath] = {
  id: anthropicSdkPath, filename: anthropicSdkPath, loaded: true,
  exports: FakeAnthropic,
}

const { askClaudeObservedFullResponse } = require('../src/services/claude')

beforeEach(() => {
  state.events = []
  state.streamImpl = makeFakeStream
})

function textDelta(text) {
  return { type: 'content_block_delta', delta: { type: 'text_delta', text } }
}

function makeSession(userText = 'สวัสดีครับ') {
  return {
    name: 'ทดสอบ',
    campaign: { script: 'สคริปต์ทดสอบ' },
    messages: [{ role: 'user', content: userText }],
  }
}

async function collect(gen) {
  const chunks = []
  for await (const c of gen) chunks.push(c)
  return chunks
}

function delay(ms) { return new Promise(r => setTimeout(r, ms)) }

test('L2a: yield ข้อความเต็มครั้งเดียวตอนจบ เหมือน askClaudeStream() เดิมทุกประการ (raw concat ไม่แทรกช่องว่างเอง, trim แค่ปลายสุด)', async () => {
  state.events = [textDelta('สวัสดีค่ะ '), textDelta('ยินดีต้อนรับค่ะ')]
  const chunks = await collect(askClaudeObservedFullResponse(makeSession(), null, null))
  assert.deepEqual(chunks, ['สวัสดีค่ะ ยินดีต้อนรับค่ะ'])
})

test('L2a: onMilestone เรียก requestAt ทันทีก่อนเรียก stream เลย แม้ยังไม่มี delta ใดๆ มาถึง', async () => {
  state.events = [textDelta('ok')]
  const milestones = []
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k, v) => milestones.push(k)))
  assert.ok(milestones.includes('requestAt'))
  assert.equal(milestones.indexOf('requestAt'), 0, 'requestAt ต้องมาก่อน milestone อื่นเสมอ')
})

test('L2a: onMilestone firstDeltaAt เรียกครั้งเดียวตอน delta แรกเท่านั้น ไม่เรียกซ้ำทุก delta', async () => {
  state.events = [textDelta('หนึ่ง'), textDelta('สอง'), textDelta('สาม')]
  let firstDeltaCalls = 0
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k) => { if (k === 'firstDeltaAt') firstDeltaCalls++ }))
  assert.equal(firstDeltaCalls, 1)
})

test('L2a: onMilestone firstSafeAt เรียกเมื่อเจอ strong/soft boundary ในข้อความจริง (ผ่าน findChunkBoundary() ตรงๆ ไม่ประดิษฐ์ logic ใหม่)', async () => {
  state.events = [textDelta('สวัสดีค่ะ ยินดีต้อนรับสมาชิกใหม่ค่ะ')] // มี "ค่ะ" ซึ่งเป็น Thai soft boundary
  const milestones = {}
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k, v) => { milestones[k] = v }))
  assert.ok(milestones.firstSafeAt != null, 'ต้องเจอ safe boundary จากคำว่า "ค่ะ"')
  assert.ok(milestones.firstSafeAt >= milestones.firstDeltaAt, 'firstSafeAt ต้องมาหลัง firstDeltaAt เสมอ')
})

test('L2a: fullAt เรียกเฉพาะตอน stream จบตามปกติเท่านั้น และมาหลัง milestone อื่นทั้งหมด', async () => {
  state.events = [textDelta('คำตอบสั้นๆ')]
  const milestones = []
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k) => milestones.push(k)))
  assert.ok(milestones.includes('fullAt'))
  assert.equal(milestones[milestones.length - 1], 'fullAt', 'fullAt ต้องเป็น milestone สุดท้ายเสมอ')
})

test('L2a: history ว่างเปล่า (ไม่ควรเกิดจริงในทางปฏิบัติ แต่ต้องสอดคล้องกับ askClaudeStream()) → yield ค่า default ไม่เรียก onMilestone เลย ไม่เรียก stream เลย', async () => {
  const session = { name: 'ทดสอบ', campaign: { script: 'x' }, messages: [] }
  const milestones = []
  const chunks = await collect(askClaudeObservedFullResponse(session, null, (k) => milestones.push(k)))
  assert.deepEqual(chunks, ['สวัสดีค่ะ'])
  assert.equal(milestones.length, 0, 'ไม่ควร fabricate milestone ใดๆ ถ้าไม่เคยเรียก Claude จริง')
})

// หมายเหตุ: pattern การ throw AbortError จาก iterator เองเมื่อ signal.aborted ตรงกับที่
// test/askClaudeStreamChunked.test.js ใช้อยู่แล้ว (บรรทัด 109-117 ของไฟล์นั้น) — ทั้ง askClaudeStream()/
// askClaudeStreamChunked() ไม่เคยกลืน AbortError เองเลย ปล่อยให้ propagate ตรงๆ ผู้เรียกจริง (audioStream.js's
// runAttemptWithWatchdog wrapper) เป็นคน normalize เป็น outcome:'aborted' เอง — askClaudeObservedFullResponse()
// ต้องทำเหมือนกันเป๊ะเพื่อ consistency ไม่ใช่กลืน error เองข้างใน
test('L2a: abort หลัง first delta มาแล้ว (ก่อน full completion) → firstDeltaAt ถูกเก็บไว้จริง, fullAt เป็น null, AbortError propagate ออกไปตรงๆ ไม่ถูกกลืน (ผู้เรียกจริงเป็นคน normalize)', async () => {
  const delays = [0, 50]
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delays, signal)
  state.events = [textDelta('ok'), textDelta('ต่อ')]

  const controller = new AbortController()
  const milestones = {}
  setTimeout(() => controller.abort(), 20) // abort หลัง delta แรก (ที่ 0ms) แต่ก่อน delta สอง (ที่ 50ms)

  await assert.rejects(
    () => collect(askClaudeObservedFullResponse(makeSession(), controller.signal, (k, v) => { milestones[k] = v })),
    (err) => err.name === 'AbortError'
  )

  assert.ok(milestones.firstDeltaAt != null, 'firstDeltaAt ที่มาถึงแล้วก่อน abort ต้องยังถูกเก็บไว้ (เขียนทันทีที่เกิด ไม่รอ callback เดียวตอนจบ)')
  assert.equal(milestones.fullAt, undefined, 'fullAt ต้องไม่ถูกเรียกเลยถ้า abort ก่อนจบ')
})

test('L2a: provider error กลางทาง (ไม่ใช่ abort) → error propagate ออกจาก generator ตรงๆ ไม่ถูกกลืน ไม่มี fullAt', async () => {
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('เริ่มตอบ')
      throw new Error('boom — จำลอง Claude API ล้มกลางสตรีม')
    },
  })

  const milestones = {}
  await assert.rejects(
    () => collect(askClaudeObservedFullResponse(makeSession(), null, (k, v) => { milestones[k] = v })),
    /boom/
  )
  assert.ok(milestones.firstDeltaAt != null, 'delta ที่มาถึงก่อน error ต้องยังถูกเก็บไว้')
  assert.equal(milestones.fullAt, undefined)
})

test('L2a: buffer ลงท้ายด้วยตัวเลข ("รับ 2,000") แล้ว Claude เงียบไปนานเกิน HARD_MAX_MS โดยไม่มี delta ใหม่เลยตลอดช่วงนั้น → firstSafeAt ต้องมาถึงผ่าน numeric-protection wall-clock timer (mirror chunkedTurn.js drainReadyChunks()) ที่ ~800ms ไม่ใช่รอจนจบ stream จริงที่ 1200ms', async () => {
  // เดิมเทสนี้ยัง yield delta ที่สองตอน 900ms ("นะคะ") ซึ่งดันไปแมตช์ Thai soft boundary เองพอดี ทำให้ firstSafeAt
  // ถูกจับได้จาก delta-arrival path ปกติ ไม่ใช่จาก numeric-protection timer เลย (วัดได้ ~909ms ซึ่งบังเอิญใกล้กับ
  // 900ms ของ delta ที่สอง ไม่ใช่ใกล้ 800ms ของ timer) — แก้โดยไม่ให้มี delta ใหม่มาอีกเลยตลอดช่วง 1200ms เพื่อพิสูจน์
  // ว่า timer ทำงานเองได้จริง ไม่ต้องพึ่ง delta ถัดไปมาปลุก
  //
  // แก้รอบสอง: ยังพลาดเรื่อง trailing space — buffer เดิม "...รับ 2,000" (ไม่มี space ต่อท้าย) ทำให้
  // findLastSafeBoundary() หาช่องว่างล่าสุดได้แค่ระหว่าง "รับ" กับ "2,000" (ตัดก่อนตัวเลข ไม่ใช่หลัง) candidate
  // ที่ได้จึงเป็น "...รับ" ซึ่งไม่ได้ลงท้ายด้วยตัวเลขเลย → ไม่เข้าเงื่อนไข numeric-protection ตั้งแต่แรก (พิสูจน์จาก
  // firstSafeAt เป็น null ตลอด ไม่ใช่ timer ไม่ทำงาน) ต้องมี space ต่อท้าย "2,000 " เหมือนสถานการณ์จริงที่ Claude
  // ส่ง delta มาเป็น "...รับ 2,000 " ก่อนแล้วค่อยตามด้วย "พอยต์" ทีหลัง — candidate ถึงจะลงท้ายด้วยตัวเลขจริง
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 ') // มี space ต่อท้ายตัวเลข ยาวพอผ่าน FALLBACK_MIN_LENGTH แล้ว
      await new Promise(r => setTimeout(r, 1200)) // เกิน HARD_MAX_MS (800ms) มาก ไม่มี delta ใหม่มาปลุก chunker เลยตลอดช่วงนี้
      // stream จบเฉยๆ ไม่มี delta เพิ่มอีกเลย
    },
  })

  const milestones = {}
  const startedAt = Date.now()
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k, v) => {
    milestones[k] = v
    milestones[`__${k}ElapsedMs`] = Date.now() - startedAt
  }))

  assert.ok(milestones.firstSafeAt != null, 'firstSafeAt ต้องมาถึงจริงผ่าน wall-clock timer แม้ไม่มี delta ใหม่มาปลุกเลย')
  assert.ok(milestones.__firstSafeAtElapsedMs >= 750 && milestones.__firstSafeAtElapsedMs < 1000,
    `firstSafeAt ต้องมาถึงใกล้ HARD_MAX_MS (~800ms) จาก timer เอง ไม่ใช่รอจนจบ stream ที่ 1200ms — วัดได้จริง ${milestones.__firstSafeAtElapsedMs}ms`)
  assert.ok(milestones.__fullAtElapsedMs >= 1150,
    `fullAt ต้องมาทีหลัง firstSafeAt มากพอสมควร (ที่ ~1200ms ตอน stream จบจริง) พิสูจน์ว่า firstSafeAt ไม่ได้ผูกกับตอนจบ stream — วัดได้จริง ${milestones.__fullAtElapsedMs}ms`)
})

test('L2a: no dangling timer หลัง stream จบตามปกติ — เจอ boundary ผ่าน delta-path เร็วแล้ว ไม่ arm numeric-protection timer ค้างไว้', async () => {
  state.events = [textDelta('สวัสดีค่ะ')] // มี strong/soft boundary ทันที ไม่ต้องรอ timer เลย
  const milestones = {}
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k, v) => { milestones[k] = v }))
  assert.ok(milestones.firstSafeAt != null)
  // ไม่มี assertion โดยตรงว่า timer ถูก clear (ไม่ expose ออกมา) แต่ถ้า process ไม่ค้าง (เทสจบได้ปกติ ไม่ timeout)
  // ก็ยืนยันทางอ้อมว่าไม่มี dangling setTimeout หลุดออกไปนอก scope ของเทสนี้
})

// ===== Round-3 correction verification (mandatory ก่อน commit gate) =====

test('L2a (round 2, correction 2): elapsedMs สำหรับ boundary detection ต้องนับจาก firstDeltaAt ไม่ใช่ requestAt — พิสูจน์ด้วยการหน่วง delta แรกไป 400ms ก่อน', async () => {
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      await new Promise(r => setTimeout(r, 400)) // Claude ใช้เวลา 400ms กว่าจะส่ง delta แรก (จำลอง TTFT ช้า)
      yield textDelta('ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 ') // ลงท้ายด้วยตัวเลข มี trailing space
      await new Promise(r => setTimeout(r, 1200)) // เงียบยาวหลัง delta แรก ไม่มี delta ใหม่มาปลุกเลย
    },
  })

  const milestones = {}
  await collect(askClaudeObservedFullResponse(makeSession(), null, (k, v) => { milestones[k] = v }))

  assert.ok(milestones.firstSafeAt != null)
  const elapsedSinceFirstDelta = milestones.firstSafeAt - milestones.firstDeltaAt
  const elapsedSinceRequest = milestones.firstSafeAt - milestones.requestAt
  // ถ้า elapsedMs ผิดพลาดนับจาก requestAt (รวม 400ms delay ก่อนหน้าด้วย) timer จะ arm ด้วย remainingMs สั้นกว่าที่ควร
  // (HARD_MAX_MS - 400 = ~400ms แทนที่จะเป็น HARD_MAX_MS เต็มๆ นับจาก delta แรก) ทำให้ firstSafeAt มาถึงเร็วกว่าที่ควรมาก
  assert.ok(elapsedSinceFirstDelta >= 750 && elapsedSinceFirstDelta < 1000,
    `elapsedMs ต้องนับจาก firstDeltaAt — วัดได้จริง ${elapsedSinceFirstDelta}ms นับจาก firstDeltaAt (ควรใกล้ 800ms)`)
  assert.ok(elapsedSinceRequest >= 1150,
    `เทียบกับ requestAt ต้องไกลกว่ามาก (รวม 400ms delay ก่อนหน้าด้วย) ยืนยันว่าไม่ได้ใช้ requestAt เป็นฐาน — วัดได้จริง ${elapsedSinceRequest}ms นับจาก requestAt`)
})

test('L2a (round 3, correction — defense-in-depth): abort ที่ SDK จบ stream อย่างสะอาด (ไม่ throw AbortError เลย) → explicit signal.aborted check ต้องกัน fullAt ได้เองแม้ SDK behavior เปลี่ยนไปในอนาคต', async () => {
  const controller = new AbortController()
  state.streamImpl = (events, signal) => ({
    async *[Symbol.asyncIterator]() {
      yield textDelta('เริ่มตอบ')
      controller.abort() // abort ทันทีหลัง delta แรก
      // ไม่ throw อะไรเลย แค่จบ iterator เฉยๆ (จำลอง SDK เวอร์ชันอนาคตที่ end stream สะอาดแทนที่จะ throw AbortError —
      // การ throw AbortError ถูกพิสูจน์แยกไปแล้วในเทส "abort หลัง first delta มาแล้ว" ด้านบน เทสนี้พิสูจน์ path ที่ต่างออกไป
      // โดยเฉพาะ: for-await loop จบแบบไม่มี exception เลย ต้องพึ่ง explicit check ที่ claude.js:394 เท่านั้น ไม่ใช่
      // try/catch ใดๆ)
    },
  })

  const milestones = {}
  const chunks = await collect(askClaudeObservedFullResponse(makeSession(), controller.signal, (k, v) => { milestones[k] = v }))

  assert.deepEqual(chunks, [], 'ห้าม yield ผลลัพธ์เต็มออกไปเลยแม้ stream จะจบแบบสะอาดก็ตาม')
  assert.ok(milestones.firstDeltaAt != null, 'firstDelta ที่มาถึงก่อน abort ต้องยังถูกเก็บไว้')
  assert.equal(milestones.fullAt, undefined, 'fullAt ต้องไม่ถูกบันทึกเด็ดขาด ไม่ว่า SDK จะ throw หรือจบสะอาดก็ตาม')
})

test('L2a: abort หลัง firstSafeAt ถูกบันทึกไปแล้ว (ไม่ใช่แค่หลัง firstDelta เฉยๆ) → firstSafeAt ยังถูกเก็บไว้จริง, fullAt เป็น null, ไม่ yield', async () => {
  const delays = [0, 50]
  state.streamImpl = (events, signal) => makeSlowFakeStream(events, delays, signal)
  state.events = [textDelta('สวัสดีค่ะ'), textDelta('ต่อ')] // delta แรกมี strong/soft boundary ทันที ("ค่ะ") — firstSafeAt ต้องมาพร้อม firstDeltaAt เลย

  const controller = new AbortController()
  const milestones = {}
  setTimeout(() => controller.abort(), 20) // abort หลัง delta แรก (0ms, firstSafeAt ควรมาแล้ว) แต่ก่อน delta สอง (50ms)

  await assert.rejects(
    () => collect(askClaudeObservedFullResponse(makeSession(), controller.signal, (k, v) => { milestones[k] = v })),
    (err) => err.name === 'AbortError'
  )

  assert.ok(milestones.firstSafeAt != null, 'firstSafeAt ที่พบไปแล้วก่อน abort ต้องยังถูกเก็บไว้')
  assert.equal(milestones.fullAt, undefined, 'fullAt ต้องไม่ถูกเรียกเลยแม้ firstSafeAt จะเจอไปแล้วก่อนหน้า')
})

test('L2a: onMilestone(fullAt) ต้องถูกเรียกก่อนที่ caller จะได้รับ yielded text เสมอ (ลำดับที่รับประกันว่า TTS ไม่มีทางเริ่มก่อน fullAt ถูกบันทึก)', async () => {
  state.events = [textDelta('คำตอบสั้นๆ')]
  let fullAtRecorded = false
  const gen = askClaudeObservedFullResponse(makeSession(), null, (k) => { if (k === 'fullAt') fullAtRecorded = true })
  let sawYield = false
  for await (const chunk of gen) {
    sawYield = true
    assert.ok(fullAtRecorded, 'onMilestone(fullAt) ต้องถูกเรียกไปแล้วก่อนที่ caller จะได้รับ yielded text')
  }
  assert.ok(sawYield, 'sanity check: ต้องมี yield จริงในเคสนี้ ไม่งั้น assertion ข้างในไม่เคยถูกรันเลย')
})
