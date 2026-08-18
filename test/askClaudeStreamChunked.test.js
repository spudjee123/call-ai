const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
require('dotenv').config()

// stub @anthropic-ai/sdk ทั้งโมดูล — คุม event ที่ stream ส่งกลับมาเองได้ ไม่ยิง API จริง
// (integration test จริง 1-3 เคสแยกทำไปแล้วต่างหากด้วยมือ ยืนยันว่า raw concatenation ถูกต้องกับ API จริง)
const state = { events: [], throwAfterIndex: -1, throwError: null }

function makeFakeStream(events, signal) {
  return {
    async *[Symbol.asyncIterator]() {
      for (let i = 0; i < events.length; i++) {
        if (signal?.aborted) {
          const err = new Error('Request was aborted.')
          err.name = 'AbortError'
          throw err
        }
        if (i === state.throwAfterIndex) throw state.throwError
        yield events[i]
      }
    },
  }
}

class FakeAnthropic {
  constructor() {
    this.messages = {
      stream: (params, options) => makeFakeStream(state.events, options?.signal),
    }
  }
}

const anthropicSdkPath = require.resolve('@anthropic-ai/sdk')
require.cache[anthropicSdkPath] = {
  id: anthropicSdkPath, filename: anthropicSdkPath, loaded: true,
  exports: FakeAnthropic,
}

const { askClaudeStreamChunked } = require('../src/services/claude')

beforeEach(() => {
  state.events = []
  state.throwAfterIndex = -1
  state.throwError = null
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

test('unicode/ภาษาไทยแตกข้ามหลาย delta ต่อ raw ตรงๆ ต้องได้ข้อความถูกต้อง ไม่มีช่องว่างแทรก', async () => {
  // จำลองสิ่งที่เกิดจริงกับ API จริง (ยืนยันด้วยมือแล้ว): "ค่ะ" ถูกตัดกลางตัวอักษรระหว่าง delta
  state.events = [
    { type: 'message_start' },
    textDelta('สวัสดีค'),
    textDelta('่ะ คุณลูกค้า'),
    { type: 'content_block_stop' },
  ]
  const chunks = await collect(askClaudeStreamChunked(makeSession()))
  assert.equal(chunks.join(''), 'สวัสดีค่ะ คุณลูกค้า')
})

test('ignore event ชนิดอื่นที่ไม่ใช่ content_block_delta/text_delta (message_start, content_block_start, message_delta, message_stop)', async () => {
  state.events = [
    { type: 'message_start' },
    { type: 'content_block_start', content_block: { type: 'text' } },
    textDelta('ค่ะ'),
    { type: 'content_block_stop' },
    { type: 'message_delta', delta: { stop_reason: 'end_turn' } },
    { type: 'message_stop' },
  ]
  const chunks = await collect(askClaudeStreamChunked(makeSession()))
  assert.deepEqual(chunks, ['ค่ะ'])
})

test('empty delta (text ว่างเปล่า) ไม่ทำให้พัง แค่ yield string ว่าง', async () => {
  state.events = [textDelta('เริ่ม'), textDelta(''), textDelta('จบ')]
  const chunks = await collect(askClaudeStreamChunked(makeSession()))
  assert.deepEqual(chunks, ['เริ่ม', '', 'จบ'])
  assert.equal(chunks.join(''), 'เริ่มจบ')
})

test('หลายประโยคมาใน delta เดียว — ยัง yield ผ่านมาทั้งก้อนตรงๆ (การตัดประโยคเป็นหน้าที่ของ speechChunker ไม่ใช่ที่นี่)', async () => {
  state.events = [textDelta('ยินดีค่ะ พี่สนใจไหมคะ ถ้าสนใจแจ้งได้เลยนะคะ')]
  const chunks = await collect(askClaudeStreamChunked(makeSession()))
  assert.deepEqual(chunks, ['ยินดีค่ะ พี่สนใจไหมคะ ถ้าสนใจแจ้งได้เลยนะคะ'])
})

test('จบ stream โดยไม่มีเครื่องหมายจบประโยค (. ? !) — ยัง yield ครบทุก chunk ปกติ ไม่ error', async () => {
  state.events = [textDelta('อยากทราบว่า'), textDelta('ตอนนี้')]
  const chunks = await collect(askClaudeStreamChunked(makeSession()))
  assert.equal(chunks.join(''), 'อยากทราบว่าตอนนี้')
})

test('abort ก่อน delta แรกมาถึง — โยน AbortError ไม่ yield อะไรเลย', async () => {
  state.events = [textDelta('ไม่ควรมาถึง')]
  const ac = new AbortController()
  ac.abort()
  await assert.rejects(
    () => collect(askClaudeStreamChunked(makeSession(), ac.signal)),
    (err) => err.name === 'AbortError'
  )
})

test('abort กลาง stream (หลัง chunk แรกมาแล้ว) — หยุดทันที ไม่ yield chunk หลังจุด abort', async () => {
  state.events = [textDelta('ท่อนแรก'), textDelta('ท่อนสอง'), textDelta('ท่อนสาม')]
  const ac = new AbortController()
  const chunks = []
  await assert.rejects(async () => {
    for await (const c of askClaudeStreamChunked(makeSession(), ac.signal)) {
      chunks.push(c)
      if (chunks.length === 1) ac.abort() // abort หลังรับ chunk แรก
    }
  })
  assert.deepEqual(chunks, ['ท่อนแรก'], 'ต้องไม่มี chunk หลังจุดที่ abort')
})

test('Claude API error กลางสตรีม — โยน error ออกไปตรงๆ ไม่กลืนเงียบ ไม่ yield อะไรเพิ่มหลังจากนั้น', async () => {
  state.events = [textDelta('เริ่มพูด'), textDelta('พูดต่อ'), textDelta('ไม่ควรมาถึง')]
  state.throwAfterIndex = 2 // error ก่อนถึง event ที่ 3 (index 2)
  state.throwError = new Error('Connection reset')
  const chunks = []
  await assert.rejects(async () => {
    for await (const c of askClaudeStreamChunked(makeSession())) chunks.push(c)
  }, /Connection reset/)
  assert.deepEqual(chunks, ['เริ่มพูด', 'พูดต่อ'], 'ต้อง yield เฉพาะ chunk ก่อนเจอ error เท่านั้น')
})

test('session.messages ว่างเปล่า → yield ทักทายเริ่มต้น "สวัสดีค่ะ" ทันที ไม่เรียก API', async () => {
  const session = { name: 'ทดสอบ', campaign: { script: 'x' }, messages: [] }
  const chunks = await collect(askClaudeStreamChunked(session))
  assert.deepEqual(chunks, ['สวัสดีค่ะ'])
})

// ==========================================================================
// Checkpoint B2 — end_call เป็น tool call แยกจาก text channel (8 เคสตามที่ล็อกไว้)
// ==========================================================================

function toolBlockStart(index, name) {
  return { type: 'content_block_start', index, content_block: { type: 'tool_use', id: 'toolu_test', name, input: {} } }
}
function textBlockStart(index) {
  return { type: 'content_block_start', index, content_block: { type: 'text', text: '' } }
}
function blockStop(index) {
  return { type: 'content_block_stop', index }
}
function inputJsonDelta(index, partialJson) {
  return { type: 'content_block_delta', index, delta: { type: 'input_json_delta', partial_json: partialJson } }
}
function indexedTextDelta(index, text) {
  return { type: 'content_block_delta', index, delta: { type: 'text_delta', text } }
}

test('B2-1: text อย่างเดียว (มี tools ประกาศไว้แต่ไม่ได้เรียก) → output เหมือนเดิม 100% ไม่มี control event', async () => {
  state.events = [
    textBlockStart(0),
    indexedTextDelta(0, 'ยินดีค่ะ '),
    indexedTextDelta(0, 'สนใจไหมคะ'),
    blockStop(0),
  ]
  const controls = []
  const chunks = await collect(askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c)))
  assert.equal(chunks.join(''), 'ยินดีค่ะ สนใจไหมคะ')
  assert.deepEqual(controls, [])
})

test('B2-2: end_call tool อย่างเดียว (ไม่มี text block เลย) → ไม่มี text หลุดออกมา มี control event เดียว', async () => {
  state.events = [
    toolBlockStart(0, 'end_call'),
    inputJsonDelta(0, '{}'),
    blockStop(0),
  ]
  const controls = []
  const chunks = await collect(askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c)))
  assert.deepEqual(chunks, [])
  assert.deepEqual(controls, [{ type: 'end_call' }])
})

test('B2-3: text ตามด้วย end_call → ได้ text ก่อน แล้ว control ตามหลัง (ลำดับถูกต้อง)', async () => {
  const order = []
  state.events = [
    textBlockStart(0),
    indexedTextDelta(0, 'ขอบคุณที่สนใจนะคะ'),
    blockStop(0),
    toolBlockStart(1, 'end_call'),
    inputJsonDelta(1, '{}'),
    blockStop(1),
  ]
  const chunks = []
  for await (const c of askClaudeStreamChunked(makeSession(), null, () => order.push('control'))) {
    chunks.push(c)
    order.push('text:' + c)
  }
  assert.equal(chunks.join(''), 'ขอบคุณที่สนใจนะคะ')
  assert.deepEqual(order, ['text:ขอบคุณที่สนใจนะคะ', 'control'])
})

test('B2-4: input_json_delta ของ end_call แตกหลายชิ้น → ยัง emit control ได้ถูกต้องตอน block จบ', async () => {
  state.events = [
    toolBlockStart(0, 'end_call'),
    inputJsonDelta(0, '{'),
    inputJsonDelta(0, '}'),
    blockStop(0),
  ]
  const controls = []
  await collect(askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c)))
  assert.deepEqual(controls, [{ type: 'end_call' }])
})

test('B2-5: empty input_json_delta (partial_json ว่างเปล่า) ไม่ทำให้พัง ยัง emit control ปกติ', async () => {
  state.events = [
    toolBlockStart(0, 'end_call'),
    inputJsonDelta(0, ''),
    blockStop(0),
  ]
  const controls = []
  await assert.doesNotReject(async () => {
    for await (const _ of askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c))) { /* noop */ }
  })
  assert.deepEqual(controls, [{ type: 'end_call' }])
})

test('B2-6: tool อื่นที่ไม่รู้จัก (ไม่ใช่ end_call) → ห้ามกลายเป็น control event end_call', async () => {
  state.events = [
    toolBlockStart(0, 'some_other_tool'),
    inputJsonDelta(0, '{}'),
    blockStop(0),
  ]
  const controls = []
  await collect(askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c)))
  assert.deepEqual(controls, [], 'tool ที่ไม่ใช่ end_call ต้องไม่ trigger control event ใดๆ')
})

test('B2-7: error ระหว่าง tool block กำลังสะสม (ก่อนถึง content_block_stop) → ห้าม emit end_call หลอก', async () => {
  state.events = [
    toolBlockStart(0, 'end_call'),
    inputJsonDelta(0, '{'),
    blockStop(0), // ไม่ควรถูก yield จริง เพราะ throw จะแทรกก่อนถึงตำแหน่งนี้ (index 2)
  ]
  state.throwAfterIndex = 2 // ตัดก่อนถึง content_block_stop จริงๆ — จำลอง connection หลุดกลางทาง
  state.throwError = new Error('stream disconnected')
  const controls = []
  await assert.rejects(async () => {
    for await (const _ of askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c))) { /* noop */ }
  }, /stream disconnected/)
  assert.deepEqual(controls, [], 'block ยังไม่จบ (ไม่มี content_block_stop) ต้องไม่ยิง control event')
})

test('B2-8: ข้อความมี literal "[END_CALL]" อยู่ในเนื้อ text_delta → ถือเป็นข้อความธรรมดา ไม่ trigger control ใดๆ (ไม่มี parser เก่าค้าง)', async () => {
  state.events = [
    textBlockStart(0),
    indexedTextDelta(0, 'ลองพิมพ์ [END_CALL] ดูตรงนี้เฉยๆ ไม่ใช่คำสั่งอะไร'),
    blockStop(0),
  ]
  const controls = []
  const chunks = await collect(askClaudeStreamChunked(makeSession(), null, (c) => controls.push(c)))
  assert.equal(chunks.join(''), 'ลองพิมพ์ [END_CALL] ดูตรงนี้เฉยๆ ไม่ใช่คำสั่งอะไร')
  assert.deepEqual(controls, [], '[END_CALL] ในข้อความล้วนๆ ต้องไม่มีอำนาจควบคุมสายอีกต่อไป')
})
