// Dual Conversation Provider A/B — tests for conversationAI.js's resolveExplicitProvider() (pure logic) and
// askConversationConditionalStream() (dispatch). The dispatch test stubs '../src/services/claude' and
// '../src/services/gemini' directly (require.cache injection, same technique as claudeConditional.test.js
// uses for the Anthropic SDK) so it verifies ROUTING only, never making a real call to either provider.
const { test } = require('node:test')
const assert = require('node:assert/strict')

const claudePath = require.resolve('../src/services/claude')
const geminiPath = require.resolve('../src/services/gemini')

const calls = []
require.cache[claudePath] = {
  id: claudePath, filename: claudePath, loaded: true,
  exports: {
    askClaudeConditionalStream: async function* (session, signal, onMilestone) {
      calls.push({ fn: 'claude', session, signal, onMilestone })
      yield 'claude-response'
    },
    buildSystemPrompt: (p) => p,
    MAX_HISTORY: 20,
  },
}
require.cache[geminiPath] = {
  id: geminiPath, filename: geminiPath, loaded: true,
  exports: {
    askGeminiConditionalStream: async function* (session, signal, onMilestone) {
      calls.push({ fn: 'gemini', session, signal, onMilestone })
      yield 'gemini-response'
    },
    GEMINI_MODEL: 'gemini-3.7-flash',
  },
}

const { resolveExplicitProvider, askConversationConditionalStream, CLAUDE_MODEL, GEMINI_MODEL } = require('../src/services/conversationAI')

test('resolveExplicitProvider: campaign ไม่มี field llm_provider เลย → null (ไม่ใช่ default เป็น claude)', () => {
  assert.equal(resolveExplicitProvider({}), null)
  assert.equal(resolveExplicitProvider({ llm_provider: '' }), null)
  assert.equal(resolveExplicitProvider(undefined), null)
  assert.equal(resolveExplicitProvider(null), null)
})

test('resolveExplicitProvider: "claude" → { provider: claude, model: claude-sonnet-5 }', () => {
  const r = resolveExplicitProvider({ llm_provider: 'claude' })
  assert.deepEqual(r, { provider: 'claude', model: CLAUDE_MODEL })
  assert.equal(CLAUDE_MODEL, 'claude-sonnet-5')
})

test('resolveExplicitProvider: "gemini" → { provider: gemini, model: gemini-3.7-flash }', () => {
  const r = resolveExplicitProvider({ llm_provider: 'gemini' })
  assert.deepEqual(r, { provider: 'gemini', model: GEMINI_MODEL })
  assert.equal(GEMINI_MODEL, 'gemini-3.7-flash')
})

test('resolveExplicitProvider: case-insensitive และตัด whitespace รอบข้าง', () => {
  assert.deepEqual(resolveExplicitProvider({ llm_provider: ' Gemini ' }), { provider: 'gemini', model: GEMINI_MODEL })
  assert.deepEqual(resolveExplicitProvider({ llm_provider: 'CLAUDE' }), { provider: 'claude', model: CLAUDE_MODEL })
})

test('resolveExplicitProvider: ค่าที่ไม่รู้จัก (typo/ค่าเก่า) → null แทน error — campaign data ต้องไม่ทำให้สาย 500', () => {
  assert.equal(resolveExplicitProvider({ llm_provider: 'gpt4' }), null)
  assert.equal(resolveExplicitProvider({ llm_provider: 'sonnet' }), null)
})

test('askConversationConditionalStream: session.llmProvider="gemini" → เรียก Gemini เท่านั้น ไม่แตะ Claude เลย', async () => {
  calls.length = 0
  const session = { llmProvider: 'gemini', messages: [] }
  const out = []
  for await (const chunk of askConversationConditionalStream(session, null, null)) out.push(chunk)
  assert.deepEqual(out, ['gemini-response'])
  assert.equal(calls.length, 1)
  assert.equal(calls[0].fn, 'gemini')
})

test('askConversationConditionalStream: session.llmProvider="claude" → เรียก Claude เท่านั้น', async () => {
  calls.length = 0
  const session = { llmProvider: 'claude', messages: [] }
  const out = []
  for await (const chunk of askConversationConditionalStream(session, null, null)) out.push(chunk)
  assert.deepEqual(out, ['claude-response'])
  assert.equal(calls.length, 1)
  assert.equal(calls[0].fn, 'claude')
})

test('askConversationConditionalStream: session.llmProvider=null (default/legacyEarlyTts rollout) → default ไป Claude', async () => {
  calls.length = 0
  const session = { llmProvider: null, messages: [] }
  const out = []
  for await (const chunk of askConversationConditionalStream(session, null, null)) out.push(chunk)
  assert.deepEqual(out, ['claude-response'])
  assert.equal(calls[0].fn, 'claude')
})

test('askConversationConditionalStream: ส่ง signal/onMilestone ผ่านไปยัง provider ที่เลือกแบบตรงๆ ไม่ดัดแปลง', async () => {
  calls.length = 0
  const fakeSignal = { aborted: false }
  const fakeMilestone = () => {}
  const session = { llmProvider: 'gemini', messages: [] }
  for await (const _ of askConversationConditionalStream(session, fakeSignal, fakeMilestone)) { /* drain */ }
  assert.equal(calls[0].signal, fakeSignal)
  assert.equal(calls[0].onMilestone, fakeMilestone)
})
