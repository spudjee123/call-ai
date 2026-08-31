// Track 3 (Gemini Abort Fix + Lifecycle Diagnostics + Controlled Harness, Implementation Gate 2026-08-31,
// RCA revision 3 locked spec) — standalone diagnostic script. NEVER imported by, or run as part of, any
// production code path (src/, test/, webhook/campaign routes) — it calls the Gemini API directly with
// synthetic prompts to separate Hypothesis A (Google-side latency)/D (LOCAL vs UPSTREAM request overlap)/
// F (prompt size) with real data, per the RCA's own explicit rule against retrying blind.
//
// Isolation guard (locked spec, non-negotiable): this script must never generate uncontrolled concurrent
// load against the Gemini API — it is specifically testing the request-overlap hypothesis, so unbounded
// concurrency here would contaminate the exact thing it's trying to measure. Every test below runs
// attempts sequentially except Test E, which deliberately (and only) fires exactly 2 concurrent requests
// per iteration — never more, never as a background/parallel batch.
//
// Usage: node gemini-diagnostic-harness.js [iterationsPerTest]
//   iterationsPerTest defaults to 30 (locked spec's minimum). Pass a small number (e.g. 2) for a cheap
//   smoke run to confirm the harness itself works before committing to the full paid run.
//
// Cost/time note: the full default run issues ~30*2 (A/B) + 30*2 (C/D, each pair = 2 real requests) +
// 30*2 (E) = 300 real Gemini API requests, most against the ~19k-char prompt. This has real Gemini API
// cost and takes real wall-clock time (several minutes, since C/D deliberately wait between requests) —
// running it is a separate operational decision from writing this script, not something this script
// decides for you.

require('dotenv').config()
const { performance } = require('perf_hooks')
const fs = require('fs')
const path = require('path')
const { GoogleGenAI } = require('@google/genai')
const { buildSystemPrompt } = require('./src/services/claude')
const { GEMINI_MODEL } = require('./src/services/gemini')

// ===== Controlled variables (must match production exactly — src/services/gemini.js is authoritative;
// kept here as literals rather than imported because gemini.js doesn't export them as named constants,
// and this script intentionally makes zero changes to production code (locked spec: "ไม่กระทบ production
// code เลย"). If gemini.js's inline values ever change, update these two lines to match.) =====
const THINKING_LEVEL = 'MINIMAL'
const MAX_OUTPUT_TOKENS = 200

const API_KEY = process.env.GEMINI_API_KEY
if (!API_KEY) {
  console.error('Missing GEMINI_API_KEY in .env')
  process.exit(1)
}

const client = new GoogleGenAI({ apiKey: API_KEY })

// ===== Synthetic prompts — character-count-matched to real production observations (RCA 2026-08-31),
// NOT real campaign business content (this script never reads Google Sheets or any real campaign data —
// prompt SIZE is the variable under test, not semantic content). Built through the same buildSystemPrompt()
// wrapper production actually uses, for structural fidelity. =====
function padThaiText(unit, targetChars) {
  let out = ''
  while (out.length < targetChars) out += unit
  return out.slice(0, targetChars)
}
const SALES_SCRIPT_UNIT = 'สวัสดีค่ะ วันนี้ทางเรามีโปรโมชั่นพิเศษมาแจ้งให้ทราบ สำหรับสมาชิกที่สนใจสามารถสอบถามรายละเอียดเพิ่มเติมได้ค่ะ ห้ามพูดเรื่องอื่นที่ไม่เกี่ยวข้องกับโปรโมชั่นเด็ดขาด ตอบคำถามลูกค้าอย่างสุภาพและกระชับที่สุดเสมอ '
const FULL_CAMPAIGN_PROMPT = padThaiText(SALES_SCRIPT_UNIT, 17386) // matches real l2bCampaignPromptCharCount observed in production logs
const SHORT_CAMPAIGN_PROMPT = padThaiText(SALES_SCRIPT_UNIT, 2000) // conservative short anchor vs. the ~6,193 baseline noted in the earlier comparison

const FULL_SYSTEM_PROMPT = buildSystemPrompt(FULL_CAMPAIGN_PROMPT, 'คุณทดสอบ')
const SHORT_SYSTEM_PROMPT = buildSystemPrompt(SHORT_CAMPAIGN_PROMPT, 'คุณทดสอบ')

const USER_QUESTION = 'มีโปรโมชั่นอะไรบ้างคะ' // fixed constant across every attempt — the only thing that varies between A and B is prompt size

// ===== One measured attempt — mirrors src/services/gemini.js's Track 2 instrumentation exactly, so
// results here are directly comparable to production [Metrics] data. =====
async function runOneAttempt({ systemPrompt, signal } = {}) {
  const requestAt = performance.now()
  let streamCreatedAt = null, firstRawChunkAt = null, firstTextAt = null, completedAt = null
  let errorMessage = null, aborted = false

  try {
    const stream = await client.models.generateContentStream({
      model: GEMINI_MODEL,
      contents: [{ role: 'user', parts: [{ text: USER_QUESTION }] }],
      config: {
        systemInstruction: { parts: [{ text: systemPrompt }] },
        maxOutputTokens: MAX_OUTPUT_TOKENS,
        thinkingConfig: { thinkingLevel: THINKING_LEVEL },
        abortSignal: signal || undefined,
      },
    })
    streamCreatedAt = performance.now()

    for await (const chunk of stream) {
      if (firstRawChunkAt === null) firstRawChunkAt = performance.now()
      if (chunk?.text && firstTextAt === null) firstTextAt = performance.now()
    }
    completedAt = performance.now()
  } catch (err) {
    errorMessage = err.message
    if (signal?.aborted || err.name === 'AbortError') aborted = true
  }

  return {
    requestAt, streamCreatedAt, firstRawChunkAt, firstTextAt, completedAt, errorMessage, aborted,
    streamCreateMs: streamCreatedAt != null ? streamCreatedAt - requestAt : null,
    firstRawChunkMs: firstRawChunkAt != null ? firstRawChunkAt - requestAt : null,
    firstTextMs: firstTextAt != null ? firstTextAt - requestAt : null,
  }
}

// Test C/D — abort a first request partway through, then issue a SECOND, independent request either
// immediately (C) or after an 8-10s cooldown (D). The result that matters is the SECOND request's
// latency, not the first (aborted) one — this is what separates Hypothesis D's LOCAL-overlap-provable
// part from its UPSTREAM-overlap-unprovable part (see RCA Part 8, Hypothesis D revision 3 table).
async function runAbortPair({ waitMs }) {
  const controller = new AbortController()
  const firstPromise = runOneAttempt({ systemPrompt: FULL_SYSTEM_PROMPT, signal: controller.signal })
  await new Promise(r => setTimeout(r, 800)) // let the first request actually get past stream-creation before cutting it
  controller.abort()
  await firstPromise.catch(() => {}) // discarded — only the second request's timing is the measurement

  if (waitMs > 0) await new Promise(r => setTimeout(r, waitMs))

  return runOneAttempt({ systemPrompt: FULL_SYSTEM_PROMPT })
}

// Test E — exactly 2 concurrent requests, never more (isolation guard).
async function runConcurrentPair() {
  return Promise.all([
    runOneAttempt({ systemPrompt: FULL_SYSTEM_PROMPT }),
    runOneAttempt({ systemPrompt: FULL_SYSTEM_PROMPT }),
  ])
}

function percentile(sortedArr, p) {
  if (!sortedArr.length) return null
  const idx = Math.min(sortedArr.length - 1, Math.floor((p / 100) * sortedArr.length))
  return Math.round(sortedArr[idx])
}

function summarize(results) {
  const n = results.length
  const streamCreateMs = results.map(r => r.streamCreateMs).filter(v => v != null).sort((a, b) => a - b)
  const firstRawChunkMs = results.map(r => r.firstRawChunkMs).filter(v => v != null).sort((a, b) => a - b)
  const firstTextMs = results.map(r => r.firstTextMs).filter(v => v != null).sort((a, b) => a - b)
  const errorCount = results.filter(r => r.errorMessage && !r.aborted).length
  // "over Xs" counts a never-arrived firstText (null) as over threshold too — a hang IS a >6s case, not excluded from the rate
  const over3sCount = results.filter(r => r.firstTextMs == null || r.firstTextMs > 3000).length
  const over6sCount = results.filter(r => r.firstTextMs == null || r.firstTextMs > 6000).length
  const pct = (arr) => ({ p50: percentile(arr, 50), p90: percentile(arr, 90), p95: percentile(arr, 95) })
  return {
    n,
    errorRate: n ? +(errorCount / n).toFixed(3) : null,
    over3sRate: n ? +(over3sCount / n).toFixed(3) : null,
    over6sRate: n ? +(over6sCount / n).toFixed(3) : null,
    streamCreateMs: pct(streamCreateMs),
    firstRawChunkMs: pct(firstRawChunkMs),
    firstTextMs: pct(firstTextMs),
  }
}

function printSummaryTable(summary) {
  const rows = Object.entries(summary)
  console.log('\n===== SUMMARY =====')
  for (const [label, s] of rows) {
    console.log(`\n${label}  (n=${s.n})`)
    console.log(`  errorRate=${s.errorRate}  over3sRate=${s.over3sRate}  over6sRate=${s.over6sRate}`)
    console.log(`  streamCreateMs   p50=${s.streamCreateMs.p50} p90=${s.streamCreateMs.p90} p95=${s.streamCreateMs.p95}`)
    console.log(`  firstRawChunkMs  p50=${s.firstRawChunkMs.p50} p90=${s.firstRawChunkMs.p90} p95=${s.firstRawChunkMs.p95}`)
    console.log(`  firstTextMs      p50=${s.firstTextMs.p50} p90=${s.firstTextMs.p90} p95=${s.firstTextMs.p95}`)
  }
}

async function main() {
  const iterations = parseInt(process.argv[2], 10) || 30

  console.log(`Gemini Diagnostic Harness (Track 3) — ${iterations} iterations/test`)
  console.log(`model=${GEMINI_MODEL} thinkingLevel=${THINKING_LEVEL} maxOutputTokens=${MAX_OUTPUT_TOKENS}`)
  console.log(`FULL prompt length=${FULL_SYSTEM_PROMPT.length} chars, SHORT prompt length=${SHORT_SYSTEM_PROMPT.length} chars`)

  // A/B interleaved (locked spec requirement — never block-run) to keep provider-load drift from confounding the prompt-size comparison
  const resultsA = [], resultsB = []
  for (let i = 0; i < iterations; i++) {
    resultsA.push(await runOneAttempt({ systemPrompt: FULL_SYSTEM_PROMPT }))
    resultsB.push(await runOneAttempt({ systemPrompt: SHORT_SYSTEM_PROMPT }))
    process.stdout.write(`\r  A/B progress: ${i + 1}/${iterations}`)
  }
  console.log()

  // C/D interleaved with each other (not with A/B/E — isolation guard: overlap tests run in their own block)
  const resultsC = [], resultsD = []
  for (let i = 0; i < iterations; i++) {
    resultsC.push(await runAbortPair({ waitMs: 0 }))
    resultsD.push(await runAbortPair({ waitMs: 9000 }))
    process.stdout.write(`\r  C/D progress: ${i + 1}/${iterations}`)
  }
  console.log()

  // E — controlled concurrency of exactly 2, its own isolated block
  const resultsE = []
  for (let i = 0; i < iterations; i++) {
    const [r1, r2] = await runConcurrentPair()
    resultsE.push(r1, r2)
    process.stdout.write(`\r  E progress: ${i + 1}/${iterations}`)
  }
  console.log()

  const summary = {
    'A: full prompt (~19k), clean': summarize(resultsA),
    'B: short prompt (~2k), clean': summarize(resultsB),
    'C: full prompt, immediate request after abort': summarize(resultsC),
    'D: full prompt, request after 9s cooldown post-abort': summarize(resultsD),
    'E: full prompt, 2 concurrent requests': summarize(resultsE),
  }
  printSummaryTable(summary)

  console.log(`
===== READING THE RESULTS (see RCA Part 8/31 for full context) =====
A vs B:  if B's over6sRate is much lower than A's → Hypothesis F (prompt size) has real weight
C vs D:  if C's firstTextMs p90/p95 is much worse than D's → Hypothesis D (request overlap) has real weight
         (this is INDIRECT evidence of UPSTREAM overlap — activeGeminiAttemptCountAtStart in production
         only ever proves LOCAL overlap; C vs D is the one experiment that can support the upstream claim)
E:       high error/timeout rate here on top of A's baseline → supports concurrency/quota-pressure as a factor
If A and B are both bad and C ≈ D ≈ A → points toward Hypothesis A (Google-side/network) carrying the most weight
`)

  const outPath = path.join(__dirname, `gemini-diagnostic-results-${Date.now()}.json`)
  fs.writeFileSync(outPath, JSON.stringify({
    config: { model: GEMINI_MODEL, thinkingLevel: THINKING_LEVEL, maxOutputTokens: MAX_OUTPUT_TOKENS, iterations, fullPromptChars: FULL_SYSTEM_PROMPT.length, shortPromptChars: SHORT_SYSTEM_PROMPT.length },
    summary,
    resultsA, resultsB, resultsC, resultsD, resultsE,
  }, null, 2))
  console.log(`Full raw results written to ${outPath}`)
}

main().catch(err => { console.error('Harness failed:', err); process.exit(1) })
