const speech = require('@google-cloud/speech')
const { mulawBufferToPcm16 } = require('../utils/audioConverter')
const healthMonitor = require('../utils/healthMonitor')
const { getGoogleClientOptions } = require('../utils/googleCredentials')

const client = new speech.SpeechClient(getGoogleClientOptions())

const STT_CONFIG = {
  encoding: 'LINEAR16',
  sampleRateHertz: 8000,
  languageCode: 'th-TH',
  model: 'latest_short',
  useEnhanced: true,
  speechContexts: [{
    phrases: [
      'สวัสดี', 'ครับ', 'ค่ะ', 'สนใจ', 'ราคา', 'โปรโมชั่น', 'ไม่สนใจ', 'ขอบคุณ',
      'PGDOG', 'พีจีด็อก', 'แอดไลน์', 'พอยต์', 'ฝาก', 'สมัคร', 'โบนัส',
      'รับ', 'อยากรับ', 'สมัครรับ', 'ต้องทำยังไง',
      'ฮัลโหล', 'ฮัลโหลค่ะ', 'ฮัลโหลครับ', 'อัลโหล', // คำแรกที่ลูกค้ามักพูดตอนรับสาย — เดิมไม่มีใน list เลย ทำให้ STT บางทีจับผิดเป็นคำอื่นที่ฟังคล้ายกัน
      'ได้ยิน', 'ได้ยินครับ', 'ได้ยินค่ะ', 'ได้ยินไหมคะ', 'ได้ยินไหมครับ', // อีกคำที่พูดบ่อยตอนรับสาย เจอสับสนกับ "อยากได้" เพราะไม่มีใน list
      'ฮัลโหลครับได้ยินไหม', 'ฮัลโหลค่ะได้ยินไหม', // ลูกค้าพูดรวมประโยคเดียว เจอ STT จับผิดเป็น "ครับรอครับ" ทั้งที่คำย่อยแต่ละคำอยู่ใน list แล้ว — ลองเพิ่มทั้งประโยคเผื่อช่วย
      'พอยต์เอาไปทำอะไรได้', 'พอยต์ทำอะไรได้บ้าง', 'พอยต์ใช้ทำอะไรได้', // "พอยต์" พ้องเสียงกับ "พอ" (เหมือนกัน) เจอ STT ฟังเป็น "พอแล้วพอแล้ว..." ทั้งที่ "พอยต์" เดี่ยวๆ อยู่ใน list แล้ว — ลองเพิ่มทั้งวลีที่ลูกค้ามักถามแทน
    ],
    boost: 15
  }],
  enableAutomaticPunctuation: true,
}

// Design A (interim regression protection, production incident 2026-08-20) — production call ยืนยันว่า Google
// ส่ง interim เดินหน้า/ถอยหลังสลับกันได้จริง ("ok" → "ok ครับ" → "ok") เดิม interimText ถูก overwrite แบบไม่มี
// เงื่อนไขทุกครั้งที่ interim ใหม่มา ทำให้ 900ms timer หยิบค่าล่าสุดไปเสมอแม้จะเป็น candidate ที่ "ถอยหลัง" จริง —
// ป้องกันเฉพาะกรณี strict-prefix regression เท่านั้น (candidate ใหม่สั้นกว่าและเป็น prefix ของ candidate เดิม
// เป๊ะ หลัง normalize) ไม่ใช้ naive "longest-wins" เพราะ recognizer แก้คำผิดจริงได้ (เช่น "สงคราม"→"สนใจครับ" ไม่ใช่
// prefix relation เลย ต้องยอม overwrite ตามปกติ) — normalize ใช้เพื่อเปรียบเทียบเท่านั้น ค่าที่เก็บ/deliver จริง
// ยังเป็นข้อความดิบจาก Google เป๊ะ ไม่ lowercase/trim ทิ้ง
function normalizeCandidate(text) {
  return text.trim().replace(/\s+/g, ' ').toLowerCase()
}

function isInterimRegression(previous, candidate) {
  if (!previous) return false
  const prevNorm = normalizeCandidate(previous)
  const candNorm = normalizeCandidate(candidate)
  if (candNorm.length >= prevNorm.length) return false
  return prevNorm.startsWith(candNorm)
}

// STT-A2 (diagnostic only, 2026-08-21): normalize Google's alternatives array into a stable shape for
// [STT_DIAG] — never assume exactly 3 (Google may return fewer than maxAlternatives requested, or exactly 1
// when A2 is OFF). confidence follows the same null-vs-fabricated-0 rule as STT-A1's finalConfidence — Google
// typically only sets confidence on the top hypothesis and mainly on final results, not every alternative.
function normalizeAlternatives(alternatives) {
  return (alternatives || []).map((alt, index) => ({
    index,
    text: alt.transcript,
    confidence: (typeof alt.confidence === 'number' && alt.confidence > 0) ? alt.confidence : null,
    selected: index === 0,
  }))
}

// A2.1 Shadow Google Final Diagnostics (design revision 2026-08-21, Design Gate v2 PASS) — diagnostic
// observation cap only, NOT a latency budget. No user-facing path (onTranscript/TIMER_FINAL delivery/TTS/
// Claude) ever waits on this timer — it only bounds how long a settled-pending shadow object is allowed to
// wait for a possible late native final on an already-demoted (rotated-away) stream before giving up.
// Locked at 2000ms for the first production evidence run (Design Review). If that run shows 100% TIMEOUT
// or 100% STREAM_END, do NOT guess-raise this value (5000/10000) — rotateForNextUtterance() below calls
// draining.end() on the old stream immediately after TIMER_FINAL, which may half-close the gRPC call
// before Google would ever send a late final at all, in which case a longer timeout changes nothing. That
// scenario is its own follow-up design (A2.2 — not calling .end() immediately), evaluated separately
// because it changes actual stream/resource lifetime, unlike this diagnostic-only observation window.
const SHADOW_TIMEOUT_MS = 2000

// L1a (latency optimization, rollout-scoped STT endpoint experiment): interimFinalizeMs รับจากภายนอกได้แล้ว
// default 900ms เดิมทุกประการถ้า caller ไม่ส่งอะไรมา — googleSTT.js เป็น STT ตัวเดียวที่ legacy และ chunked
// path ใช้ร่วมกัน เปลี่ยนค่า default ตรงๆ จะกระทบ legacy production ทุกสายทันทีโดยไม่ผูกกับ chunked rollout เลย
// ต้องให้ audioStream.js เป็นคนตัดสินใจค่าต่อสาย (ตาม rollout ที่ freeze แล้วตอน 'start') แล้วส่งเข้ามาแทน
//
// A2.1 (design revision 2026-08-21): enableShadow/onShadowDiagnostic คนละ option จาก maxAlternatives ตั้งใจ
// — A2 ON ไม่ได้แปลว่า shadow ต้อง ON เสมอ (independent rollout gate ของตัวเอง อยู่ที่ audioStream.js) ตัว
// googleSTT.js เองยังบังคับซ้ำอีกชั้น (ดู activeShadow setup ด้านล่าง) ว่า shadow ทำงานได้เฉพาะเมื่อ
// enableShadow===true "และ" maxAlternatives>1 พร้อมกันเท่านั้น — invariant "A2 OFF → A2.1 OFF" จึงเป็นจริง
// ภายใน module นี้เอง ไม่ต้องพึ่ง caller (audioStream.js) ส่งค่ามาถูกเสมอ
function transcribeStream(onTranscript, onInterim, { interimFinalizeMs = 900, maxAlternatives = null, enableShadow = false, onShadowDiagnostic = null } = {}) {
  let destroyed = false
  let currentStream = null
  let nextStream = null
  let errorRetryCount = 0

  // A2.1 Shadow — at most ONE pending shadow observation per call (O(1) memory, design-approved). NOT reset
  // by resetUtteranceState() — that function scopes to the CURRENT utterance's live state, but activeShadow
  // tracks an OLD (already-rotated-away) stream's trailing native-final observation, spanning across the
  // utterance boundary by design. { stream, streamId, utteranceId, timerFinalText, timerFinalAt, settled,
  // timeoutHandle } while pending; null when no shadow is active. See settleShadow() below for the only
  // place this is ever mutated to null or marked settled.
  let activeShadow = null

  let writeCount = 0
  let code11Count = 0

  let interimText = ''
  let interimTimer = null
  let utteranceClosed = false  // true after timer delivers transcript — blocks trailing interims and isFinal duplicate

  // STT-A1 (observability only, 2026-08-21): telemetry-only state — ไม่มีค่าใดในบล็อกนี้ถูกใช้ตัดสิน recognition
  // behavior/filter ใดๆ เลย มีไว้แค่ประกอบ metadata ส่งออกไปให้ audioStream.js log เท่านั้น
  let streamIdCounter = 0
  let utteranceIdCounter = 0
  let utteranceIdAssigned = false   // reset คู่กับ utteranceClosed ใน resetUtteranceState() เสมอ — กัน utteranceId ถูกใช้ซ้ำข้าม utterance
  let currentUtteranceId = null
  let interimCount = 0
  let regressionCount = 0
  let firstInterimAt = null
  let lastInterimAt = null
  let lastStability = null   // null = "ไม่มีค่าจริง" (0 กับ missing แยกไม่ออกใน proto3 float — ไม่ fabricate 0)
  let maxStability = null
  let coldMutePackets = 0
  // STT-A2 (diagnostic only): snapshot ของ alternatives ที่ผูกกับ interim ที่ "ยอมรับจริง" เท่านั้น (ตัวเดียวกับ
  // ที่ update interimText) — ต้อง update ใน branch เดียวกันเป๊ะ ไม่งั้น TIMER_FINAL อาจ deliver alternatives ของ
  // interim ที่ถูก regression reject ไปแล้วแทนที่จะเป็นของ interimText จริงที่ deliver ออกไป
  let acceptedAlternatives = null
  // เดิม 1500ms — วัดจาก log จริงพบว่า Google isFinal แทบไม่เคยมาทัน ต้องพึ่ง timer นี้ตัดสินใจแทบทุกครั้ง
  // ทำให้ลูกค้าต้องรอเงียบเต็ม 1.5 วิทุกรอบก่อน AI จะเริ่มคิดคำตอบด้วยซ้ำ ลดลงมาก่อนเป็นค่าที่ยังกันลูกค้าหยุดคิดกลางประโยคได้
  const INTERIM_FINALIZE_MS = interimFinalizeMs

  // จากสายจริงหลายสาย เจอคำหลอน ("สงคราม", "โทรศัพท์", "โลตัส") ที่ไม่เกี่ยวกับที่ลูกค้าพูดเลย
  // เกิดเป็นคำแรกทันทีหลัง "Creating new stream" ทุกครั้ง — คาดว่าเป็นเสียงสะท้อน/สัญญาณตกค้างตรงรอยต่อสาย
  // ทิ้งเสียงช่วงสั้นๆ แรกของ stream ใหม่ ก่อนเริ่มส่งให้ Google ฟังจริง
  const STREAM_MUTE_MS = 200
  let coldStreamMuteUntil = 0

  function resetUtteranceState() {
    clearTimeout(interimTimer)
    interimTimer = null
    interimText = ''
    utteranceClosed = false
    // STT-A1: utterance-scoped diagnostic counters — reset พร้อมกับ utterance state จริงเสมอ
    utteranceIdAssigned = false
    currentUtteranceId = null
    interimCount = 0
    regressionCount = 0
    firstInterimAt = null
    lastInterimAt = null
    lastStability = null
    maxStability = null
    coldMutePackets = 0
    acceptedAlternatives = null
  }

  // A2.1 Shadow — the ONLY place a shadow is ever settled/cleared. Atomic ordering is mandatory (Design
  // Review Blocker 2): mark settled → clear timer → snapshot payload → null out activeShadow, ALL before the
  // diagnostic callback ever runs — so no shadow state is left half-updated no matter what the callback does.
  // onShadowDiagnostic is called last, wrapped in try/catch: a throwing diagnostic callback must never affect
  // STT lifecycle (observability failing must not break the call) — it only drops that one diagnostic emit.
  //
  // Implementation Review fix (identity binding): takes the exact `shadow` object being settled as a
  // parameter instead of reading module-level `activeShadow` directly, and rejects unless
  // `activeShadow === shadow` — this must not rely on clearTimeout() alone to prevent a stale/superseded
  // shadow's timeout callback from settling whatever shadow happens to be active by the time it fires (a
  // narrow but real race: clearTimeout() only reliably prevents a callback that hasn't already been queued
  // to run). Every call site below captures `const shadow = activeShadow` locally first and passes that
  // same reference through, so the check is against the shadow the caller actually intended to settle.
  function settleShadow(shadow, outcome, resultData = {}) {
    if (!shadow || activeShadow !== shadow || shadow.settled) return
    shadow.settled = true
    clearTimeout(shadow.timeoutHandle)

    const payload = {
      streamId: shadow.streamId,
      utteranceId: shadow.utteranceId,
      timerFinalText: shadow.timerFinalText,
      timerFinalAt: shadow.timerFinalAt,
      shadowFinalAt: resultData.shadowFinalAt ?? null,
      shadowFinalDelayMs: resultData.shadowFinalDelayMs ?? null,
      shadowAlternatives: resultData.shadowAlternatives ?? null,
      shadowOutcome: outcome,
    }

    activeShadow = null

    try {
      onShadowDiagnostic?.(payload)
    } catch (err) {
      console.error('[STT_SHADOW_DIAG] onShadowDiagnostic threw, diagnostic dropped:', err.message)
    }
  }

  function activatePrewarm() {
    resetUtteranceState()
    currentStream = nextStream
    nextStream = null
    coldStreamMuteUntil = 0 // stream ที่ prewarm ไว้ฟังมาสักพักแล้ว ไม่ใช่ cold start ไม่ต้อง mute
  }

  // C6c follow-up (production discovery 2026-08-19): singleUtterance:true ทำให้ Google หยุดฟังทันทีที่จับ
  // utterance จบ — เดิมพึ่ง audioStream.js เรียก reset() ตอนได้รับ Twilio 'mark' (ยืนยันเสียง AI เล่นจบสมบูรณ์)
  // ถึงจะเปิดฟังใหม่ ทำให้ตลอดช่วงที่ AI กำลังพูด (หลายวินาที) ไม่มี stream ไหนฟังอยู่เลย พิสูจน์จาก production call
  // จริงที่ลูกค้าพูดแทรกแล้วไม่มี STT event ใดๆ เกิดขึ้นเลย ไม่ใช่แค่ barge-in ไม่ทำงาน — ไม่มีจุดไหนแม้แต่ได้ยิน
  //
  // แก้โดยเปิดฟัง utterance ถัดไปทันทีที่ onTranscript() ของ utterance ปัจจุบัน deliver ออกไปแล้ว (ไม่ต้องรอ mark
  // เลย) — ต้องเรียกจากจุดที่ transcript deliver ไปแล้วเท่านั้น (ไม่ใช่แค่เห็น END_OF_SINGLE_UTTERANCE event) กัน
  // final ตัวจริงของ utterance เดิมหายไปก่อนถูกส่งออก และต้องเป็นคำสั่งสุดท้ายที่ caller เรียกเสมอ (หลัง
  // interimText/utteranceClosed ของ utterance เดิมถูกจัดการเสร็จแล้ว) ไม่งั้น resetUtteranceState() ข้างในนี้
  // (ที่ต้องเป็นคนกำหนด state เริ่มต้นของ stream ใหม่) จะถูกทับด้วย utteranceClosed=true ของ utterance เดิมทีหลัง
  // ทำให้ stream ใหม่โดน mark ว่า closed ทั้งที่ยังไม่เคยเริ่มฟังอะไรเลย บล็อก interim ของ barge-in ตั้งแต่ต้น
  function rotateForNextUtterance() {
    if (destroyed) return
    const draining = currentStream
    if (nextStream) {
      activatePrewarm()
      console.log('[STT] Rotated to pre-warmed stream — listening continues through AI playback')
    } else {
      currentStream = null
      resetUtteranceState()
      createStream(false)
      console.log('[STT] No pre-warm ready — creating fresh stream immediately (no wait for mark)')
    }
    // ปิด stream เก่าอย่างสุภาพเพื่อคืน resource — trailing event ของมันถูกกันด้วย `stream !== currentStream`
    // ที่ต้นทาง .on('data') อยู่แล้วไม่ว่าจะปิดตรงนี้หรือไม่ ไม่มีความเสี่ยงเรื่อง final หายซ้ำ
    if (draining) { try { draining.end() } catch (_) {} }
  }

  function createStream(isPrewarm = false) {
    if (destroyed) return
    if (!isPrewarm && currentStream) return

    console.log(isPrewarm ? '[STT] Pre-warming next stream' : '[STT] Creating new stream')

    const thisStreamId = ++streamIdCounter // STT-A1: monotonic per-call, ครอบคลุมทั้ง prewarm และ non-prewarm

    // STT-A2 (diagnostic only): เมื่อ OFF (maxAlternatives ไม่ใช่ number > 1) ส่ง STT_CONFIG object เดิมตรงๆ
    // ไม่ spread เลยแม้แต่ key เดียว — request shape ต้องเหมือน production เดิมทุกบิตตอน A2 ปิดอยู่
    const recognitionConfig = (typeof maxAlternatives === 'number' && maxAlternatives > 1)
      ? { ...STT_CONFIG, maxAlternatives }
      : STT_CONFIG

    const stream = client.streamingRecognize({
      config: recognitionConfig,
      interimResults: true,
      singleUtterance: true,
    })
    .on('error', (err) => {
      if (destroyed) return
      // A2.1 Shadow — must settle BEFORE the stale-stream guard below, since a shadowed stream is by
      // definition neither currentStream nor nextStream (it was already rotated away when the shadow
      // started) and would otherwise be silently discarded by that guard with no ERROR outcome ever
      // recorded. Returns immediately — must never fall into the recovery logic below (errorRetryCount,
      // stream recreation) since that logic only applies to the live current/next streams.
      const errorShadow = activeShadow
      if (errorShadow && stream === errorShadow.stream && !errorShadow.settled) {
        settleShadow(errorShadow, 'ERROR', {})
        return
      }
      if (stream !== currentStream && stream !== nextStream) return

      if (err.code === 11) {
        code11Count++
        if (code11Count % 5 === 0) console.log(`[STT] Stream reset (code 11) ×${code11Count}`)
      } else {
        console.error('[STT error]', err.message)
        healthMonitor.reportError('stt', err.message)
      }

      if (stream === currentStream) {
        currentStream = null
        errorRetryCount++
        if (errorRetryCount >= 10) {
          console.error('[STT] Too many consecutive errors, stopping recreation')
          return
        }
        if (nextStream) {
          activatePrewarm()
          console.log('[STT] Error recovery: switched to pre-warmed stream')
        } else {
          resetUtteranceState()
          setTimeout(() => createStream(false), 100)
        }
      } else {
        nextStream = null
      }
    })
    .on('data', (data) => {
      // A2.1 Shadow — must run before the stale-stream guard below (a shadowed stream is always
      // `!== currentStream`) and must `return` before touching errorRetryCount or any live utterance state
      // (interimText/counters) — shadow observation is structurally a separate path from live processing,
      // never onTranscript()'d, never mutating anything the live path reads.
      const dataShadow = activeShadow
      if (dataShadow && stream === dataShadow.stream && !dataShadow.settled) {
        const shadowResult = data.results?.[0]
        if (shadowResult?.isFinal) {
          const nowTs = Date.now()
          settleShadow(dataShadow, 'FINAL', {
            shadowFinalAt: nowTs,
            shadowFinalDelayMs: nowTs - dataShadow.timerFinalAt,
            shadowAlternatives: normalizeAlternatives(shadowResult.alternatives),
          })
        }
        return
      }

      if (stream !== currentStream) return
      errorRetryCount = 0

      const result = data.results[0]
      if (!result) {
        if (data.speechEventType) console.log(`[STT] Event: ${data.speechEventType}`)
        return
      }
      const text = result.alternatives?.[0]?.transcript || ''

      if (!result.isFinal) {
        if (!text || utteranceClosed) return

        // STT-A1: diagnostic snapshot — ต้องจับ nowTs และ update ก่อนเรียก onInterim?.() เสมอ (Review Gate round 4
        // fix) เพราะ onInterim ใน audioStream.js ไม่ใช่ noop จริง (ทำ barge-in detection/prewarm trigger แบบ
        // synchronous) ถ้าจับเวลาหลัง callback firstInterimAt/lastInterimAt จะเลื่อนช้าไปเท่ากับเวลาที่ callback ใช้
        // ไม่ใช่เวลาที่ Google interim มาถึงจริง — บล็อกนี้ไม่มีผลต่อ interimText/regression/filter behavior ใดๆ เลย
        if (!utteranceIdAssigned) { currentUtteranceId = ++utteranceIdCounter; utteranceIdAssigned = true }
        interimCount++
        const nowTs = Date.now()
        if (firstInterimAt === null) firstInterimAt = nowTs
        lastInterimAt = nowTs
        // Review Gate round 1 fix: เดิม lastStability เก็บเฉพาะตอน >0 เท่านั้น ทำให้ถ้า interim ล่าสุดมี
        // stability=0/missing แต่ interim ก่อนหน้ามีค่าจริง ค่าที่ log จะเป็น "ค่าที่ไม่ใช่ศูนย์ล่าสุด" ไม่ใช่
        // "ค่าของ interim ล่าสุดจริง" ตามชื่อ field — ต้อง set ทุกครั้งให้ตรงกับ interim ปัจจุบันเป๊ะ (null ถ้าใช้ไม่ได้)
        // ส่วน maxStabilityยังอัปเดตเฉพาะค่าที่ >0 เท่านั้นเหมือนเดิม (ไม่มีเหตุผลให้ null ไปลด max ที่เคยเจอ)
        const normalizedStability = (typeof result.stability === 'number' && result.stability > 0) ? result.stability : null
        lastStability = normalizedStability
        if (normalizedStability !== null) {
          maxStability = maxStability === null ? normalizedStability : Math.max(maxStability, normalizedStability)
        }

        console.log(`[STT interim] "${text}"`)
        onInterim?.(text)

        const isRegression = isInterimRegression(interimText, text)
        if (isRegression) {
          console.log(`[STT] Interim regression ignored: "${interimText}" <- "${text}"`)
          regressionCount++
        } else {
          interimText = text
          // STT-A2: update เฉพาะ branch เดียวกันนี้เป๊ะ ที่ update interimText — กัน TIMER_FINAL หยิบ alternatives
          // ของ interim ที่เพิ่ง regression-reject ไปแทนที่จะเป็นของ interimText จริงที่กำลังจะ deliver
          if (typeof maxAlternatives === 'number' && maxAlternatives > 1) {
            acceptedAlternatives = normalizeAlternatives(result.alternatives)
          }
        }

        if (!nextStream && !destroyed) createStream(true)

        clearTimeout(interimTimer)
        interimTimer = setTimeout(() => {
          if (interimText && !destroyed) {
            console.log(`[STT] Interim→Final (${INTERIM_FINALIZE_MS}ms silence): "${interimText}"`)
            const deliveredText = interimText
            interimText = ''
            utteranceClosed = true
            const finalAt = Date.now()
            onTranscript(deliveredText, {
              streamId: thisStreamId,
              utteranceId: currentUtteranceId,
              source: 'TIMER_FINAL',
              interimCount,
              regressionCount,
              firstInterimAt,
              lastInterimAt,
              finalAt,
              firstInterimToFinalMs: firstInterimAt !== null ? finalAt - firstInterimAt : null,
              lastStability,
              maxStability,
              finalConfidence: null, // TIMER_FINAL คือการตัดสินใจของเราเอง ไม่ใช่ final ที่ Google ส่ง confidence มาด้วย
              coldMutePackets,
              alternatives: acceptedAlternatives, // STT-A2: snapshot ที่ผูกกับ interimText นี้เป๊ะ (null เมื่อ A2 OFF)
            })

            // A2.1 Shadow — start observing THIS stream (about to be rotated away) for a possible late
            // native final, only when both the independent shadow gate (enableShadow) and A2 itself
            // (maxAlternatives>1) are on — enforced here defense-in-depth, not just trusted from the caller.
            // Must be set up before rotateForNextUtterance() below demotes `stream` from currentStream — the
            // .on('data')/.on('error')/.on('end') branches above match on `stream === activeShadow.stream`
            // by reference, so this has to run while `stream` is still the value being rotated away, not after.
            if (enableShadow === true && typeof maxAlternatives === 'number' && maxAlternatives > 1) {
              // at most one active shadow per call — supersede whatever's still pending via its own captured reference
              if (activeShadow && !activeShadow.settled) settleShadow(activeShadow, 'SUPERSEDED', {})
              const shadow = {
                stream,
                streamId: thisStreamId,
                utteranceId: currentUtteranceId,
                timerFinalText: deliveredText,
                timerFinalAt: finalAt,
                settled: false,
                timeoutHandle: null,
              }
              // timeout closure captures THIS shadow object by reference (not activeShadow) — settleShadow()
              // rejects it once activeShadow has moved on (SUPERSEDED/settled elsewhere), so a stale timer
              // from an old, already-superseded shadow can never settle whatever shadow is active by the
              // time it fires, regardless of clearTimeout() timing (Implementation Review fix)
              shadow.timeoutHandle = setTimeout(() => settleShadow(shadow, 'TIMEOUT', {}), SHADOW_TIMEOUT_MS)
              activeShadow = shadow
            }

            rotateForNextUtterance() // ต้องอยู่ท้ายสุดเสมอ (ดูหมายเหตุที่ rotateForNextUtterance ด้านบน)
          }
        }, INTERIM_FINALIZE_MS)
        return
      }

      // isFinal
      clearTimeout(interimTimer)
      interimTimer = null
      const finalText = text.trim()
      const shouldDeliver = !utteranceClosed
      interimText = ''
      utteranceClosed = false
      if (shouldDeliver) {
        if (finalText) {
          // STT-A1: GOOGLE_FINAL อาจมาโดยไม่มี interim นำมาก่อนเลย — ถ้ายังไม่เคย assign utteranceId ให้ assign ตอนนี้
          if (!utteranceIdAssigned) { currentUtteranceId = ++utteranceIdCounter; utteranceIdAssigned = true }
          const finalAt = Date.now()
          const rawConfidence = result.alternatives?.[0]?.confidence
          onTranscript(finalText, {
            streamId: thisStreamId,
            utteranceId: currentUtteranceId,
            source: 'GOOGLE_FINAL',
            interimCount,
            regressionCount,
            firstInterimAt,
            lastInterimAt,
            finalAt,
            firstInterimToFinalMs: firstInterimAt !== null ? finalAt - firstInterimAt : null,
            lastStability,
            maxStability,
            finalConfidence: (typeof rawConfidence === 'number' && rawConfidence > 0) ? rawConfidence : null,
            coldMutePackets,
            // STT-A2: final event ของ Google เอง ใช้ alternatives ของ final result นั้นตรงๆ ไม่ผ่าน acceptedAlternatives
            // (ไม่มี regression concept ฝั่ง final — result นี้คือ candidate ที่ Google ยืนยันแล้ว)
            alternatives: (typeof maxAlternatives === 'number' && maxAlternatives > 1) ? normalizeAlternatives(result.alternatives) : null,
          })
        } else {
          console.log('[STT] Final result but empty transcript')
        }
        rotateForNextUtterance() // utterance นี้จบแล้วจริงไม่ว่าจะมี text หรือไม่ — เปิดฟังต่อทันที ไม่รอ mark
      }
    })
    .on('end', () => {
      if (destroyed) return
      // A2.1 Shadow — a shadowed stream's 'end' previously fell through to the silent no-op case (it's
      // neither currentStream nor nextStream). Must settle here before either branch below.
      const endShadow = activeShadow
      if (endShadow && stream === endShadow.stream && !endShadow.settled) {
        settleShadow(endShadow, 'STREAM_END', {})
        return
      }
      if (stream === currentStream) {
        currentStream = null
        if (nextStream) {
          console.log('[STT] Switched to pre-warmed stream ✓')
          activatePrewarm()
        } else {
          console.log('[STT] No pre-warm ready — cold start fallback')
          resetUtteranceState()
          setTimeout(() => createStream(false), 50)
        }
      } else if (stream === nextStream) {
        nextStream = null
        if (!destroyed && currentStream) {
          console.log('[STT] Pre-warm ended early — recreating')
          setTimeout(() => { if (!nextStream && !destroyed && currentStream) createStream(true) }, 300)
        }
      }
    })

    if (isPrewarm) {
      nextStream = stream
    } else {
      currentStream = stream
      coldStreamMuteUntil = Date.now() + STREAM_MUTE_MS
    }
  }

  console.log(`[STT Config] interimFinalizeMs=${interimFinalizeMs}`) // ยืนยันได้จาก production log ว่าสายนี้เข้า branch ไหนจริง (legacy 900 / chunked experiment 600)
  createStream(false)

  return {
    write(mulawBuffer) {
      if (destroyed || !currentStream) {
        if (!destroyed) console.log('[STT] write skipped — no stream')
        return
      }
      if (Date.now() < coldStreamMuteUntil) { coldMutePackets++; return } // ทิ้งเสียงช่วงรอยต่อสั้นๆ กันคำหลอนจากสัญญาณตกค้าง — STT-A1: นับไว้เฉยๆ เพื่อ diagnostic
      try {
        const pcm = mulawBufferToPcm16(mulawBuffer)
        currentStream.write(pcm)
        if (++writeCount % 100 === 0) console.log(`[STT] Audio flowing: ${writeCount} packets sent`)
      } catch (e) {
        console.error('[STT] write error, recreating stream:', e.message)
        currentStream = null
        if (nextStream) {
          activatePrewarm()
          console.log('[STT] Write error recovery: switched to pre-warmed stream')
        } else {
          resetUtteranceState()
          setTimeout(() => createStream(false), 100)
        }
      }
    },
    end() {
      if (destroyed) return
      // A2.1 Shadow — settle as CALL_ENDED BEFORE destroyed=true / stream teardown (Design Review lock):
      // deterministic outcome, not a silent drop. Settling first (not after currentStream/nextStream.end())
      // also removes any race where a teardown-triggered stream event could steal the outcome as STREAM_END
      // instead of CALL_ENDED.
      if (activeShadow) settleShadow(activeShadow, 'CALL_ENDED', {})
      destroyed = true
      clearTimeout(interimTimer)
      interimTimer = null
      try { currentStream?.end() } catch (_) {}
      try { nextStream?.end() } catch (_) {}
      currentStream = null
      nextStream = null
    }
  }
}

module.exports = { transcribeStream }
