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

// L1a (latency optimization, rollout-scoped STT endpoint experiment): interimFinalizeMs รับจากภายนอกได้แล้ว
// default 900ms เดิมทุกประการถ้า caller ไม่ส่งอะไรมา — googleSTT.js เป็น STT ตัวเดียวที่ legacy และ chunked
// path ใช้ร่วมกัน เปลี่ยนค่า default ตรงๆ จะกระทบ legacy production ทุกสายทันทีโดยไม่ผูกกับ chunked rollout เลย
// ต้องให้ audioStream.js เป็นคนตัดสินใจค่าต่อสาย (ตาม rollout ที่ freeze แล้วตอน 'start') แล้วส่งเข้ามาแทน
function transcribeStream(onTranscript, onInterim, { interimFinalizeMs = 900 } = {}) {
  let destroyed = false
  let currentStream = null
  let nextStream = null
  let errorRetryCount = 0

  let writeCount = 0
  let code11Count = 0

  let interimText = ''
  let interimTimer = null
  let utteranceClosed = false  // true after timer delivers transcript — blocks trailing interims and isFinal duplicate
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

    const stream = client.streamingRecognize({
      config: STT_CONFIG,
      interimResults: true,
      singleUtterance: true,
    })
    .on('error', (err) => {
      if (destroyed) return
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

        console.log(`[STT interim] "${text}"`)
        onInterim?.(text)

        if (isInterimRegression(interimText, text)) {
          console.log(`[STT] Interim regression ignored: "${interimText}" <- "${text}"`)
        } else {
          interimText = text
        }

        if (!nextStream && !destroyed) createStream(true)

        clearTimeout(interimTimer)
        interimTimer = setTimeout(() => {
          if (interimText && !destroyed) {
            console.log(`[STT] Interim→Final (${INTERIM_FINALIZE_MS}ms silence): "${interimText}"`)
            const deliveredText = interimText
            interimText = ''
            utteranceClosed = true
            onTranscript(deliveredText)
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
          onTranscript(finalText)
        } else {
          console.log('[STT] Final result but empty transcript')
        }
        rotateForNextUtterance() // utterance นี้จบแล้วจริงไม่ว่าจะมี text หรือไม่ — เปิดฟังต่อทันที ไม่รอ mark
      }
    })
    .on('end', () => {
      if (destroyed) return
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
      if (Date.now() < coldStreamMuteUntil) return // ทิ้งเสียงช่วงรอยต่อสั้นๆ กันคำหลอนจากสัญญาณตกค้าง
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
