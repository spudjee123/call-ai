const axios = require('axios')
const { pcm16BufferToMulaw } = require('../utils/audioConverter')

const API_KEY = process.env.ELEVENLABS_API_KEY
const BASE_URL = 'https://api.elevenlabs.io/v1'
// Production incident (2026-08-25) — 'GolXPCpsnS5QBmdAYjj4' started returning ElevenLabs 404
// voice_not_found (voice no longer exists in the account) — every call whose campaign didn't set its own
// voice_id fell back to this and got no audio at all. Replaced with a verified-working voice id.
const DEFAULT_VOICE_ID = process.env.ELEVENLABS_VOICE_ID || 'kdmDKE6EkgrWrrykO9Qt'

// ขอ 16kHz PCM จาก ElevenLabs แทน ulaw_8000 โดยตรง
// เพราะ ElevenLabs model สร้างเสียงคุณภาพสูงกว่าที่ 16kHz
// แล้วเรา downsample 16k→8k เอง ได้คุณภาพดีกว่า
const OUTPUT_FORMAT = 'pcm_16000'

// L1c2a incident follow-up (production 2026-08-20) — err.message ของ axios ("Request failed with status code
// 400") ไม่บอกสาเหตุจริงเลย ทำให้ incident แรกวิเคราะห์ root cause ได้แค่จาก correlation ไม่มี response body จริง
// มายืนยัน — readErrorResponseBody() อ่าน err.response.data แบบ defensive เพราะ responseType:'stream' ทำให้
// data เป็น readable stream แม้ตอน error ก็ตาม (ไม่ใช่ parsed JSON/string อัตโนมัติเหมือน request ปกติ) bounded
// ไว้ที่ MAX_ERROR_BODY_BYTES กัน body ใหญ่ผิดปกติหลุดเข้า log เต็มก้อน
const MAX_ERROR_BODY_BYTES = 8192

// byte cap (break ที่ MAX_ERROR_BODY_BYTES) กัน "อ่านเยอะเกินไป" แต่ไม่ได้กัน "อ่านนานเกินไป" — ถ้า connection
// ค้างกลางทาง (ไม่มี data/error/end event เพิ่มมาเลย) for-await จะรอ next() เฉยๆ ไม่มีที่สิ้นสุด ซึ่งจะไปบล็อก
// throw err ตัวเดิมใน synthesizeSpeechStream ด้วย — ทำให้ observability (telemetry) กลายเป็น availability
// dependency ของ error path จริง จึงต้องมี wall-clock timeout คู่กับ byte cap เสมอ
const ERROR_BODY_READ_TIMEOUT_MS = 2000

async function readBoundedStream(stream) {
  let timeoutHandle
  const timeoutPromise = new Promise((resolve) => {
    timeoutHandle = setTimeout(() => {
      try { stream.destroy?.() } catch { /* best-effort cleanup เท่านั้น ไม่ให้ throw ทับ timeout result */ }
      resolve(null)
    }, ERROR_BODY_READ_TIMEOUT_MS)
    timeoutHandle.unref?.()
  })

  const readPromise = (async () => {
    try {
      const parts = []
      let total = 0
      for await (const chunk of stream) {
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
        parts.push(buf)
        total += buf.length
        if (total >= MAX_ERROR_BODY_BYTES) break
      }
      return Buffer.concat(parts).subarray(0, MAX_ERROR_BODY_BYTES).toString('utf8')
    } catch {
      return null
    }
  })()

  try {
    return await Promise.race([readPromise, timeoutPromise])
  } finally {
    clearTimeout(timeoutHandle)
  }
}

async function readErrorResponseBody(err) {
  const data = err.response?.data
  if (data == null) return null
  if (typeof data === 'string') return data.slice(0, MAX_ERROR_BODY_BYTES)
  if (Buffer.isBuffer(data)) return data.subarray(0, MAX_ERROR_BODY_BYTES).toString('utf8')
  if (typeof data[Symbol.asyncIterator] === 'function' || typeof data.on === 'function') {
    return readBoundedStream(data)
  }
  try { return JSON.stringify(data).slice(0, MAX_ERROR_BODY_BYTES) } catch { return null }
}

// TTS retry-once (Hardening Batch, 2026-08-30) — only covers the CONNECTION-ESTABLISHMENT phase (the
// initial axios.post() before any audio byte has streamed back), never a failure mid-stream after chunks
// have already been yielded — retrying there would mean re-synthesizing from the start while the caller
// may have already sent earlier chunks to Twilio, producing duplicated/garbled audio. This mirrors the
// same precommit/postcommit boundary already used throughout audioStream.js (totalSent === 0): a retry
// here only ever happens before the caller could have committed anything. MAX RETRY = 1 — a realtime call
// can't afford more; barge-in (ERR_CANCELED/CanceledError) is never retried, only genuinely transient
// provider/network failures are.
const TTS_RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504])
const TTS_RETRYABLE_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN'])

function isTransientTtsError(err) {
  if (err.code === 'ERR_CANCELED' || err.name === 'CanceledError') return false
  const status = err.response?.status
  if (status != null) return TTS_RETRYABLE_STATUS.has(status)
  return TTS_RETRYABLE_CODES.has(err.code)
}

// Greeting pregen retry (production incident 2026-09-03) — ElevenLabs 429 ยืนยันจาก production error body จริง
// ว่าเป็น concurrent-request limit ของ subscription (ปัจจุบันจำกัดที่ 5 requests พร้อมกัน) — สองสายที่เริ่มโทร
// ออกใกล้กันมาก (pregen ของทั้งคู่ยิงพร้อมกัน) ชนกันได้ง่ายๆ small delay+jitter ก่อน retry ครั้งเดียวช่วยให้พ้น
// burst สั้นๆ แบบนี้โดยไม่ทำให้ retry ของหลายสายชนกันซ้ำที่จังหวะเดิมเป๊ะๆ (ถ้า delay คงที่ไม่มี jitter) —
// เฉพาะ pregen เท่านั้น (ไม่แตะ synthesizeSpeechStream ที่ใช้ตอนคุยสดเลย เพื่อไม่เปลี่ยน timing ของ live TTS)
const PREGEN_RETRY_DELAY_BASE_MS = 200
const PREGEN_RETRY_DELAY_JITTER_MS = 150

// ไม่เคย throw จาก path นี้เอง (ห้าม logging ทำให้ error path ของ caller พังซ้ำ) และไม่เคยสร้าง error ใหม่ — แค่
// log แล้วให้ caller เป็นคน rethrow err ตัวเดิมเป๊ะ เพราะ upstream (chunkedTurn.js/audioStream.js) ใช้ err.source/
// error object เดิมในการจัดหมวด fallback อยู่แล้ว ห้าม log headers/api key หรือ axios config ทั้งก้อนเด็ดขาด
// requestType/attempt (production incident 2026-09-03, TTS 429 investigation) — เพิ่ม field เพื่อแยกได้ว่า
// error มาจาก path ไหน (pregen ก่อนสายต่อ vs stream ระหว่างคุยสด) และเป็น attempt ที่เท่าไหร่ (เดิม log เดียวกัน
// ไม่บอกเลยว่าเป็นครั้งแรกหรือหลัง retry แล้ว) errorType/errorCode/requestId ดึงจาก err.response.data.detail
// จริง — ยืนยันจาก production 429 log ตรงๆ ว่า ElevenLabs ส่ง {"detail":{"type":"rate_limit_error",
// "code":"concurrent_limit_exceeded","request_id":...}} มาเสมอตอน rate-limit (ข้อความเต็มยังบอกด้วยว่า
// concurrent limit ของ subscription ปัจจุบันคือ 5 requests พร้อมกัน) — parse แบบ defensive เพราะ error body
// ของสถานะอื่น (เช่น 5xx บางตัว) อาจไม่ใช่ JSON รูปแบบนี้ ไม่ throw ถ้า parse ไม่ได้ ปล่อย null แทน
async function logElevenLabsProviderError(err, { modelId, hasPreviousText, textLength, previousTextLength, attempt, requestType }) {
  let responseData = 'unavailable'
  try {
    const body = await readErrorResponseBody(err)
    if (body != null) responseData = body
  } catch {
    // เก็บ 'unavailable' ไว้ตามเดิม
  }
  let errorType = null, errorCode = null, requestId = null
  try {
    const parsed = JSON.parse(responseData)
    errorType = parsed?.detail?.type ?? null
    errorCode = parsed?.detail?.code ?? null
    requestId = parsed?.detail?.request_id ?? null
  } catch {
    // responseData ไม่ใช่ JSON ที่ parse ได้ (หรือยังเป็น 'unavailable') — ปล่อย null ทั้งหมด ไม่ throw
  }
  console.error('[TTSProviderError]', JSON.stringify({
    provider: 'elevenlabs',
    requestType: requestType ?? null,
    attempt: attempt ?? null,
    status: err.response?.status ?? null,
    statusText: err.response?.statusText ?? null,
    errorType,
    errorCode,
    requestId,
    modelId,
    hasPreviousText,
    textLength,
    previousTextLength,
    responseData,
  }))
}

// Downsample 16kHz PCM → 8kHz PCM โดย average ทุก 2 samples
// integer ratio 2:1 = clean, ไม่มี interpolation artifact
// การ average เป็น low-pass filter ตัด frequency > 4kHz (Nyquist สำหรับ 8kHz)
function downsample16to8(pcm16k) {
  const outSamples = Math.floor(pcm16k.length / 4)  // 2 bytes/sample, 2:1 ratio
  const out = Buffer.alloc(outSamples * 2)
  for (let i = 0; i < outSamples; i++) {
    const s1 = pcm16k.readInt16LE(i * 4)
    const s2 = pcm16k.readInt16LE(i * 4 + 2)
    out.writeInt16LE(Math.round((s1 + s2) / 2), i * 2)
  }
  return out
}

// Retry-once-on-transient-error (production incident 2026-09-03) — เดิมฟังก์ชันนี้ (ใช้เฉพาะตอน greeting
// pregen ใน twilio.js) ไม่มี retry เลยสักครั้ง ต่างจาก synthesizeSpeechStream() ด้านล่างที่มี retry ครั้งเดียว
// สำหรับ transient error อยู่แล้ว — ผลคือ 429 ชั่ววูบตอนสองสายเริ่มโทรพร้อมกัน (ดู PREGEN_RETRY_DELAY_* ด้านบน)
// ทำให้ pregen fail ทันทีไม่มีทางกู้คืน แล้วไปตกที่ audioStream.js's live-fallback generation แทน (สร้าง Opening
// ใหม่ + สังเคราะห์เสียงใหม่หลังลูกค้ารับสายไปแล้ว) เกิดเป็นช่วงหน่วง/สะดุดตอนต้นสายที่ได้ยินจริง — reuse
// isTransientTtsError() ตัวเดียวกับ streaming path ทุกประการ ไม่สร้าง classification ใหม่
async function synthesizeSpeech(text, voiceId) {
  voiceId = voiceId || DEFAULT_VOICE_ID
  console.log(`[ElevenLabs] Requesting voiceId=${voiceId} text="${text.substring(0, 60)}"`)

  const requestUrl = `${BASE_URL}/text-to-speech/${voiceId}?output_format=${OUTPUT_FORMAT}`
  const requestBody = {
    text,
    model_id: 'eleven_v3',
    voice_settings: {
      stability: 0.5,         // ลดจาก 0.85 — เดิมนิ่งเกินจนเสียงราบเรียบไม่มีอารมณ์
      similarity_boost: 0.90, // สูง = ใกล้เสียงต้นฉบับที่ clone มา
      style: 0.25,             // เพิ่มจาก 0 — ดึงอารมณ์/บุคลิกจากเสียงต้นฉบับออกมาให้ฟังเป็นธรรมชาติขึ้น
      use_speaker_boost: true
    },
  }
  const requestOptions = {
    headers: {
      'xi-api-key': API_KEY,
      'Content-Type': 'application/json',
    },
    responseType: 'arraybuffer',
  }

  let response
  let attempt = 0
  for (;;) {
    attempt++
    try {
      response = await axios.post(requestUrl, requestBody, requestOptions)
      break
    } catch (err) {
      const canRetry = attempt === 1 && isTransientTtsError(err)
      if (canRetry) {
        const delayMs = PREGEN_RETRY_DELAY_BASE_MS + Math.floor(Math.random() * PREGEN_RETRY_DELAY_JITTER_MS)
        console.warn(`[ElevenLabs] transient error (${err.response?.status || err.code}) ระหว่าง pregen — retry อีก 1 ครั้งหลัง ${delayMs}ms`)
        await new Promise(resolve => setTimeout(resolve, delayMs))
        continue
      }
      if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
        await logElevenLabsProviderError(err, {
          modelId: 'eleven_v3',
          hasPreviousText: false,
          textLength: text?.length ?? 0,
          previousTextLength: 0,
          attempt,
          requestType: 'pregen',
        })
      }
      throw err
    }
  }

  const pcm16k = Buffer.from(response.data)
  console.log(`[ElevenLabs] Got ${pcm16k.length} bytes PCM@16kHz`)

  // downsample 16kHz → 8kHz → encode μ-law
  const pcm8k = downsample16to8(pcm16k)
  const mulaw = pcm16BufferToMulaw(pcm8k)
  console.log(`[ElevenLabs] Converted: ${pcm16k.length}B@16k → ${pcm8k.length}B@8k → ${mulaw.length}B μ-law`)

  // 160 bytes = 20ms @ 8kHz μ-law
  const chunks = []
  for (let i = 0; i < mulaw.length; i += 160) {
    chunks.push(mulaw.slice(i, i + 160))
  }

  console.log(`[ElevenLabs] ${chunks.length} chunks ready`)
  return chunks
}

// Streaming version — yields 160-byte μ-law chunks as ElevenLabs generates them
// ลด latency: Twilio เล่นเสียงได้ทันทีโดยไม่ต้องรอ TTS เสร็จทั้งหมด
//
// L1c2a (production defect follow-up) — previousText (optional): ข้อความของ speech chunk ก่อนหน้าที่เพิ่งพูด
// จบไปจริง (ถ้ามี) ส่งเป็น previous_text ใน request body — ElevenLabs ใช้ช่วยรักษา prosody ต่อเนื่องเวลานำหลาย
// generation มาต่อกัน (แก้ "เสียงคนละโทน" ระหว่าง chunk ที่ chunker ต้องแยกจริงๆ เช่น ตัวเลข/หน่วยนับที่ยัง
// protect ไม่ทันในบางจังหวะ) — ใส่ key นี้เฉพาะเมื่อมีค่าจริง (falsy เช่น undefined/null/'' ไม่ใส่เลย ไม่ใช่ส่ง
// previous_text ว่างเปล่า) เพราะ chunk แรกของเทิร์นไม่มี predecessor จริง
async function* synthesizeSpeechStream(text, voiceId, signal, previousText) {
  voiceId = voiceId || DEFAULT_VOICE_ID
  console.log(`[ElevenLabs Stream] voiceId=${voiceId} text="${text.substring(0, 60)}"`)

  const body = {
    text,
    model_id: 'eleven_v3',
    voice_settings: {
      stability: 0.5,   // ลดจาก 0.85 — เดิมนิ่งเกินจนเสียงราบเรียบไม่มีอารมณ์
      similarity_boost: 0.90,
      style: 0.25,       // เพิ่มจาก 0 — ดึงอารมณ์/บุคลิกจากเสียงต้นฉบับออกมาให้ฟังเป็นธรรมชาติขึ้น
      use_speaker_boost: true
    },
  }
  if (previousText) body.previous_text = previousText

  let response
  let attempt = 0
  const requestUrl = `${BASE_URL}/text-to-speech/${voiceId}/stream?output_format=${OUTPUT_FORMAT}`
  const requestOptions = {
    headers: {
      'xi-api-key': API_KEY,
      'Content-Type': 'application/json',
    },
    responseType: 'stream',
    signal,  // AbortController signal สำหรับ barge-in
  }
  for (;;) {
    attempt++
    try {
      response = await axios.post(requestUrl, body, requestOptions)
      break
    } catch (err) {
      const canRetry = attempt === 1 && !signal?.aborted && isTransientTtsError(err)
      if (canRetry) {
        console.warn(`[ElevenLabs Stream] transient error (${err.response?.status || err.code}) ก่อนได้เสียงสักไบต์ — retry อีก 1 ครั้ง`)
        continue
      }
      // barge-in ที่ยกเลิก request กลางทางไม่ใช่ provider error จริง — ไม่ log เป็น [TTSProviderError] (จะกลาย
      // เป็น log noise มหาศาลเพราะ barge-in เกิดเป็นปกติทุกสาย) ใช้ convention เดียวกับที่อื่นในโค้ดฐาน (audioStream.js)
      // ที่กัน ERR_CANCELED/CanceledError ไว้แล้วก่อน log เป็น error จริง
      if (err.code !== 'ERR_CANCELED' && err.name !== 'CanceledError') {
        await logElevenLabsProviderError(err, {
          modelId: 'eleven_v3',
          hasPreviousText: Boolean(previousText),
          textLength: text?.length ?? 0,
          previousTextLength: previousText?.length ?? 0,
          attempt,
          requestType: 'stream',
        })
      }
      throw err
    }
  }

  let pcmBuffer = Buffer.alloc(0)   // incomplete 4-byte PCM frames
  let mulawBuffer = Buffer.alloc(0) // incomplete 160-byte μ-law chunks

  for await (const rawChunk of response.data) {
    if (signal?.aborted) return
    pcmBuffer = Buffer.concat([pcmBuffer, Buffer.from(rawChunk)])

    // ประมวลผลเฉพาะ complete 4-byte frames (2 samples × 2 bytes each)
    const frames = Math.floor(pcmBuffer.length / 4)
    if (frames === 0) continue

    const usable = frames * 4
    const pcm16k = pcmBuffer.slice(0, usable)
    pcmBuffer = pcmBuffer.slice(usable)

    const pcm8k = downsample16to8(pcm16k)
    const mulaw = pcm16BufferToMulaw(pcm8k)

    mulawBuffer = Buffer.concat([mulawBuffer, mulaw])

    // yield ทีละ 160 bytes (20ms) ให้ Twilio
    while (mulawBuffer.length >= 160) {
      yield mulawBuffer.slice(0, 160)
      mulawBuffer = mulawBuffer.slice(160)
    }
  }

  // flush ส่วนที่เหลือ (chunk สุดท้ายอาจสั้นกว่า 160 bytes)
  if (mulawBuffer.length > 0) {
    yield mulawBuffer
  }

  console.log(`[ElevenLabs Stream] Done`)
}

module.exports = { synthesizeSpeech, synthesizeSpeechStream }
