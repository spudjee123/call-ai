const axios = require('axios')
const { pcm16BufferToMulaw } = require('../utils/audioConverter')

const API_KEY = process.env.ELEVENLABS_API_KEY
const BASE_URL = 'https://api.elevenlabs.io/v1'

// ขอ 16kHz PCM จาก ElevenLabs แทน ulaw_8000 โดยตรง
// เพราะ ElevenLabs model สร้างเสียงคุณภาพสูงกว่าที่ 16kHz
// แล้วเรา downsample 16k→8k เอง ได้คุณภาพดีกว่า
const OUTPUT_FORMAT = 'pcm_16000'

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

async function synthesizeSpeech(text, voiceId) {
  voiceId = voiceId || process.env.ELEVENLABS_VOICE_ID || 'GolXPCpsnS5QBmdAYjj4'
  console.log(`[ElevenLabs] Requesting voiceId=${voiceId} text="${text.substring(0, 60)}"`)

  const response = await axios.post(
    `${BASE_URL}/text-to-speech/${voiceId}?output_format=${OUTPUT_FORMAT}`,
    {
      text,
      model_id: 'eleven_v3',
      voice_settings: {
        stability: 0.85,        // สูง = เสียงสม่ำเสมอ ไม่สั่น เหมาะกับ cloned voice
        similarity_boost: 0.90, // สูง = ใกล้เสียงต้นฉบับที่ clone มา
        style: 0.0,
        use_speaker_boost: true
      },
    },
    {
      headers: {
        'xi-api-key': API_KEY,
        'Content-Type': 'application/json',
      },
      responseType: 'arraybuffer',
    }
  )

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
async function* synthesizeSpeechStream(text, voiceId, signal) {
  voiceId = voiceId || process.env.ELEVENLABS_VOICE_ID || 'GolXPCpsnS5QBmdAYjj4'
  console.log(`[ElevenLabs Stream] voiceId=${voiceId} text="${text.substring(0, 60)}"`)

  const response = await axios.post(
    `${BASE_URL}/text-to-speech/${voiceId}/stream?output_format=${OUTPUT_FORMAT}`,
    {
      text,
      model_id: 'eleven_v3',
      voice_settings: {
        stability: 0.85,
        similarity_boost: 0.90,
        style: 0.0,
        use_speaker_boost: true
      },
    },
    {
      headers: {
        'xi-api-key': API_KEY,
        'Content-Type': 'application/json',
      },
      responseType: 'stream',
      signal,  // AbortController signal สำหรับ barge-in
    }
  )

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
