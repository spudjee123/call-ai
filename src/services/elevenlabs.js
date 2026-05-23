const axios = require('axios')

const API_KEY = process.env.ELEVENLABS_API_KEY
const BASE_URL = 'https://api.elevenlabs.io/v1'

// ulaw 8kHz — ตรงกับ Twilio Media Streams โดยไม่ต้องแปลง
const OUTPUT_FORMAT = 'ulaw_8000'

async function synthesizeSpeech(text, voiceId) {
  voiceId = voiceId || process.env.ELEVENLABS_VOICE_ID
  console.log(`[ElevenLabs] Requesting voiceId=${voiceId} format=${OUTPUT_FORMAT} text="${text.substring(0, 60)}"`)

  const response = await axios.post(
    `${BASE_URL}/text-to-speech/${voiceId}`,
    {
      text,
      model_id: 'eleven_flash_v2_5',
      voice_settings: {
        stability: 0.5,
        similarity_boost: 0.8,
        style: 0.0,
        use_speaker_boost: true
      },
      output_format: OUTPUT_FORMAT,
    },
    {
      headers: {
        'xi-api-key': API_KEY,
        'Content-Type': 'application/json',
        'Accept': 'audio/basic',
      },
      responseType: 'arraybuffer',
    }
  )

  const contentType = response.headers['content-type'] || ''
  let audioBuffer = Buffer.from(response.data)
  const first4 = audioBuffer.slice(0, 4).toString('ascii')
  console.log(`[ElevenLabs] Got ${audioBuffer.length} bytes, contentType="${contentType}", first4="${first4.replace(/[^\x20-\x7E]/g, '?')}"`)

  // Strip WAV header if ElevenLabs returned a WAV container instead of raw μ-law
  if (first4 === 'RIFF') {
    let offset = 12
    while (offset < audioBuffer.length - 8) {
      const chunkId = audioBuffer.slice(offset, offset + 4).toString('ascii')
      const chunkSize = audioBuffer.readUInt32LE(offset + 4)
      if (chunkId === 'data') {
        audioBuffer = audioBuffer.slice(offset + 8, offset + 8 + chunkSize)
        console.log(`[ElevenLabs] Stripped WAV header, raw audio: ${audioBuffer.length} bytes`)
        break
      }
      offset += 8 + chunkSize
    }
  }

  // 160 bytes = 20ms @ 8kHz μ-law (1 byte/sample)
  const chunkSize = 160
  const chunks = []
  for (let i = 0; i < audioBuffer.length; i += chunkSize) {
    chunks.push(audioBuffer.slice(i, i + chunkSize))
  }

  console.log(`[ElevenLabs] ${chunks.length} chunks (${chunkSize}B each) ready`)
  return chunks
}

module.exports = { synthesizeSpeech }
