const axios = require('axios')

const API_KEY = process.env.ELEVENLABS_API_KEY
const BASE_URL = 'https://api.elevenlabs.io/v1'

// ulaw 8kHz — ตรงกับ Twilio Media Streams โดยไม่ต้องแปลง
const OUTPUT_FORMAT = 'ulaw_8000'

async function synthesizeSpeech(text, voiceId) {
  voiceId = voiceId || process.env.ELEVENLABS_VOICE_ID

  const response = await axios.post(
    `${BASE_URL}/text-to-speech/${voiceId}/stream`,
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
      },
      responseType: 'arraybuffer',
    }
  )

  // แบ่ง audio เป็น chunks ขนาด 320 bytes (20ms @ 8kHz PCM16)
  const audioBuffer = Buffer.from(response.data)
  const chunkSize = 320
  const chunks = []

  for (let i = 0; i < audioBuffer.length; i += chunkSize) {
    chunks.push(audioBuffer.slice(i, i + chunkSize))
  }

  return chunks
}

module.exports = { synthesizeSpeech }
