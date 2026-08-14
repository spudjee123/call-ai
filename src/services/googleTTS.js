const textToSpeech = require('@google-cloud/text-to-speech')
const { getGoogleClientOptions } = require('../utils/googleCredentials')

const client = new textToSpeech.TextToSpeechClient(getGoogleClientOptions())

// Thai Neural2 voices — สูงสุดในตระกูล Google TTS
// th-TH-Neural2-C = หญิง, th-TH-Neural2-D = ชาย
const DEFAULT_VOICE = 'th-TH-Neural2-C'

async function synthesizeSpeechThai(text, voiceName) {
  const voice = voiceName || DEFAULT_VOICE

  console.log(`[GoogleTTS] Requesting voice=${voice} text="${text.substring(0, 60)}"`)

  const [response] = await client.synthesizeSpeech({
    input: { text },
    voice: {
      languageCode: 'th-TH',
      name: voice,
    },
    audioConfig: {
      audioEncoding: 'MULAW',
      sampleRateHertz: 8000,
      effectsProfileId: ['telephony-class-application'],
    },
  })

  const audioBuffer = Buffer.from(response.audioContent)
  console.log(`[GoogleTTS] Got ${audioBuffer.length} bytes`)

  const chunkSize = 160
  const chunks = []
  for (let i = 0; i < audioBuffer.length; i += chunkSize) {
    chunks.push(audioBuffer.slice(i, i + chunkSize))
  }

  console.log(`[GoogleTTS] ${chunks.length} chunks ready`)
  return chunks
}

// ตรวจสอบว่า voice_id เป็น Google TTS format ไหม (เช่น th-TH-Neural2-C)
function isGoogleVoice(voiceId) {
  return /^[a-z]{2}-[A-Z]{2}-(Neural2|Wavenet|Standard)-[A-Z]$/.test(voiceId || '')
}

// สร้างไฟล์ WAV เล่นได้จริงจาก URL (ใช้กับ TwiML <Play>) — ต่างจาก synthesizeSpeechThai ที่ทำ mulaw chunks สำหรับสตรีมสด
// Google TTS แนบ WAV header มาให้เองอยู่แล้วตอนขอ LINEAR16 (เอกสาร Google ยืนยันชัดเจน) — ไม่ต้องห่อ header ซ้ำเอง
// (เคยเขียน pcm16ToWav ห่อซ้ำไปรอบนึง ทำให้ได้ไฟล์เสีย เพราะข้างในกลายเป็น WAV header ซ้อน WAV header)
async function synthesizeSpeechWavThai(text, voiceName) {
  const voice = voiceName || DEFAULT_VOICE
  const [response] = await client.synthesizeSpeech({
    input: { text },
    voice: { languageCode: 'th-TH', name: voice },
    audioConfig: { audioEncoding: 'LINEAR16', sampleRateHertz: 8000 },
  })
  return Buffer.from(response.audioContent)
}

module.exports = { synthesizeSpeechThai, synthesizeSpeechWavThai, isGoogleVoice, DEFAULT_VOICE }
