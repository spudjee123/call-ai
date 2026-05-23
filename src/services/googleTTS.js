const textToSpeech = require('@google-cloud/text-to-speech')

const clientOptions = {}
if (process.env.GOOGLE_CREDENTIALS_JSON) {
  clientOptions.credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON)
}
const client = new textToSpeech.TextToSpeechClient(clientOptions)

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

module.exports = { synthesizeSpeechThai, isGoogleVoice, DEFAULT_VOICE }
