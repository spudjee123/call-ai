const { synthesizeSpeechThai, isGoogleVoice, DEFAULT_VOICE } = require('./googleTTS')
const { synthesizeSpeech: elevenLabsSynthesize } = require('./elevenlabs')

// Route TTS ไปยัง provider ที่เหมาะสม:
//   - voice_id เป็น Google format (th-TH-Neural2-C) → Google TTS (Thai คมชัดกว่า)
//   - voice_id เป็น ElevenLabs hash → ElevenLabs
//   - ไม่มี voice_id → Google TTS ด้วย default Thai voice
async function synthesizeSpeech(text, voiceId) {
  if (!voiceId || isGoogleVoice(voiceId)) {
    // Google TTS — native Thai Neural2, MULAW 8kHz ตรง
    return synthesizeSpeechThai(text, voiceId || DEFAULT_VOICE)
  }
  // ElevenLabs — custom voice ที่ user เลือก
  return elevenLabsSynthesize(text, voiceId)
}

module.exports = { synthesizeSpeech }
