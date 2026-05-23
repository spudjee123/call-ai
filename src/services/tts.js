const { synthesizeSpeechThai, isGoogleVoice } = require('./googleTTS')
const { synthesizeSpeech: elevenLabsSynthesize } = require('./elevenlabs')

// Route TTS:
//   - voice_id เป็น Google format (th-TH-Neural2-C) → Google TTS
//   - ทุกกรณีอื่น (ElevenLabs hash หรือว่าง) → ElevenLabs
async function synthesizeSpeech(text, voiceId) {
  if (isGoogleVoice(voiceId)) {
    return synthesizeSpeechThai(text, voiceId)
  }
  // ElevenLabs — default, รองรับ cloned voice
  return elevenLabsSynthesize(text, voiceId)
}

module.exports = { synthesizeSpeech }
