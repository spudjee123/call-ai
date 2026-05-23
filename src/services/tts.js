const { synthesizeSpeechThai, isGoogleVoice } = require('./googleTTS')
const { synthesizeSpeech: elevenLabsSynthesize, synthesizeSpeechStream: elevenLabsStream } = require('./elevenlabs')

async function synthesizeSpeech(text, voiceId) {
  if (isGoogleVoice(voiceId)) {
    return synthesizeSpeechThai(text, voiceId)
  }
  return elevenLabsSynthesize(text, voiceId)
}

// Streaming version — async generator yielding 160-byte μ-law chunks
// Google TTS ไม่รองรับ streaming → fall back to batch แล้ว yield ทีละ chunk
async function* synthesizeSpeechStream(text, voiceId) {
  if (isGoogleVoice(voiceId)) {
    const chunks = await synthesizeSpeechThai(text, voiceId)
    for (const chunk of chunks) yield chunk
  } else {
    yield* elevenLabsStream(text, voiceId)
  }
}

module.exports = { synthesizeSpeech, synthesizeSpeechStream }
