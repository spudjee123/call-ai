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
//
// L1c2a: previousText (optional) — thread ผ่านไปยัง ElevenLabs เท่านั้น (Google TTS batch API ไม่มี concept นี้)
async function* synthesizeSpeechStream(text, voiceId, signal, previousText) {
  if (isGoogleVoice(voiceId)) {
    const chunks = await synthesizeSpeechThai(text, voiceId)
    for (const chunk of chunks) {
      if (signal?.aborted) return
      yield chunk
    }
  } else {
    yield* elevenLabsStream(text, voiceId, signal, previousText)
  }
}

module.exports = { synthesizeSpeech, synthesizeSpeechStream }
