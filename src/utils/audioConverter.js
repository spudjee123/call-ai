// แปลง mulaw (Twilio) ↔ PCM 16-bit (Google STT / ElevenLabs)

const MULAW_BIAS = 0x84
const MULAW_CLIP = 32635

function mulawToLinear(mulawByte) {
  mulawByte = ~mulawByte
  const sign = mulawByte & 0x80
  const exponent = (mulawByte >> 4) & 0x07
  const mantissa = mulawByte & 0x0f
  let sample = ((mantissa << 3) + MULAW_BIAS) << exponent
  return sign ? MULAW_BIAS - sample : sample - MULAW_BIAS
}

function linearToMulaw(sample) {
  const sign = sample < 0 ? 0x80 : 0
  if (sign) sample = -sample
  if (sample > MULAW_CLIP) sample = MULAW_CLIP
  sample += MULAW_BIAS
  let exponent = 7
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent--, expMask >>= 1) {}
  const mantissa = (sample >> (exponent + 3)) & 0x0f
  return ~(sign | (exponent << 4) | mantissa) & 0xff
}

// Buffer mulaw → Buffer PCM16 LE
function mulawBufferToPcm16(mulawBuffer) {
  const pcm = Buffer.alloc(mulawBuffer.length * 2)
  for (let i = 0; i < mulawBuffer.length; i++) {
    const sample = mulawToLinear(mulawBuffer[i])
    pcm.writeInt16LE(sample, i * 2)
  }
  return pcm
}

// Buffer PCM16 LE → Buffer mulaw
function pcm16BufferToMulaw(pcmBuffer) {
  const mulaw = Buffer.alloc(pcmBuffer.length / 2)
  for (let i = 0; i < mulaw.length; i++) {
    const sample = pcmBuffer.readInt16LE(i * 2)
    mulaw[i] = linearToMulaw(sample)
  }
  return mulaw
}

module.exports = { mulawBufferToPcm16, pcm16BufferToMulaw }
