const twilio = require('twilio')

// Twilio Webhook Signature Verification (Hardening Batch, 2026-08-30) — audit finding: no route ever
// checked X-Twilio-Signature, so any POST to /webhook/* that guessed a valid CallSid could inject fake
// call events. Defaults to LOG_ONLY, not ENFORCE: validateRequest() needs the EXACT public URL Twilio
// used to sign the request, reconstructed here from BASE_URL + req.url — a wrong BASE_URL, a proxy that
// rewrites the path, or an unexpected query string would make every real Twilio callback fail validation
// too. ENFORCE must only be switched on after LOG_ONLY has been observed clean against real production
// traffic on both services (see Hardening Batch rollout plan) — this file only provides the mechanism.
function getMode() {
  const mode = (process.env.WEBHOOK_SIGNATURE_MODE || 'LOG_ONLY').toUpperCase()
  return mode === 'ENFORCE' ? 'ENFORCE' : 'LOG_ONLY'
}

function buildRequestUrl(req) {
  const base = (process.env.BASE_URL || '').replace(/\/$/, '')
  return `${base}${req.url}`
}

function isValidTwilioSignature(req) {
  const authToken = process.env.TWILIO_AUTH_TOKEN
  const signature = req.headers['x-twilio-signature']
  if (!authToken || !signature) return false
  return twilio.validateRequest(authToken, signature, buildRequestUrl(req), req.body || {})
}

// Fastify preHandler, same (req, reply, done) callback shape as requireAuth (src/utils/auth.js) for
// consistency with the rest of this codebase's route guards.
function twilioSignatureGuard(req, reply, done) {
  const mode = getMode()
  if (!isValidTwilioSignature(req)) {
    if (mode === 'ENFORCE') {
      console.warn(`[Webhook Security] REJECTED invalid X-Twilio-Signature — path=${req.url}`)
      reply.code(403).send({ error: 'invalid signature' })
      return
    }
    console.warn(`[Webhook Security] invalid X-Twilio-Signature (LOG_ONLY, not blocking) — path=${req.url}`)
  }
  done()
}

module.exports = { twilioSignatureGuard, isValidTwilioSignature, getMode, buildRequestUrl }
