const crypto = require('crypto')

const TOKEN_TTL_MS = 7 * 24 * 60 * 60 * 1000 // 7 days
const LOGIN_WINDOW_MS = 15 * 60 * 1000
const LOGIN_MAX_ATTEMPTS = 10

const tokens = new Map() // token -> expiresAt
const loginAttempts = new Map() // ip -> { count, resetAt }

function generateToken() {
  const token = crypto.randomBytes(32).toString('hex')
  tokens.set(token, Date.now() + TOKEN_TTL_MS)
  return token
}

function validateToken(token) {
  if (!token) return false
  const expiresAt = tokens.get(token)
  if (!expiresAt) return false
  if (Date.now() > expiresAt) { tokens.delete(token); return false }
  return true
}

function revokeToken(token) {
  tokens.delete(token)
}

// กัน brute-force รหัสผ่านแบบง่ายๆ — จำกัดจำนวนครั้งต่อ IP ต่อช่วงเวลา
function isRateLimited(ip) {
  const entry = loginAttempts.get(ip)
  if (!entry || Date.now() > entry.resetAt) return false
  return entry.count >= LOGIN_MAX_ATTEMPTS
}

function recordLoginAttempt(ip) {
  const entry = loginAttempts.get(ip)
  if (!entry || Date.now() > entry.resetAt) {
    loginAttempts.set(ip, { count: 1, resetAt: Date.now() + LOGIN_WINDOW_MS })
  } else {
    entry.count++
  }
}

function requireAuth(req, reply, done) {
  const authHeader = req.headers.authorization || ''
  const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null
  if (!validateToken(token)) {
    reply.code(401).send({ error: 'Unauthorized' })
    return
  }
  done()
}

module.exports = { generateToken, validateToken, revokeToken, isRateLimited, recordLoginAttempt, requireAuth }
