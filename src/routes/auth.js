const crypto = require('crypto')
const { generateToken, revokeToken, isRateLimited, recordLoginAttempt } = require('../utils/auth')

// เทียบรหัสผ่านแบบ constant-time กัน timing attack เดารหัสผ่านทีละตัวอักษรจากเวลาตอบสนอง
function passwordMatches(input, expected) {
  const a = Buffer.from(String(input))
  const b = Buffer.from(String(expected))
  if (a.length !== b.length) {
    crypto.timingSafeEqual(a, a) // ทำงานเวลาคงที่แม้ความยาวไม่ตรง กันสังเกตความยาวรหัสผ่านจากเวลา
    return false
  }
  return crypto.timingSafeEqual(a, b)
}

module.exports = async function authRoutes(fastify) {

  fastify.post('/api/auth/login', async (req, reply) => {
    const ip = req.ip
    if (isRateLimited(ip)) {
      return reply.code(429).send({ error: 'Too many attempts, try again later' })
    }

    const { password } = req.body || {}
    if (!password || !passwordMatches(password, process.env.ADMIN_PASSWORD || '')) {
      recordLoginAttempt(ip)
      return reply.code(401).send({ error: 'Invalid password' })
    }

    const token = generateToken()
    return reply.send({ token })
  })

  fastify.post('/api/auth/logout', async (req, reply) => {
    const authHeader = req.headers.authorization || ''
    const token = authHeader.startsWith('Bearer ') ? authHeader.slice(7) : null
    if (token) revokeToken(token)
    return reply.send({ ok: true })
  })
}
