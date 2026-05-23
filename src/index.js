require('dotenv').config()
const fastify = require('fastify')({ logger: true })
const { registerWebSocket } = require('./websocket/audioStream')

fastify.register(require('@fastify/formbody'))
fastify.register(require('@fastify/websocket'))

fastify.register(require('./routes/webhook'))
fastify.register(require('./routes/campaign'))
fastify.register(require('./routes/dashboard'))

fastify.get('/health', async () => ({ status: 'ok', timestamp: new Date().toISOString() }))

registerWebSocket(fastify)

const start = async () => {
  try {
    await fastify.listen({ port: process.env.PORT || 3000, host: '0.0.0.0' })
    console.log(`Server running on port ${process.env.PORT || 3000}`)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
