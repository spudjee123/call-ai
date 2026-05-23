require('dotenv').config()
const fastify = require('fastify')({ logger: true })
const { registerWebSocket } = require('./websocket/audioStream')

fastify.register(require('@fastify/formbody'))

fastify.get('/health', async () => ({ status: 'ok', timestamp: new Date().toISOString() }))

fastify.register(require('./routes/webhook'))
fastify.register(require('./routes/campaign'))
fastify.register(require('./routes/dashboard'))

// WebSocket plugin + route must be registered together so the plugin
// is loaded before the route handler is declared
fastify.register(async function wsPlugin(f) {
  await f.register(require('@fastify/websocket'))
  registerWebSocket(f)
})

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
