const fs = require('fs')
const path = require('path')

const adminHtml = fs.readFileSync(path.join(__dirname, '../public/admin.html'), 'utf8')

module.exports = async function adminPageRoutes(fastify) {
  fastify.get('/admin', async (req, reply) => {
    reply.type('text/html')
    return adminHtml
  })
}
