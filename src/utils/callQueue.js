const twilioService = require('../services/twilio')
const callSessions = require('./callSessions')

const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT_CALLS) || 5

const queue = []
let running = 0

async function processQueue() {
  while (queue.length > 0 && running < MAX_CONCURRENT) {
    const job = queue.shift()
    running++
    runCall(job).finally(() => {
      running--
      processQueue()
    })
  }
}

async function runCall({ contact, campaign }) {
  try {
    await twilioService.makeOutboundCall(contact, campaign)
  } catch (err) {
    console.error(`Call failed for ${contact.phone}:`, err.message)
  }
}

const callQueue = {
  add(job) {
    queue.push(job)
    processQueue()
  },
  clear(campaignId) {
    const before = queue.length
    for (let i = queue.length - 1; i >= 0; i--) {
      if (!campaignId || queue[i].campaign.id === campaignId) {
        queue.splice(i, 1)
      }
    }
    console.log(`Cleared ${before - queue.length} jobs from queue`)
  },
  size() { return queue.length },
  runningCount() { return running }
}

module.exports = { callQueue }
