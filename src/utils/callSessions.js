// เก็บ session ของแต่ละสาย (in-memory)
const sessions = new Map()

module.exports = {
  get: (callSid) => sessions.get(callSid),
  set: (callSid, data) => sessions.set(callSid, data),
  delete: (callSid) => sessions.delete(callSid),
  has: (callSid) => sessions.has(callSid),
  entries: () => sessions.entries(),
  size: () => sessions.size
}
