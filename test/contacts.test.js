// Contacts routes — route-level tests (fastify.inject(), same pattern as test/campaign.test.js). Focused on
// the new bulk-delete/bulk-restore endpoints (Design Freeze "Contacts Bulk Soft-Delete + Row-Safe Undo",
// 2026-09-02): input validation, correct forwarding to sheetsService, and that a service-layer throw
// propagates as a real HTTP error (never a false-success 200) — the exact class of bug this whole track
// exists to close on the client side too.
const { test, beforeEach } = require('node:test')
const assert = require('node:assert/strict')
const Fastify = require('fastify')

const state = { bulkDeleteCalls: [], bulkRestoreCalls: [], bulkDeleteResult: null, bulkRestoreResult: null, bulkDeleteThrows: null, bulkRestoreThrows: null }

const googleSheetsPath = require.resolve('../src/services/googleSheets')
require.cache[googleSheetsPath] = {
  id: googleSheetsPath, filename: googleSheetsPath, loaded: true,
  exports: {
    sheetsService: {
      bulkDeleteContacts: async (phones) => {
        state.bulkDeleteCalls.push(phones)
        if (state.bulkDeleteThrows) throw state.bulkDeleteThrows
        return state.bulkDeleteResult
      },
      bulkRestoreContacts: async (undo) => {
        state.bulkRestoreCalls.push(undo)
        if (state.bulkRestoreThrows) throw state.bulkRestoreThrows
        return state.bulkRestoreResult
      },
    },
  },
}

// contacts.js require ตัวนี้ด้วยตอนโหลดโมดูล (ใช้ใน /api/contacts/call) — stub กันผลข้างเคียงแม้ test นี้จะไม่ยิง route นั้น
const callQueuePath = require.resolve('../src/utils/callQueue')
require.cache[callQueuePath] = {
  id: callQueuePath, filename: callQueuePath, loaded: true,
  exports: { callQueue: { add: () => {}, clear: () => {}, clearByNumber: () => 0, size: () => 0, runningCount: () => 0, statusByNumber: () => ({}) } },
}

const contactsRoutes = require('../src/routes/contacts')

async function buildApp() {
  const fastify = Fastify()
  fastify.register(contactsRoutes)
  await fastify.ready()
  return fastify
}

beforeEach(() => {
  state.bulkDeleteCalls = []
  state.bulkRestoreCalls = []
  state.bulkDeleteResult = { requestedPhones: 1, deletedPhones: 1, deletedRows: 1, alreadyDeletedRows: 0, notFoundPhones: [], undo: [] }
  state.bulkRestoreResult = { restoredRows: 1, conflicts: [] }
  state.bulkDeleteThrows = null
  state.bulkRestoreThrows = null
})

test('POST /api/contacts/bulk-delete: phones[] ว่างเปล่า/ไม่ส่งมา → 400 ไม่เรียก service เลย', async () => {
  const app = await buildApp()
  const res1 = await app.inject({ method: 'POST', url: '/api/contacts/bulk-delete', payload: {} })
  const res2 = await app.inject({ method: 'POST', url: '/api/contacts/bulk-delete', payload: { phones: [] } })
  assert.equal(res1.statusCode, 400)
  assert.equal(res2.statusCode, 400)
  assert.equal(state.bulkDeleteCalls.length, 0)
})

test('POST /api/contacts/bulk-delete: normalize + dedupe phones ก่อนส่งให้ service', async () => {
  const app = await buildApp()
  const res = await app.inject({
    method: 'POST', url: '/api/contacts/bulk-delete',
    payload: { phones: ['0812345678', '0812345678', '+66812345678', ''] },
  })
  assert.equal(res.statusCode, 200)
  assert.equal(state.bulkDeleteCalls.length, 1)
  // normalizePhone ทำให้ 0812345678 กับ +66812345678 กลายเป็นค่าเดียวกัน + ตัวว่างถูกกรองทิ้ง — ควรเหลือ 1 รายการ
  assert.equal(state.bulkDeleteCalls[0].length, 1, 'ต้อง normalize รูปแบบเบอร์แล้ว dedupe ก่อนส่งเข้า service')
})

test('POST /api/contacts/bulk-delete: forward ผลจริงจาก service กลับไปทั้งหมด (ไม่ตัดทอน/แปลงข้อมูล)', async () => {
  state.bulkDeleteResult = { requestedPhones: 5, deletedPhones: 3, deletedRows: 4, alreadyDeletedRows: 1, notFoundPhones: ['0899999999', '0888888888'], undo: [{ rowNumber: 2, phone: '0812345678', previousStatus: 'pending' }] }
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/contacts/bulk-delete', payload: { phones: ['0812345678'] } })
  assert.equal(res.statusCode, 200)
  assert.deepEqual(res.json(), state.bulkDeleteResult)
})

test('POST /api/contacts/bulk-delete: service throw (เช่น Sheets API ล้มเหลว) → HTTP error จริง ไม่ใช่ 200 หลอกว่าสำเร็จ', async () => {
  state.bulkDeleteThrows = new Error('Sheets API unavailable')
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/contacts/bulk-delete', payload: { phones: ['0812345678'] } })
  assert.notEqual(res.statusCode, 200, 'ต้องไม่ใช่ 200 — client ฝั่ง admin.html เช็ค res.ok เพื่อไม่ประกาศว่าลบสำเร็จตอนที่ยังไม่สำเร็จ')
  assert.ok(res.statusCode >= 500)
})

test('POST /api/contacts/bulk-restore: undo[] ว่างเปล่า/ไม่ส่งมา → 400 ไม่เรียก service เลย', async () => {
  const app = await buildApp()
  const res1 = await app.inject({ method: 'POST', url: '/api/contacts/bulk-restore', payload: {} })
  const res2 = await app.inject({ method: 'POST', url: '/api/contacts/bulk-restore', payload: { undo: [] } })
  assert.equal(res1.statusCode, 400)
  assert.equal(res2.statusCode, 400)
  assert.equal(state.bulkRestoreCalls.length, 0)
})

test('POST /api/contacts/bulk-restore: กรอง entry ที่รูปร่างผิด (rowNumber ไม่ใช่ integer / phone ไม่ใช่ string) ทิ้งก่อนส่งเข้า service', async () => {
  const app = await buildApp()
  const res = await app.inject({
    method: 'POST', url: '/api/contacts/bulk-restore',
    payload: { undo: [
      { rowNumber: 2, phone: '0812345678', previousStatus: 'pending' }, // valid
      { rowNumber: 'x', phone: '0899999999', previousStatus: 'pending' }, // rowNumber ผิด type
      { phone: '0888888888', previousStatus: 'pending' }, // ไม่มี rowNumber เลย
      null, // entry เพี้ยนไปเลย
    ] },
  })
  assert.equal(res.statusCode, 200)
  assert.equal(state.bulkRestoreCalls[0].length, 1, 'ต้องเหลือแค่ entry ที่รูปร่างถูกต้อง 1 รายการ')
})

test('POST /api/contacts/bulk-restore: service throw → HTTP error จริง', async () => {
  state.bulkRestoreThrows = new Error('Sheets API unavailable')
  const app = await buildApp()
  const res = await app.inject({ method: 'POST', url: '/api/contacts/bulk-restore', payload: { undo: [{ rowNumber: 2, phone: '0812345678', previousStatus: 'pending' }] } })
  assert.ok(res.statusCode >= 500)
})
