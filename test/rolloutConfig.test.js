const { test } = require('node:test')
const assert = require('node:assert/strict')
const { createRolloutConfig, parseRolloutPercent } = require('../src/utils/rolloutConfig')

function delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)) }

// หมายเหตุเรื่อง timing: Windows default timer resolution อยู่ที่ประมาณ 15.6ms ทำให้ setTimeout ค่าน้อยๆ
// (ต่ำกว่า ~15ms) ไม่แม่นยำพอจะใช้เทส "ไม่ควรเกิดขึ้นก่อนเวลานี้" ได้เลย (เจอบั๊กจริงระหว่างพัฒนา: delay(5) สองครั้ง
// ติดกันใช้เวลาจริงพอที่ interval 15ms จะ fire ไปแล้วถึง 2 รอบ) ทุกเทสด้านล่างจึงตั้ง margin ห่างจาก threshold
// อย่างน้อย 4-10 เท่า ไม่ใช้ตัวเลขติดกันแบบ 5ms/15ms/20ms ที่ใกล้กันเกินไป

// ---------------------------------------------------------------------------
// parseRolloutPercent — pure function, ทดสอบ edge case ของ validation ตรงๆ
// ---------------------------------------------------------------------------

test('parseRolloutPercent: ยอมรับ integer 0-100 ที่ถูกต้อง', () => {
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '0' }]), 0)
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '5' }]), 5)
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '100' }]), 100)
})

test('parseRolloutPercent: ปฏิเสธค่านอกช่วง/ไม่ใช่ integer/ว่างเปล่า ทั้งหมด', () => {
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '-1' }]), null)
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '101' }]), null)
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '5.5' }]), null)
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: 'abc' }]), null)
  assert.equal(parseRolloutPercent([{ key: 'rollout_percent', value: '' }]), null)
})

test('parseRolloutPercent: ไม่มีแถว rollout_percent เลย → null', () => {
  assert.equal(parseRolloutPercent([]), null)
  assert.equal(parseRolloutPercent([{ key: 'other_key', value: '5' }]), null)
})

test('parseRolloutPercent: มีแถว rollout_percent ซ้ำกันมากกว่า 1 แถว → null ทั้งชุด ไม่เดา first/last', () => {
  assert.equal(parseRolloutPercent([
    { key: 'rollout_percent', value: '5' },
    { key: 'rollout_percent', value: '50' },
  ]), null)
})

// ---------------------------------------------------------------------------
// createRolloutConfig — cache/polling/last-known-good behavior
// ---------------------------------------------------------------------------

test('cold start (ยังไม่เคย fetch เลย, ยังไม่เรียก start()) → getCurrentRolloutPercent() คืน 0 เสมอ', () => {
  const rc = createRolloutConfig({ getRows: async () => [{ key: 'rollout_percent', value: '50' }] })
  assert.equal(rc.getCurrentRolloutPercent(), 0)
})

test('start() ยิง refresh ทันทีตอน bootstrap ไม่ต้องรอสายแรกเข้ามาก่อน (proactive ไม่ใช่ lazy-only)', async () => {
  let fetchCount = 0
  const rc = createRolloutConfig({
    getRows: async () => { fetchCount++; return [{ key: 'rollout_percent', value: '5' }] },
    refreshIntervalMs: 10000, // ใหญ่มาก กันไม่ให้ interval ตัวเองมา fire ปนในเทสนี้
  })
  rc.start()
  await delay(30) // รอ background refresh (fire-and-forget) settle
  assert.equal(fetchCount, 1, 'ต้องมีการ fetch เกิดขึ้นทันทีจาก start() เอง ไม่ใช่ต้องมีคนเรียก getCurrentRolloutPercent() ก่อน')
  assert.equal(rc.getCurrentRolloutPercent(), 5)
  rc.stop()
})

test('background polling refresh ต่อเนื่องเป็นระยะแม้ไม่มีใครเรียก getCurrentRolloutPercent() เลย', async () => {
  let value = 5
  let fetchCount = 0
  const rc = createRolloutConfig({
    getRows: async () => { fetchCount++; return [{ key: 'rollout_percent', value: String(value) }] },
    refreshIntervalMs: 50,
  })
  rc.start()
  await delay(20)
  assert.equal(fetchCount, 1) // จาก start() เอง — 20ms < 50ms interval ไม่ควรมี tick เพิ่ม
  value = 20
  await delay(100) // นานกว่า 2 รอบ poll (50ms) แน่นอน
  assert.ok(fetchCount >= 2, `คาดว่า fetch อย่างน้อย 2 ครั้งจาก background polling ได้จริง ${fetchCount}`)
  assert.equal(rc.getCurrentRolloutPercent(), 20)
  rc.stop()
})

test('getCurrentRolloutPercent() เป็น synchronous เสมอ อ่านจาก memory ไม่รอ Sheets I/O', async () => {
  let resolveFetch
  const rc = createRolloutConfig({
    getRows: () => new Promise(resolve => { resolveFetch = resolve }), // ค้างไม่ resolve จนกว่าจะเรียกเอง
    refreshIntervalMs: 10000,
  })
  rc.start()
  const before = Date.now()
  const value = rc.getCurrentRolloutPercent() // ต้อง return ทันที แม้ fetch ที่ start() ยิงไปยังค้างอยู่
  const elapsed = Date.now() - before
  assert.ok(elapsed < 20, `getCurrentRolloutPercent() ต้อง return เกือบทันที ไม่รอ fetch ที่ค้างอยู่ ใช้เวลาไป ${elapsed}ms`)
  assert.equal(value, 0) // cold start เพราะ fetch ที่ค้างอยู่ยังไม่เคย resolve สำเร็จ
  resolveFetch([{ key: 'rollout_percent', value: '10' }])
  rc.stop()
})

test('concurrent refresh dedupe: หลาย getCurrentRolloutPercent() เรียกพร้อมกันตอน stale ไม่ยิง fetch ซ้ำ', async () => {
  let fetchCount = 0
  const rc = createRolloutConfig({
    getRows: async () => { fetchCount++; await delay(30); return [{ key: 'rollout_percent', value: '5' }] },
    refreshIntervalMs: 10000, // ไม่เกี่ยวกับเทสนี้ — ตั้งใหญ่ไว้กันปน
  })
  // ไม่เรียก start() — ทดสอบ dedupe ผ่าน refreshIfStale (backup path) แทน โดยเรียก getCurrentRolloutPercent() รัวๆ
  rc.getCurrentRolloutPercent()
  rc.getCurrentRolloutPercent()
  rc.getCurrentRolloutPercent()
  await delay(60) // นานกว่า getRows() ที่ค้างอยู่ (30ms) แน่นอน
  assert.equal(fetchCount, 1, 'เรียกพร้อมกันหลายครั้งตอนยัง fetch เดิมค้างอยู่ ต้อง dedupe เหลือ fetch เดียว')
})

test('Sheets throw error ระหว่าง refresh → ไม่แตะ cachedPercent เดิม (last-known-good คงอยู่)', async () => {
  let shouldFail = false
  const rc = createRolloutConfig({
    getRows: async () => {
      if (shouldFail) throw new Error('Sheets down')
      return [{ key: 'rollout_percent', value: '20' }]
    },
    refreshIntervalMs: 50,
  })
  rc.start()
  await delay(20)
  assert.equal(rc.getCurrentRolloutPercent(), 20) // fetch แรกสำเร็จ
  shouldFail = true
  await delay(100) // รอบ poll ถัดไป (50ms) เกิดแน่นอน แล้ว fail
  assert.equal(rc.getCurrentRolloutPercent(), 20, 'error ชั่วคราวไม่ควรทำให้ค่าเดิมหายไป ต้องยังเป็น last-known-good')
  rc.stop()
})

test('แถว config มี format ผิด (เช่น ซ้ำ key) ระหว่าง refresh → ไม่แตะ cachedPercent เดิมเช่นกัน', async () => {
  let rows = [{ key: 'rollout_percent', value: '15' }]
  const rc = createRolloutConfig({ getRows: async () => rows, refreshIntervalMs: 50 })
  rc.start()
  await delay(20)
  assert.equal(rc.getCurrentRolloutPercent(), 15)
  rows = [{ key: 'rollout_percent', value: '15' }, { key: 'rollout_percent', value: '99' }] // ซ้ำ key ผิดพลาดจากการแก้ sheet
  await delay(100)
  assert.equal(rc.getCurrentRolloutPercent(), 15, 'config รูปแบบผิด (ซ้ำ key) ต้องไม่เปลี่ยนค่าเดิมที่ใช้อยู่')
  rc.stop()
})

test('lastAttemptAt แยกจาก lastSuccessAt: error ต่อเนื่องไม่ทำให้ยิง Sheets ถี่กว่ารอบ poll ปกติ', async () => {
  let fetchCount = 0
  const rc = createRolloutConfig({
    getRows: async () => { fetchCount++; throw new Error('down') },
    refreshIntervalMs: 300,
  })
  rc.start()
  await delay(30)
  assert.equal(fetchCount, 1)
  // เรียก getCurrentRolloutPercent() รัวๆ ระหว่างที่ยัง error ต่อเนื่อง ต้องไม่ยิงซ้ำจนกว่าจะครบ refreshIntervalMs
  rc.getCurrentRolloutPercent()
  rc.getCurrentRolloutPercent()
  await delay(30)
  assert.equal(fetchCount, 1, 'lastAttemptAt ต้องคุม backoff แม้ error ต่อเนื่อง ไม่ hammer Sheets ซ้ำก่อนถึงรอบถัดไป (รวมเวลาผ่านไป ~60ms << 300ms interval)')
  rc.stop()
})

test('rollout 0 ใน Sheets → หลัง refresh สายใหม่ทั้งหมดได้ 0% (คืนสู่ legacy)', async () => {
  let value = 20
  const rc = createRolloutConfig({ getRows: async () => [{ key: 'rollout_percent', value: String(value) }], refreshIntervalMs: 50 })
  rc.start()
  await delay(20)
  assert.equal(rc.getCurrentRolloutPercent(), 20)
  value = 0
  await delay(100)
  assert.equal(rc.getCurrentRolloutPercent(), 0)
  rc.stop()
})

test('stop() หยุด timer ได้จริง (ไม่มี fetch เพิ่มหลังจากนั้น) — สำหรับ test isolation/graceful shutdown', async () => {
  let fetchCount = 0
  const rc = createRolloutConfig({ getRows: async () => { fetchCount++; return [{ key: 'rollout_percent', value: '5' }] }, refreshIntervalMs: 30 })
  rc.start()
  await delay(20)
  const countAtStop = fetchCount
  rc.stop()
  await delay(150) // นานกว่าหลายรอบ poll ถ้า timer ยังทำงานอยู่
  assert.equal(fetchCount, countAtStop, 'หลัง stop() ต้องไม่มี fetch เพิ่มขึ้นอีกเลย')
})

test('start() เรียกซ้ำหลายครั้งไม่สร้าง timer ซ้อนกัน (ไม่ fetch ถี่กว่าที่ควร)', async () => {
  let fetchCount = 0
  const rc = createRolloutConfig({ getRows: async () => { fetchCount++; return [{ key: 'rollout_percent', value: '5' }] }, refreshIntervalMs: 10000 })
  rc.start()
  rc.start()
  rc.start()
  await delay(30)
  assert.equal(fetchCount, 1, 'start() ซ้ำต้องไม่สร้าง timer ซ้อนหลายตัว ไม่งั้นจะ fetch ถี่กว่ารอบ poll ที่ตั้งไว้')
  rc.stop()
})

// ---------------------------------------------------------------------------
// C6b — state-change logging: log เฉพาะตอนผลลัพธ์เปลี่ยน ไม่ log ซ้ำทุกรอบ poll
// ---------------------------------------------------------------------------

function captureLogs() {
  const logs = { log: [], warn: [], error: [] }
  const original = { log: console.log, warn: console.warn, error: console.error }
  console.log = (...args) => logs.log.push(args.join(' '))
  console.warn = (...args) => logs.warn.push(args.join(' '))
  console.error = (...args) => logs.error.push(args.join(' '))
  return { logs, restore: () => { console.log = original.log; console.warn = original.warn; console.error = original.error } }
}

test('logging: refresh สำเร็จครั้งแรก → log ทันที', async () => {
  const { logs, restore } = captureLogs()
  try {
    const rc = createRolloutConfig({ getRows: async () => [{ key: 'rollout_percent', value: '0' }], refreshIntervalMs: 10000 })
    rc.start()
    await delay(20)
    assert.ok(logs.log.some(l => l.includes('refresh success percent=0')))
    rc.stop()
  } finally { restore() }
})

test('logging: refresh สำเร็จซ้ำด้วยค่าเดิม → ไม่ log ซ้ำ', async () => {
  const { logs, restore } = captureLogs()
  try {
    const rc = createRolloutConfig({ getRows: async () => [{ key: 'rollout_percent', value: '5' }], refreshIntervalMs: 30 })
    rc.start()
    await delay(20)
    const countAfterFirst = logs.log.length
    await delay(120) // ผ่านไปหลายรอบ poll ค่าเดิมตลอด
    assert.equal(logs.log.length, countAfterFirst, 'ค่าเดิมซ้ำๆ ไม่ควร log เพิ่มเลย')
    rc.stop()
  } finally { restore() }
})

test('logging: ค่าเปลี่ยน (0 → 5) → log บรรทัดใหม่', async () => {
  const { logs, restore } = captureLogs()
  try {
    let value = 0
    const rc = createRolloutConfig({ getRows: async () => [{ key: 'rollout_percent', value: String(value) }], refreshIntervalMs: 30 })
    rc.start()
    await delay(20)
    assert.ok(logs.log.some(l => l.includes('percent=0')))
    value = 5
    await delay(60)
    assert.ok(logs.log.some(l => l.includes('percent=5')), 'ค่าที่เปลี่ยนไปต้อง log ให้เห็นทันที ไม่ถูก dedupe ทิ้ง')
    rc.stop()
  } finally { restore() }
})

test('logging: refresh ล้มเหลวครั้งแรก → log ด้วย console.error', async () => {
  const { logs, restore } = captureLogs()
  try {
    const rc = createRolloutConfig({ getRows: async () => { throw new Error('Sheets down') }, refreshIntervalMs: 10000 })
    rc.start()
    await delay(20)
    assert.ok(logs.error.some(l => l.includes('refresh failed') && l.includes('Sheets down')))
    rc.stop()
  } finally { restore() }
})

test('logging: error เดิมซ้ำๆ ต่อเนื่อง → ไม่ log ซ้ำ', async () => {
  const { logs, restore } = captureLogs()
  try {
    const rc = createRolloutConfig({ getRows: async () => { throw new Error('Sheets down') }, refreshIntervalMs: 30 })
    rc.start()
    await delay(20)
    const countAfterFirst = logs.error.length
    await delay(120)
    assert.equal(logs.error.length, countAfterFirst, 'error ชนิดเดิมซ้ำๆ ไม่ควร log เพิ่มเลย')
    rc.stop()
  } finally { restore() }
})

test('logging: fail แล้วฟื้นตัว (recovery) → log success อีกครั้งตอนกลับมาใช้ได้', async () => {
  const { logs, restore } = captureLogs()
  try {
    let shouldFail = true
    const rc = createRolloutConfig({
      getRows: async () => { if (shouldFail) throw new Error('Sheets down'); return [{ key: 'rollout_percent', value: '10' }] },
      refreshIntervalMs: 30,
    })
    rc.start()
    await delay(20)
    assert.ok(logs.error.some(l => l.includes('refresh failed')))
    shouldFail = false
    await delay(60)
    assert.ok(logs.log.some(l => l.includes('refresh success percent=10')), 'ฟื้นตัวจาก error ต้อง log success ใหม่ทันที ไม่ถูก dedupe ทิ้งเพราะเคย log อย่างอื่นมาก่อน')
    rc.stop()
  } finally { restore() }
})

test('logging: config ผิดรูปแบบ (ซ้ำ key) → log ด้วย console.warn และ cachedPercent ไม่เปลี่ยน (LKG)', async () => {
  const { logs, restore } = captureLogs()
  try {
    const rc = createRolloutConfig({
      getRows: async () => [{ key: 'rollout_percent', value: '5' }, { key: 'rollout_percent', value: '50' }],
      refreshIntervalMs: 10000,
    })
    rc.start()
    await delay(20)
    assert.ok(logs.warn.some(l => l.includes('refresh invalid') && l.includes('duplicate_rollout_percent')))
    assert.equal(rc.getCurrentRolloutPercent(), 0, 'ไม่เคย fetch สำเร็จเลยตั้งแต่ต้น (cold start) → ยังเป็น 0 ตามปกติ ไม่ใช่ 5 หรือ 50')
    rc.stop()
  } finally { restore() }
})
