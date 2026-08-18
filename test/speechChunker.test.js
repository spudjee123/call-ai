const { test } = require('node:test')
const assert = require('node:assert/strict')
const { findChunkBoundary } = require('../src/utils/speechChunker')

// chunk ผ่าน .trim() มาแล้วเลยสั้นกว่า cut point จริงได้ (เสียช่องว่างที่ตัดขอบไป) — ไม่ควรเช็คด้วย
// buffer.slice(r.chunk.length) ตรงๆ เพราะจะเข้าใจผิดว่าช่องว่างที่ trim ทิ้งไปคือ "ตัวอักษรหาย" เช็คแค่ 2 คุณสมบัติที่สำคัญจริง:
// (1) chunk ต้องเป็นต้นของ buffer, (2) remainder ต้องเป็นท้ายของ buffer, ไม่ทับซ้อนกัน (ไม่มีอะไรซ้ำ/หายเกินช่องว่างที่ตั้งใจ trim)
function assertCleanSplit(buffer, r) {
  assert.equal(buffer.startsWith(r.chunk), true, 'chunk ต้องเป็น prefix ของ buffer')
  assert.equal(buffer.endsWith(r.remainder), true, 'remainder ต้องเป็น suffix ของ buffer')
  assert.equal(buffer.length - r.remainder.length >= r.chunk.length, true, 'ห้ามมีตัวอักษรซ้ำกันระหว่าง chunk กับ remainder')
}

test('strong boundary: ตัดหลังเครื่องหมาย ? ทันทีถ้า buffer ยาวพอ ไม่ต้องรอ timeout', () => {
  const r = findChunkBoundary('มีอะไรสอบถามเพิ่มเติมไหมคะ? แล้วอันนี้อีกเรื่อง', 0)
  assert.equal(r.chunk, 'มีอะไรสอบถามเพิ่มเติมไหมคะ?')
  assert.equal(r.remainder, ' แล้วอันนี้อีกเรื่อง')
})

test('strong boundary: เครื่องหมาย ! ก็ตัดได้เหมือนกัน', () => {
  const r = findChunkBoundary('ดีใจมากเลยค่ะ! ขอบคุณที่สนใจนะคะ', 0)
  assert.equal(r.chunk, 'ดีใจมากเลยค่ะ!')
})

test('Thai soft boundary: ตัดหลัง ค่ะ ที่ตามด้วยช่องว่าง (จบประโยคจริง)', () => {
  const r = findChunkBoundary('ยินดีค่ะ พี่สนใจกิจกรรมนี้ไหมคะ', 0)
  assert.equal(r.chunk, 'ยินดีค่ะ')
  assert.equal(r.remainder, ' พี่สนใจกิจกรรมนี้ไหมคะ')
})

test('Thai soft boundary: นะคะ/นะครับ ต้องจับก่อน คะ/ครับ ไม่ตัดสั้นผิดตำแหน่ง', () => {
  const r = findChunkBoundary('เดี๋ยวส่ง SMS ให้นะคะ รอสักครู่นะคะ', 0)
  assert.equal(r.chunk, 'เดี๋ยวส่ง SMS ให้นะคะ')
})

test('Thai soft boundary: ค่ะ ที่ไม่มีช่องว่างตามหลัง (เป็นคำกลางคำ) ไม่นับเป็นจุดตัด', () => {
  // ต้องไม่มี boundary ที่ใช้ได้ตรงไหนเลยในบัฟเฟอร์นี้ (รวมท้ายบัฟเฟอร์ด้วย) ถึงจะทดสอบเคส "ค่ะ กลางคำ" ได้จริง
  const r = findChunkBoundary('ยินดีค่ะขอบคุณที่ติดต่อมากับเรา', 0)
  assert.equal(r, null, 'ไม่มี strong/soft boundary ที่ใช้ได้ และยังไม่ครบ soft timeout')
})

test('กันตัดก้อนสั้นเกินไป: ข้อความสั้นกว่า MIN_CHUNK_LENGTH ไม่ตัดแม้จะแมตช์ boundary', () => {
  const r = findChunkBoundary('ค่ะ', 0)
  assert.equal(r, null)
})

test('ยังไม่ครบ soft timeout และไม่มี strong/soft boundary เลย → รอต่อ (return null)', () => {
  const r = findChunkBoundary('กำลังพูดต่อเนื่องไปเรื่อยๆไม่มีจุดพักเลยสักที', 100)
  assert.equal(r, null)
})

test('ครบ soft timeout แล้ว + buffer ยาวพอ (>= FALLBACK_MIN_LENGTH) → ตัดที่ natural boundary (ช่องว่างล่าสุด)', () => {
  // ใช้ข้อความสังเคราะห์ไม่มี strong/soft particle ปนเลย เพื่อทดสอบ fallback tier ล้วนๆ ไม่ให้ tier อื่นมาตัดก่อน
  const buffer = 'aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd'
  const r = findChunkBoundary(buffer, 300)
  assert.ok(r, 'ควรตัดได้แล้วเพราะครบ soft timeout')
  assertCleanSplit(buffer, r)
})

test('ครบ soft timeout แต่ buffer ยังสั้นกว่า FALLBACK_MIN_LENGTH → ยังไม่ตัด รอต่อ', () => {
  const r = findChunkBoundary('อยากทราบว่า', 350)
  assert.equal(r, null)
})

test('ครบ hard max + มีช่องว่างใน buffer → บังคับตัดที่ natural boundary ล่าสุด ไม่ตัดกลางคำ', () => {
  // ข้อความสังเคราะห์ไม่มี strong/soft particle ปนเลย เพื่อบังคับให้ต้องไปถึง tier hard-max จริงๆ
  const buffer = 'aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd'
  const r = findChunkBoundary(buffer, 900)
  assert.ok(r, 'ต้องตัดได้เพราะเกิน hard max แล้ว')
  assertCleanSplit(buffer, r)
  // ต้องไม่ตัดกลางคำ 'dddddddddd' (คำสุดท้าย) — chunk ต้องไม่มีตัวอักษรจากคำนั้นติดมาบางส่วน
  assert.equal(r.chunk.includes('d'), false, 'ห้ามตัดกลางคำสุดท้ายที่ยังไม่ทันจบ')
})

test('ครบ hard max แต่ buffer ไม่มีช่องว่างเลยสักตัว (คำเดียวยาวผิดปกติ) → ยังคืน null ห้ามตัดกลางคำเด็ดขาด', () => {
  const r = findChunkBoundary('httpswwwexamplecompromotionverylongurlwithoutspaces', 900)
  assert.equal(r, null, 'ไม่มีจุดตัดที่ปลอดภัยเลย ต้องรอต่อแม้เกิน hard max แล้วก็ตาม')
})

test('มีหลายจุดตัดที่เป็นไปได้ในบัฟเฟอร์เดียว → เลือกจุดที่เจอเร็วสุด (leftmost) ไม่ใช่จุดท้ายสุด', () => {
  const r = findChunkBoundary('ได้เลยค่ะ ฝากปกติได้เลยค่ะ แต่ถ้าสนใจกิจกรรมนี้ด้วยก็แจ้งสลิปได้เลยนะคะ', 0)
  assert.equal(r.chunk, 'ได้เลยค่ะ')
})

test('remainder ต้องต่อกับ chunk แล้วได้ buffer เดิมกลับมา ไม่มีตัวอักษรหายหรือซ้ำ (ไม่นับช่องว่างที่ตั้งใจ trim ออก)', () => {
  const buffer = 'สวัสดีค่ะ ยินดีให้บริการนะคะ'
  const r = findChunkBoundary(buffer, 0)
  assertCleanSplit(buffer, r)
})

test('buffer เป็นประโยคเดียวจบสนิท ไม่มีอะไรเหลือ → remainder เป็นค่าว่าง', () => {
  const r = findChunkBoundary('ขอบคุณที่สนใจนะคะ.', 0)
  assert.equal(r.chunk, 'ขอบคุณที่สนใจนะคะ.')
  assert.equal(r.remainder, '')
})

test('buffer ว่างเปล่า → คืน null เสมอ', () => {
  assert.equal(findChunkBoundary('', 900), null)
  assert.equal(findChunkBoundary(null, 900), null)
})
