const { test } = require('node:test')
const assert = require('node:assert/strict')
const { findChunkBoundary, getNumericProtectionRemainingMs, evaluateNumericProtectionDiagnostic, CHUNK_REASON } = require('../src/utils/speechChunker')

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

// ---------------------------------------------------------------------------
// L1c1 — protected numeric boundary (production defect 2026-08-19): natural-boundary cut ที่ candidate
// chunk ลงท้ายด้วยตัวเลข ("รับ 2,000" | "พอยต์นะคะ" เป็นคนละ ElevenLabs request กัน ฟังเป็นคนละโทนเสียง) ต้อง
// ถูกกันไว้จนกว่าจะครบ HARD_MAX_MS (reuse ceiling เดิม ไม่เพิ่ม constant ใหม่) — strong/soft boundary ไม่โดนกระทบ
// ---------------------------------------------------------------------------

const NUMERIC_TAIL_BUFFER = 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 ' // ไม่มี strong/soft boundary ปนเลย ยาวเกิน FALLBACK_MIN_LENGTH

test('L1c1-1) natural-boundary candidate ลงท้ายตัวเลข ที่ 300ms (พอดี SOFT_TIMEOUT) → ยังไม่ตัด (protected)', () => {
  const r = findChunkBoundary(NUMERIC_TAIL_BUFFER, 300)
  assert.equal(r, null, 'ต้องกันไว้ก่อน รอ delta ถัดไปที่น่าจะเป็นหน่วยนับ')
})

test('L1c1-2) ตัวเดียวกันที่ 799ms (ยังไม่ครบ HARD_MAX_MS) → ยังไม่ตัด (protected)', () => {
  const r = findChunkBoundary(NUMERIC_TAIL_BUFFER, 799)
  assert.equal(r, null, 'ยังไม่ครบ ceiling — ยังต้องกันไว้')
})

test('L1c1-3) ตัวเดียวกันที่ 800ms (ครบ HARD_MAX_MS พอดี) → protection หมดอายุ ตัดได้ตามปกติ', () => {
  const r = findChunkBoundary(NUMERIC_TAIL_BUFFER, 800)
  assert.ok(r, 'ครบ ceiling แล้ว ต้องยอมตัดกันไม่ให้รอไม่มีที่สิ้นสุดถ้า Claude เงียบผิดปกติจริง')
  assertCleanSplit(NUMERIC_TAIL_BUFFER, r)
})

test('L1c1-4) "รับ 2,000 พอยต์นะคะ" (หน่วยนับมาครบแล้ว) → soft boundary "นะคะ" ตัดทั้ง semantic unit รวดเดียว ไม่แยก', () => {
  const buffer = NUMERIC_TAIL_BUFFER + 'พอยต์นะคะ'
  const r = findChunkBoundary(buffer, 300) // elapsedMs ไม่สำคัญ — soft boundary ตัดทันทีเสมอ
  assert.ok(r.chunk.includes('2,000') && r.chunk.includes('พอยต์'), 'ตัวเลขกับหน่วยนับต้องอยู่ chunk เดียวกัน')
  assert.equal(r.chunk, 'ตอนนี้สมาชิกใหม่ฝาก 100 บาท รับ 2,000 พอยต์นะคะ')
})

test('L1c1-5) natural boundary ที่ไม่ได้ลงท้ายตัวเลข ที่ 300ms → พฤติกรรมเดิมเป๊ะ ไม่ถูก protection ทำให้ช้าลง', () => {
  const buffer = 'aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd' // ลงท้ายตัวอักษร ไม่ใช่ตัวเลข
  const r = findChunkBoundary(buffer, 300)
  assert.ok(r, 'ต้องตัดได้ทันทีเหมือนก่อนมี L1c1 เพราะ candidate ไม่ได้ลงท้ายด้วยตัวเลข')
  assertCleanSplit(buffer, r)
})

test('L1c1-6) strong boundary "รับ 2,000." → ตัดทันทีที่ elapsedMs=0 ไม่โดน numeric protection เลย (strong/soft มาก่อนเสมอ)', () => {
  const r = findChunkBoundary('ยอดรับสิทธิ์ตอนนี้คือ 2,000. เยอะมากเลยนะคะ', 0)
  assert.equal(r.chunk, 'ยอดรับสิทธิ์ตอนนี้คือ 2,000.')
})

// ---------------------------------------------------------------------------
// L1c1 follow-up — getNumericProtectionRemainingMs(): ใช้โดย createChunkedProducer() ใน chunkedTurn.js สำหรับ
// arm wall-clock timer เอง (ดูหมายเหตุที่ speechChunker.js) — ทดสอบ contract ตรงๆ แยกจาก findChunkBoundary()
// ---------------------------------------------------------------------------

test('getNumericProtectionRemainingMs: candidate ลงท้ายตัวเลข ยังไม่ครบ HARD_MAX_MS → คืนจำนวน ms ที่เหลือถูกต้อง', () => {
  assert.equal(getNumericProtectionRemainingMs(NUMERIC_TAIL_BUFFER, 0), 800)
  assert.equal(getNumericProtectionRemainingMs(NUMERIC_TAIL_BUFFER, 350), 450)
  assert.equal(getNumericProtectionRemainingMs(NUMERIC_TAIL_BUFFER, 799), 1)
})

test('getNumericProtectionRemainingMs: elapsedMs >= HARD_MAX_MS → null (protection หมดอายุแล้ว)', () => {
  assert.equal(getNumericProtectionRemainingMs(NUMERIC_TAIL_BUFFER, 800), null)
  assert.equal(getNumericProtectionRemainingMs(NUMERIC_TAIL_BUFFER, 1200), null)
})

test('getNumericProtectionRemainingMs: buffer ที่มี strong/soft boundary อยู่แล้ว → null (ตัดได้เลย ไม่ใช่ numeric-protected case)', () => {
  assert.equal(getNumericProtectionRemainingMs(NUMERIC_TAIL_BUFFER + 'พอยต์นะคะ', 100), null)
})

test('getNumericProtectionRemainingMs: candidate ไม่ได้ลงท้ายตัวเลข → null', () => {
  assert.equal(getNumericProtectionRemainingMs('aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd', 100), null)
})

test('getNumericProtectionRemainingMs: buffer สั้นกว่า FALLBACK_MIN_LENGTH → null (ไม่มี natural-boundary candidate ให้ protect)', () => {
  assert.equal(getNumericProtectionRemainingMs('รับ 100', 100), null)
})

test('getNumericProtectionRemainingMs: buffer ว่างเปล่า → null', () => {
  assert.equal(getNumericProtectionRemainingMs('', 100), null)
  assert.equal(getNumericProtectionRemainingMs(null, 100), null)
})

// ---------------------------------------------------------------------------
// Track M (diagnostic only, design R3 LOCKED 2026-08-22) — reason field ใน findChunkBoundary() ต้องตรงกับ
// 4 branch จริงเป๊ะ และ evaluateNumericProtectionDiagnostic() ต้อง gate ด้วย SOFT_TIMEOUT_MS เหมือน
// findChunkBoundary() จริง (แก้ false-positive ที่ R1 เคยพลาดตอนใช้ getNumericProtectionRemainingMs() ตรงๆ)
// ---------------------------------------------------------------------------

test('Track M reason: strong boundary → CHUNK_REASON.STRONG_BOUNDARY', () => {
  const r = findChunkBoundary('มีอะไรสอบถามเพิ่มเติมไหมคะ? แล้วอันนี้อีกเรื่อง', 0)
  assert.equal(r.reason, CHUNK_REASON.STRONG_BOUNDARY)
})

test('Track M reason: Thai soft boundary → CHUNK_REASON.SOFT_BOUNDARY', () => {
  const r = findChunkBoundary('ยินดีค่ะ พี่สนใจกิจกรรมนี้ไหมคะ', 0)
  assert.equal(r.reason, CHUNK_REASON.SOFT_BOUNDARY)
})

test('Track M reason: natural boundary ใน soft-window (elapsed=300, buffer>=25) → CHUNK_REASON.NATURAL_BOUNDARY', () => {
  const buffer = 'aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd'
  const r = findChunkBoundary(buffer, 300)
  assert.equal(r.reason, CHUNK_REASON.NATURAL_BOUNDARY)
})

test('Track M reason: natural boundary ที่ hard max (elapsed=900, buffer>=25) → CHUNK_REASON.NATURAL_BOUNDARY_HARD_MAX', () => {
  const buffer = 'aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd'
  const r = findChunkBoundary(buffer, 900)
  assert.equal(r.reason, CHUNK_REASON.NATURAL_BOUNDARY_HARD_MAX)
})

test('Track M Final Blocker 1: buffer สั้นกว่า FALLBACK_MIN_LENGTH(25) แต่ยาวกว่า MIN_CHUNK_LENGTH(6) — ต้องยังไม่ตัดก่อน HARD_MAX แต่ตัดได้ที่ HARD_MAX พอดี (source บรรทัด 99-104 ไม่มี FALLBACK_MIN_LENGTH gate)', () => {
  const buffer = 'aaaaaaaaaa bb' // 13 ตัวอักษร: 6<=13<25, มี natural boundary ที่ position 10 (>=MIN_CHUNK_LENGTH)
  assert.equal(buffer.length < 25 && buffer.length >= 6, true, 'sanity: buffer ต้องอยู่ในช่วงที่ R2 เคยพลาด')

  const before = findChunkBoundary(buffer, 799)
  assert.equal(before, null, 'ก่อน HARD_MAX, buffer<FALLBACK_MIN_LENGTH → soft-window path ปฏิเสธ ต้องยังไม่ตัด')

  const after = findChunkBoundary(buffer, 800)
  assert.ok(after, 'ที่ HARD_MAX พอดี ต้องตัดได้แม้ buffer<25 เพราะ branch นี้ gate แค่ MIN_CHUNK_LENGTH')
  assert.equal(after.reason, CHUNK_REASON.NATURAL_BOUNDARY_HARD_MAX)
  assert.equal(after.chunk, 'aaaaaaaaaa')
  assert.equal(after.remainder, 'bb', 'naturalCut อยู่หลัง space ทันที (findLastSafeBoundary คืน i+1) — remainder จึงไม่มี space นำหน้า')
})

// ใช้ NUMERIC_TAIL_BUFFER (ประกาศไว้ด้านบน มี trailing space ท้าย "2,000") ตรงๆ — ไม่ retype literal เอง เพราะ
// ถ้าพลาด trailing space จะทำให้ findLastSafeBoundary หาจุดตัดผิดตำแหน่ง (candidate ไม่ลงท้ายตัวเลขอีกต่อไป)
test('Track M evaluateNumericProtectionDiagnostic: elapsedMs=50 (ก่อน SOFT_TIMEOUT_MS) แม้ buffer ลงท้ายตัวเลขและยาวพอ → candidateEligible=false, blockedByNumericProtection=false (R2 false-positive fix)', () => {
  const d = evaluateNumericProtectionDiagnostic(NUMERIC_TAIL_BUFFER, 50)
  assert.equal(d.candidateEligible, false)
  assert.equal(d.blockedByNumericProtection, false)
})

test('Track M evaluateNumericProtectionDiagnostic: elapsedMs=400, natural candidate ยาวพอ ไม่ลงท้ายตัวเลข → candidateEligible=true, blocked=false', () => {
  const d = evaluateNumericProtectionDiagnostic('aaaaaaaaaa bbbbbbbbbb cccccccccc dddddddddd', 400)
  assert.equal(d.candidateEligible, true)
  assert.equal(d.blockedByNumericProtection, false)
})

test('Track M evaluateNumericProtectionDiagnostic: elapsedMs=400, natural candidate ลงท้ายตัวเลข → candidateEligible=true, blocked=true', () => {
  const d = evaluateNumericProtectionDiagnostic(NUMERIC_TAIL_BUFFER, 400)
  assert.equal(d.candidateEligible, true)
  assert.equal(d.blockedByNumericProtection, true)
})

test('Track M evaluateNumericProtectionDiagnostic: elapsedMs=850 (>=HARD_MAX) เดียวกับข้างบน → blocked=false (protection หมดอายุ)', () => {
  const d = evaluateNumericProtectionDiagnostic(NUMERIC_TAIL_BUFFER, 850)
  assert.equal(d.candidateEligible, true)
  assert.equal(d.blockedByNumericProtection, false)
})

test('Track M regression: .chunk/.remainder เดิมไม่เปลี่ยนค่าจากการเพิ่ม .reason (byte-for-byte เดิม)', () => {
  const buffer = 'ยินดีค่ะ พี่สนใจกิจกรรมนี้ไหมคะ'
  const r = findChunkBoundary(buffer, 0)
  assertCleanSplit(buffer, r)
  assert.equal(r.chunk, 'ยินดีค่ะ')
  assert.equal(r.remainder, ' พี่สนใจกิจกรรมนี้ไหมคะ')
})
