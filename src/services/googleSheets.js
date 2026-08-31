const { google } = require('googleapis')
const { getGoogleClientOptions } = require('../utils/googleCredentials')

const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_ID

// Sheet names
const SHEETS = {
  CONTACTS: 'Contacts',
  CAMPAIGNS: 'Campaigns',
  RESULTS: 'Call Results',
  TEMPLATES: 'SMS Templates',
  BLOCKLIST: 'Blocklist',
  SENDER_NAMES: 'Sender Names',
  TWILIO_NUMBERS: 'Twilio Numbers',
  STREAMING_CONFIG: 'Streaming Config'
}

let sheets = null

// Serialize read-modify-write ต่อชีต — Sheets API ไม่มี optimistic lock ให้
// ถ้าหลายสายจบพร้อมกันแล้ว updateRowByKey เข้ามาพร้อมกัน อาจอ่าน row เก่าคนละรอบแล้วเขียนทับกันเอง (lost update)
const sheetLocks = new Map()

function withSheetLock(sheetName, fn) {
  const prev = sheetLocks.get(sheetName) || Promise.resolve()
  const run = prev.then(fn, fn)
  sheetLocks.set(sheetName, run.catch(() => {}))
  return run
}

// Google Sheets API มี quota "requests per minute per user" — พัง 100% ทันทีไม่มีการลองใหม่เดิม
// ทำให้เจอ error นี้เมื่อไหร่ (ช่วงแอดมิน+สายที่กำลังคุยยิง sheet พร้อมกันเยอะ) ผลการโทร/ข้อมูลที่ควรบันทึกหายไปเงียบๆ
// retry เฉพาะ error ที่เป็น rate-limit จริงๆ เท่านั้น (429/503 หรือข้อความ quota) — error อื่น (เช่น column ไม่มีจริง)
// ต้อง fail ทันทีเหมือนเดิม ไม่งั้นบั๊กจริงจะถูกดีเลย์การแจ้งเตือนโดยไม่จำเป็น
function isRetryableSheetsError(err) {
  const status = err?.code || err?.response?.status
  if (status === 429 || status === 503) return true
  return /quota exceeded/i.test(err?.message || '')
}

// exponential backoff + jitter — jitter กันหลาย request ที่โดน quota พร้อมกันแล้วลองใหม่จังหวะเดียวกันเป๊ะๆ
// จนไปชนกำแพง quota ซ้ำอีกรอบ (thundering herd) จำกัดไว้ 3 ครั้งลองใหม่ (รวมดีเลย์สูงสุด ~5s) กันค้างนานเกินไป
async function withRetry(fn, { maxAttempts = 4, baseDelayMs = 500 } = {}) {
  let lastErr
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastErr = err
      if (attempt === maxAttempts || !isRetryableSheetsError(err)) throw err
      const delay = baseDelayMs * 2 ** (attempt - 1) * (0.75 + Math.random() * 0.5)
      console.warn(`[Sheets] ${(err.message || '').slice(0, 100)} — retry ${attempt}/${maxAttempts - 1} in ${Math.round(delay)}ms`)
      await new Promise(r => setTimeout(r, delay))
    }
  }
  throw lastErr
}

async function getClient() {
  if (sheets) return sheets
  const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    ...getGoogleClientOptions(),
  })
  sheets = google.sheets({ version: 'v4', auth })
  return sheets
}

// Schema guard (Hardening Batch, 2026-08-30) — audit finding: updateRowByKey/appendRowByFields already
// silently skip any field whose name doesn't match a header (colIdx === -1 → no-op), so a renamed/missing
// critical column doesn't throw — it just quietly stops reading/writing that field, with nothing in the
// logs to say why. This only ADDS a warning log when a sheet's actual headers are missing a column this
// codebase depends on elsewhere (id/status for Campaigns, phone/status for Contacts) — never blocks a
// read/write, never changes what gets returned. Warned once per sheet per process (not every read — this
// function runs on nearly every Sheets call) so it can't spam the logs.
const CRITICAL_HEADERS = {
  [SHEETS.CAMPAIGNS]: ['id', 'name', 'status'],
  [SHEETS.CONTACTS]: ['phone', 'campaign', 'status'],
}
const schemaWarnedSheets = new Set()

function warnMissingCriticalHeaders(sheetName, headers) {
  const critical = CRITICAL_HEADERS[sheetName]
  if (!critical || schemaWarnedSheets.has(sheetName)) return
  schemaWarnedSheets.add(sheetName)
  const missing = critical.filter(h => !headers.includes(h))
  if (missing.length) {
    console.error(`[SCHEMA_WARNING] Sheet "${sheetName}" ขาดคอลัมน์สำคัญ: ${missing.join(', ')} — field เหล่านี้จะอ่าน/เขียนไม่ได้เงียบๆ จนกว่าจะแก้หัวคอลัมน์`)
  }
}

// ดึงข้อมูลดิบ (headers ที่ normalize แล้ว + rows แบบ array) เก็บ row index ไว้ใช้ update ทีหลัง
async function getSheetData(sheetName) {
  const client = await getClient()
  const res = await withRetry(() => client.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
  }))
  const values = res.data.values || []
  if (!values.length) return { headers: [], rows: [] }
  const headers = values[0].map(h => h.toLowerCase().trim().replace(/\s+/g, '_'))
  warnMissingCriticalHeaders(sheetName, headers)
  return { headers, rows: values.slice(1) }
}

async function getRows(sheetName) {
  const { headers, rows } = await getSheetData(sheetName)
  return rows.map(row => {
    const obj = {}
    headers.forEach((h, i) => { obj[h] = row[i] || '' })
    return obj
  })
}

// หา index ของแถวด้วย key column — ใช้ร่วมกันทั้ง updateRowByKey และ deleteRowByKey กันตรรกะ drift แยกกัน
function findRowIndex(headers, rows, keyField, keyValue) {
  const keyIdx = headers.indexOf(keyField)
  if (keyIdx === -1) return -1
  return rows.findIndex(r => r[keyIdx] === keyValue)
}

// หา row ด้วย key column แล้วอัปเดตหลาย field พร้อมกันในครั้งเดียว (เขียนทับทั้งแถว)
// ล็อกต่อชีต กัน read-modify-write ชนกันเมื่อหลายสายจบพร้อมกัน
async function updateRowByKey(sheetName, keyField, keyValue, updates) {
  return withSheetLock(sheetName, async () => {
    const { headers, rows } = await getSheetData(sheetName)
    if (headers.indexOf(keyField) === -1) throw new Error(`Column '${keyField}' not found in ${sheetName}`)

    const rowIdx = findRowIndex(headers, rows, keyField, keyValue)
    if (rowIdx === -1) return false

    const row = [...rows[rowIdx]]
    while (row.length < headers.length) row.push('')
    Object.entries(updates).forEach(([field, value]) => {
      const colIdx = headers.indexOf(field.toLowerCase().trim().replace(/\s+/g, '_'))
      if (colIdx !== -1) row[colIdx] = value
    })

    const client = await getClient()
    const lastCol = String.fromCharCode(65 + headers.length - 1)
    await withRetry(() => client.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetName}!A${rowIdx + 2}:${lastCol}${rowIdx + 2}`,
      valueInputOption: 'RAW',
      requestBody: { values: [row] },
    }))
    return true
  })
}

// Sheets API ต้องใช้ sheetId ตัวเลข (ไม่ใช่ชื่อชีต) สำหรับ batchUpdate อย่าง deleteDimension
// ไม่ cache ตั้งใจ — deleteRowByKey ใช้ไม่บ่อย (unblock เป็น action ที่กดนานๆ ครั้ง) การ cache ไว้เสี่ยงได้ sheetId
// เก่าถ้ามีคนลบ/สร้างแท็บซ้ำชื่อเดิมระหว่าง process ยังรันอยู่ ซึ่งจะไปลบผิดแถวผิดชีตแบบเงียบๆ ไม่คุ้มกับที่ประหยัดได้
async function getSheetId(sheetName) {
  const client = await getClient()
  const meta = await withRetry(() => client.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID }))
  const sheet = (meta.data.sheets || []).find(s => s.properties.title === sheetName)
  if (!sheet) throw new Error(`Sheet '${sheetName}' not found`)
  return sheet.properties.sheetId
}

// ลบทั้งแถวจริง (ต่างจาก updateRowByKey ที่แก้ค่าในแถวเดิม) — ใช้กับชีตที่ไม่มี status column ให้ soft-delete เช่น Blocklist
async function deleteRowByKey(sheetName, keyField, keyValue) {
  return withSheetLock(sheetName, async () => {
    const { headers, rows } = await getSheetData(sheetName)
    const rowIdx = findRowIndex(headers, rows, keyField, keyValue)
    if (rowIdx === -1) return false

    const sheetId = await getSheetId(sheetName)
    const client = await getClient()
    await withRetry(() => client.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          deleteDimension: {
            range: { sheetId, dimension: 'ROWS', startIndex: rowIdx + 1, endIndex: rowIdx + 2 }, // +1 เพราะแถวแรกคือ header
          },
        }],
      },
    }))
    return true
  })
}

async function appendRow(sheetName, values) {
  const client = await getClient()
  await withRetry(() => client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
    valueInputOption: 'RAW',
    requestBody: { values: [values] },
  }))
}

// เติมค่าตามชื่อ column (ไม่สนลำดับ) แล้ว append — กันพังถ้า column ในชีตสลับตำแหน่ง
async function appendRowByFields(sheetName, fields) {
  const { headers } = await getSheetData(sheetName)
  const row = headers.map(h => (fields[h] !== undefined ? fields[h] : ''))
  await appendRow(sheetName, row)
}

// append หลายแถวพร้อมกันในครั้งเดียว (1 Sheets API call แทนที่จะยิงทีละแถว) — ใช้ตอน bulk import
async function appendRowsByFields(sheetName, fieldsArray) {
  const { headers } = await getSheetData(sheetName)
  const client = await getClient()
  const values = fieldsArray.map(fields => headers.map(h => (fields[h] !== undefined ? fields[h] : '')))
  await withRetry(() => client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
    valueInputOption: 'RAW',
    requestBody: { values },
  }))
}

// Blocklist อาจยังไม่ถูกสร้างเป็นชีตแยก — กันพังถ้ายังไม่มี ให้ถือว่าลิสต์ว่าง
async function getBlocklistRows() {
  try {
    return await getRows(SHEETS.BLOCKLIST)
  } catch (err) {
    return []
  }
}

// C5 — ชีต "Streaming Config" เป็น key/value config tab (คอลัมน์ key, value) สำหรับ chunked-streaming rollout
// เป็นฟีเจอร์ใหม่เหมือนกัน ผู้ใช้อาจยังไม่ได้สร้างแท็บนี้ — ให้ rolloutConfig.js ตีความ [] ว่า "อ่านค่าไม่ได้"
// (last-known-good/cold-start 0% แล้วแต่กรณี) ไม่ใช่ throw ทำให้ระบบสายเข้า/ออกอื่นพังไปด้วย
async function getStreamingConfigRows() {
  try {
    return await getRows(SHEETS.STREAMING_CONFIG)
  } catch (err) {
    return []
  }
}

// ชีต "Twilio Numbers" เป็นฟีเจอร์ใหม่ ผู้ใช้อาจยังไม่ได้สร้างแท็บนี้ในชีตจริง — กันพัง getInboundCampaignForNumber
// (เรียกจาก webhook สายเข้าจริงทุกครั้ง) ล่มทั้งระบบสายเข้าถ้ายังไม่ได้สร้างชีต ให้ถือว่ายังไม่มีเบอร์ไหนลงทะเบียนไว้เลยแทน
async function getTwilioNumberRows() {
  try {
    return await getRows(SHEETS.TWILIO_NUMBERS)
  } catch (err) {
    return []
  }
}

const sheetsService = {
  async getCampaign(campaignId) {
    const rows = await getRows(SHEETS.CAMPAIGNS)
    return rows.find(r => r.id === campaignId) || null
  },

  // หา inbound campaign ที่เบอร์นี้ถูกผูกไว้ (ผูกจากหน้า "จัดการเบอร์" ไม่ใช่จากหน้า Campaigns อีกต่อไป)
  // เบอร์ที่ไม่ได้ลงทะเบียนในระบบเลย หรือลงทะเบียนแล้วแต่ไม่ได้ผูก campaign ไว้ → คืน null ให้ webhook.js ปฏิเสธสายแทนที่จะเดาเอา campaign อื่นมาใช้
  async getInboundCampaignForNumber(toNumber) {
    const numberRows = await getTwilioNumberRows()
    const numberRow = numberRows.find(r => r.phone_number === toNumber)
    if (!numberRow || !numberRow.campaign_id) return null

    const campaigns = await getRows(SHEETS.CAMPAIGNS)
    return campaigns.find(r => r.id === numberRow.campaign_id && r.status === 'active' && r.type === 'inbound') || null
  },

  async getCampaigns() {
    return getRows(SHEETS.CAMPAIGNS)
  },

  async addCampaign(fields) {
    await appendRowByFields(SHEETS.CAMPAIGNS, { status: 'active', type: 'outbound', ...fields })
  },

  async updateCampaign(id, updates) {
    return updateRowByKey(SHEETS.CAMPAIGNS, 'id', id, updates)
  },

  async deleteCampaign(id) {
    return deleteRowByKey(SHEETS.CAMPAIGNS, 'id', id)
  },

  async getPendingContacts(campaignId) {
    const rows = await getRows(SHEETS.CONTACTS)
    return rows.filter(r => r.campaign === campaignId && r.status === 'pending')
  },

  async getContacts({ campaignId, status } = {}) {
    const rows = await getRows(SHEETS.CONTACTS)
    return rows.filter(r =>
      (!campaignId || r.campaign === campaignId) &&
      (!status || r.status === status)
    )
  },

  async getContact(phone) {
    const rows = await getRows(SHEETS.CONTACTS)
    return rows.find(r => r.phone === phone) || null
  },

  async addContact(fields) {
    await appendRowByFields(SHEETS.CONTACTS, { status: 'pending', ...fields })
  },

  // นำเข้าหลายเบอร์พร้อมกัน (paste จาก Excel/Sheets) — 1 request แทนยิงทีละเบอร์
  async addContactsBulk(contactsArr) {
    const rows = contactsArr.map(c => ({ status: 'pending', ...c }))
    await appendRowsByFields(SHEETS.CONTACTS, rows)
  },

  async updateContact(phone, updates) {
    return updateRowByKey(SHEETS.CONTACTS, 'phone', phone, updates)
  },

  async updateContactStatus(phone, status) {
    return updateRowByKey(SHEETS.CONTACTS, 'phone', phone, { status })
  },

  async getBlocklist() {
    return getBlocklistRows()
  },

  async isBlocked(phone) {
    const list = await getBlocklistRows()
    return list.some(b => b.phone === phone)
  },

  async addToBlocklist(phone, reason) {
    await appendRowByFields(SHEETS.BLOCKLIST, { phone, reason: reason || '', blocked_at: new Date().toISOString() })
  },

  async removeFromBlocklist(phone) {
    return deleteRowByKey(SHEETS.BLOCKLIST, 'phone', phone)
  },

  async saveCallResult(result) {
    await appendRowByFields(SHEETS.RESULTS, { ...result, timestamp: new Date().toISOString() })
  },

  // ผูก URL ไฟล์บันทึกเสียงเข้ากับแถวผลการโทรที่มีอยู่แล้ว (เรียกจาก recordingStatusCallback ของ Twilio)
  async saveRecordingUrl(callSid, recordingUrl) {
    return updateRowByKey(SHEETS.RESULTS, 'call_sid', callSid, { recording_url: recordingUrl })
  },

  // บันทึก message_id ตอนส่ง SMS follow-up เสร็จ (ผูกกับแถวผลการโทรผ่าน call_sid) — ต้องมีคอลัมน์ sms_message_id, sms_status ในชีต Call Results
  async updateCallResultSmsStatus(callSid, { messageId, status }) {
    return updateRowByKey(SHEETS.RESULTS, 'call_sid', callSid, { sms_message_id: messageId || '', sms_status: status || '' })
  },

  // /webhook/sms-dr เรียกตอน ThaiBulkSMS แจ้งผลส่งจริง — หาแถวด้วย message_id ที่บันทึกไว้ตอนส่ง แล้วอัปเดตสถานะทับ
  async updateSmsDeliveryStatus(messageId, status) {
    return updateRowByKey(SHEETS.RESULTS, 'sms_message_id', messageId, { sms_status: status })
  },

  // เทมเพลตเป็น library แยกอิสระ ไม่ผูกกับ outcome โดยตรง — campaign แต่ละอันเลือกเองว่า outcome ไหนจะใช้เทมเพลตไหน (เก็บ template id ไว้ที่ campaign.sms_<outcome>)
  async getSmsTemplateById(id) {
    const rows = await getRows(SHEETS.TEMPLATES)
    return rows.find(r => r.id === id) || null
  },

  async getSmsTemplates() {
    return getRows(SHEETS.TEMPLATES)
  },

  async addSmsTemplate({ id, name, template_text, sender, credit_type }) {
    await appendRowByFields(SHEETS.TEMPLATES, { id, name, template_text, sender: sender || '', credit_type: credit_type || '' })
  },

  async updateSmsTemplate(id, updates) {
    return updateRowByKey(SHEETS.TEMPLATES, 'id', id, updates)
  },

  async deleteSmsTemplate(id) {
    return deleteRowByKey(SHEETS.TEMPLATES, 'id', id)
  },

  // ชื่อผู้ส่ง SMS ที่ลงทะเบียนไว้กับ ThaiBulkSMS แล้ว — template.sender เก็บ id นี้ไว้ (ไม่ใช่ตัวชื่อตรงๆ)
  // เพื่อให้แก้ไข/ลบชื่อได้โดยไม่ต้องไล่แก้ทุก template ที่อ้างอิงอยู่
  async getSenderNameById(id) {
    const rows = await getRows(SHEETS.SENDER_NAMES)
    return rows.find(r => r.id === id) || null
  },

  async getSenderNames() {
    return getRows(SHEETS.SENDER_NAMES)
  },

  async addSenderName({ id, name }) {
    await appendRowByFields(SHEETS.SENDER_NAMES, { id, name })
  },

  async updateSenderName(id, updates) {
    return updateRowByKey(SHEETS.SENDER_NAMES, 'id', id, updates)
  },

  async deleteSenderName(id) {
    return deleteRowByKey(SHEETS.SENDER_NAMES, 'id', id)
  },

  // เบอร์ Twilio ที่ลงทะเบียนไว้ในระบบนี้ (ต่างจาก twilioService.listPhoneNumbers() ที่ดึงลิสต์ดิบทั้งหมดจากบัญชี Twilio จริง)
  // นี่คือแหล่งข้อมูลเดียวที่หน้า Dashboard/เบอร์ติดต่อ/ทดสอบ/ประวัติการโทรใช้แสดง+เลือกเบอร์ร่วมกัน
  async getTwilioNumbers() {
    return getTwilioNumberRows()
  },

  async getTwilioNumberByPhone(phone) {
    const rows = await getTwilioNumberRows()
    return rows.find(r => r.phone_number === phone) || null
  },

  // เบอร์ที่ผูกไว้กับ campaign นี้ (ถ้ามี) — ใช้เป็นเบอร์โทรออกเริ่มต้นตอนสั่งเริ่ม campaign (/api/campaign/start)
  async getTwilioNumberForCampaign(campaignId) {
    const rows = await getTwilioNumberRows()
    return rows.find(r => r.campaign_id === campaignId) || null
  },

  async addTwilioNumber({ phone_number, label, campaign_id, notes }) {
    await appendRowByFields(SHEETS.TWILIO_NUMBERS, { phone_number, label: label || '', campaign_id: campaign_id || '', notes: notes || '' })
  },

  async updateTwilioNumber(phone_number, updates) {
    return updateRowByKey(SHEETS.TWILIO_NUMBERS, 'phone_number', phone_number, updates)
  },

  // เอาออกจากระบบนี้เท่านั้น — ไม่แตะเบอร์จริงในบัญชี Twilio (ไม่ release/ยกเลิกเบอร์)
  async deleteTwilioNumber(phone_number) {
    return deleteRowByKey(SHEETS.TWILIO_NUMBERS, 'phone_number', phone_number)
  },

  async getCallResults({ limit = 50, campaignId, phone } = {}) {
    const rows = await getRows(SHEETS.RESULTS)
    let filtered = rows
    if (campaignId) filtered = filtered.filter(r => r.campaign_id === campaignId)
    if (phone) filtered = filtered.filter(r => r.phone === phone)
    return filtered.slice(-limit).reverse()
  },

  // allTime: true ใช้ตอนต้องการยอดสะสมทั้งหมดไม่ผูกกับตัวกรองวัน (เช่นสถิติต่อ campaign ในหน้า Campaigns)
  // dateFrom/dateTo: ช่วงวันที่กำหนดเองแบบ absolute (YYYY-MM-DD) — ใช้แทน days ถ้าระบุมาทั้งคู่ (เลือกวันที่จากปฏิทินในหน้า Dashboard)
  async getStats({ days = 7, allTime = false, dateFrom, dateTo } = {}) {
    const rows = await getRows(SHEETS.RESULTS)
    const today = new Date().toISOString().slice(0, 10)

    let filteredRows, dayBuckets

    if (dateFrom && dateTo) {
      // จำกัดสูงสุด 366 วัน กันเผลอเลือกช่วงกว้างเกินไป (เช่นพิมพ์ปีผิด) แล้ว bucket เยอะจนกราฟช้า/พัง
      // ครอบทั้ง filteredRows และ dayBuckets ด้วยขอบเขตเดียวกันเป๊ะๆ — เดิมจำกัดแค่ dayBuckets ทำให้ total/outcomes
      // (นับจาก filteredRows ที่ไม่ถูกตัด) รวมแถวเกิน 366 วันเข้าไปด้วย ตัวเลขสรุปเลยไม่ตรงกับผลรวมของกราฟรายวัน
      // ต้องต่อท้ายด้วย Z (UTC) เสมอ — ถ้าไม่ใส่ new Date() จะ parse เป็นเวลาท้องถิ่นของเครื่อง แล้ว toISOString()
      // แปลงกลับเป็น UTC จะได้วันที่เลื่อนไป 1 วันถ้าเครื่องรันอยู่ใน timezone ที่ไม่ใช่ UTC (เช่น Asia/Bangkok = UTC+7)
      const maxEnd = new Date(dateFrom + 'T00:00:00Z')
      maxEnd.setUTCDate(maxEnd.getUTCDate() + 365)
      const cappedDateTo = new Date(dateTo + 'T00:00:00Z') > maxEnd ? maxEnd.toISOString().slice(0, 10) : dateTo

      filteredRows = rows.filter(r => {
        const day = (r.timestamp || '').slice(0, 10)
        return day >= dateFrom && day <= cappedDateTo
      })
      dayBuckets = []
      const cursor = new Date(dateFrom + 'T00:00:00Z')
      const end = new Date(cappedDateTo + 'T00:00:00Z')
      while (cursor <= end) {
        dayBuckets.push({ key: cursor.toISOString().slice(0, 10), calls: 0, interested: 0 })
        cursor.setUTCDate(cursor.getUTCDate() + 1)
      }
    } else {
      const cutoff = new Date(Date.now() - (days - 1) * 86400000).toISOString().slice(0, 10)
      // total/outcomes/byCampaign/avgDuration/conversionRate ผูกกับช่วงวันที่เลือกจริง (เดิมไม่ผูก ทำให้ตัวเลขไม่ตรงกับกราฟที่กรองอยู่)
      filteredRows = allTime ? rows : rows.filter(r => (r.timestamp || '').slice(0, 10) >= cutoff)
      dayBuckets = []
      for (let i = days - 1; i >= 0; i--) {
        const key = new Date(Date.now() - i * 86400000).toISOString().slice(0, 10)
        dayBuckets.push({ key, calls: 0, interested: 0 })
      }
    }

    const total = filteredRows.length
    const outcomes = {}
    const byCampaign = {}
    const interestedByCampaign = {}
    const byTwilioNumber = {}
    const interestedByTwilioNumber = {}
    let durationSum = 0
    let durationCount = 0
    let callsToday = 0
    const smsStats = { sent: 0, delivered: 0, failed: 0 }
    // แยกตามชั่วโมง/วันในสัปดาห์ตามเวลาไทย (UTC+7) — timestamp เก็บเป็น UTC เสมอ ต้องแปลงก่อนอ่านชั่วโมง/วัน
    // ไม่งั้นชั่วโมงจะเพี้ยนไป 7 ชม. และวันอาจข้ามคืนผิดวันได้ด้วยถ้าใช้ modulo ตรงๆ แทนการเลื่อน timestamp จริง
    const byHour = Array.from({ length: 24 }, () => ({ calls: 0, interested: 0 }))
    const byDayOfWeek = Array.from({ length: 7 }, () => ({ calls: 0, interested: 0 })) // index ตาม Date#getUTCDay(): 0=อาทิตย์

    const bucketMap = new Map(dayBuckets.map(b => [b.key, b]))

    filteredRows.forEach(r => {
      outcomes[r.outcome] = (outcomes[r.outcome] || 0) + 1
      if (r.campaign_id) {
        byCampaign[r.campaign_id] = (byCampaign[r.campaign_id] || 0) + 1
        if (r.outcome === 'interested') interestedByCampaign[r.campaign_id] = (interestedByCampaign[r.campaign_id] || 0) + 1
      }
      // แยกสถิติตามเบอร์ Twilio ที่ใช้โทรจริง — สำหรับ Dashboard สลับมุมมองระหว่าง "ต่อ Campaign" / "ต่อเบอร์"
      if (r.twilio_number) {
        byTwilioNumber[r.twilio_number] = (byTwilioNumber[r.twilio_number] || 0) + 1
        if (r.outcome === 'interested') interestedByTwilioNumber[r.twilio_number] = (interestedByTwilioNumber[r.twilio_number] || 0) + 1
      }
      const duration = Number(r.duration)
      if (!Number.isNaN(duration) && duration > 0) { durationSum += duration; durationCount++ }

      if (r.sms_status === 'sent') smsStats.sent++
      else if (r.sms_status === 'delivery') smsStats.delivered++
      else if (r.sms_status === 'failed') smsStats.failed++

      if (r.timestamp) {
        const d = new Date(r.timestamp)
        if (!Number.isNaN(d.getTime())) {
          const thai = new Date(d.getTime() + 7 * 3600000)
          const hour = thai.getUTCHours()
          const dow = thai.getUTCDay()
          byHour[hour].calls++
          byDayOfWeek[dow].calls++
          if (r.outcome === 'interested') { byHour[hour].interested++; byDayOfWeek[dow].interested++ }
        }
      }
    })

    // callsToday และกราฟรายวัน อิงจากข้อมูลทั้งหมดเสมอ ไม่ขึ้นกับตัวกรองช่วงวัน (bucket ของกราฟกรองตัวเองอยู่แล้วผ่าน dayBuckets)
    rows.forEach(r => {
      if ((r.timestamp || '').startsWith(today)) callsToday++
      const bucket = bucketMap.get((r.timestamp || '').slice(0, 10))
      if (bucket) {
        bucket.calls++
        if (r.outcome === 'interested') bucket.interested++
      }
    })

    const avgDuration = durationCount ? Math.round(durationSum / durationCount) : 0
    const conversionRate = total ? Math.round(((outcomes.interested || 0) / total) * 1000) / 10 : 0
    const smsAttempted = smsStats.sent + smsStats.delivered + smsStats.failed
    const smsDeliveryRate = smsAttempted ? Math.round((smsStats.delivered / smsAttempted) * 1000) / 10 : 0

    return {
      total, outcomes, byCampaign, interestedByCampaign, byTwilioNumber, interestedByTwilioNumber, avgDuration, callsToday, conversionRate,
      smsStats: { ...smsStats, attempted: smsAttempted, deliveryRate: smsDeliveryRate },
      byHour, byDayOfWeek,
      dailyTrend: {
        labels: dayBuckets.map(b => b.key),
        calls: dayBuckets.map(b => b.calls),
        interested: dayBuckets.map(b => b.interested),
      },
    }
  },

  // C5 — key/value rows จากแท็บ "Streaming Config" (เช่น { key: 'rollout_percent', value: '5' }) ใช้โดย
  // rolloutConfig.js เท่านั้น — คืน [] ถ้ายังไม่มีแท็บนี้ ไม่ throw
  async getStreamingConfig() {
    return getStreamingConfigRows()
  },
}

// isRetryableSheetsError/withRetry exported แยกจาก sheetsService ตั้งใจ — ไม่ใช่ API ที่ route อื่นควรเรียกใช้ตรงๆ
// export ไว้แค่ให้เทสยิง withRetry({baseDelayMs: เล็กๆ}) ได้ตรงๆ กันเทสช้าจากรอ backoff จริงหลักวินาที
module.exports = {
  sheetsService,
  isRetryableSheetsError,
  withRetry,
  _resetSchemaWarningsForTest: () => schemaWarnedSheets.clear(),
}
