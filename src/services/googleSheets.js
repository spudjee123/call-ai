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
  SENDER_NAMES: 'Sender Names'
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

async function getClient() {
  if (sheets) return sheets
  const auth = new google.auth.GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    ...getGoogleClientOptions(),
  })
  sheets = google.sheets({ version: 'v4', auth })
  return sheets
}

// ดึงข้อมูลดิบ (headers ที่ normalize แล้ว + rows แบบ array) เก็บ row index ไว้ใช้ update ทีหลัง
async function getSheetData(sheetName) {
  const client = await getClient()
  const res = await client.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
  })
  const values = res.data.values || []
  if (!values.length) return { headers: [], rows: [] }
  const headers = values[0].map(h => h.toLowerCase().trim().replace(/\s+/g, '_'))
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
    await client.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range: `${sheetName}!A${rowIdx + 2}:${lastCol}${rowIdx + 2}`,
      valueInputOption: 'RAW',
      requestBody: { values: [row] },
    })
    return true
  })
}

// Sheets API ต้องใช้ sheetId ตัวเลข (ไม่ใช่ชื่อชีต) สำหรับ batchUpdate อย่าง deleteDimension
// ไม่ cache ตั้งใจ — deleteRowByKey ใช้ไม่บ่อย (unblock เป็น action ที่กดนานๆ ครั้ง) การ cache ไว้เสี่ยงได้ sheetId
// เก่าถ้ามีคนลบ/สร้างแท็บซ้ำชื่อเดิมระหว่าง process ยังรันอยู่ ซึ่งจะไปลบผิดแถวผิดชีตแบบเงียบๆ ไม่คุ้มกับที่ประหยัดได้
async function getSheetId(sheetName) {
  const client = await getClient()
  const meta = await client.spreadsheets.get({ spreadsheetId: SPREADSHEET_ID })
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
    await client.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{
          deleteDimension: {
            range: { sheetId, dimension: 'ROWS', startIndex: rowIdx + 1, endIndex: rowIdx + 2 }, // +1 เพราะแถวแรกคือ header
          },
        }],
      },
    })
    return true
  })
}

async function appendRow(sheetName, values) {
  const client = await getClient()
  await client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
    valueInputOption: 'RAW',
    requestBody: { values: [values] },
  })
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
  await client.spreadsheets.values.append({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
    valueInputOption: 'RAW',
    requestBody: { values },
  })
}

// Blocklist อาจยังไม่ถูกสร้างเป็นชีตแยก — กันพังถ้ายังไม่มี ให้ถือว่าลิสต์ว่าง
async function getBlocklistRows() {
  try {
    return await getRows(SHEETS.BLOCKLIST)
  } catch (err) {
    return []
  }
}

const sheetsService = {
  async getCampaign(campaignId) {
    const rows = await getRows(SHEETS.CAMPAIGNS)
    return rows.find(r => r.id === campaignId) || null
  },

  async getDefaultInboundCampaign() {
    const rows = await getRows(SHEETS.CAMPAIGNS)
    // เดิม fallback ไป rows[0] แบบไม่กรอง type — ถ้า campaign แรกสุดในชีตเป็น outbound
    // สายเข้าจะได้ script/prompt ของ outbound ไปใช้แบบผิดๆ เงียบๆ ตอนนี้ fallback แค่ในกลุ่ม inbound เท่านั้น
    // ไม่มี inbound campaign เลยจริงๆ → คืน null ให้ webhook.js ตัดสินใจปฏิเสธสายแทนที่จะเดาเอา campaign อื่นมาใช้
    return rows.find(r => r.status === 'active' && r.type === 'inbound')
      || rows.find(r => r.type === 'inbound')
      || null
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

  // เทมเพลตเป็น library แยกอิสระ ไม่ผูกกับ outcome โดยตรง — campaign แต่ละอันเลือกเองว่า outcome ไหนจะใช้เทมเพลตไหน (เก็บ template id ไว้ที่ campaign.sms_<outcome>)
  async getSmsTemplateById(id) {
    const rows = await getRows(SHEETS.TEMPLATES)
    return rows.find(r => r.id === id) || null
  },

  async getSmsTemplates() {
    return getRows(SHEETS.TEMPLATES)
  },

  async addSmsTemplate({ id, name, template_text, sender }) {
    await appendRowByFields(SHEETS.TEMPLATES, { id, name, template_text, sender: sender || '' })
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
    let durationSum = 0
    let durationCount = 0
    let callsToday = 0

    const bucketMap = new Map(dayBuckets.map(b => [b.key, b]))

    filteredRows.forEach(r => {
      outcomes[r.outcome] = (outcomes[r.outcome] || 0) + 1
      if (r.campaign_id) {
        byCampaign[r.campaign_id] = (byCampaign[r.campaign_id] || 0) + 1
        if (r.outcome === 'interested') interestedByCampaign[r.campaign_id] = (interestedByCampaign[r.campaign_id] || 0) + 1
      }
      const duration = Number(r.duration)
      if (!Number.isNaN(duration) && duration > 0) { durationSum += duration; durationCount++ }
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

    return {
      total, outcomes, byCampaign, interestedByCampaign, avgDuration, callsToday, conversionRate,
      dailyTrend: {
        labels: dayBuckets.map(b => b.key),
        calls: dayBuckets.map(b => b.calls),
        interested: dayBuckets.map(b => b.interested),
      },
    }
  }
}

module.exports = { sheetsService }
