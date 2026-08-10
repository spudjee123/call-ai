const { google } = require('googleapis')

const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_ID

// Sheet names
const SHEETS = {
  CONTACTS: 'Contacts',
  CAMPAIGNS: 'Campaigns',
  RESULTS: 'Call Results',
  TEMPLATES: 'SMS Templates',
  BLOCKLIST: 'Blocklist'
}

let sheets = null

async function getClient() {
  if (sheets) return sheets
  const authOptions = { scopes: ['https://www.googleapis.com/auth/spreadsheets'] }
  if (process.env.GOOGLE_CREDENTIALS_JSON) {
    authOptions.credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON)
  } else {
    authOptions.keyFile = '/etc/secrets/google-credentials.json'
  }
  const auth = new google.auth.GoogleAuth(authOptions)
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

// หา row ด้วย key column แล้วอัปเดตหลาย field พร้อมกันในครั้งเดียว (เขียนทับทั้งแถว)
async function updateRowByKey(sheetName, keyField, keyValue, updates) {
  const { headers, rows } = await getSheetData(sheetName)
  const keyIdx = headers.indexOf(keyField)
  if (keyIdx === -1) throw new Error(`Column '${keyField}' not found in ${sheetName}`)

  const rowIdx = rows.findIndex(r => r[keyIdx] === keyValue)
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
    return rows.find(r => r.status === 'active' && r.type === 'inbound') || rows[0] || {}
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

  async saveCallResult(result) {
    await appendRowByFields(SHEETS.RESULTS, { ...result, timestamp: new Date().toISOString() })
  },

  async getSmsTemplate(outcome) {
    const rows = await getRows(SHEETS.TEMPLATES)
    const tmpl = rows.find(r => r.outcome === outcome)
    return tmpl ? tmpl.template_text : null
  },

  async getCallResults({ limit = 50, campaignId } = {}) {
    const rows = await getRows(SHEETS.RESULTS)
    let filtered = campaignId ? rows.filter(r => r.campaign_id === campaignId) : rows
    return filtered.slice(-limit).reverse()
  },

  async getStats({ days = 7 } = {}) {
    const rows = await getRows(SHEETS.RESULTS)
    const total = rows.length
    const outcomes = {}
    const byCampaign = {}
    const today = new Date().toISOString().slice(0, 10)
    let durationSum = 0
    let durationCount = 0
    let callsToday = 0

    const dayBuckets = []
    for (let i = days - 1; i >= 0; i--) {
      const key = new Date(Date.now() - i * 86400000).toISOString().slice(0, 10)
      dayBuckets.push({ key, calls: 0, interested: 0 })
    }
    const bucketMap = new Map(dayBuckets.map(b => [b.key, b]))

    rows.forEach(r => {
      outcomes[r.outcome] = (outcomes[r.outcome] || 0) + 1
      if (r.campaign_id) byCampaign[r.campaign_id] = (byCampaign[r.campaign_id] || 0) + 1
      const duration = Number(r.duration)
      if (!Number.isNaN(duration) && duration > 0) { durationSum += duration; durationCount++ }
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
      total, outcomes, byCampaign, avgDuration, callsToday, conversionRate,
      dailyTrend: {
        labels: dayBuckets.map(b => b.key),
        calls: dayBuckets.map(b => b.calls),
        interested: dayBuckets.map(b => b.interested),
      },
    }
  }
}

module.exports = { sheetsService }
