const { google } = require('googleapis')

const SPREADSHEET_ID = process.env.GOOGLE_SHEETS_ID

// Sheet names
const SHEETS = {
  CONTACTS: 'Contacts',
  CAMPAIGNS: 'Campaigns',
  RESULTS: 'Call Results',
  TEMPLATES: 'SMS Templates'
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

  async addContact(fields) {
    await appendRowByFields(SHEETS.CONTACTS, { status: 'pending', ...fields })
  },

  async updateContact(phone, updates) {
    return updateRowByKey(SHEETS.CONTACTS, 'phone', phone, updates)
  },

  async updateContactStatus(phone, status) {
    return updateRowByKey(SHEETS.CONTACTS, 'phone', phone, { status })
  },

  async saveCallResult(result) {
    await appendRow(SHEETS.RESULTS, [
      result.call_id,
      result.phone,
      result.name,
      result.campaign_id,
      result.outcome,
      result.summary,
      result.key_points,
      result.duration,
      result.transcript,
      new Date().toISOString()
    ])
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

  async getStats() {
    const rows = await getRows(SHEETS.RESULTS)
    const total = rows.length
    const outcomes = {}
    rows.forEach(r => {
      outcomes[r.outcome] = (outcomes[r.outcome] || 0) + 1
    })
    return { total, outcomes }
  }
}

module.exports = { sheetsService }
