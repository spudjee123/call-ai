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

async function getRows(sheetName) {
  const client = await getClient()
  const res = await client.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: sheetName,
  })
  const rows = res.data.values || []
  if (!rows.length) return []
  const headers = rows[0].map(h => h.toLowerCase().trim().replace(/\s+/g, '_'))
  return rows.slice(1).map(row => {
    const obj = {}
    headers.forEach((h, i) => { obj[h] = row[i] || '' })
    return obj
  })
}

async function updateCell(sheetName, rowIndex, colIndex, value) {
  const client = await getClient()
  const col = String.fromCharCode(65 + colIndex)
  await client.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!${col}${rowIndex + 2}`,
    valueInputOption: 'RAW',
    requestBody: { values: [[value]] },
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

const sheetsService = {
  async getCampaign(campaignId) {
    const rows = await getRows(SHEETS.CAMPAIGNS)
    return rows.find(r => r.id === campaignId) || null
  },

  async getDefaultInboundCampaign() {
    const rows = await getRows(SHEETS.CAMPAIGNS)
    return rows.find(r => r.status === 'active' && r.type === 'inbound') || rows[0] || {}
  },

  async getPendingContacts(campaignId) {
    const rows = await getRows(SHEETS.CONTACTS)
    return rows.filter(r => r.campaign === campaignId && r.status === 'pending')
  },

  async updateContactStatus(phone, status) {
    const rows = await getRows(SHEETS.CONTACTS)
    const idx = rows.findIndex(r => r.phone === phone)
    if (idx >= 0) {
      const headers = Object.keys(rows[0])
      const statusCol = headers.indexOf('status')
      await updateCell(SHEETS.CONTACTS, idx, statusCol, status)
    }
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
