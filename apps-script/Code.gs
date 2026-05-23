const BACKEND_URL = 'https://your-app.onrender.com' // เปลี่ยนหลัง deploy

function doGet(e) {
  return HtmlService.createHtmlOutputFromFile('index')
    .setTitle('AI Call Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
}

function startCampaign(campaignId) {
  const res = UrlFetchApp.fetch(`${BACKEND_URL}/api/campaign/start`, {
    method: 'POST',
    contentType: 'application/json',
    payload: JSON.stringify({ campaignId }),
    muteHttpExceptions: true,
  })
  return JSON.parse(res.getContentText())
}

function stopCampaign(campaignId) {
  const res = UrlFetchApp.fetch(`${BACKEND_URL}/api/campaign/stop`, {
    method: 'POST',
    contentType: 'application/json',
    payload: JSON.stringify({ campaignId }),
    muteHttpExceptions: true,
  })
  return JSON.parse(res.getContentText())
}

function getActiveCalls() {
  const res = UrlFetchApp.fetch(`${BACKEND_URL}/api/calls/active`, { muteHttpExceptions: true })
  return JSON.parse(res.getContentText())
}

function getCallLogs(campaignId) {
  const url = campaignId
    ? `${BACKEND_URL}/api/calls?campaignId=${campaignId}&limit=100`
    : `${BACKEND_URL}/api/calls?limit=100`
  const res = UrlFetchApp.fetch(url, { muteHttpExceptions: true })
  return JSON.parse(res.getContentText())
}

function getStats() {
  const res = UrlFetchApp.fetch(`${BACKEND_URL}/api/stats`, { muteHttpExceptions: true })
  return JSON.parse(res.getContentText())
}

function getCampaigns() {
  const ss = SpreadsheetApp.openById(SpreadsheetApp.getActiveSpreadsheet().getId())
  const sheet = ss.getSheetByName('Campaigns')
  if (!sheet) return []
  const data = sheet.getDataRange().getValues()
  const headers = data[0]
  return data.slice(1).map(row => {
    const obj = {}
    headers.forEach((h, i) => obj[h] = row[i])
    return obj
  })
}
