/**
 * Walton End of Shift — web app backend (Google Apps Script), Microsoft 365 edition.
 *
 * The page (Index.html) is unchanged. On submit this script writes the rows into the
 * End of Shift log workbook in SharePoint through Microsoft Graph, using the Entra app
 * the pipeline already has, and sends the notice from the Outlook mailbox. Google holds
 * nothing but this script: no Sheet, no Gmail. Free on any Google account.
 *
 * Deploy once (setup/LABOR_CAPTURE.md has the click-by-click version):
 *   1. script.google.com -> New project -> paste this file as Code.gs and Index.html alongside it.
 *   2. Project Settings -> Script Properties, add:
 *        GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET   the Entra app (needs Files.ReadWrite.All + Mail.Send)
 *        GRAPH_MAILBOX              the mailbox the notice is sent from, e.g. zubair@plusmaterials.com
 *        EOS_WORKBOOK_DRIVE_ID, EOS_WORKBOOK_ITEM_ID   the log workbook, as in the GitHub secrets
 *        NOTIFY_EMAIL               who gets the notice, comma-separated (default: GRAPH_MAILBOX)
 *      and set the time zone to America/New_York.
 *   3. Run selfTest from the editor once: it fetches a token, reads the log and sends a test notice.
 *   4. Deploy -> New deployment -> Web app -> Execute as: Me -> Who has access: Anyone. Copy the URL.
 *
 * A re-submission for the same date + shift is logged as a new row set and flagged
 * "replaces"; the pipeline keeps the latest one.
 */

var MACHINES = [
  ['Auto tie', 'AUTO TIE BALER'],
  ['Baler 1', 'BALER 1'],
  ['Baler 2', 'BALER 2'],
  ['Big densifier', 'AVANGUARD DENSIFIER (OLD)'],
  ['New densifier', 'GREEN MAX DENSIFIER (NEW)'],
  ['Extruder', 'EXTRUDER'],
  ['Guillotine', 'GUILLOTINE'],
  ['Shredder', 'SHREDDER'],
  ['Shredder/Grinder', 'GRINDER'],
];
var OPERATORS = ['Tony', 'Daniel', 'Steven', 'Vince', 'Clarence', 'Kemonica', 'Shynequa', 'Montez', 'Tevin', 'Andrew', 'Brandon', 'Darius', 'Connor', 'James'];
var MATERIALS = ['BOPP', 'BOPP resin', 'BOPP slabs', 'Mixed plastic', 'SBS', 'Cores', 'EPS fines', 'LDPE', 'PET regrind', 'HIPS', 'Cardboard', 'Toll bags'];
var DOWNTIME_REASONS = ['Blades', 'No material', 'Breakdown', 'Changeover', 'No forklift', 'Waiting on material', 'Other'];
var ENTRY_HEADERS = ['Timestamp', 'Submission ID', 'Date', 'Shift', 'Machine', 'Machine hours operated', 'Total man hours',
                     'Operator(s)', 'Material run', 'Downtime minutes', 'Downtime reason', 'Comments', 'Submitted by'];
var SUBMISSION_HEADERS = ['Timestamp', 'Submission ID', 'Date', 'Shift', 'Submitted by', 'Shift notes', 'Machines reported', 'Replaces earlier'];
var GRAPH = 'https://graph.microsoft.com/v1.0';

// ---- the page ----
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('End of Shift')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1, maximum-scale=1')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

/** Lists the page needs on load. Edit the arrays above; redeploy is not needed for a script-only change
 *  only when the deployment is set to the HEAD version, otherwise Deploy -> Manage deployments -> New version. */
function getLists() {
  var props = PropertiesService.getScriptProperties();
  var ops = (props.getProperty('OPERATORS') || '').split(',').map(function (s) { return s.trim(); }).filter(String);
  return { machines: MACHINES.map(function (m) { return m[0]; }), operators: ops.length ? ops : OPERATORS,
           materials: MATERIALS, downtimeReasons: DOWNTIME_REASONS };
}

// ---- settings ----
function prop_(name, optional) {
  var v = PropertiesService.getScriptProperties().getProperty(name);
  if (!v && !optional) throw new Error('Script property ' + name + ' is not set (Project Settings -> Script Properties).');
  return v || '';
}

// ---- Microsoft Graph, app-only ----
function token_() {
  var cache = CacheService.getScriptCache(), hit = cache.get('graph_token');
  if (hit) return hit;
  var r = UrlFetchApp.fetch('https://login.microsoftonline.com/' + prop_('GRAPH_TENANT_ID') + '/oauth2/v2.0/token', {
    method: 'post', muteHttpExceptions: true,
    payload: { client_id: prop_('GRAPH_CLIENT_ID'), client_secret: prop_('GRAPH_CLIENT_SECRET'),
               scope: 'https://graph.microsoft.com/.default', grant_type: 'client_credentials' } });
  var body = JSON.parse(r.getContentText());
  if (r.getResponseCode() !== 200 || !body.access_token) throw new Error('Microsoft sign-in failed: ' + (body.error_description || r.getContentText()));
  cache.put('graph_token', body.access_token, Math.min(3000, (body.expires_in || 3600) - 300));
  return body.access_token;
}

function graph_(method, path, payload) {
  var opts = { method: method, muteHttpExceptions: true, headers: { Authorization: 'Bearer ' + token_() } };
  if (payload !== undefined) { opts.contentType = 'application/json'; opts.payload = JSON.stringify(payload); }
  var r = UrlFetchApp.fetch(GRAPH + path, opts), code = r.getResponseCode(), text = r.getContentText();
  if (code >= 400) {
    var msg = text; try { msg = JSON.parse(text).error.message; } catch (e) {}
    throw new Error('Microsoft ' + code + ' on ' + path.split('?')[0] + ': ' + msg);
  }
  return text ? JSON.parse(text) : {};
}

function workbook_() {
  return '/drives/' + prop_('EOS_WORKBOOK_DRIVE_ID') + '/items/' + prop_('EOS_WORKBOOK_ITEM_ID') + '/workbook';
}

function addRows_(table, rows) {
  if (!rows.length) return;
  graph_('post', workbook_() + '/tables/' + table + '/rows', { values: rows });
}

function tableValues_(table) {
  var rows = graph_('get', workbook_() + '/tables/' + table + '/rows?$select=values').value || [];
  return rows.map(function (r) { return r.values[0]; });
}

function sendMail_(to, subject, text) {
  graph_('post', '/users/' + prop_('GRAPH_MAILBOX') + '/sendMail', {
    message: { subject: subject, body: { contentType: 'Text', content: text },
               toRecipients: to.map(function (a) { return { emailAddress: { address: a } }; }) },
    saveToSentItems: true });
}

// ---- the report ----
function num_(v) {
  if (v === '' || v === null || v === undefined) return '';
  var n = Number(v);
  return isNaN(n) ? '' : n;
}

/** Excel keeps the dates the script writes as date cells, and Graph returns those as serial
 *  numbers (days since 1899-12-30, fractions for the time of day, in the wall clock they were
 *  written in). This turns one back into a Date whose UTC fields are that wall clock. */
function serialToDate_(v) {
  return new Date(Math.round((Number(v) - 25569) * 86400000));
}

function isSerial_(v) { return typeof v === 'number' && isFinite(v) && v > 20000; }

/** 'yyyy-MM-dd' from whatever the cell holds: a serial, a Date, or the text as written. */
function normDate_(v) {
  if (isSerial_(v)) return Utilities.formatDate(serialToDate_(v), 'UTC', 'yyyy-MM-dd');
  if (v instanceof Date) return Utilities.formatDate(v, 'UTC', 'yyyy-MM-dd');
  return String(v || '').slice(0, 10);
}

/** Has this date + shift already been submitted? Used to warn before a duplicate. */
function getExisting(date, shift) {
  var hits = tableValues_('Submissions').filter(function (r) { return normDate_(r[2]) === date && String(r[3]) === shift; });
  if (!hits.length) return null;
  var last = hits[hits.length - 1];
  return { at: fmtTime_(last[0]), by: String(last[4]), machines: Number(last[6]) || 0 };
}

function fmtTime_(stamp) {
  if (isSerial_(stamp)) return Utilities.formatDate(serialToDate_(stamp), 'UTC', 'MMM d, h:mm a');
  var d = new Date(String(stamp).replace(' ', 'T'));
  return isNaN(d.getTime()) ? String(stamp) : Utilities.formatDate(d, Session.getScriptTimeZone(), 'MMM d, h:mm a');
}

/** Append one report: N machine rows to Entries plus one row to Submissions, then the notice. */
function submitReport(p) {
  if (!p || !/^\d{4}-\d{2}-\d{2}$/.test(String(p.date || '')) || ['1st', '2nd', '3rd'].indexOf(p.shift) < 0) {
    throw new Error('Date and shift are required.');
  }
  var rows = (p.machines || []).filter(function (m) { return m && m.ran; });
  var notes = String(p.shiftNotes || '').trim();
  if (!rows.length && !notes) throw new Error('Nothing to submit — mark at least one machine as run, or add a shift note.');
  var bad = rows.filter(function (m) { return num_(m.machineHours) === '' && num_(m.manHours) === '' && !String(m.operators || '').trim(); });
  if (bad.length) throw new Error('Fill in hours or operators for: ' + bad.map(function (m) { return m.machine; }).join(', '));

  var lock = LockService.getScriptLock();
  lock.waitLock(10000);
  try {
    var now = new Date(), tz = Session.getScriptTimeZone();
    var stamp = Utilities.formatDate(now, tz, 'yyyy-MM-dd HH:mm:ss');
    var id = Utilities.formatDate(now, tz, 'yyyyMMdd-HHmmss') + '-' + p.shift;
    var prev = null;
    try { prev = getExisting(p.date, p.shift); } catch (e) { prev = null; }   // a read problem must not block the write
    var by = String(p.submittedBy || '').trim();
    var out = rows.map(function (m) {
      return [stamp, id, p.date, p.shift, String(m.machine), num_(m.machineHours), num_(m.manHours),
              String(m.operators || '').trim(), String(m.material || '').trim(), num_(m.downtimeMinutes),
              String(m.downtimeReason || '').trim(), String(m.comments || '').trim(), by];
    });
    addRows_('Entries', out);
    addRows_('Submissions', [[stamp, id, p.date, p.shift, by, notes, out.length, prev ? 'yes' : '']]);
    notify_(p, rows, notes, by, now, !!prev);
    return { ok: true, id: id, entries: out.length, replaced: !!prev };
  } finally {
    lock.releaseLock();
  }
}

/** Any HTTP client can file a report here too: a text/plain body holding the JSON that
 *  submitReport takes ({date, shift, submittedBy, shiftNotes, machines: [{machine, ran, ...}]}). */
function doPost(e) {
  var out = {};
  try {
    var p = JSON.parse((e && e.postData && e.postData.contents) || '{}');
    out = submitReport(p);
  } catch (err) {
    out = { ok: false, error: String(err && err.message ? err.message : err) };
  }
  return ContentService.createTextOutput(JSON.stringify(out)).setMimeType(ContentService.MimeType.JSON);
}

// ---- submission notice: a few lines by email, the moment a report is filed ----
function notifyRecipients_() {
  var raw = prop_('NOTIFY_EMAIL', true) || prop_('GRAPH_MAILBOX');
  return raw.split(',').map(function (s) { return s.trim(); }).filter(String);
}

function summaryLines_(p, rows, notes, by, now, replaced) {
  var tz = Session.getScriptTimeZone();
  var day = Utilities.formatDate(new Date(p.date + 'T12:00:00'), tz, 'EEE MMM d');
  var head = day + ', ' + p.shift + ' shift — filed' + (by ? ' by ' + by : '') + ' at ' + Utilities.formatDate(now, tz, 'h:mm a')
           + (replaced ? ' (replaces an earlier report)' : '');
  var lines = rows.map(function (m) {
    var bits = [];
    if (num_(m.machineHours) !== '') bits.push(num_(m.machineHours) + ' h');
    if (num_(m.manHours) !== '') bits.push(num_(m.manHours) + ' man-h');
    if (String(m.operators || '').trim()) bits.push(String(m.operators).trim());
    if (String(m.material || '').trim()) bits.push(String(m.material).trim());
    var dt = num_(m.downtimeMinutes);
    if (dt !== '' && dt > 0) bits.push('down ' + dt + ' min' + (String(m.downtimeReason || '').trim() ? ' (' + String(m.downtimeReason).trim() + ')' : ''));
    if (String(m.comments || '').trim()) bits.push('"' + String(m.comments).trim() + '"');
    return m.machine + ': ' + bits.join(' · ');
  });
  if (notes) lines.push('Shift notes: ' + notes);
  return { subject: 'End of Shift · ' + day + ' · ' + p.shift + (by ? ' · ' + by : '') + (replaced ? ' · revised' : ''),
           head: head, lines: lines };
}

function notify_(p, rows, notes, by, now, replaced) {
  try {
    var s = summaryLines_(p, rows, notes, by, now, replaced);
    sendMail_(notifyRecipients_(), s.subject, s.head + '\n\n' + s.lines.join('\n') + '\n\nDashboard: https://zabdulla.github.io/walton_production/');
  } catch (e) {
    console.error('notify failed: ' + e);   // never fatal: the rows are already in the log
  }
}

/** Run this once from the editor after setting the properties: token, read, write nothing, one test notice. */
function selfTest() {
  var subs = tableValues_('Submissions');
  var entries = tableValues_('Entries');
  var msg = 'Log reachable: ' + entries.length + ' entries, ' + subs.length + ' submissions. Notice recipients: ' + notifyRecipients_().join(', ');
  if (subs.length) {                                   // shows the date handling on a real row
    var last = subs[subs.length - 1];
    msg += '. Last submission: ' + normDate_(last[2]) + ' ' + last[3] + ' filed ' + fmtTime_(last[0]) + ' by ' + last[4];
  }
  sendMail_(notifyRecipients_(), 'End of Shift · test from the web app', 'The End of Shift web app can reach the log workbook and send from ' + prop_('GRAPH_MAILBOX') + '.\n\n' + msg);
  console.log(msg + ' — test notice sent.');
  return msg;
}
