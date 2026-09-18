/**
 * Walton End of Shift — web app backend (Google Apps Script).
 *
 * Deploy once:
 *   1. script.google.com -> New project. Add this file as Code.gs and Index.html alongside it.
 *   2. Deploy -> New deployment -> type "Web app" -> Execute as: Me -> Who has access: Anyone.
 *      ("Anyone" means no Google login on the floor; the URL is the only key, so treat it like one.)
 *   3. Open the URL once yourself. The first request creates the spreadsheet
 *      "Walton End of Shift (log)" in your Drive with these tabs:
 *        Entries      one row per machine reported (what the pipeline reads)
 *        Submissions  one row per report (who, when, shift notes)
 *        Operators    names offered as one-tap chips — fill this in
 *        Materials    materials offered as chips — prefilled, edit freely
 *   4. Put the spreadsheet ID in ~/.config/walton/labor_sheet.json as {"spreadsheet_id": "...", "range": "Entries"}.
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
var DEFAULT_MATERIALS = ['BOPP', 'BOPP resin', 'BOPP slabs', 'Mixed plastic', 'SBS', 'Cores', 'EPS fines', 'LDPE', 'PET regrind', 'HIPS', 'Cardboard', 'Toll bags'];
var DOWNTIME_REASONS = ['Blades', 'No material', 'Breakdown', 'Changeover', 'No forklift', 'Waiting on material', 'Other'];
var ENTRY_HEADERS = ['Timestamp', 'Submission ID', 'Date', 'Shift', 'Machine', 'Machine hours operated', 'Total man hours',
                     'Operator(s)', 'Material run', 'Downtime minutes', 'Downtime reason', 'Comments', 'Submitted by'];
var SUBMISSION_HEADERS = ['Timestamp', 'Submission ID', 'Date', 'Shift', 'Submitted by', 'Shift notes', 'Machines reported', 'Replaces earlier'];

function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('End of Shift')
    .addMetaTag('viewport', 'width=device-width, initial-scale=1, maximum-scale=1')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getSpreadsheet_() {
  var props = PropertiesService.getScriptProperties();
  var id = props.getProperty('EOS_SPREADSHEET_ID');
  var ss = null;
  if (id) { try { ss = SpreadsheetApp.openById(id); } catch (e) { ss = null; } }
  if (!ss) {
    ss = SpreadsheetApp.create('Walton End of Shift (log)');
    props.setProperty('EOS_SPREADSHEET_ID', ss.getId());
  }
  ensureSheet_(ss, 'Entries', ENTRY_HEADERS);
  ensureSheet_(ss, 'Submissions', SUBMISSION_HEADERS);
  ensureSheet_(ss, 'Operators', ['Name']);
  var mats = ensureSheet_(ss, 'Materials', ['Material']);
  if (mats.getLastRow() < 2) mats.getRange(2, 1, DEFAULT_MATERIALS.length, 1).setValues(DEFAULT_MATERIALS.map(function (m) { return [m]; }));
  var first = ss.getSheets()[0];
  if (first.getName() === 'Sheet1' && ss.getSheets().length > 1) ss.deleteSheet(first);
  return ss;
}

function ensureSheet_(ss, name, headers) {
  var sh = ss.getSheetByName(name);
  if (!sh) { sh = ss.insertSheet(name); sh.appendRow(headers); sh.setFrozenRows(1); }
  return sh;
}

function columnValues_(sh) {
  var n = sh.getLastRow();
  if (n < 2) return [];
  return sh.getRange(2, 1, n - 1, 1).getValues().map(function (r) { return String(r[0]).trim(); }).filter(String);
}

/** Lists the page needs on load: machine order, operator chips, material chips, downtime reasons. */
function getLists() {
  var ss = getSpreadsheet_();
  var mats = columnValues_(ss.getSheetByName('Materials'));
  return {
    machines: MACHINES.map(function (m) { return m[0]; }),
    operators: columnValues_(ss.getSheetByName('Operators')),
    materials: mats.length ? mats : DEFAULT_MATERIALS,
    downtimeReasons: DOWNTIME_REASONS,
  };
}

/** Has this date + shift already been submitted? Used to warn before a duplicate. */
function getExisting(date, shift) {
  var sh = getSpreadsheet_().getSheetByName('Submissions');
  var n = sh.getLastRow();
  if (n < 2) return null;
  var rows = sh.getRange(2, 1, n - 1, SUBMISSION_HEADERS.length).getValues();
  var hits = rows.filter(function (r) { return normDate_(r[2]) === date && String(r[3]) === shift; });
  if (!hits.length) return null;
  var last = hits[hits.length - 1];
  return {
    at: Utilities.formatDate(new Date(last[0]), Session.getScriptTimeZone(), 'MMM d, h:mm a'),
    by: String(last[4]),
    machines: Number(last[6]) || 0,
  };
}

function normDate_(v) {
  if (v instanceof Date) return Utilities.formatDate(v, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  return String(v).slice(0, 10);
}

function num_(v) {
  if (v === '' || v === null || v === undefined) return '';
  var n = Number(v);
  return isNaN(n) ? '' : n;
}

/** Append one report: N machine rows to Entries plus one row to Submissions. */
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
    var ss = getSpreadsheet_();
    var now = new Date();
    var id = Utilities.formatDate(now, Session.getScriptTimeZone(), 'yyyyMMdd-HHmmss') + '-' + p.shift;
    var prev = getExisting(p.date, p.shift);
    var by = String(p.submittedBy || '').trim();
    var out = rows.map(function (m) {
      return [now, id, p.date, p.shift, String(m.machine), num_(m.machineHours), num_(m.manHours),
              String(m.operators || '').trim(), String(m.material || '').trim(), num_(m.downtimeMinutes),
              String(m.downtimeReason || '').trim(), String(m.comments || '').trim(), by];
    });
    if (out.length) {
      var e = ss.getSheetByName('Entries');
      e.getRange(e.getLastRow() + 1, 1, out.length, ENTRY_HEADERS.length).setValues(out);
    }
    ss.getSheetByName('Submissions').appendRow([now, id, p.date, p.shift, by, notes, out.length, prev ? 'yes' : '']);
    notify_(p, rows, notes, by, now, !!prev, ss.getUrl());
    return { ok: true, id: id, entries: out.length, replaced: !!prev };
  } finally {
    lock.releaseLock();
  }
}

// ---- submission notice: a few lines by email, the moment a report is filed ----
// Recipient: script property NOTIFY_EMAIL, else the account the app runs as.
// Never fatal — a mail problem must not fail the submit.
function notifyRecipient_() {
  return PropertiesService.getScriptProperties().getProperty('NOTIFY_EMAIL') || Session.getEffectiveUser().getEmail();
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

function notify_(p, rows, notes, by, now, replaced, sheetUrl) {
  try {
    var s = summaryLines_(p, rows, notes, by, now, replaced);
    MailApp.sendEmail({
      to: notifyRecipient_(),
      subject: s.subject,
      body: s.head + '\n\n' + s.lines.join('\n') + '\n\n' + sheetUrl,
      name: 'Walton End of Shift'
    });
  } catch (e) {
    console.error('notify failed: ' + e);
  }
}

// Run this once from the editor after pasting a new Code.gs. It calls MailApp
// directly (no try/catch) so a missing mail permission surfaces as the
// editor's "Authorization required" dialog instead of a swallowed error.
function sendTestNotification() {
  var sample = { date: Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd'), shift: '1st', submittedBy: 'Test' };
  var rows = [{ machine: 'Extruder', ran: true, machineHours: 7.5, manHours: 15, operators: 'Tony, Daniel', material: 'BOPP', downtimeMinutes: 30, downtimeReason: 'Blades', comments: '' }];
  var s = summaryLines_(sample, rows, 'Sample notice — the app emails this summary on every submission.', 'Test', new Date(), false);
  MailApp.sendEmail({ to: notifyRecipient_(), subject: s.subject, body: s.head + '\n\n' + s.lines.join('\n') + '\n\n' + getSpreadsheet_().getUrl(), name: 'Walton End of Shift' });
  console.log('Sent to ' + notifyRecipient_());
  return 'Sent to ' + notifyRecipient_();
}
