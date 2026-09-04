/**
 * Creates the "Walton End of Shift" Google Form and its response sheet.
 *
 * One-time setup, no API enablement needed:
 *   1. Open https://script.google.com -> New project.
 *   2. Paste this file over the default Code.gs and click Run (function: createForm).
 *   3. Approve the permissions prompt (it only touches the form and sheet it creates).
 *   4. Open View -> Logs: it prints the form link to share with supervisors and the
 *      spreadsheet ID for ~/.config/walton/labor_sheet.json.
 *
 * Design: ONE SUBMISSION PER MACHINE PER SHIFT. Only 3-4 machines run in a shift, so a
 * supervisor taps "Submit another response" a few times and is done in about two
 * minutes. This keeps every question short enough for a phone and avoids a 40-field
 * grid that is mostly blank. The machine list mirrors the paper sheet.
 */
function createForm() {
  var form = FormApp.create('Walton End of Shift');
  form.setDescription(
    'One submission per machine that ran this shift. ' +
    'After you submit, tap "Submit another response" for the next machine.')
    .setProgressBar(false)
    .setShowLinkToRespondAgain(true)
    .setConfirmationMessage('Saved. Tap "Submit another response" for the next machine.')
    .setAllowResponseEdits(true);

  form.addDateItem().setTitle('Date').setHelpText('The production day for this shift').setRequired(true);

  form.addMultipleChoiceItem().setTitle('Shift')
    .setChoiceValues(['1st', '2nd', '3rd']).setRequired(true);

  // Same order as the paper sheet; labels are mapped to canonical machine names by the pipeline.
  form.addListItem().setTitle('Machine').setChoiceValues([
    'Auto tie baler', 'Baler 1', 'Baler 2', 'Big densifier (Avanguard)',
    'New densifier (Green Max)', 'Extruder', 'Guillotine', 'Shredder',
    'Shredder/Grinder', 'Small grinder'
  ]).setRequired(true);

  var hours = form.addTextItem().setTitle('Machine hours operated')
    .setHelpText('Hours the machine actually ran, e.g. 7.25').setRequired(true);
  hours.setValidation(FormApp.createTextValidation()
    .setHelpText('Enter a number between 0 and 24').requireNumberBetween(0, 24).build());

  var man = form.addTextItem().setTitle('Total man hours')
    .setHelpText('All operators on this machine added together (2 people x 7 h = 14)').setRequired(true);
  man.setValidation(FormApp.createTextValidation()
    .setHelpText('Enter a number between 0 and 120').requireNumberBetween(0, 120).build());

  form.addTextItem().setTitle('Operator(s)')
    .setHelpText('First names, comma separated: Montez, Tevin').setRequired(true);

  form.addTextItem().setTitle('Material run')
    .setHelpText('What went through the machine: BOPP, Mixed Plastic, Foil bags ...');

  form.addParagraphTextItem().setTitle('Comments')
    .setHelpText('Anything about this machine: blade change, downtime, came in late');

  form.addParagraphTextItem().setTitle('Shift notes (anything else)')
    .setHelpText('People on other duties, events not tied to one machine. Fill once per shift.');

  var ss = SpreadsheetApp.create('Walton End of Shift (responses)');
  form.setDestination(FormApp.DestinationType.SPREADSHEET, ss.getId());

  Logger.log('FORM LINK for supervisors: ' + form.getPublishedUrl());
  Logger.log('Edit the form at:          ' + form.getEditUrl());
  Logger.log('Response spreadsheet ID:   ' + ss.getId());
  Logger.log('Response spreadsheet URL:  ' + ss.getUrl());
  Logger.log('Put in ~/.config/walton/labor_sheet.json: {"spreadsheet_id": "' + ss.getId() + '", "range": "Form Responses 1"}');
}
