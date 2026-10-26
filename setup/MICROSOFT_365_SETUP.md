# Microsoft 365 setup — Outlook, Excel in SharePoint, no computer in the loop

The pipeline can run on Google (Gmail API, Google Sheets) or on Microsoft 365
(Outlook and an Excel log in SharePoint, both through Microsoft Graph). Two
switches choose, read at run time, default `google`:

| Switch | Values | Decides |
|---|---|---|
| `MAIL_PROVIDER` | `google` / `microsoft` | the mailbox the daily email is sent from |
| `EOS_SOURCE` | `google` / `microsoft` | where End of Shift rows come from: the web app's Sheet, or the Excel log in SharePoint |

On GitHub they are **repository variables** (Settings > Secrets and variables >
Actions > Variables). On a Mac, environment variables. Nothing else changes:
the same scripts, the same files on disk, the same dashboards and email.

## 1. The Entra app registration (once, needs a Microsoft 365 admin)

Done in the Microsoft Entra admin center (entra.microsoft.com) by someone with
the Application Administrator or Global Administrator role.

1. **Identity > Applications > App registrations > New registration.** Name
   `Walton production pipeline`, single tenant, no redirect URI. Note the
   **Application (client) ID** and the **Directory (tenant) ID**.
2. **Certificates & secrets > New client secret.** Description `GitHub Actions`,
   expiry 24 months (the maximum). Copy the **Value** now; it is shown once.
   Put the expiry date in a calendar: the heartbeat workflow warns 30 days ahead,
   but someone has to make a new one and update the secret.
3. **API permissions > Add a permission > Microsoft Graph > Application
   permissions**, add:
   - `Mail.Send` — send the daily email (nothing is read: production comes from cieTrade and the End of Shift log)
   - `Sites.Selected` — read the one site that holds the End of Shift log
   Then **Grant admin consent**.
4. **Limit mail access to the one mailbox.** Application `Mail.*` permissions
   cover every mailbox in the tenant until an access policy narrows them. In
   Exchange Online PowerShell (`Connect-ExchangeOnline`):

       New-ApplicationAccessPolicy -AppId <client id> -PolicyScopeGroupId <mailbox address> -AccessRight RestrictAccess -Description "Walton pipeline: one mailbox"
       Test-ApplicationAccessPolicy -AppId <client id> -Identity <mailbox address>

   The mailbox is the one the daily email goes out from (`GRAPH_MAILBOX`). A shared mailbox such as production@ keeps it
   off a person's account; your own works too.
5. **Grant the app the End of Shift site** (`Sites.Selected` grants nothing by
   itself). With the site's id (`https://<tenant>.sharepoint.com/sites/<name>/_api/site/id`),
   as an admin in Graph Explorer or PowerShell:

       POST https://graph.microsoft.com/v1.0/sites/{site-id}/permissions
       {"roles": ["read"], "grantedToIdentities": [{"application": {"id": "<client id>", "displayName": "Walton production pipeline"}}]}

   If the log lives in a personal OneDrive instead, `Files.Read.All` replaces
   `Sites.Selected` (broader; a site is the better home).

## 2. Repository secrets and variables

    gh secret set GRAPH_TENANT_ID --body "<tenant id>"
    gh secret set GRAPH_CLIENT_ID --body "<client id>"
    gh secret set GRAPH_CLIENT_SECRET --body "<secret value>"
    gh secret set GRAPH_MAILBOX --body "production@plusmaterials.com"
    gh secret set EOS_WORKBOOK_URL --body "<sharing link of the End of Shift log workbook>"

The sharing link is SharePoint's **Copy link** on the workbook (any link the app's
site permission covers; the pipeline reads it through Graph's `/shares` endpoint).
Alternatively `EOS_WORKBOOK_DRIVE_ID` plus `EOS_WORKBOOK_PATH`.

`DIGEST_TO` and `DIGEST_BCC` stay as they are. Then flip the variables when
ready, one at a time:

    gh variable set MAIL_PROVIDER --body microsoft
    gh variable set EOS_SOURCE --body microsoft

Flipping back is the same command with `google`; the Google secrets stay in
place until the cleanup phase.

## 3. The End of Shift log workbook

An Excel workbook, `Walton End of Shift (log).xlsx`, with two sheets that carry
the same headings the Google Sheet had (the reader matches on headings, so
column order is free):

- **Entries**: Timestamp, Submission ID, Date, Shift, Machine, Machine hours
  operated, Total man hours, Operator(s), Material run, Downtime minutes,
  Downtime reason, Comments, Submitted by
- **Submissions**: Timestamp, Submission ID, Date, Shift, Submitted by, Shift
  notes, Machines reported, Replaces earlier

The floor's form page posts each report to a Power Automate flow ("When an HTTP
request is received") that adds the rows to those two tables and sends the
notification email; see setup/LABOR_CAPTURE.md for the flow and the page.

## 4. Checking it from a Mac (optional)

Delegated sign-in, no client secret: set `GRAPH_CLIENT_ID` and `GRAPH_TENANT_ID`
(the same app, with the matching *delegated* permissions added and
"Allow public client flows" on), then

    python3 src/graph_client.py          # device-code sign-in once; token cached in ~/.config/walton/
    EOS_SOURCE=microsoft python3 src/labor_sheet.py --dry-run
    MAIL_PROVIDER=microsoft python3 src/daily_digest.py --send --to you@plusmaterials.com

## 5. What runs where afterwards

Everything runs in GitHub Actions and Microsoft 365: the ten-minute poll and
site deploy, the 7 AM email, the Monday rebuild (`weekly-cloud.yml`), the End
of Shift flow. No Mac is needed;
the launchd jobs in `scripts/` can be uninstalled (`scripts/uninstall_schedule.sh`).
The payroll dashboard is the exception while the employee roster stays a
local-only file.
