# Google OAuth client (while the pipeline still uses Google)

One Google Cloud OAuth client serves the two Google calls the pipeline still
makes until the Microsoft 365 move completes (setup/MICROSOFT_365_SETUP.md):
the daily email send (`src/daily_digest.py --authorize`, scope `gmail.send`) and
the End of Shift Sheet reader (`src/labor_sheet.py`, scope `spreadsheets.readonly`).
The weekly email fetch this guide was written for (`fetch_emails.py`) is retired:
the workbooks and pay-period PDFs are no longer sent.

---

## Step 1 — Create a Google Cloud project

1. Open [console.cloud.google.com](https://console.cloud.google.com).
2. In the top bar, click the project dropdown → **New Project**.
   - Name: `Walton Production Pipeline` (or whatever you like)
   - Click **Create**.
3. Wait a few seconds, then make sure the new project is selected in the top bar.

## Step 2 — Enable the Gmail API

1. In the left sidebar, navigate to **APIs & Services → Library**.
2. Search for `Gmail API`, click it, then click **Enable**.

## Step 3 — Configure the OAuth consent screen

1. Left sidebar → **APIs & Services → OAuth consent screen**.
2. Choose **External**, click **Create**.
3. Fill required fields (only the starred ones matter):
   - App name: `Walton Production Pipeline`
   - User support email: your email
   - Developer contact email: your email
   - Click **Save and Continue**.
4. **Scopes** screen → click **Save and Continue** (no scopes needed here; we'll request them at runtime).
5. **Test users** screen → click **Add Users** and add your own Gmail address (`zubair@plusmaterials.com`).
   - Click **Save and Continue**, then **Back to Dashboard**.

> *Note:* While the app is in "Testing" mode (the default), the token issued to you stays valid forever. You don't need to publish the app.

## Step 4 — Create OAuth credentials

1. Left sidebar → **APIs & Services → Credentials**.
2. Click **+ Create Credentials → OAuth client ID**.
3. Application type: **Desktop app**.
4. Name: `Walton production pipeline`.
5. Click **Create**.
6. A modal pops up. Click **Download JSON** — this saves a file like `client_secret_<long-id>.apps.googleusercontent.com.json`.

## Step 5 — Place the credentials file

```bash
mkdir -p ~/.config/walton
mv ~/Downloads/client_secret_*.apps.googleusercontent.com.json ~/.config/walton/gmail_credentials.json
chmod 600 ~/.config/walton/gmail_credentials.json
```

## Step 6 — Install Python dependencies

```bash
cd ~/Projects/processing_analysis
pip install -r requirements.txt
```

## Step 7 — Run an OAuth flow once

```bash
python3 src/daily_digest.py --authorize
```

Your browser opens. Click through:
1. Choose your Google account.
2. You'll see a "Google hasn't verified this app" warning — click **Advanced → Go to Walton Production Pipeline (unsafe)**. (This is expected because the app is in Testing mode and only you can use it.)
3. Click **Continue** to grant read-only Gmail access.
4. The browser shows "The authentication flow has completed."

Token is saved to `~/.config/walton/gmail_token.json` (chmod 600). Auto-refreshes from now on.

---

## Usage

```bash
python3 src/daily_digest.py --authorize     # consent once for gmail.send -> ~/.config/walton/gmail_send_token.json
python3 src/labor_sheet.py --dry-run        # consent once for Sheets read -> ~/.config/walton/sheets_token.json
```

Both tokens become repository secrets (`GMAIL_SEND_TOKEN_JSON`, `SHEETS_TOKEN_JSON`)
so the GitHub runner can use them; see setup/CIETRADE_API.md and setup/LABOR_CAPTURE.md.

---

## Troubleshooting

**`FileNotFoundError: OAuth credentials not found`**
You haven't placed the JSON at `~/.config/walton/gmail_credentials.json`. Re-do step 5.

**`Token has been expired or revoked`**
Delete `~/.config/walton/gmail_token.json` and re-run `--auth`.

**`access_denied` during browser flow**
You're not on the test users list. Add your email under OAuth consent screen → Test users.

**`Quota exceeded`**
Gmail API has very generous limits (1 billion quota units/day for free). Unlikely unless something is looping.

---

## Security notes

- `gmail_credentials.json` only identifies *the app*, not you. It's safe to keep on disk.
- `gmail_token.json` is your refresh token — treat it like a password. Stored at chmod 600.
- The scope is `gmail.readonly` only — the script *cannot* send, delete, or modify emails.
- Both files live outside the repo (`~/.config/walton/`) so they can't accidentally be committed.
