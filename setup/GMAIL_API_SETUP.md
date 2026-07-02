# Gmail API Setup for `fetch_emails.py`

This is a one-time setup. After completing it, `src/fetch_emails.py` can run unattended (cron, launchd, scheduled task) without needing Chrome or any UI.

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
4. Name: `Walton fetch_emails CLI`.
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

## Step 7 — Run the OAuth flow once

```bash
python3 src/fetch_emails.py --auth
```

Your browser opens. Click through:
1. Choose your Google account.
2. You'll see a "Google hasn't verified this app" warning — click **Advanced → Go to Walton Production Pipeline (unsafe)**. (This is expected because the app is in Testing mode and only you can use it.)
3. Click **Continue** to grant read-only Gmail access.
4. The browser shows "The authentication flow has completed."

Token is saved to `~/.config/walton/gmail_token.json` (chmod 600). Auto-refreshes from now on.

---

## Daily usage

```bash
# Fetch latest weekly processing-weights xlsx files (last 14 days)
python3 src/fetch_emails.py --processing-weights

# Fetch latest pay-period PDFs (last 90 days)
python3 src/fetch_emails.py --payroll

# Both
python3 src/fetch_emails.py --all

# Dry-run — show what would download without writing anything
python3 src/fetch_emails.py --all --list
```

The script is idempotent: files that already exist on disk are skipped. Safe to run multiple times.

---

## Replace the Chrome-based Monday cron

Old workflow (Chrome-MCP click marathon):
1. Open Chrome
2. Navigate to Gmail
3. Click each email
4. Click "Download all" or hover for download icon
5. Move zip to project, extract, rename, etc.

New workflow:
```bash
python3 src/fetch_emails.py --all
python3 src/aggregate_daily_data.py
python3 src/parse_payroll_pdf.py --pdf-dir data/payroll_pdfs
python3 src/build_interactive_dashboard.py
python3 src/build_daily_dashboard.py
python3 src/build_payroll_dashboard.py
python3 src/build_profit_dashboard.py
python3 src/build_operator_dashboard.py
git add data/ docs/ && git commit -m "Weekly auto-update" && git push
```

This is what the Monday morning cron should run instead of opening Chrome.

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
