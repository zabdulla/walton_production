# cieTrade pilot page

Interim daily production page built from cieTrade converting jobs. The rules live in
`src/cietrade_model.py` (shared with the dashboard pipeline, see `setup/CIETRADE_API.md`);
this folder only renders the page.

    python3 explorations/cietrade_pilot/build.py    # writes out/*.html from data/cietrade_exports + data/cietrade
    python3 explorations/cietrade_pilot/ingest.py   # (rarely needed now) archive a manual export from ~/Downloads

The poller (`src/cietrade_poll.py --rebuild`, every 10 minutes) rebuilds `out/` and copies the
standalone page to `config.LIVE_PAGE_COPY`, a OneDrive folder, so the live view opens on a
phone. `out/walton_daily_pilot.html` is the fragment published as the artifact
"Walton Daily Production Pilot"; `out/walton_daily_pilot_standalone.html` opens directly.
