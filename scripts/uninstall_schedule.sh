#!/usr/bin/env bash
#
# Uninstall a Walton launchd job: scripts/uninstall_schedule.sh [weekly_update|cietrade_poll|daily_update|all]

set -euo pipefail
JOB="${1:-weekly_update}"

remove_one() {
    local dest="$HOME/Library/LaunchAgents/com.walton.$1.plist"
    if [ ! -f "$dest" ]; then
        echo "Nothing to uninstall for $1 — $dest not found"
        return
    fi
    launchctl bootout "gui/$(id -u)" "$dest" 2>/dev/null || true
    rm -f "$dest"
    echo "✓ com.walton.$1 uninstalled (logs kept)"
}

case "$JOB" in
    all) for j in weekly_update cietrade_poll daily_update; do remove_one "$j"; done ;;
    weekly_update|cietrade_poll|daily_update) remove_one "$JOB" ;;
    *) echo "unknown job: $JOB" >&2; exit 1 ;;
esac
