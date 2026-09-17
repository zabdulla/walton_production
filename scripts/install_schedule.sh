#!/usr/bin/env bash
#
# Install one of the Walton launchd jobs (re-running is safe: the existing job is replaced).
#
#   scripts/install_schedule.sh                 # weekly_update  (Mondays 12:00)
#   scripts/install_schedule.sh cietrade_poll   # cieTrade poller (every 10 minutes)
#   scripts/install_schedule.sh daily_update    # daily dashboard update (07:30)
#   scripts/install_schedule.sh all

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOB="${1:-weekly_update}"

install_one() {
    local name="$1"
    local template="$PROJECT_ROOT/scripts/com.walton.$name.plist"
    local dest="$HOME/Library/LaunchAgents/com.walton.$name.plist"
    local label="com.walton.$name"
    if [ ! -f "$template" ]; then
        echo "ERROR: Template not found: $template" >&2
        exit 1
    fi
    mkdir -p "$HOME/Library/LaunchAgents" "$PROJECT_ROOT/logs"
    sed -e "s|__PROJECT_ROOT__|$PROJECT_ROOT|g" -e "s|__HOME__|$HOME|g" "$template" > "$dest"
    if launchctl list | grep -q "$label"; then
        launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
        sleep 1
    fi
    launchctl bootstrap "gui/$(id -u)" "$dest"
    launchctl enable "gui/$(id -u)/$label"
    echo "✓ $label installed ($dest)"
}

case "$JOB" in
    all) for j in weekly_update cietrade_poll daily_update; do install_one "$j"; done ;;
    weekly_update|cietrade_poll|daily_update) install_one "$JOB" ;;
    *) echo "unknown job: $JOB (weekly_update | cietrade_poll | daily_update | all)" >&2; exit 1 ;;
esac

echo
echo "Manual trigger:   launchctl kickstart -k gui/$(id -u)/com.walton.<job>"
echo "Status:           launchctl print gui/$(id -u)/com.walton.<job> | grep -E 'state|last_exit_status'"
echo "Logs:             tail -f $PROJECT_ROOT/logs/*.log"
echo "Uninstall:        scripts/uninstall_schedule.sh <job>"
