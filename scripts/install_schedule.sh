#!/usr/bin/env bash
#
# Install the Walton weekly update launchd job.
# Schedules src/weekly_update.py to run every Monday at 12:00 PM local time.
#
# Re-running this script is safe: it removes any existing job first.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TEMPLATE="$PROJECT_ROOT/scripts/com.walton.weekly_update.plist"
DEST="$HOME/Library/LaunchAgents/com.walton.weekly_update.plist"
LABEL="com.walton.weekly_update"

if [ ! -f "$TEMPLATE" ]; then
    echo "ERROR: Template not found: $TEMPLATE" >&2
    exit 1
fi

echo "→ Generating launchd plist with paths substituted..."
mkdir -p "$HOME/Library/LaunchAgents"
mkdir -p "$PROJECT_ROOT/logs"

# Substitute placeholders → real paths
sed -e "s|__PROJECT_ROOT__|$PROJECT_ROOT|g" \
    -e "s|__HOME__|$HOME|g" \
    "$TEMPLATE" > "$DEST"

echo "  written → $DEST"

# Unload existing job (if any) before reloading
if launchctl list | grep -q "$LABEL"; then
    echo "→ Unloading existing job..."
    launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
    # Give launchd a moment to fully unregister the previous instance,
    # otherwise bootstrap can fail with "Input/output error".
    sleep 1
fi

echo "→ Loading job into launchd..."
launchctl bootstrap "gui/$(id -u)" "$DEST"
launchctl enable "gui/$(id -u)/$LABEL"

echo
echo "✓ Installed. Schedule: every Monday 12:00 PM local time."
echo
echo "Manual trigger (test it now):"
echo "  launchctl kickstart -k gui/$(id -u)/$LABEL"
echo
echo "Check status:"
echo "  launchctl print gui/$(id -u)/$LABEL | grep -E 'state|last_exit_status'"
echo
echo "View logs:"
echo "  tail -f $PROJECT_ROOT/logs/weekly_stdout.log"
echo "  tail -f $PROJECT_ROOT/logs/weekly_update.log"
echo
echo "Uninstall:"
echo "  scripts/uninstall_schedule.sh"
