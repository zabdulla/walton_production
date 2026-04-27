#!/usr/bin/env bash
#
# Uninstall the Walton weekly update launchd job.

set -euo pipefail

DEST="$HOME/Library/LaunchAgents/com.walton.weekly_update.plist"
LABEL="com.walton.weekly_update"

if [ ! -f "$DEST" ]; then
    echo "Nothing to uninstall — plist not found at $DEST"
    exit 0
fi

echo "→ Unloading launchd job..."
launchctl bootout "gui/$(id -u)" "$DEST" 2>/dev/null || true

echo "→ Removing plist..."
rm -f "$DEST"

echo "✓ Uninstalled. Logs in logs/ are preserved."
