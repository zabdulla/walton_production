"""Tests for src/weekly_update.py — webhook notifications."""
from __future__ import annotations

import urllib.request

import weekly_update


def test_webhook_posts_when_env_set(monkeypatch) -> None:
    calls = []
    monkeypatch.setenv("WALTON_WEBHOOK_URL", "https://example.com/hook")
    monkeypatch.setattr(urllib.request, "urlopen",
                        lambda req, timeout=10: calls.append(req))
    weekly_update._send_webhook("Pipeline blocked", "3 duplicate rows", success=False)
    assert len(calls) == 1
    assert calls[0].full_url == "https://example.com/hook"
    assert b"Pipeline blocked" in calls[0].data
    assert b"\\ud83d\\udea8" in calls[0].data or "🚨".encode() in calls[0].data


def test_webhook_noop_without_env(monkeypatch) -> None:
    monkeypatch.delenv("WALTON_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    def explode(*a, **k):
        raise AssertionError("urlopen must not be called without env var")

    monkeypatch.setattr(urllib.request, "urlopen", explode)
    weekly_update._send_webhook("T", "M", success=True)  # must not raise


def test_webhook_failure_never_raises(monkeypatch) -> None:
    monkeypatch.setenv("WALTON_WEBHOOK_URL", "https://example.com/hook")

    def explode(*a, **k):
        raise OSError("network down")

    monkeypatch.setattr(urllib.request, "urlopen", explode)
    weekly_update._send_webhook("T", "M", success=True)  # swallowed, logged
