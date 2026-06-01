"""
Pytest configuration: make ``src/`` importable as top-level modules.

The codebase predates a proper package layout — modules in ``src/`` are
imported as siblings (``import config``, ``from atomic import ...``). For
tests to follow the same conventions, we prepend ``src/`` to ``sys.path``.

If/when we convert ``src/`` into a real package (``src/walton/__init__.py``),
this can be replaced with editable-install + proper imports.
"""
from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC = PROJECT_ROOT / "src"
FIXTURES = Path(__file__).resolve().parent / "fixtures"

# Prepend so tests can do `from atomic import write_atomic_text` etc.
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


# ---------------------------------------------------------------------------
# Common fixtures
# ---------------------------------------------------------------------------

import pytest


@pytest.fixture
def fixtures_dir() -> Path:
    """Path to the directory holding test data files."""
    FIXTURES.mkdir(parents=True, exist_ok=True)
    return FIXTURES


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """A throwaway data directory rooted in pytest's tmp_path."""
    d = tmp_path / "data"
    d.mkdir()
    return d
