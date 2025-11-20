# tests/conftest.py
"""
Shared pytest configuration and fixtures.

- Ensures the project root is on sys.path so `import src...` works.
- Provides fixtures for raw data, validated data, transformed data, and a DB engine.
"""

import os
import sys
import pathlib

import pytest

# --- Make src importable: add project root to sys.path ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def project_root() -> pathlib.Path:
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def config_path(project_root) -> str:
    """Return the default config path used by the pipeline."""
    return str(project_root / "src" / "config" / "config.yaml")
