"""App-wide configuration helpers."""
import json
import os
from datetime import datetime
from pathlib import Path

_MANIFEST = Path("seed/manifest.json")


def is_offline() -> bool:
    """True when running in offline/cloud mode — skip all live HTTP fetches."""
    env = os.environ.get("OFFLINE_MODE", "")
    if env.lower() in ("1", "true", "yes"):
        return True
    if env.lower() in ("0", "false", "no"):
        return False
    try:
        import streamlit as st
        return bool(st.secrets.get("app", {}).get("offline_mode", False))
    except Exception:
        return False


def seed_refreshed_at() -> datetime | None:
    """Return when seed data was last refreshed, or None."""
    try:
        data = json.loads(_MANIFEST.read_text())
        return datetime.fromisoformat(data["refreshed_at"])
    except Exception:
        return None
