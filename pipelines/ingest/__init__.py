"""Ingest subpackage exposing public API.

Typical usage:
    from pipelines.ingest import run_ingest

But normally you call CLI:
    python pipelines/ingest/main.py <dataset>

Exports:
    load_cfg, resolve_window, EsiosClient, normalize helpers (limited)
"""

from .esios_client import EsiosClient
from .main import load_cfg  # re-export
from .utils import now_utc, resolve_window

__all__ = [
    "load_cfg",
    "resolve_window",
    "now_utc",
    "EsiosClient",
]
