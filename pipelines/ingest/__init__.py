"""Ingest subpackage exposing public API.

Typical usage:
    from pipelines.ingest import run_ingest

But normally you call CLI:
    python pipelines/ingest/main.py <dataset>

Exports:
    load_cfg, resolve_window, EsiosClient, normalize helpers (limited)
"""
from .main import load_cfg  # re-export
from .utils import resolve_window, now_utc
from .esios_client import EsiosClient

__all__ = [
    "load_cfg",
    "resolve_window",
    "now_utc",
    "EsiosClient",
]
