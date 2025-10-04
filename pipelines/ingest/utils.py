from __future__ import annotations

import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

TZ_MADRID = ZoneInfo("Europe/Madrid")


def now_utc():
    return datetime.now(timezone.utc)


def resolve_window(strategy: dict, target_date: date | None = None):
    td = target_date or now_utc().astimezone(TZ_MADRID).date()
    if strategy["type"] == "last_hours":
        hours = int(strategy.get("hours", 6))
        end = now_utc()
        start = end - timedelta(hours=hours)
        return start, end, td
    if strategy["type"] == "minutes":
        minutes = int(strategy.get("minutes", 60))
        end = now_utc()
        start = end - timedelta(minutes=minutes)
        return start, end, td
    if strategy["type"] == "next_day_dstsafe":
        tomorrow = td + timedelta(days=1)
        start = datetime.combine(tomorrow, time(0, 0), tzinfo=timezone.utc) - timedelta(
            hours=3
        )
        end = datetime.combine(
            tomorrow + timedelta(days=1), time(0, 0), tzinfo=timezone.utc
        ) + timedelta(hours=1)
        return start, end, tomorrow
    if strategy["type"] == "today_dstsafe":
        start = datetime.combine(td, time(0, 0), tzinfo=timezone.utc) - timedelta(
            hours=3
        )
        end = datetime.combine(
            td + timedelta(days=1), time(0, 0), tzinfo=timezone.utc
        ) + timedelta(hours=1)
        return start, end, td
    if strategy["type"] == "last_complete_hour_local":
        # Última hora completa en horario de Madrid (CET/CEST), devuelta en UTC
        now_local = now_utc().astimezone(TZ_MADRID)
        end_local = now_local.replace(minute=0, second=0, microsecond=0)
        start_local = end_local - timedelta(hours=1)
        start = start_local.astimezone(timezone.utc)
        end = end_local.astimezone(timezone.utc)
        return start, end, start_local.date()
    raise ValueError(f"Estrategia no soportada: {strategy}")


def _ensure_local_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def write_raw(
    df: pd.DataFrame,
    path_tpl: str,
    dataset: str,
    run_ts: datetime,
    bucket_root: dict,
    io_mode: str = "gcs",
) -> str:
    if df is None:
        return ""
    year = run_ts.astimezone(TZ_MADRID).year
    month = f"{run_ts.astimezone(TZ_MADRID).month:02d}"
    day = f"{run_ts.astimezone(TZ_MADRID).day:02d}"
    iso_run = run_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    iso_run_safe = iso_run.replace(":", "-") if io_mode == "local" else iso_run
    path = path_tpl.format(
        dataset=dataset,
        year=year,
        month=month,
        day=day,
        iso_run=iso_run_safe,
        **bucket_root,
    )
    if io_mode == "local":
        _ensure_local_dir(path)
        df.to_csv(path, index=False, encoding="utf-8")
        return path
    print(
        "GOOGLE_APPLICATION_CREDENTIALS:",
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    )
    print("GCLOUD_PROJECT:", os.environ.get("GCLOUD_PROJECT"))
    print("DATA_BUCKET:", os.environ.get("DATA_BUCKET"))
    # Escribimos usando fsspec directamente sobre la ruta gs://
    df.to_csv(path, index=False, encoding="utf-8")
    return path


def write_parquet_partitioned(
    df: pd.DataFrame,
    path_tpl: str,
    table: str,
    target_date: date,
    bucket_root: dict,
    io_mode: str = "gcs",
) -> str:
    if df is None or df.empty:
        return ""
    year = target_date.year
    month = f"{target_date.month:02d}"
    day = f"{target_date.day:02d}"
    p = path_tpl.format(
        table=table,
        year=year,
        month=month,
        day=day,
        uuid=str(uuid.uuid4()),
        **bucket_root,
    )
    if io_mode == "local":
        _ensure_local_dir(p)
        df.to_parquet(p, index=False)
        return p
    # Escribimos usando fsspec directamente sobre la ruta gs://
    df.to_parquet(p, index=False)
    return p


def dedupe(df: pd.DataFrame, key: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    return df.sort_values(by=key).drop_duplicates(subset=key, keep="last")
