from __future__ import annotations

import argparse
import json
import uuid
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import yaml

# Permitir ejecución tanto como módulo (-m) como script directo.
try:  # relative (cuando se importa como pipelines.ingest.main)
    from .esios_client import EsiosClient
    from .hooks import compute_mix_pct, validate_pvpc_complete_day
    from .normalize import (normalize_interconn_pairs, normalize_long_tech,
                            normalize_prices, normalize_wide_by_indicator,
                            parse_values_to_df)
    from .utils import (dedupe, now_utc, resolve_window,
                        write_parquet_partitioned, write_raw)
except (
    ImportError
):  # fallback absoluto para ejecución directa: python pipelines/ingest/main.py
    # Insertamos raíz del repo en sys.path si no está para que 'pipelines' sea resoluble
    import os
    import sys

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    from pipelines.ingest.esios_client import EsiosClient  # type: ignore
    from pipelines.ingest.hooks import compute_mix_pct  # type: ignore
    from pipelines.ingest.hooks import validate_pvpc_complete_day
    from pipelines.ingest.normalize import (  # type: ignore
        normalize_interconn_pairs, normalize_long_tech, normalize_prices,
        normalize_wide_by_indicator, parse_values_to_df)
    from pipelines.ingest.utils import now_utc  # type: ignore
    from pipelines.ingest.utils import (dedupe, resolve_window,
                                        write_parquet_partitioned, write_raw)

HOOKS = {
    "compute_mix_pct": compute_mix_pct,
    "validate_pvpc_complete_day": validate_pvpc_complete_day,
}


def load_cfg(path="config/ingest.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_dataset(
    client: EsiosClient,
    base_url: str,
    indicator_ids,
    start_iso,
    end_iso,
    cfg_defaults,
    time_trunc: str | None = "hour",
):
    dfs_by_id = {}
    for ind in indicator_ids:
        last_err = None
        for _ in range(int(cfg_defaults.get("retries", 3))):
            try:
                payload = client.get_indicator(
                    ind, start_iso, end_iso, base_url, time_trunc=time_trunc
                )
                df = parse_values_to_df(payload)
                if not df.empty:
                    df["indicator_id"] = ind
                dfs_by_id[ind] = df
                break
            except Exception as e:
                last_err = e
                import time

                time.sleep(int(cfg_defaults.get("backoff_seconds", 2)))
        if ind not in dfs_by_id:
            raise last_err or RuntimeError(f"Fallo indicador {ind}")
    return dfs_by_id


def normalize_dataset(kind: str, dfs_by_id, ds_cfg):
    ncfg = ds_cfg.get("normalize", {})
    if kind == "prices":
        frames = []
        for ind, df in dfs_by_id.items():
            source = ncfg.get("column_map", {}).get("source", f"ID_{ind}")
            frames.append(normalize_prices(df, ncfg.get("column_map", {}), source))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if kind == "wide_by_indicator":
        return normalize_wide_by_indicator(
            dfs_by_id, ncfg.get("column_map", {}), ncfg.get("id_rename")
        )
    if kind == "long_tech":
        return normalize_long_tech(
            dfs_by_id, ncfg.get("tech_map", {}), ncfg.get("column_map", {})
        )
    if kind == "interconn_pairs":
        return normalize_interconn_pairs(
            dfs_by_id, ncfg.get("to_pairs", {}), ncfg.get("column_map", {})
        )
    raise ValueError(f"kind no soportado: {kind}")


def apply_post(ds_cfg, df):
    post = ds_cfg.get("normalize", {}).get("post_hook")
    if not post:
        return df
    fn = HOOKS.get(post)
    if not fn:
        raise ValueError(f"post_hook '{post}' no registrado")
    return fn(df)


def apply_validators(ds_cfg, df):
    validators = ds_cfg.get("normalize", {}).get("validators", [])
    for v in validators:
        fn = HOOKS.get(v)
        if not fn:
            raise ValueError(f"validator '{v}' no registrado")
        df = fn(df)
    return df


def _log(level: str, run_id: str, **fields):
    rec = {"ts": datetime.utcnow().isoformat() + "Z", "level": level, "run_id": run_id}
    rec.update(fields)
    print(json.dumps(rec))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", help="Nombre del dataset en config/ingest.yaml")
    parser.add_argument("--target-date", help="YYYY-MM-DD (solo para *_dstsafe)")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Usa modo local: escribe en disco según paths_local",
    )
    parser.add_argument(
        "--backfill-day",
        help="YYYY-MM-DD para forzar ingesta del día completo especificado (modo today_dstsafe)",
    )
    parser.add_argument(
        "--backfill-range",
        help="Rango START:END (YYYY-MM-DD:YYYY-MM-DD) para backfill por días completos (inclusive)",
    )
    args = parser.parse_args()

    cfg = load_cfg()
    ds = cfg["datasets"].get(args.dataset)
    if not ds or not ds.get("enabled", True):
        available = ", ".join(
            sorted(
                [
                    k
                    for k, v in cfg.get("datasets", {}).items()
                    if v.get("enabled", True)
                ]
            )
        )
        raise SystemExit(
            f"Dataset '{args.dataset}' no existe o está deshabilitado. Disponibles: {available}"
        )

    run_id = str(uuid.uuid4())
    t_global_start = datetime.now(timezone.utc)
    _log(
        "info",
        run_id,
        action="run_start",
        dataset=args.dataset,
        mode=(
            "range"
            if args.backfill_range
            else ("backfill_day" if args.backfill_day else "normal")
        ),
    )

    # Modo backfill por rango: iterar días completos
    if args.backfill_range:
        try:
            start_s, end_s = [s.strip() for s in args.backfill_range.split(":", 1)]
            start_d = date.fromisoformat(start_s)
            end_d = date.fromisoformat(end_s)
        except Exception:
            raise SystemExit(
                "--backfill-range debe tener formato START:END con YYYY-MM-DD:YYYY-MM-DD"
            )

        if end_d < start_d:
            raise SystemExit("--backfill-range END debe ser >= START")

        # Crear cliente y preparar plantillas de salida según modo
        client = EsiosClient(
            rate_limit_per_sec=cfg["defaults"].get("rate_limit_per_sec", 1),
            timeout_seconds=cfg["defaults"].get("timeout_seconds", 30),
        )
        local_root = cfg.get("paths_local", {}).get("root", "./data")
        raw_tpl = cfg.get("paths_local", {}).get("raw") if args.local else None
        curated_tpl = cfg.get("paths_local", {}).get("curated") if args.local else None
        if args.local and (not raw_tpl or not curated_tpl):
            raise SystemExit(
                "paths_local.raw/curated no definido en config/ingest.yaml"
            )
        if args.local:
            assert isinstance(raw_tpl, str) and isinstance(curated_tpl, str)

        agg_days = 0
        agg_raw_rows = 0
        agg_cur_rows = 0
        current = start_d
        while current <= end_d:
            t_start = datetime.now(timezone.utc)
            start_dt, end_dt, target_day = resolve_window(
                {"type": "today_dstsafe"}, target_date=current
            )
            start_iso = start_dt.isoformat().replace("+00:00", "Z")
            end_iso = end_dt.isoformat().replace("+00:00", "Z")

            # time_trunc: minuto para demand/gen/interconn, hora para precios
            time_trunc = "minute" if ds.get("granularity") == "minute" else "hour"
            dfs_by_id = fetch_dataset(
                client,
                cfg["defaults"]["base_url"],
                ds["indicator_ids"],
                start_iso,
                end_iso,
                cfg["defaults"],
                time_trunc=time_trunc,
            )
            raw_df = (
                pd.concat([v for v in dfs_by_id.values()], ignore_index=True)
                if dfs_by_id
                else pd.DataFrame()
            )
            keep_cols = cfg.get("defaults", {}).get("raw_keep_columns")
            if keep_cols and not raw_df.empty:
                cols = [c for c in keep_cols if c in raw_df.columns]
                if cols:
                    raw_df = raw_df[cols]
            if args.local:
                assert isinstance(raw_tpl, str)
                raw_path = write_raw(
                    raw_df,
                    raw_tpl,
                    dataset=args.dataset,
                    run_ts=now_utc(),
                    bucket_root={"root": local_root},
                    io_mode="local",
                )
            else:
                raw_path = write_raw(
                    raw_df,
                    cfg["paths"]["raw"],
                    dataset=args.dataset,
                    run_ts=now_utc(),
                    bucket_root={"bucket": cfg["paths"]["bucket"]},
                )
            raw_rows = len(raw_df)
            _log(
                "info",
                run_id,
                action="raw_written",
                dataset=args.dataset,
                date=str(target_day),
                rows=raw_rows,
                path=raw_path,
            )

            kind = ds.get("normalize", {}).get("kind")
            curated_df = normalize_dataset(kind, dfs_by_id, ds)
            curated_df = apply_post(ds, curated_df)
            # Filtra solo el día objetivo (local) para evitar arrastres
            if "hour_ts" in curated_df.columns:
                curated_df = curated_df[
                    (curated_df["hour_ts"].dt.tz_localize(None).dt.date == target_day)
                    | (curated_df["hour_ts"].dt.date == target_day)
                ]
            elif "minute_ts" in curated_df.columns:
                curated_df = curated_df[
                    (curated_df["minute_ts"].dt.tz_localize(None).dt.date == target_day)
                    | (curated_df["minute_ts"].dt.date == target_day)
                ]
            if args.dataset in ["prices_pvpc", "prices_pvpc_tomorrow", "prices_spot"]:
                if (
                    "hour_ts" not in curated_df.columns
                    and "datetime" in curated_df.columns
                ):
                    curated_df["hour_ts"] = pd.to_datetime(
                        curated_df["datetime"], utc=True, errors="coerce"
                    ).dt.floor("h")
                if (
                    "zone" not in curated_df.columns
                    and "geo_name" in curated_df.columns
                ):
                    curated_df["zone"] = curated_df["geo_name"]
                for col in curated_df.columns:
                    if col not in [
                        "hour_ts",
                        "price_eur_mwh",
                        "zone",
                        "source",
                        "indicator_id",
                    ]:
                        if col.upper() in ["PVPC", "SPOT_ES"]:
                            curated_df.drop(columns=[col], inplace=True)
                if "indicator_id" not in curated_df.columns:
                    curated_df["indicator_id"] = (
                        ds["indicator_ids"][0] if "indicator_ids" in ds else "unknown"
                    )
            curated_df = dedupe(curated_df, ds.get("dedupe_key", []))
            curated_df = apply_validators(ds, curated_df)

            if args.local:
                assert isinstance(curated_tpl, str)
                curated_path = write_parquet_partitioned(
                    curated_df,
                    curated_tpl,
                    ds["curated_table"],
                    target_day,
                    {"root": local_root},
                    io_mode="local",
                )
            else:
                curated_path = write_parquet_partitioned(
                    curated_df,
                    cfg["paths"]["curated"],
                    ds["curated_table"],
                    target_day,
                    {"bucket": cfg["paths"]["bucket"]},
                )
            cur_rows = len(curated_df)
            _log(
                "info",
                run_id,
                action="curated_written",
                dataset=args.dataset,
                date=str(target_day),
                rows=cur_rows,
                path=curated_path,
            )
            agg_days += 1
            agg_raw_rows += raw_rows
            agg_cur_rows += cur_rows
            _log(
                "info",
                run_id,
                action="day_summary",
                dataset=args.dataset,
                date=str(target_day),
                raw_rows=raw_rows,
                curated_rows=cur_rows,
                seconds=round(
                    (datetime.now(timezone.utc) - t_start).total_seconds(), 2
                ),
            )

            current = current + timedelta(days=1)
        _log(
            "info",
            run_id,
            action="backfill_range_summary",
            dataset=args.dataset,
            days=agg_days,
            raw_rows=agg_raw_rows,
            curated_rows=agg_cur_rows,
            duration_seconds=round(
                (datetime.now(timezone.utc) - t_global_start).total_seconds(), 2
            ),
        )
        raise SystemExit(0)

    # Ventana normal (o --backfill-day)
    backfill_td = date.fromisoformat(args.backfill_day) if args.backfill_day else None
    if backfill_td is not None:
        start_dt, end_dt, target_day = resolve_window(
            {"type": "today_dstsafe"}, target_date=backfill_td
        )
    else:
        td = date.fromisoformat(args.target_date) if args.target_date else None
        start_dt, end_dt, target_day = resolve_window(
            ds.get("window_strategy", {"type": "last_hours", "hours": 6}),
            target_date=td,
        )
    start_iso = start_dt.isoformat().replace("+00:00", "Z")
    end_iso = end_dt.isoformat().replace("+00:00", "Z")

    client = EsiosClient(
        rate_limit_per_sec=cfg["defaults"].get("rate_limit_per_sec", 1),
        timeout_seconds=cfg["defaults"].get("timeout_seconds", 30),
    )

    time_trunc = "minute" if ds.get("granularity") == "minute" else "hour"
    dfs_by_id = fetch_dataset(
        client,
        cfg["defaults"]["base_url"],
        ds["indicator_ids"],
        start_iso,
        end_iso,
        cfg["defaults"],
        time_trunc=time_trunc,
    )

    raw_df = (
        pd.concat([v for v in dfs_by_id.values()], ignore_index=True)
        if dfs_by_id
        else pd.DataFrame()
    )
    # Aplica trimming de columnas RAW si está configurado (para evitar duplicados de timestamps)
    keep_cols = cfg.get("defaults", {}).get("raw_keep_columns")
    if keep_cols and not raw_df.empty:
        cols = [c for c in keep_cols if c in raw_df.columns]
        if cols:
            raw_df = raw_df[cols]
    local_root = cfg.get("paths_local", {}).get("root", "./data")
    raw_tpl = cfg.get("paths_local", {}).get("raw") if args.local else None
    curated_tpl = cfg.get("paths_local", {}).get("curated") if args.local else None
    if args.local:
        if not raw_tpl or not curated_tpl:
            raise SystemExit(
                "paths_local.raw/curated no definido en config/ingest.yaml"
            )
        assert isinstance(raw_tpl, str) and isinstance(curated_tpl, str)
        raw_path = write_raw(
            raw_df,
            raw_tpl,
            dataset=args.dataset,
            run_ts=now_utc(),
            bucket_root={"root": local_root},
            io_mode="local",
        )
    else:
        raw_path = write_raw(
            raw_df,
            cfg["paths"]["raw"],
            dataset=args.dataset,
            run_ts=now_utc(),
            bucket_root={"bucket": cfg["paths"]["bucket"]},
        )
    raw_rows = len(raw_df)
    _log(
        "info",
        run_id,
        action="raw_written",
        dataset=args.dataset,
        rows=raw_rows,
        path=raw_path,
    )

    kind = ds.get("normalize", {}).get("kind")
    curated_df = normalize_dataset(kind, dfs_by_id, ds)
    curated_df = apply_post(ds, curated_df)
    # Filtrado por día objetivo también para granularidad minuto
    if "hour_ts" in curated_df.columns:
        curated_df = curated_df[
            (curated_df["hour_ts"].dt.tz_localize(None).dt.date == target_day)
            | (curated_df["hour_ts"].dt.date == target_day)
        ]
    elif "minute_ts" in curated_df.columns:
        curated_df = curated_df[
            (curated_df["minute_ts"].dt.tz_localize(None).dt.date == target_day)
            | (curated_df["minute_ts"].dt.date == target_day)
        ]
    # Asegura columnas para deduplicado y partición en precios
    if args.dataset in ["prices_pvpc", "prices_pvpc_tomorrow", "prices_spot"]:
        if "hour_ts" not in curated_df.columns and "datetime" in curated_df.columns:
            curated_df["hour_ts"] = (
                pd.to_datetime(curated_df["datetime"], utc=True, errors="coerce")
                .dt.tz_convert("Europe/Madrid")
                .dt.floor("h")
            )
        if "zone" not in curated_df.columns and "geo_name" in curated_df.columns:
            curated_df["zone"] = curated_df["geo_name"]
        # Elimina cualquier columna con nombre de indicador y asegura solo 'source'
        for col in curated_df.columns:
            if col not in [
                "hour_ts",
                "price_eur_mwh",
                "zone",
                "source",
                "indicator_id",
            ]:
                if col.upper() in ["PVPC", "SPOT_ES"]:
                    curated_df.drop(columns=[col], inplace=True)
        # Asegura que indicator_id esté presente
        if "indicator_id" not in curated_df.columns:
            curated_df["indicator_id"] = (
                ds["indicator_ids"][0] if "indicator_ids" in ds else "unknown"
            )
    curated_df = dedupe(curated_df, ds.get("dedupe_key", []))
    curated_df = apply_validators(ds, curated_df)

    if args.local:
        assert isinstance(curated_tpl, str)
        curated_path = write_parquet_partitioned(
            curated_df,
            curated_tpl,
            ds["curated_table"],
            target_day,
            {"root": local_root},
            io_mode="local",
        )
    else:
        curated_path = write_parquet_partitioned(
            curated_df,
            cfg["paths"]["curated"],
            ds["curated_table"],
            target_day,
            {"bucket": cfg["paths"]["bucket"]},
        )
    cur_rows = len(curated_df)
    _log(
        "info",
        run_id,
        action="curated_written",
        dataset=args.dataset,
        rows=cur_rows,
        path=curated_path,
    )
    _log(
        "info",
        run_id,
        action="run_summary",
        dataset=args.dataset,
        raw_rows=raw_rows,
        curated_rows=cur_rows,
        duration_seconds=round(
            (datetime.now(timezone.utc) - t_global_start).total_seconds(), 2
        ),
    )
