from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
import uuid
from typing import Iterable
from zoneinfo import ZoneInfo

import yaml

try:
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None  # type: ignore

try:
    import pyarrow as pa
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover
    pa = None  # type: ignore
    pq = None  # type: ignore
    ds = None  # type: ignore

TZ_MADRID = ZoneInfo("Europe/Madrid")

DEFAULT_SORT_CANDIDATES = ["minute_ts", "hour_ts", "datetime"]


def load_cfg(path: str = "config/ingest.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_current_year_month(year: int, month: int) -> bool:
    now = datetime.now(TZ_MADRID)
    return now.year == year and now.month == month


def list_month_paths(
    root: str, table: str, local: bool = False
) -> list[tuple[int, int, str]]:
    """Return list of (year, month, normalized_month_path) under a curated table root.

    We reconstruct paths instead of trusting fs.listdir return values because some
    filesystems (gcsfs) return entries without the scheme (e.g. 'bucket/curated/...')
    which later breaks pyarrow.dataset. By rebuilding, we preserve 'gs://' when present.
    """
    import fsspec

    fs = fsspec.get_fs_token_paths(root)[0]
    results: list[tuple[int, int, str]] = []

    # Ensure root has no trailing slash
    root_clean = root.rstrip("/")

    try:
        year_entries = fs.listdir(root_clean)  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(f"No se pudo listar root {root_clean}: {e}")

    for year_entry in year_entries:
        y_name = year_entry.get("name") if isinstance(year_entry, dict) else year_entry
        if not isinstance(y_name, str):
            continue
        # Extraer year=YYYY del final del path
        m = re.search(r"year=(\d{4})$", y_name)
        if not m:
            continue
        year_int = int(m.group(1))
        # Reconstruir ruta de year (no usar y_name directamente si perdió esquema)
        year_path = f"{root_clean}/year={year_int:04d}"
        try:
            month_entries = fs.listdir(year_path)  # type: ignore
        except Exception:
            continue
        for month_entry in month_entries:
            m_name = (
                month_entry.get("name")
                if isinstance(month_entry, dict)
                else month_entry
            )
            if not isinstance(m_name, str):
                continue
            mm = re.search(r"month=(\d{2})$", m_name)
            if not mm:
                continue
            month_int = int(mm.group(1))
            month_path = f"{year_path}/month={month_int:02d}"
            results.append((year_int, month_int, month_path))
    return sorted(results)


def detect_closed_months(
    month_paths: list[tuple[int, int, str]], include_current: bool = False
) -> list[tuple[int, int, str]]:
    closed = []
    for y, m, p in month_paths:
        if is_current_year_month(y, m) and not include_current:
            continue
        closed.append((y, m, p))
    return sorted(closed)


def pick_sort_keys(schema_names: Iterable[str]) -> list[str]:
    for c in DEFAULT_SORT_CANDIDATES:
        if c in schema_names:
            return [c]
    # fallback: if both hour_ts and minute_ts absent, just return empty to skip sort
    return []


def pick_dedupe_keys(schema_names: Iterable[str]) -> list[str]:
    names = set(schema_names)
    # Minute-level tables first
    if "minute_ts" in names:
        if {"minute_ts", "zone", "tech"}.issubset(names):  # gen_mix
            return ["minute_ts", "zone", "tech"]
        if {"minute_ts", "country"}.issubset(names):  # interconn
            return ["minute_ts", "country"]
        if {"minute_ts", "zone"}.issubset(names):  # demand
            return ["minute_ts", "zone"]
        return ["minute_ts"]
    # Hour-level prices
    if "hour_ts" in names:
        if {"hour_ts", "zone", "source"}.issubset(names):
            return ["hour_ts", "zone", "source"]
        if {"hour_ts", "zone"}.issubset(names):
            return ["hour_ts", "zone"]
        return ["hour_ts"]
    # Fallback to datetime if present
    if "datetime" in names:
        return ["datetime"]
    return []


def read_month_dataset(month_path: str):
    """Return (table, file_row_count, file_count) excluding existing compact.parquet.

    file_row_count: suma de filas de cada micro-file antes de dedupe.
    """
    if ds is None:
        raise SystemExit("pyarrow.dataset no disponible. Instala pyarrow.")
    dataset_all = ds.dataset(month_path, format="parquet")
    file_row_count = 0
    micro_files = 0
    tables = []
    # Enumerar ficheros concretos (fragmentos) para contar filas individuales
    for frag in dataset_all.get_fragments():  # type: ignore
        path = getattr(frag, "path", "")
        if path.endswith("compact.parquet"):
            continue
        try:
            # Leer solo columnas mínimas para contar (schema completo necesario para sort posterior)
            t = frag.to_table()  # type: ignore
            file_row_count += t.num_rows
            micro_files += 1
            tables.append(t)
        except Exception as e:  # pragma: no cover
            print(
                json.dumps(
                    {
                        "level": "error",
                        "msg": "Error leyendo fragmento",
                        "path": path,
                        "error": str(e),
                    }
                )
            )
    if not tables:
        if pa is None:
            raise SystemExit("pyarrow no disponible para tabla vacía")
        empty = pa.table({})
        return empty, 0, 0  # vacío
    if pa is None:
        raise SystemExit("pyarrow no disponible para concatenar")
    # pyarrow >=14 depreca 'promote'; el comportamiento por defecto ya realiza la promoción necesaria
    try:  # pyarrow 13/14 compat
        table = pa.concat_tables(tables)
    except TypeError:
        # fallback por si alguna versión antigua exige promote=True
        table = pa.concat_tables(tables, promote=True)  # type: ignore[arg-type]
    return table, file_row_count, micro_files


def write_compacted(
    table,
    month_path: str,
    compression: str = "zstd",
    row_group_size: int = 50_000,
    force: bool = False,
):
    if pq is None:
        raise SystemExit("pyarrow.parquet no disponible. Instala pyarrow.")
    out_path = month_path.rstrip("/") + "/compact.parquet"
    import fsspec

    fs = fsspec.get_fs_token_paths(out_path)[0]
    if fs.exists(out_path) and not force:  # type: ignore
        raise SystemExit(f"Ya existe {out_path}. Usa --force para sobrescribir.")
    pq.write_table(
        table, out_path, compression=compression, row_group_size=row_group_size
    )
    return out_path


def delete_micro_files(month_path: str):
    """Delete micro parquet files inside day=DD folders and remove now-empty day folders.

    Returns a dict with counts for logging.
    """
    import fsspec

    fs = fsspec.get_fs_token_paths(month_path)[0]
    micro_files_deleted = 0
    day_dirs_removed = 0
    # First pass: delete parquet files inside day=DD folders (and any stray parquet at month root except compact)
    for info in fs.listdir(month_path):  # type: ignore
        name = info.get("name") if isinstance(info, dict) else info
        if not isinstance(name, str):
            continue
        if name.endswith("compact.parquet"):
            continue
        if name.endswith(".parquet"):
            fs.rm(name)  # type: ignore
            micro_files_deleted += 1
            continue
        # Potential day= directory
        if "day=" in name:
            # Delete parquet files inside
            inner_list = fs.listdir(name)  # type: ignore
            for inner in inner_list:
                in_name = inner.get("name") if isinstance(inner, dict) else inner
                if not isinstance(in_name, str):
                    continue
                if in_name.endswith("compact.parquet"):
                    continue
                if in_name.endswith(".parquet"):
                    fs.rm(in_name)  # type: ignore
                    micro_files_deleted += 1
            # After deletions, if directory is empty, remove it
            # Some FS (e.g. GCS) may show directory placeholders; try-catch removal
            try:
                if not fs.listdir(name):  # type: ignore
                    fs.rm(name, recursive=True)  # type: ignore
                    day_dirs_removed += 1
            except Exception:  # pragma: no cover - best effort
                pass
    return {
        "micro_files_deleted": micro_files_deleted,
        "day_dirs_removed": day_dirs_removed,
    }


def log(level: str, **fields):
    rec = {"ts": datetime.utcnow().isoformat() + "Z", "level": level}
    rec.update(fields)
    print(json.dumps(rec))


def main():  # noqa: C901
    parser = argparse.ArgumentParser(
        description="Compaction de particiones mensuales curated"
    )
    parser.add_argument(
        "tables",
        nargs="*",
        help="Tablas curated a compactar (p.ej. prices demand gen_mix). Vacío = todas.",
    )
    parser.add_argument(
        "--dataset",
        dest="datasets",
        action="append",
        help="Alias de tablas curated (se puede repetir). Útil para usar --dataset interconn en vez de posicionals.",
    )
    parser.add_argument(
        "--include-current", action="store_true", help="Incluir mes en curso"
    )
    parser.add_argument(
        "--months-back",
        type=int,
        help="Solo procesar los últimos N meses cerrados (después de filtros)",
    )
    parser.add_argument(
        "--month",
        help="Procesar únicamente el mes YYYY-MM indicado (después se puede combinar con --dataset)",
    )
    parser.add_argument(
        "--force", action="store_true", help="Sobrescribir compact.parquet existente"
    )
    parser.add_argument(
        "--delete-originals",
        action="store_true",
        help="Borrar micro-files tras compactar",
    )
    parser.add_argument("--local", action="store_true", help="Usar paths_local.curated")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No escribe ni borra, solo muestra acciones",
    )
    args = parser.parse_args()

    cfg = load_cfg()
    curated_tpl = cfg["paths_local" if args.local else "paths"]["curated"]
    # Derive curated root (remove trailing template components)
    # Template: {bucket}/curated/{table}/year=YYYY/month=MM/day=DD/part-{uuid}.parquet
    # Root we need: {bucket}/curated
    if args.local:
        root_base = cfg["paths_local"]["root"]
        curated_root = f"{root_base}/curated"
    else:
        curated_root = f"{cfg['paths']['bucket']}/curated"

    # Gather tables from cfg datasets curated_table values
    tables_cfg = set()
    for ds_name, ds_cfg in cfg.get("datasets", {}).items():
        if ds_cfg.get("enabled", True):
            tables_cfg.add(ds_cfg.get("curated_table"))
    # Resolver tablas: prioridad --dataset (puede repetirse), luego posicionales, si ninguno => todas
    if args.datasets:
        target_tables = [t for t in args.datasets if t]
    elif args.tables:
        target_tables = args.tables
    else:
        target_tables = sorted(tables_cfg)

    run_id = str(uuid.uuid4())
    t_start = datetime.now(timezone.utc)
    global_stats = {
        "months": 0,
        "rows_final_total": 0,
        "rows_files_sum_total": 0,
        "dedup_removed_total": 0,
    }
    log("info", action="tables_selected", run_id=run_id, tables=target_tables)
    for table in target_tables:
        table_root = f"{curated_root}/{table}"
        try:
            month_paths = list_month_paths(table_root, table, local=args.local)
        except Exception as e:
            log(
                "warn", action="list_failed", table=table, root=table_root, error=str(e)
            )
            continue
        closed = detect_closed_months(month_paths, include_current=args.include_current)
        # Filtro por --month específico
        if args.month:
            try:
                year_s, month_s = args.month.split("-", 1)
                fy, fm = int(year_s), int(month_s)
                closed = [(y, m, p) for (y, m, p) in closed if y == fy and m == fm]
            except Exception:
                raise SystemExit("--month debe tener formato YYYY-MM")
        # Recorte por últimos N meses tras aplicar filtros
        if args.months_back:
            closed = closed[-args.months_back :]
        if not closed:
            log("info", action="no_closed_months", table=table)
            continue
        for y, m, path in closed:
            log(
                "info",
                action="processing_month",
                run_id=run_id,
                table=table,
                year=y,
                month=m,
                path=path,
            )
            try:
                table_pa, raw_row_count, micro_file_count = read_month_dataset(path)
            except Exception as e:
                log(
                    "error",
                    action="read_failed",
                    run_id=run_id,
                    path=path,
                    error=str(e),
                )
                continue
            if table_pa.num_rows == 0:
                log(
                    "info",
                    action="empty_month",
                    run_id=run_id,
                    table=table,
                    year=y,
                    month=m,
                )
                continue
            log(
                "info",
                action="pre_stats",
                run_id=run_id,
                table=table,
                year=y,
                month=m,
                micro_files=micro_file_count,
                rows_sum_files=raw_row_count,
                rows_concat=table_pa.num_rows,
            )
            sort_keys = pick_sort_keys(table_pa.schema.names)
            if sort_keys:
                table_pa = table_pa.sort_by([(k, "ascending") for k in sort_keys])

            # Infer better dedupe keys (can be wider than sort keys)
            dedupe_keys = pick_dedupe_keys(table_pa.schema.names)
            if dedupe_keys:
                pdf = table_pa.to_pandas()
                before = len(pdf)
                pdf = pdf.drop_duplicates(subset=dedupe_keys, keep="last")
                removed = before - len(pdf)
                if removed:
                    log(
                        "info",
                        action="dedupe",
                        run_id=run_id,
                        removed=removed,
                        keys=dedupe_keys,
                    )
                    # Heurística: si usamos sólo timestamp y existen columnas de dimensión, alertar posible colapso accidental
                    if len(dedupe_keys) == 1 and any(
                        c in pdf.columns for c in ["tech", "country", "source"]
                    ):
                        log(
                            "warn",
                            action="possible_dimension_collapse",
                            run_id=run_id,
                            key_used=dedupe_keys,
                            dims_present=[
                                c
                                for c in ["tech", "country", "source"]
                                if c in pdf.columns
                            ],
                            suggestion="Revisar pick_dedupe_keys: quizá faltan columnas de dimensión",
                        )
                if pa is None:
                    raise SystemExit(
                        "pyarrow no disponible para reconstruir tabla tras dedupe"
                    )
                table_pa = pa.Table.from_pandas(pdf, preserve_index=False)
            # Validación: filas concat (antes dedupe) == suma micro-files
            if table_pa.num_rows > raw_row_count:
                log(
                    "warn",
                    action="row_mismatch_excess",
                    run_id=run_id,
                    table=table,
                    expected=raw_row_count,
                    actual=table_pa.num_rows,
                )
            # Estadísticas finales
            log(
                "info",
                action="post_stats",
                run_id=run_id,
                table=table,
                year=y,
                month=m,
                rows_final=table_pa.num_rows,
            )
            global_stats["months"] += 1
            global_stats["rows_final_total"] += table_pa.num_rows
            global_stats["rows_files_sum_total"] += raw_row_count
            # dedup_removed sum captured via individual logs; approximate by diff
            if raw_row_count >= table_pa.num_rows:
                global_stats["dedup_removed_total"] += raw_row_count - table_pa.num_rows
            out_path = path.rstrip("/") + "/compact.parquet"
            if args.dry_run:
                log(
                    "info",
                    action="dry_write",
                    run_id=run_id,
                    path=out_path,
                    rows=table_pa.num_rows,
                )
            else:
                try:
                    written = write_compacted(table_pa, path, force=args.force)
                    log(
                        "info",
                        action="write_ok",
                        run_id=run_id,
                        path=written,
                        rows=table_pa.num_rows,
                    )
                except SystemExit as se:  # e.g. file exists
                    log("info", action="write_skip", run_id=run_id, reason=str(se))
                    continue
            if args.delete_originals and not args.dry_run:
                try:
                    del_stats = delete_micro_files(path)
                    log(
                        "info",
                        action="micro_delete_ok",
                        run_id=run_id,
                        path=path,
                        micro_files_deleted=del_stats.get("micro_files_deleted"),
                        day_dirs_removed=del_stats.get("day_dirs_removed"),
                    )
                except Exception as e:
                    log(
                        "warn",
                        action="micro_delete_failed",
                        run_id=run_id,
                        path=path,
                        error=str(e),
                    )

    t_end = datetime.now(timezone.utc)
    duration_s = (t_end - t_start).total_seconds()
    log(
        "info",
        action="run_summary",
        run_id=run_id,
        months_processed=global_stats["months"],
        rows_final_total=global_stats["rows_final_total"],
        rows_files_sum_total=global_stats["rows_files_sum_total"],
        dedup_removed_total=global_stats["dedup_removed_total"],
        duration_seconds=round(duration_s, 2),
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
