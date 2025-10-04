#!/usr/bin/env python
"""QC rápido de datos ingeridos antes de compactación.

Uso:
    Local:
        python scripts/qc_month.py <curated_table> <year> <month> [--local-root ./data]
    GCS (lectura directa gs://):
        python scripts/qc_month.py <curated_table> <year> <month> --gcs-root gs://energia-tfm-bucket

Notas:
    - Si se proporciona --gcs-root tiene prioridad sobre --local-root.
    - Si el mes no existe se imprime NO_EXISTE y se sale con código 0 (gracia) para no marcar Job como fallo temprano.
    - Para detectar duplicados se usan claves heurísticas si están presentes.
    - Requiere fsspec/gcsfs instalados para modo GCS.
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from typing import Iterable

try:  # fsspec es opcional; sólo necesario en modo gcs
    import fsspec  # type: ignore
except Exception:  # pragma: no cover
    fsspec = None  # type: ignore

try:
    import pandas as pd
except ImportError as e:  # pragma: no cover
    print(f"ERROR: requiere pandas: {e}")
    sys.exit(1)

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None  # type: ignore


def pick_pk(cols: set[str]) -> list[str]:
    # Heurística basada en config
    for cand in [
        ["hour_ts", "zone", "source"],      # precios
        ["hour_ts", "zone"],                 # precios sin source
        ["minute_ts", "zone", "tech"],      # gen_mix
        ["minute_ts", "zone"],               # demanda
        ["minute_ts", "country"],            # interconn
    ]:
        if all(c in cols for c in cand):
            return cand
    return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("table", help="Nombre de la tabla curated (prices, demand, gen_mix, interconn)")
    ap.add_argument("year")
    ap.add_argument("month", help="Mes 01-12")
    ap.add_argument("--local-root", default="./data", help="Raíz local (default ./data)")
    ap.add_argument("--gcs-root", default="", help="Raíz GCS gs://bucket (si se indica se ignora --local-root)")
    args = ap.parse_args()
    use_gcs = bool(args.gcs_root)
    parquet_files: list[str] = []

    if use_gcs:
        if fsspec is None:
            print("ERROR: fsspec/gcsfs no disponibles para modo GCS", file=sys.stderr)
            sys.exit(1)
        fs = fsspec.filesystem("gcs")  # rely on ADC or env creds
        # Construimos prefijo
        month_prefix = f"{args.gcs_root.rstrip('/')}/curated/{args.table}/year={args.year}/month={args.month}/"
        if not fs.exists(month_prefix):
            print(f"NO_EXISTE: {month_prefix}")
            sys.exit(0)
        # Listado recursivo
        try:
            for path in fs.find(month_prefix):  # devuelve lista de todos los paths
                if path.endswith('.parquet') and not path.endswith('compact.parquet'):
                    parquet_files.append(f"gs://{path}" if not path.startswith('gs://') else path)
        except FileNotFoundError:
            print(f"NO_EXISTE: {month_prefix}")
            sys.exit(0)
    else:
        month_path = os.path.join(args.local_root, "curated", args.table, f"year={args.year}", f"month={args.month}")
        if not os.path.isdir(month_path):
            print(f"NO_EXISTE: {month_path}")
            sys.exit(0)
        for root, _dirs, files in os.walk(month_path):
            for f in files:
                if f.endswith('.parquet') and f != 'compact.parquet':
                    parquet_files.append(os.path.join(root, f))

    if not parquet_files:
        print("SIN_FICHEROS")
        sys.exit(0)

    day_counts = defaultdict(int)
    pk_dupes = 0
    pk_total = 0
    total_rows = 0

    # Determinar primary key dinámica una sola vez con el primer fichero
    first_df = pd.read_parquet(parquet_files[0])
    pk = pick_pk(set(first_df.columns))
    if pk:
        # Construir set de keys ya vistas
        seen = set()
        for file in parquet_files:
            df = pd.read_parquet(file, columns=pk)
            total_rows += len(df)  # este total provisional si solo columnas pk
            for row in df.itertuples(index=False):
                pk_total += 1
                key = tuple(row)
                if key in seen:
                    pk_dupes += 1
                else:
                    seen.add(key)
        # Necesitamos total real de filas, no sólo PK (pero ya contamos len(df) que == filas)
    else:
        # Releer con filas completas agrupando por día y contando
        total_rows = 0
        pk_total = 0
        for file in parquet_files:
            df = pd.read_parquet(file)
            total_rows += len(df)
            # Intento extraer day del path: .../day=DD/
            parts = file.replace('\\', '/').split('/')
            day = next((p.split('=')[1] for p in parts if p.startswith('day=')), None)
            if day:
                day_counts[day] += len(df)
        pk_dupes = -1  # señal de 'no calculado'

    # Si calculamos pk con sólo columnas pk, obtener day_counts ahora
    if pk:
        for file in parquet_files:
            parts = file.replace('\\', '/').split('/')
            day = next((p.split('=')[1] for p in parts if p.startswith('day=')), None)
            if day:
                # necesitamos leer cantidad real de filas; usamos el truco de contar con pk ya leído (mismo len)
                # sumamos de nuevo separadamente para day_counts
                df_rows = len(pd.read_parquet(file, columns=pk))
                day_counts[day] += df_rows

    print(f"TABLE={args.table} YEAR={args.year} MONTH={args.month}")
    print(f"FILES={len(parquet_files)} ROWS_TOTAL={total_rows}")
    if pk:
        pct = (pk_dupes / pk_total * 100) if pk_total else 0
        print(f"PK={'+'.join(pk)} DUPES={pk_dupes} ({pct:.3f}% de {pk_total})")
    else:
        print("PK=NA DUPES=NA")
    # Ordenar días
    for d in sorted(day_counts):
        print(f"DAY={d} ROWS={day_counts[d]}")


if __name__ == "__main__":  # pragma: no cover
    main()
