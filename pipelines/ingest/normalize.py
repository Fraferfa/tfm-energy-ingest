from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def parse_values_to_df(payload: Dict[str, Any]) -> pd.DataFrame:
    values = payload.get("indicator", {}).get("values", [])
    df = pd.DataFrame(values)
    if df.empty:
        return df
    if "datetime" in df:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True, errors="coerce")
    return df


def normalize_prices(df: pd.DataFrame, column_map: Dict[str, str], source: str) -> pd.DataFrame:
    if df.empty:
        return df
    # Filtrar solo la zona 'Península' si el source es PVPC
    if source == "PVPC" and "geo_name" in df.columns:
        df = df[df["geo_name"] == "Península"].copy()
    out = pd.DataFrame()
    out[column_map.get("ts", "hour_ts")] = df["datetime"].dt.tz_convert("Europe/Madrid").dt.floor("h")
    out[column_map.get("value", "price_eur_mwh")] = pd.to_numeric(df["value"], errors="coerce")
    zone_col = df.get("geo_name", "ES")
    # Renombrar Península a España para homogeneizar con SPOT_ES
    if isinstance(zone_col, pd.Series):
        zone_col = zone_col.replace({"Península": "España"})
    out[column_map.get("zone", "zone")] = zone_col
    out["source"] = source
    return out


def normalize_wide_by_indicator(
    dfs_by_id: Dict[int, pd.DataFrame], column_map: Dict[str, str], id_rename: Dict[str, str] | None = None
) -> pd.DataFrame:
    parts = []
    ts_col = column_map.get("ts", "hour_ts")
    for key_id, df in dfs_by_id.items():
        if df.empty:
            continue
        df = df.copy()
        # Para granularidad minuto no hacemos floor a la hora; mantenemos minuto exacto
        # Si el ts_col es 'hour_ts' aplicamos floor a hora, si no, dejamos datetime en zona local
        local_ts = df["datetime"].dt.tz_convert("Europe/Madrid")
        df[ts_col] = local_ts.dt.floor("h") if ts_col == "hour_ts" else local_ts
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        parts.append(df[[ts_col, "value", "geo_name", "indicator_id"]])
    if not parts:
        return pd.DataFrame()
    big = pd.concat(parts, ignore_index=True)
    pivot = big.pivot_table(
        index=[ts_col, "geo_name"], columns="indicator_id", values="value", aggfunc="last"
    ).reset_index()
    rename = {"geo_name": column_map.get("zone", "zone")}
    out = pivot.rename(columns=rename)
    out = out.rename(
        columns={
            1293: column_map.get("real", "demand_mw"),
            544: column_map.get("forecast", "forecast_mw"),
        }
    )
    if id_rename:
        # Las columnas del pivot están como enteros (indicator_id). Convertimos claves string a int si posible.
        mapping = {}
        for k, v in id_rename.items():
            try:
                ik = int(k)
                if ik in out.columns:
                    mapping[ik] = v
            except Exception:
                if k in out.columns:
                    mapping[k] = v
        if mapping:
            out = out.rename(columns=mapping)
    return out


def normalize_long_tech(
    dfs_by_id: Dict[int, pd.DataFrame], tech_map: Dict[str, str], column_map: Dict[str, str]
) -> pd.DataFrame:
    rows = []
    ts_col = column_map.get("ts", "hour_ts")
    for key_id, df in dfs_by_id.items():
        if df.empty:
            continue
        tech = tech_map.get(str(key_id), str(key_id))
        local_ts = df["datetime"].dt.tz_convert("Europe/Madrid")
        tmp = pd.DataFrame(
            {
                ts_col: local_ts.dt.floor("h") if ts_col == "hour_ts" else local_ts,
                column_map.get("tech", "tech"): tech,
                column_map.get("value", "mw"): pd.to_numeric(df["value"], errors="coerce"),
                column_map.get("zone", "zone"): df.get("geo_name", "ES"),
            }
        )
        rows.append(tmp)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)


def normalize_interconn_pairs(
    dfs_by_id: Dict[int, pd.DataFrame], to_pairs: Dict[str, list], column_map: Dict[str, str]
) -> pd.DataFrame:
    rows = []
    ts_col = column_map.get("ts", "hour_ts")
    for key_id, df in dfs_by_id.items():
        if df.empty:
            continue
        country, field = to_pairs.get(str(key_id), ["UNK", "value"])
        local_ts = df["datetime"].dt.tz_convert("Europe/Madrid")
        tmp = pd.DataFrame(
            {
                ts_col: local_ts.dt.floor("h") if ts_col == "hour_ts" else local_ts,
                column_map.get("country", "country"): country,
                field: pd.to_numeric(df["value"], errors="coerce"),
            }
        )
        rows.append(tmp)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    group_cols = [c for c in out.columns if c not in ("export_mw", "import_mw")]
    out = out.groupby(group_cols, as_index=False).agg({c: "last" for c in out.columns if c not in group_cols})
    return out
