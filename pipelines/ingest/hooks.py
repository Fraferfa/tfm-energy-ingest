import pandas as pd

ZONES = ["Península", "Baleares", "Canarias", "Ceuta", "Melilla"]


def compute_mix_pct(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    ts_col = "minute_ts" if "minute_ts" in df.columns else "hour_ts"
    totals = df.groupby([ts_col, "zone"], as_index=False)["mw"].sum()
    totals["mw_total"] = totals["mw"]
    totals.drop(columns=["mw"], inplace=True)
    out = df.merge(totals, on=[ts_col, "zone"], how="left")
    out["pct"] = (out["mw"] / out["mw_total"]).where(out["mw_total"] > 0)
    return out


def validate_pvpc_complete_day(df: pd.DataFrame) -> pd.DataFrame:
    import warnings

    if df.empty:
        warnings.warn("PVPC vacío")
        return df
    pvt = df.pivot_table(index="hour_ts", columns="zone", values="price_eur_mwh", aggfunc="last")
    # Validación de horas: 24 salvo días de cambio DST (23/25)
    if len(pvt.index) not in (23, 24, 25):
        warnings.warn(f"PVPC horas atípicas: {len(pvt.index)} (esperado 24, tolerado 23/25 en DST)")
    # Validación de zonas: si solo trabajamos con 'Península', no avisar por otras zonas faltantes
    expected = set(["Península"]) if set(pvt.columns) == set(["Península"]) else set(ZONES)
    missing = [z for z in expected if z not in pvt.columns]
    if missing and expected != set(["Península"]):
        warnings.warn(f"Zonas PVPC faltantes: {missing}")
    if (pvt < -10).any().any() or (pvt > 1000).any().any():
        warnings.warn("Valores PVPC fuera de rango plausible")
    if pvt.isna().any().any():
        warnings.warn("PVPC con nulos")
    return df
