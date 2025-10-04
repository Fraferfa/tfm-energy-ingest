import os
from datetime import datetime, time, timedelta, timezone

import pandas as pd
import requests

# Token y cabeceras
token = os.environ.get("ESIOS_TOKEN", "").strip()
h = {
    "Accept": "application/json, application/vnd.esios-api-v1+json",
    "Content-Type": "application/json; charset=utf-8",
    "x-api-key": token,
    "Authorization": f"Token token={token}",
    "User-Agent": "tfm-energy-ingest/1.0",
}

# --- Configuración robusta de fechas según la guía ---
# Día objetivo: mañana
target_date = datetime.utcnow().date()  # + timedelta(days=1)
# Rango extendido en UTC para evitar pérdida de horas por timezone
start_day_utc = datetime.combine(target_date, time(0, 0), tzinfo=timezone.utc) - timedelta(hours=3)
end_day_utc = datetime.combine(target_date + timedelta(days=1), time(0, 0), tzinfo=timezone.utc) + timedelta(hours=1)

p = {
    "start_date": start_day_utc.isoformat().replace("+00:00", "Z"),
    "end_date": end_day_utc.isoformat().replace("+00:00", "Z"),
    "time_trunc": "hour",
    "locale": "es",
}

indicator_id = 1001  # PVPC
r = requests.get(f"https://api.esios.ree.es/indicators/{indicator_id}", headers=h, params=p, timeout=30)
print(r.status_code, r.reason)
print(r.text[:500])  # Muestra los primeros 500 caracteres de la respuesta

# Convertir a DataFrame y filtrar solo el día objetivo
if r.status_code == 200:
    data = r.json()
    if "indicator" in data and "values" in data["indicator"]:
        df_raw = pd.DataFrame(data["indicator"]["values"])
        if "datetime" in df_raw.columns:
            df_raw["datetime"] = pd.to_datetime(df_raw["datetime"])
            # Filtrar solo las filas del día objetivo
            df_target_day = df_raw[df_raw["datetime"].dt.date == target_date]
            print("Columnas del DataFrame filtrado:", df_target_day.columns.tolist())
            print(df_target_day.head())
            # Pivotar para ver zonas como columnas
            df_pivot = df_target_day.pivot(index="datetime", columns="geo_name", values="value")
            print("DataFrame pivotado (zonas como columnas):")
            print(df_pivot.head())
            # Validación: debe haber 24 horas
            print(f"Total de horas obtenidas: {len(df_pivot)}")
            if len(df_pivot) != 24:
                print(f"Advertencia: solo {len(df_pivot)} horas en lugar de 24")
            else:
                print("Datos completos: 24 horas")
            # Verificar zonas presentes
            zonas = ["Península", "Baleares", "Canarias", "Ceuta", "Melilla"]
            missing_zones = [z for z in zonas if z not in df_pivot.columns]
            if missing_zones:
                print(f"Zonas faltantes: {missing_zones}")
            else:
                print("Todas las zonas presentes")
        else:
            print(
                "La columna 'datetime' no está presente en el DataFrame. Columnas disponibles:", df_raw.columns.tolist()
            )
            print(df_raw.head())
    else:
        print("Estructura inesperada en la respuesta JSON.")
else:
    print("Error en la solicitud:", r.status_code, r.reason)
