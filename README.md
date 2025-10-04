# tfm-energy-ingest

Pipelines de ingesta/curación para ESIOS (PVPC €/MWh, DST-safe).

## Requisitos
- Python 3.11
- GCP: Artifact Registry, Cloud Run Jobs, Secret Manager, Cloud Scheduler
- Secret `ESIOS_TOKEN` en Secret Manager

## Desarrollo local (opcional)
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export ESIOS_TOKEN="TU_TOKEN"
python pipelines/ingest/main.py prices_pvpc
```

## Despliegue CI/CD
Configura en GitHub Secrets:
- `GCP_PROJECT_ID`
- `GCP_WORKLOAD_ID_PROVIDER`
- `GCP_CICD_SA`
- `GCP_JOBS_SA`

El workflow construye la imagen y despliega los **Jobs**.

## Programación (Cloud Scheduler)
Crea los cron para ejecutar los jobs:
- Horario: `ingest-prices-pvpc`, `prices_spot`, `demand`, `gen_mix`, `interconn`
- Diario 20:20: `ingest-prices-pvpc-tomorrow`

### Resumen de schedules actuales

| Nombre schedule | Cron            | Estrategia ventana                 | Uso principal                         |
|-----------------|-----------------|------------------------------------|---------------------------------------|
| hourly          | `0 * * * *`     | `last_hours` (6h rolling)          | Precios spot (últimas horas)          |
| pvpc_daily_20h  | `20 20 * * *`   | `next_day_dstsafe` (día siguiente) | PVPC día siguiente completo           |
| minute_complete_20 | `20 * * * *` | `last_complete_hour_local`         | Demanda / gen_mix / interconn (min)   |

Notas:
- `last_complete_hour_local`: toma la hora completa previa al momento de ejecución (ej: se ejecuta 11:20 -> ingiere 10:00–10:59 local convertida a UTC).
- `next_day_dstsafe`: expande rango para capturar el día local siguiente completo (23/24/25h según DST) y luego se filtra para escribir solo ese día.

### Tipos de datasets y normalización

| Dataset      | Tipo normalización        | Granularidad RAW | Columna tiempo curated | Particularidades |
|--------------|---------------------------|------------------|------------------------|------------------|
| prices_pvpc  | prices (filtra Península→España) | Hora (API)       | `hour_ts` (Europe/Madrid) | Validador día completo, zona = España |
| prices_spot  | prices                    | Hora             | `hour_ts`              | Fuente SPOT_ES   |
| demand       | wide_by_indicator         | Minuto           | `minute_ts`            | Renombra 2037/2052/2053 a nombres descriptivos |
| gen_mix      | long_tech + %             | Minuto           | `minute_ts`            | Calcula `pct` de cada tecnología |
| interconn    | interconn_pairs           | Minuto           | `minute_ts`            | Export/import por país |

### Flujo PVPC (cómo y cuándo se cargan los precios)
1. A las 20:20 (Europe/Madrid) se ejecuta `prices_pvpc` con ventana `next_day_dstsafe`, que pide un rango ampliado alrededor del día siguiente para cubrir cambios DST.
2. Se normaliza: se queda solo con zona "Península" (renombrada a "España") y se genera `hour_ts` en hora local.
3. Se filtra el dataframe para el día objetivo (día siguiente) y se valida:
	 - Número de horas (23/24/25 aceptadas).
	 - Valores dentro de rango plausible.
4. Se escribe una partición parquet (`prices`) y opcionalmente se pueden relanzar backfills para días antiguos con `--backfill-day` o `--backfill-range`.

Backfills PVPC:
- `--backfill-day YYYY-MM-DD`: fuerza ventana tipo `today_dstsafe` sobre ese día y filtra a ese día.
- `--backfill-range START:END` (inclusive): itera día a día aplicando la misma lógica; tolera DST.

### Flujo datasets de minuto (demanda, mix, interconn)
- Se ejecutan a los :20 de cada hora (`minute_complete_20`).
- Ventana `last_complete_hour_local`: ingiere la hora previa completa (sin incluir la hora actual en curso).
- No hay solape adicional: se asume que a +20 min ya están todos los minutos de la hora cerrada publicados.
- Normalización:
	- Demanda pivota indicadores y renombra a: `demanda_real_mw`, `demanda_prevista_h_mw`, `demanda_programada_h_mw`.
	- Gen mix transforma a formato largo con tecnologías y calcula la proporción `pct` dentro de la suma de MW de la hora (o minuto en este caso) y zona.
	- Interconexiones agrupa por país y determina `export_mw` / `import_mw`.

### Columnas clave generadas
- `hour_ts`: timestamps horarios en Europe/Madrid (PVPC, SPOT).
- `minute_ts`: timestamps a minuto en Europe/Madrid (demanda, mix, interconn).
- `price_eur_mwh`, `demanda_real_mw`, `demanda_prevista_h_mw`, `demanda_programada_h_mw`, `mw`, `pct`, `export_mw`, `import_mw` según dataset.
- `zone` o `country`/`tech` según el caso.

### Flags/CLI disponibles
| Flag | Uso | Notas |
|------|-----|-------|
| `--local` | Escribe salidas en disco local (`paths_local`) | Sin credenciales GCS |
| `--target-date YYYY-MM-DD` | Forzar día base (estrategias DST-safe) | PVPC día siguiente / pruebas |
| `--backfill-day YYYY-MM-DD` | Reproceso de un día completo | Usa ventana `today_dstsafe` |
| `--backfill-range START:END` | Rango de días inclusivo | Itera día a día con DST-safe |
| `ESIOS_AUTH_MODE` (env) | `x-api-key` / `authorization` / `both` | Fallback de cabeceras |

### Estructura de salida
Se han simplificado los paths curated (sin particionar por `indicator_id` ni `zone`):
```
curated/<table>/year=YYYY/month=MM/day=DD/part-UUID.parquet
```
RAW conserva solo columnas esenciales (ver `raw_keep_columns`).

### Consideraciones DST
- Para PVPC se expande la ventana y luego se filtra a la fecha objetivo: evita perder horas (23) o duplicarlas (25) en cambios de horario.
- Para minuto no se requiere ventana expandida porque sólo se toma la hora previa.

### Buenas prácticas operativas
- Retraso de 20 minutos elegido para datasets de minuto: minimiza riesgo de registros tardíos.
- Si en algún momento se observan minutos faltantes se podría añadir un job de “replay” o ampliar delay a 25.
- Backfills masivos: usar rangos y, si se busca paralelizar, dividir por años/meses externamente.

---

## Pruebas locales en Windows (PowerShell)
Nota de zona horaria:
- ESIOS devuelve `datetime` en UTC. Durante la normalización convertimos a `Europe/Madrid` y calculamos `hour_ts` en hora local (maneja CET/CEST y cambios de hora).


Estas instrucciones permiten probar cada carga (dataset) desde tu máquina usando PowerShell. Las cargas disponibles están definidas en `config/ingest.yaml` bajo `datasets`:
- `prices_pvpc` (ejecución diaria 20:20 para el día siguiente; soporta backfill de días completos)
- `prices_spot`
- `demand`
- `gen_mix`
- `interconn`

### 1) Preparar entorno Python

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2) Variables de entorno necesarias

- `ESIOS_TOKEN`: API key de ESIOS (obtener desde su portal).
- `GOOGLE_APPLICATION_CREDENTIALS`: ruta al JSON de cuenta de servicio con permisos de escritura en el bucket GCS definido en `config/ingest.yaml` (`paths.bucket`).
- (Opcional) `GCLOUD_PROJECT`: ID de tu proyecto GCP.

Ejemplo (usando el JSON incluido en el repo, o tu propio JSON):

```powershell
$env:ESIOS_TOKEN = "TU_TOKEN_ESIOS"
$env:GOOGLE_APPLICATION_CREDENTIALS = (Resolve-Path ".\tfm-energia-streamlit-cloud-814352512664.json").Path
$env:GCLOUD_PROJECT = "tu-proyecto-gcp"  # opcional
```

Importante:
- Verifica/ajusta `paths.bucket` en `config/ingest.yaml` para GCS (por defecto: `gs://energia-tfm-bucket`).
- También puedes activar modo local que escribe en disco (ver más abajo).

### 3) Smoke test rápido de conectividad a ESIOS (opcional)

```powershell
python .\scripts\test.py
```

Deberías ver código 200 y un pequeño resumen de las columnas/zonas.

### 4) Ejecutar cada carga manualmente

La CLI es: `python pipelines/ingest/main.py <dataset> [--target-date YYYY-MM-DD]`

Notas de ventana temporal:
- Por defecto la mayoría usa `last_hours: 6` (UTC) definido en `config/ingest.yaml`.
- Las cargas con estrategia DST-safe admiten `--target-date` para fijar el día objetivo en Europa/Madrid.

Comandos ejemplo (PowerShell):

```powershell
# PVPC: ejecutar "día siguiente" (lo que hace el job diario de 20:20)
python .\pipelines\ingest\main.py prices_pvpc --target-date $((Get-Date).ToString('yyyy-MM-dd'))

# PVPC: backfill de un día completo anterior (DST-safe)
python .\pipelines\ingest\main.py prices_pvpc --backfill-day 2025-09-20

# Precio spot ES (OMIE, id=600) últimas horas
python .\pipelines\ingest\main.py prices_spot

# Demanda real vs. prevista (ancho por indicador) últimas horas
python .\pipelines\ingest\main.py demand

# Mix de generación por tecnología (largo, minuto) hora completa previa
python .\pipelines\ingest\main.py gen_mix

# Interconexiones por país (minuto) hora completa previa
python .\pipelines\ingest\main.py interconn
```

Salida esperada:
- Imprime rutas `RAW -> gs://.../raw/...csv` y `CURATED -> gs://.../curated/...parquet` que se han escrito.
- Warnings informativos (por ejemplo, validación de PVPC día completo) pueden aparecer y no frenan la ejecución.

#### Modo local (sin GCS)

Puedes escribir los resultados en disco local usando `--local`. Las rutas locales se controlan en `config/ingest.yaml` bajo `paths_local`.

Ejemplos:

```powershell
# PVPC últimas horas, salida en ./data
python .\pipelines\ingest\main.py prices_pvpc --local

# PVPC de mañana (DST-safe) a disco local
python .\pipelines\ingest\main.py prices_pvpc_tomorrow --target-date $((Get-Date).AddDays(1).ToString('yyyy-MM-dd')) --local
```

Configuración por defecto de `paths_local`:

```yaml
paths_local:
	root: "./data"
	raw: "{root}/raw/{dataset}/year={year}/month={month}/day={day}/{dataset}_{iso_run}.csv"
	curated: "{root}/curated/{table}/year={year}/month={month}/day={day}/part-{uuid}.parquet"
```

### 5) Ajustar ventana temporal

Para ampliar o reducir el rango horario, edita `config/ingest.yaml` en la sección del dataset (clave `window_strategy`). Ejemplo para 24 horas:

```yaml
window_strategy: { type: "last_hours", hours: 24 }
```

Para ejecuciones diarias con horario de verano/invierno, usa `--target-date` (día base para calcular el "día siguiente"). Para backfill de un día ya pasado usa `--backfill-day` que fuerza ventana `today_dstsafe`.

### 6) Problemas comunes y soluciones

- HTTP 401/403: token inválido o cabeceras no aceptadas. Revisa `ESIOS_TOKEN` y vuelve a intentar.
- 403 persistente: prueba con distintos modos de autenticación:
	```powershell
	# Solo x-api-key
	$env:ESIOS_AUTH_MODE = "x-api-key"
	python .\pipelines\ingest\main.py prices_pvpc --local
	# Solo Authorization
	$env:ESIOS_AUTH_MODE = "authorization"
	python .\pipelines\ingest\main.py prices_pvpc --local
	# Ambos (por defecto)
	$env:ESIOS_AUTH_MODE = "both"
	```
- 429/Rate limit: el cliente aplica throttling; aumenta `defaults.rate_limit_per_sec` si tienes margen.
- Errores GCS (permisos/bucket): comprueba `GOOGLE_APPLICATION_CREDENTIALS`, que el bucket exista y que el SA tenga rol `Storage Object Admin` al menos.
- DST: para PVPC de mañana usa siempre `prices_pvpc_tomorrow` con `--target-date` para evitar pérdidas o duplicados de hora.

## Ejecución con Docker (opcional)

La imagen ahora expone un entrypoint unificado (`entrypoint.py`) que acepta un primer argumento de modo:

```
	ingest  -> pipelines/ingest/main.py
	compact -> pipelines/ingest/compact.py
	qc      -> scripts/qc_month.py
```

Construir imagen:

```powershell
docker build -t tfm-energy-ingest:local -f docker/Dockerfile .
```

### Ingesta (ejemplo interconn rango de días)
```powershell
$CredFile = "tfm-energia-streamlit-cloud-814352512664.json"  # ajusta si cambia
docker run --rm `
	-e ESIOS_TOKEN=$env:ESIOS_TOKEN `
	-e GOOGLE_APPLICATION_CREDENTIALS=/app/$CredFile `
	-v ${PWD}:/app -w /app `
	tfm-energy-ingest:local `
	ingest interconn --backfill-range 2025-09-01:2025-09-03
```

### Ingesta modo local
```powershell
docker run --rm -e ESIOS_TOKEN=$env:ESIOS_TOKEN -v ${PWD}:/app -w /app tfm-energy-ingest:local `
	ingest prices_pvpc --backfill-day 2025-09-20 --local
```

### Compactación (último mes cerrado de una tabla)
```powershell
docker run --rm -e GOOGLE_APPLICATION_CREDENTIALS=/app/$CredFile -v ${PWD}:/app -w /app tfm-energy-ingest:local `
	compact --dataset interconn --months-back 1 --dry-run
```

### Compactación mes específico y borrado de micro‑files
```powershell
docker run --rm -e GOOGLE_APPLICATION_CREDENTIALS=/app/$CredFile -v ${PWD}:/app -w /app tfm-energy-ingest:local `
	compact --dataset interconn --month 2025-09 --delete-originals
```

### Compactar todos los datasets últimos 3 meses
```powershell
docker run --rm -e GOOGLE_APPLICATION_CREDENTIALS=/app/$CredFile -v ${PWD}:/app -w /app tfm-energy-ingest:local `
	compact --months-back 3 --dry-run
```

### QC mensual (recuento y duplicados estimados)
```powershell
docker run --rm -v ${PWD}:/app -w /app tfm-energy-ingest:local `
	qc --dataset interconn --month 2025-09
```

Notas:
1. Si montas todo el repo en `/app` el `entrypoint.py` existe también en la raíz, evitando que se pierda al hacer bind mount.
2. `ESIOS_TOKEN` no es necesario para compactación ni QC; se mostrará un warning si falta (se puede ignorar).
3. Para ejecución sólo sobre GCS sin escribir local puedes omitir el bind mount y pasar solo credenciales.

	## Smoke de todos los datasets (local)

	Ejecuta todas las cargas habilitadas en `config/ingest.yaml` con salida local y resumen final:

	```powershell
	python .\scripts\smoke_all.py
	```

	Filtrar datasets y forzar modo de autenticación ESIOS:

	```powershell
	python .\scripts\smoke_all.py --only prices_pvpc demand --auth-mode x-api-key
	```

	## Backfill por rangos (inicial: 3 años)

	- Rango directo (inclusive):
	```powershell
	python .\pipelines\ingest\main.py prices_pvpc --backfill-range 2022-09-21:2025-09-20 --local
	```

	- Generar “últimos 3 años” dinámico en PowerShell y lanzar por rango:
	```powershell
	$end = Get-Date
	$start = $end.AddYears(-3)
	python .\pipelines\ingest\main.py prices_pvpc --backfill-range $($start.ToString('yyyy-MM-dd')):$($end.AddDays(-1).ToString('yyyy-MM-dd')) --local
	```

	Nota: el backfill procesa día a día en modo `today_dstsafe` para respetar cambios de hora.

	## Compactación mensual de datos curated

	Problema: la ingesta diaria (o horaria) genera muchos micro‑ficheros Parquet (uno por ejecución / partición día). Esto degrada el rendimiento de motores como DuckDB / Spark / BigQuery al aumentar el overhead de metadata & file listing.

	Objetivo: consolidar todos los ficheros de un mes cerrado en un único `compact.parquet` (tamaño orientativo 128‑512 MB) para cada tabla curated (`prices`, `demand`, `gen_mix`, `interconn`).

	Script: `python pipelines/ingest/compact.py`

	### Estrategia
	1. Detectar meses “cerrados” (todas las particiones `year=YYYY/month=MM` distintos del mes actual) salvo que se use `--include-current`.
	2. Leer todos los Parquet del mes (excluyendo uno existente `compact.parquet`).
	3. Ordenar por columna temporal disponible (`minute_ts` > `hour_ts` > `datetime`).
	4. Deduplicar por esa(s) columna(s) si procede.
	5. Escribir `compact.parquet` con compresión ZSTD y `row_group_size=50_000`.
	6. (Opcional) Borrar micro‑files (`--delete-originals`) tras validación implícita (el número de filas resultante es la suma de las fuentes tras dedupe).

	### Uso básico (PowerShell)

	```powershell
	# Dry-run (no escribe) sobre todas las tablas cerradas
	python .\pipelines\ingest\compact.py --dry-run

	# Compactar sólo tabla prices (meses cerrados)
	python .\pipelines\ingest\compact.py prices

	# Incluir mes en curso (si necesitas recomputar parcial)
	python .\pipelines\ingest\compact.py prices --include-current

	# Forzar sobrescritura si ya existe compact.parquet
	python .\pipelines\ingest\compact.py prices --force

	# Eliminar micro-files tras compactar (ATENCIÓN: operación destructiva)
	python .\pipelines\ingest\compact.py prices --delete-originals --force

	# Limitar a los últimos 2 meses cerrados
	python .\pipelines\ingest\compact.py prices demand --months-back 2

	# Modo local usando ./data/curated
	python .\pipelines\ingest\compact.py --local --dry-run
	```

	### Flags disponibles

	| Flag | Descripción |
	|------|-------------|
	| (tables) | Lista de tablas curated a procesar. Vacío = todas detectadas en config. |
	| `--dataset X` | Alias repetible para indicar tablas (equivalente a posicional). |
	| `--month YYYY-MM` | Limita el procesamiento a un único mes concreto. |
	| `--include-current` | Incluye el mes en curso (por defecto se ignora). |
	| `--months-back N` | Limita a los últimos N meses cerrados (después de filtros). |
	| `--force` | Sobrescribe `compact.parquet` existente. |
	| `--delete-originals` | Elimina micro‑files tras éxito (no recomendado hasta validar flujo). |
	| `--local` | Opera sobre `paths_local.curated`. |
	| `--dry-run` | No escribe ni borra; muestra acciones. |

	Ejemplos adicionales:

	```powershell
	# Solo prices e interconn para septiembre 2025
	python .\pipelines\ingest\compact.py --dataset prices --dataset interconn --month 2025-09 --dry-run

	# Recompactar el mes actual (parcial) para una tabla
	python .\pipelines\ingest\compact.py --dataset gen_mix --include-current --month 2025-10 --force
	```

	### Programación recomendada (Cloud Scheduler)

	| Frecuencia | Cron sugerido | Acción |
	|------------|--------------|--------|
	| Mensual | `0 2 1 * *` | Compactar todos los meses cerrados (mes anterior). |
	| Semanal (higiene) | `0 3 * * 1` | Recompactar últimos 2-3 meses para capturar cargas retrasadas. |

	Cloud Run Job: construir una imagen que contenga el script (ya presente) y ejecutar comando, por ejemplo:

	```bash
	python pipelines/ingest/compact.py --months-back 3 --force
	```

	Ejemplos usando `gcloud` (ajusta nombres reales de Job / región / proyecto):

	```powershell
	# Crear (o actualizar) Cloud Run Job para compactación
	gcloud run jobs deploy compact-curated \
	  --image=gcr.io/$env:GOOGLE_PROJECT/tfm-energy-ingest:latest \
	  --region=europe-southwest1 \
	  --args="python","pipelines/ingest/compact.py","--months-back","3","--force"

	# Scheduler mensual (día 1 a las 02:00 hora de Madrid -> cron en UTC ajusta si tu scheduler está en UTC)
	gcloud scheduler jobs create http compact-monthly \
	  --schedule="0 1 1 * *" \
	  --uri="https://region-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$env:GOOGLE_PROJECT/jobs/compact-curated:run" \
	  --http-method=POST \
	  --oauth-service-account-email "<SA_JOB>@$env:GOOGLE_PROJECT.iam.gserviceaccount.com" \
	  --location=europe-southwest1

	# Scheduler semanal (lunes 03:00)
	gcloud scheduler jobs create http compact-weekly \
	  --schedule="0 2 * * 1" \
	  --uri="https://region-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$env:GOOGLE_PROJECT/jobs/compact-curated:run" \
	  --http-method=POST \
	  --oauth-service-account-email "<SA_JOB>@$env:GOOGLE_PROJECT.iam.gserviceaccount.com" \
	  --location=europe-southwest1
	```

	### Validación / Buenas prácticas
	* Comprobar tamaño de `compact.parquet` frente a micro‑files (esperar reducción del número de ficheros, no necesariamente de bytes).
	* Si se activa `--delete-originals`, mantener primero un ciclo de pruebas sin borrado para validar queries downstream.
	* Se puede adaptar el `row_group_size` según patrón de lectura (si predominan filtros temporales, mantener 50k; para full scans grandes, aumentar a 100k–250k).
	* Para BigQuery externa / DuckDB aprovechar lectura directa de un único fichero por mes.

	### Limitaciones actuales / Futuras mejoras
	* Listado de rutas basado en fsspec genérico; para muchos miles de ficheros puede necesitar optimización (scanner directo de dataset + filters).
	* No valida explícitamente que la suma de filas de micro‑files == resultado tras dedupe (se puede añadir contador opcional).
	* No expone aún métricas de throughput por dataset.

	### Diferencias local vs nube (compactación) y flags clave

	| Caso | `--dry-run` | `--force` | `--delete-originals` | Efecto principal |
	|------|-------------|-----------|----------------------|------------------|
	| Local + `--dry-run` | Sí (simulación) | Ignorado | Ignorado | Calcula stats, NO crea `compact.parquet` |
	| Local sin `--dry-run` | No | Opcional | Opcional | Genera (o sobrescribe con `--force`) `compact.parquet` en `./data/curated/...` |
	| Cloud Run + `--dry-run` | Sí | Ignorado | Ignorado | Solo logs en Cloud Logging, sin escritura GCS |
	| Cloud Run sin `--dry-run` | No | Respeta | Respeta | Escribe en GCS; si existe y falta `--force` se omite (write_skip) |
	| Cloud Run con `--delete-originals` | No | Opcional | Sí | Tras escribir `compact.parquet` elimina micro‑files (operación destructiva) |

	Notas:
	1. `--dry-run` tiene precedencia: aunque pases `--force` o `--delete-originals` no se escribe ni borra nada.
	2. Para “ver cómo queda” localmente basta ejecutar SIN `--dry-run` y (opcional) SIN `--delete-originals` primero:
		```powershell
		python .\pipelines\ingest\compact.py --local --force
		```
	3. Después de inspeccionar el fichero resultante (`parquet-tools meta` o `python -c 'import pandas as pd; print(pd.read_parquet(".../compact.parquet").head())'`) puedes decidir borrar micro‑files:
		```powershell
		python .\pipelines\ingest\compact.py --local --force --delete-originals
		```
	4. En la nube (Cloud Run Job) se recomienda un primer job mensual sin `--delete-originals`; un segundo job (o el mismo tras X días) ya puede incluir el borrado para reducir coste/listados.
	5. Seguridad: si se interrumpe durante el borrado, algunos micro‑files pueden quedar; re‑ejecutar con `--force` recrea `compact.parquet` y `--delete-originals` limpia lo restante.
	6. Idempotencia: repetir compactación sin nuevos micro‑files produce el mismo `compact.parquet` (salvo orden interno). `--force` garantiza regeneración si cambiaste la lógica de dedupe.
	7. Dedupe keys: el script infiere claves según columnas (`minute_ts+zone+tech`, `minute_ts+country`, `hour_ts+zone+source`, etc.). Esto evita colapsar dimensiones. Logs `possible_dimension_collapse` avisan si solo se usa timestamp con columnas de dimensión presentes.

	### Verificación rápida del fichero compacto local

	Una vez generado:
	```powershell
	python - <<'PY'
	import pandas as pd, glob
	path = r'.\\data\\curated\\prices\\year=2025\\month=09\\compact.parquet'
	df = pd.read_parquet(path)
	print('ROWS', len(df), 'COLS', list(df.columns))
	print(df.head())
	PY
	```
	Comparar vs suma de micro‑files:
	```powershell
	python - <<'PY'
	import pandas as pd, glob
	files = glob.glob(r'.\\data\\curated\\prices\\year=2025\\month=09\\day=*\\*.parquet')
	total = sum(len(pd.read_parquet(f)) for f in files)
	print('MICRO_TOTAL', total)
	PY
	```
	La diferencia `MICRO_TOTAL - ROWS` = filas duplicadas eliminadas.
	* No mezcla meses parciales: si necesitas compactar el mes en curso de forma incremental usar `--include-current` y quizá `--force` periódico.

	---

## Lint & Tests

Para mantener calidad y detectar errores pronto:

### Lint (Ruff)
Analiza estilo, errores comunes (Pyflakes) y orden de imports.

Comandos:
```powershell
make lint        # Chequeo
make lint-fix    # Aplica fixes automáticos
make format      # Formateo (similar a black + isort integrados en ruff)
```

### Tests (Pytest)
Ejecución de pruebas unitarias / smoke:
```powershell
make test
```

Actualmente sólo hay un `test_smoke.py` que verifica que el script principal y `compact.py` se importan/ejecutan. Añade más tests creando archivos en `tests/` con prefijo `test_`.

Buenas prácticas:
* Añade un test por bug corregido para evitar regresiones.
* Usa fixtures para reutilizar datos fake.
* Para pruebas de red, simula (mock) respuestas de la API ESIOS.

---

## Arquitectura & Flujo de Ejecución

Componentes principales:

| Archivo / Módulo | Rol | Cuándo se ejecuta |
|------------------|-----|--------------------|
| `pipelines/ingest/main.py` | Punto de entrada CLI de ingesta | Manual (`python ...`) o por Cloud Run Job (Scheduler) |
| `pipelines/ingest/compact.py` | Compactación mensual/semanal | Manual o Scheduler (Cloud Run Job) |
| `pipelines/ingest/esios_client.py` | Cliente HTTP ESIOS con throttling/auth fallback | Importado por `main.py` cuando se lanza ingesta |
| `pipelines/ingest/normalize.py` | Funciones de normalización de datasets | Import al iniciar ingesta; ejecuta solo funciones llamadas |
| `pipelines/ingest/hooks.py` | Hooks de post-proceso / validadores | Llamados según config (`post_hook`, `validators`) |
| `config/ingest.yaml` | Config declarativa (datasets, ventanas, paths) | Leído cada vez que se invoca `main.py` o `compact.py` |
| `scripts/tasks.ps1` | Alias de tareas (entorno Windows) | Invocado manualmente desde PowerShell |
| `Makefile` | Alias de tareas (GNU Make) | Entornos Unix / WSL / CI |
| `tests/*.py` | Pruebas smoke / paquete | Sólo al ejecutar `make test` o `tasks.ps1 test` |

### Ciclo de vida de una ingesta (ejemplo `prices_pvpc`)
1. Cloud Scheduler dispara un Cloud Run Job (cron 20:20) → Contenedor arranca y ejecuta: `python pipelines/ingest/main.py prices_pvpc`.
2. `main.py` lee `config/ingest.yaml` y selecciona dataset.
3. Calcula ventana temporal (`resolve_window`) según estrategia (`next_day_dstsafe`).
4. Llama al cliente ESIOS para cada `indicator_id` (con retries y fallback de cabeceras).
5. Concatena y trimea columnas RAW → escribe CSV (raw partition) si no está vacío.
6. Normaliza según `kind` (prices/wide/long/interconn), aplica hooks y validadores.
7. Filtra al día objetivo (manejo DST) → dedup → escribe Parquet en `curated`.
8. Fin: el Job termina (stateless). Cada ejecución es independiente.

### Ciclo de vida de compactación
1. Scheduler (mensual/semanal) ejecuta: `python pipelines/ingest/compact.py --months-back 3 --force`.
2. Lista tablas curated y detecta meses cerrados (excluye mes actual salvo flag).
3. Lee micro-ficheros de cada mes (excluye `compact.parquet` existente), suma filas.
4. Ordena por timestamp disponible, deduplica (si hay clave temporal) y valida filas.
5. Escribe `compact.parquet` (ZSTD). Opcionalmente borra micro-files (`--delete-originals`).
6. Logs JSON permiten auditoría (ingesta y compactación se pueden correlacionar por timestamp si se añade después un trace-id).

### Import vs Ejecución
Importar `pipelines.ingest` NO dispara descargas: sólo define funciones/clases. El código que realmente se ejecuta está protegido por bloques `if __name__ == "__main__":` en `main.py` y `compact.py`.

### Dónde añadir nueva lógica
* Nuevo dataset → sección `datasets` en `config/ingest.yaml` + posible función en `normalize.py` y mapping en `main.normalize_dataset`.
* Nueva validación → función en `hooks.py`, añadir nombre en `validators` del dataset.
* Post-proceso adicional (por ejemplo enriquecimiento) → `post_hook`.
* Cambio de paths o bucket → editar `paths` / `paths_local` en config (no se tocan scripts).

---

## Lectura híbrida downstream (app / analytics)

Para consumir los datos de forma eficiente:

1. Mes actual (en curso): leer particiones diarias `day=DD/part-*.parquet` (todavía no existe consolidado definitivo).
2. Meses cerrados: leer únicamente `compact.parquet` en `.../year=YYYY/month=MM/`.
3. Unir DataFrames si se necesita un rango que cruza el límite mes-cerrado ↔ mes-actual.

Ventajas:
* Reduce drásticamente el número de ficheros escaneados para históricos.
* Elimina necesidad de re‑listar cientos de `part-*` viejos.
* Idempotente: si recompactas, el nombre sigue siendo `compact.parquet`.

Ejemplo de función (simplificada) para leer un mes:

```python
import fsspec, pandas as pd

def read_month(table_root: str, table: str, year: int, month: int) -> pd.DataFrame:
		base = f"{table_root}/{table}/year={year:04d}/month={month:02d}"
		fs = fsspec.get_fs_token_paths(base)[0]
		compact = f"{base}/compact.parquet"
		if fs.exists(compact):
				return pd.read_parquet(compact, filesystem=fs)
		# Mes actual: recolectar micro-files
		days = [e for e in fs.listdir(base) if isinstance(e.get('name'), str) and 'day=' in e.get('name')]  # type: ignore
		parts = []
		for d in days:
				dname = d.get('name')  # type: ignore
				for inner in fs.listdir(dname):  # type: ignore
						iname = inner.get('name')  # type: ignore
						if iname.endswith('.parquet'):
								parts.append(iname)
		return pd.concat([pd.read_parquet(p, filesystem=fs) for p in parts], ignore_index=True) if parts else pd.DataFrame()
```

Nota: Ajustar si se usan paths locales (`./data/curated`) vs GCS (`gs://.../curated`).

### Consideraciones
* Recompactar un mes cerrado con nuevos datos (reprocesos tardíos) es transparente: se sobrescribe `compact.parquet` (usar `--force`).
* No mezclar micro-files y `compact.parquet` en la misma lectura: prioriza siempre el consolidado si existe.
* Para caché en la app: clave = `{table}:{year}-{month}` + `mtime` del objeto `compact.parquet`.

---

## Programación Cloud (Scheduler + Cloud Run Jobs) para Compactación

### Objetivo
Automatizar: consolidar meses cerrados y eliminar micro‑files tras validación, con un ciclo de higiene periódico.

### Estrategia recomendada
| Job | Frecuencia | Comando | Propósito |
|-----|------------|---------|-----------|
| `compact-monthly` | Día 1 (02:00 local aprox) | `python pipelines/ingest/compact.py --months-back 3 --force` | Generar/actualizar compact del mes anterior (sin borrar aún) |
| `compact-clean` | Día 3 o 5 | `python pipelines/ingest/compact.py --months-back 3 --force --delete-originals` | Borrar micro‑files tras ventana de verificación |
| `compact-weekly` | Lunes 03:00 | `python pipelines/ingest/compact.py --months-back 3 --force` | Recompactar últimos 3 meses por si hubo retrasos |

> Puedes simplificar combinando mensual (con borrado directo) una vez verificado el proceso.

### Service Account (SA) permisos mínimos
* `roles/storage.objectAdmin` (lectura/escritura/borrado en bucket de datos)
* `roles/run.invoker` (para ser invocado por Scheduler)
* `roles/logging.logWriter` (logs a Cloud Logging)

### Despliegue del Job (ejemplo)

```bash
gcloud run jobs deploy compact-monthly \
	--image=europe-southwest1-docker.pkg.dev/$PROJECT/energy/tfm-energy-ingest:latest \
	--region=europe-southwest1 \
	--service-account=compact-sa@$PROJECT.iam.gserviceaccount.com \
	--args="python","pipelines/ingest/compact.py","--months-back","3","--force"
```

Segundo job con borrado diferido:
```bash
gcloud run jobs deploy compact-clean \
	--image=europe-southwest1-docker.pkg.dev/$PROJECT/energy/tfm-energy-ingest:latest \
	--region=europe-southwest1 \
	--service-account=compact-sa@$PROJECT.iam.gserviceaccount.com \
	--args="python","pipelines/ingest/compact.py","--months-back","3","--force","--delete-originals"
```

### Scheduler (HTTP → Run Job)

```bash
gcloud scheduler jobs create http compact-monthly \
	--schedule="0 1 1 * *" \
	--uri="https://europe-southwest1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT/jobs/compact-monthly:run" \
	--http-method=POST \
	--oauth-service-account-email=compact-sa@$PROJECT.iam.gserviceaccount.com \
	--location=europe-southwest1
```

Semanal:
```bash
gcloud scheduler jobs create http compact-weekly \
	--schedule="0 2 * * 1" \
	--uri="https://europe-southwest1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT/jobs/compact-weekly:run" \
	--http-method=POST \
	--oauth-service-account-email=compact-sa@$PROJECT.iam.gserviceaccount.com \
	--location=europe-southwest1
```

### Logs y observabilidad
Filtrar ejecuciones exitosas con consolidado:
```
resource.type="cloud_run_job" AND jsonPayload.action="run_summary" AND jsonPayload.months_processed>0
```

Alertar si `months_processed=0` dos ejecuciones seguidas (posible fallo de listado o falta de datos).

### Checklist antes de activar borrado
1. Al menos una ejecución mensual sin `--delete-originals` verificada.
2. Revisión manual de filas compact vs suma micro (script `qc_month.py`).
3. Confirmar que la app lee `compact.parquet` correctamente.
4. Activar job con `--delete-originals`.

### Recuperación ante error
* Si se borraron micro‑files y detectas incidencia: re‑ingesta día a día (backfill range) y recompacta con `--force`.
* Considera habilitar versioning en el bucket para poder restaurar `compact.parquet` previo.

---

## Próximos pasos propuestos
* (Opcional) Añadir métrica explícita de `rows_before_dedupe` vs `rows_after_dedupe` en `compact.py`.
* (Opcional) Exportar logs clave a BigQuery para auditoría histórica.
* (Opcional) Implementar checksum/manifest por mes para detección de drift.
* (Opcional) Añadir script `scripts/read_hybrid.py` con la función de lectura híbrida lista para usar.


---
