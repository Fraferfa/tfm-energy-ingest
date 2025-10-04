FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ config/
COPY pipelines/ingest/ pipelines/ingest/
COPY scripts/ scripts/
# Copiamos entrypoint al root de la imagen. OJO: cuando montas -v ${PWD}:/app
# se sobreescribe el filesystem de /app con tu host. Para no perder el entrypoint,
# también lo añadiremos al contexto del repo (root) y documentaremos copiarlo allí.
COPY docker/entrypoint.py entrypoint.py
COPY docker/entrypoint.py ./docker_entrypoint_internal.py

# Entrada unificada: "ingest" / "compact" / "qc" o ruta de script
ENTRYPOINT ["python", "entrypoint.py"]
CMD []
