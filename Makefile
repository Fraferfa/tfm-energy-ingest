# Makefile for tfm-energy-ingest
# Uso rápido: ajusta VARIABLES si es necesario.

PYTHON?=python
VENVDIR?=.venv
ACTIVATE?=$(VENVDIR)/Scripts/activate
DATASETS=prices_pvpc prices_spot demand gen_mix interconn

# ============ Setup ============
.PHONY: venv install update-deps

venv:
	$(PYTHON) -m venv $(VENVDIR)

install: venv
	"$(VENVDIR)/Scripts/pip" install -r requirements.txt

update-deps:
	"$(VENVDIR)/Scripts/pip" freeze > requirements.lock

# ============ Ingest ============
.PHONY: ingest ingest-% backfill-day backfill-range

ingest:  ## Ejecuta ingesta de todos los datasets (última ventana)
	@for ds in $(DATASETS); do \
	  echo "==> Ingest $$ds"; \
	  "$(VENVDIR)/Scripts/python" pipelines/ingest/main.py $$ds || exit 1; \
	done

ingest-%:  ## Ejecuta ingesta de un dataset: make ingest-prices_pvpc
	"$(VENVDIR)/Scripts/python" pipelines/ingest/main.py $*

backfill-day:  ## make backfill-day d=2025-09-20 DS=prices_pvpc
	@if [ -z "$(d)" ]; then echo "Falta fecha d=YYYY-MM-DD"; exit 1; fi
	@if [ -z "$(DS)" ]; then echo "Falta dataset DS=name"; exit 1; fi
	"$(VENVDIR)/Scripts/python" pipelines/ingest/main.py $(DS) --backfill-day $(d)

backfill-range: ## make backfill-range start=2025-09-01 end=2025-09-30 DS=prices_pvpc
	@if [ -z "$(start)" ] || [ -z "$(end)" ]; then echo "Falta start/end"; exit 1; fi
	@if [ -z "$(DS)" ]; then echo "Falta dataset DS=name"; exit 1; fi
	"$(VENVDIR)/Scripts/python" pipelines/ingest/main.py $(DS) --backfill-range $(start):$(end)

# ============ Compaction ============
.PHONY: compact dry-compact compact-current

compact:  ## Compactar todos los meses cerrados
	"$(VENVDIR)/Scripts/python" pipelines/ingest/compact.py --months-back 3 --force

dry-compact:
	"$(VENVDIR)/Scripts/python" pipelines/ingest/compact.py --dry-run

compact-current:
	"$(VENVDIR)/Scripts/python" pipelines/ingest/compact.py --include-current --force

# ============ Utilidades ============
.PHONY: smoke test lint lint-fix format

smoke:
	"$(VENVDIR)/Scripts/python" scripts/test.py

lint:
	"$(VENVDIR)/Scripts/ruff" check .

lint-fix:
	"$(VENVDIR)/Scripts/ruff" check --fix .

format:
	"$(VENVDIR)/Scripts/ruff" format .

# Placeholder para tests futuros
test:
	"$(VENVDIR)/Scripts/python" -m pytest -q

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?##' Makefile | awk 'BEGIN {FS=":.*?##"}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}' | sort

.DEFAULT_GOAL := help
