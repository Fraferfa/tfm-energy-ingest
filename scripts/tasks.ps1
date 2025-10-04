param(
    [Parameter(Position=0)][string]$Task,
    [string]$d,
    [string]$DS,
    [string]$start,
    [string]$end
)

$ErrorActionPreference = 'Stop'

function Header($msg) { Write-Host "==> $msg" -ForegroundColor Cyan }

$python = Join-Path -Path ".venv" -ChildPath "Scripts/python.exe"
$ruff   = Join-Path -Path ".venv" -ChildPath "Scripts/ruff.exe"

if (-not (Test-Path $python)) {
    Write-Host "No existe .venv. Ejecuta: python -m venv .venv; .\\.venv\\Scripts\\Activate.ps1; pip install -r requirements.txt" -ForegroundColor Yellow
}

switch ($Task) {
    'install' {
        Header 'Instalando dependencias'
        if (-not (Test-Path '.venv')) { python -m venv .venv }
        .\.venv\Scripts\Activate.ps1
        pip install -r requirements.txt
    }
    'lint' {
        & $ruff check .
    }
    'lint-fix' {
        & $ruff check --fix .
    }
    'format' {
        & $ruff format .
    }
    'test' {
        & $python -m pytest -q
    }
    'smoke' {
        & $python scripts/test.py
    }
    'ingest-all' {
        $datasets = @('prices_pvpc','prices_spot','demand','gen_mix','interconn')
        foreach ($ds in $datasets) {
            Header "Ingest $ds"
            & $python pipelines/ingest/main.py $ds
        }
    }
    'ingest' {
        param([string]$Dataset)
        if (-not $DS) { Write-Error 'Usa -DS <dataset>' }
        & $python pipelines/ingest/main.py $DS
    }
    'backfill-day' {
        if (-not $d -or -not $DS) { Write-Error 'Usa -d YYYY-MM-DD -DS <dataset>' }
        & $python pipelines/ingest/main.py $DS --backfill-day $d
    }
    'backfill-range' {
        if (-not $start -or -not $end -or -not $DS) { Write-Error 'Usa -start YYYY-MM-DD -end YYYY-MM-DD -DS <dataset>' }
        $range = "$start`:$end"  # Escapar ':' para evitar interpretación PowerShell
        & $python pipelines/ingest/main.py $DS --backfill-range $range
    }
    'compact' {
        & $python pipelines/ingest/compact.py --months-back 3 --force
    }
    'compact-dry' {
        & $python pipelines/ingest/compact.py --dry-run
    }
    'compact-current' {
        & $python pipelines/ingest/compact.py --include-current --force
    }
    'qc-month' {
        # Uso: tasks.ps1 qc-month -DS <tabla_curated> -y 2025 -m 09
        param([string]$y, [string]$m)
        if (-not $DS -or -not $y -or -not $m) { Write-Error 'Usa -DS <tabla> -y <YYYY> -m <MM>' }
        & $python scripts/qc_month.py $DS $y $m --local-root ./data
    }
    Default {
        Write-Host "Tareas disponibles:" -ForegroundColor Cyan
        @(
            'install','lint','lint-fix','format','test','smoke','ingest-all','ingest (-DS)','backfill-day (-d -DS)','backfill-range (-start -end -DS)','compact','compact-dry','compact-current','qc-month (-DS -y -m)'
        ) | ForEach-Object { Write-Host "  $_" }
    }
}
