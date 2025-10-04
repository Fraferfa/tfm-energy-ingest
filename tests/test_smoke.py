import importlib
import subprocess
import sys


def test_import_main():
    mod = importlib.import_module("pipelines.ingest.main")
    assert hasattr(mod, "load_cfg")


def test_help_compact_script():
    # Run the compact script with --help to ensure it executes
    result = subprocess.run(
        [sys.executable, "pipelines/ingest/compact.py", "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    # Aceptar que el help contenga la palabra 'compact' independientemente de capitalización
    assert "compact" in (result.stdout + result.stderr).lower()
