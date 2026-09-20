"""Keep the default Ruff format/lint gate runnable alongside the runtime suite."""

import subprocess
import sys
from pathlib import Path


def test_default_ruff_format_gate():
    """Check the same project-wide format command as CI."""
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "format", "--check", "."],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_default_ruff_lint_gate():
    """Check the same project-wide lint command as CI."""
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "check", "."],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
