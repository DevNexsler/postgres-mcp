"""Keep the default type gate runnable alongside the runtime regression suite."""

import json
import subprocess
import sys
from pathlib import Path


def test_default_pyright_gate():
    """Check the same project/configuration as CI, including test fixtures."""
    result = subprocess.run(
        [sys.executable, "-m", "pyright", "--outputjson"],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    report = json.loads(result.stdout)
    assert result.returncode == 0, report["generalDiagnostics"]
    assert report["summary"]["errorCount"] == 0
