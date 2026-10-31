import importlib.util
import json
import sys
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "verifier",
    Path(__file__).resolve().parents[2] / "scripts/ai/verify.py",
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


@pytest.mark.parametrize("checks", [[], None, [{"command": [], "timeout": 1}]])
def test_invalid_configuration_fails_closed(tmp_path, checks):
    (tmp_path / ".ai").mkdir()
    (tmp_path / ".ai/pipeline.json").write_text(json.dumps({"required_checks": checks}))
    assert verifier.verify(tmp_path) != 0


@pytest.mark.parametrize(
    "command,timeout",
    [
        (["missing-executable-12345"], 1),
        ([sys.executable, "-c", "raise SystemExit(7)"], 1),
        ([sys.executable, "-c", "import time;time.sleep(1)"], 0.01),
    ],
)
def test_command_failure_fails_closed(tmp_path, command, timeout):
    (tmp_path / ".ai").mkdir()
    (tmp_path / ".ai/pipeline.json").write_text(
        json.dumps(
            {"required_checks": [{"name": "failure", "command": command, "timeout": timeout}]}
        )
    )
    assert verifier.verify(tmp_path) != 0
