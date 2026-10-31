"""Fail-closed project verifier. Run with the project's configured Python."""

import argparse
import json
import subprocess
import sys
from pathlib import Path


def verify(project: Path, integration: bool = False) -> int:
    try:
        config = json.loads((project / ".checks/pipeline.json").read_text(encoding="utf-8"))
        checks = config["required_checks"]
        if not isinstance(checks, list) or not checks:
            raise ValueError("required_checks must be a nonempty list")
        if integration:
            extra = config["integration_checks"]
            if not isinstance(extra, list) or not extra:
                raise ValueError("integration_checks must be a nonempty list")
            checks = checks + extra
        # Validate all commands before running any.
        for check in checks:
            command, timeout = check["command"], check["timeout"]
            if (
                not isinstance(command, list)
                or not command
                or any(not isinstance(arg, str) or not arg.strip() for arg in command)
                or not isinstance(timeout, (int, float))
                or not 0 < timeout <= 600
            ):
                raise ValueError("Each check requires argv and timeout (0, 600]")
        for check in checks:
            command = [arg.replace("{python}", sys.executable) for arg in check["command"]]
            print(f"CHECK {check['name']}: {' '.join(command)}", flush=True)
            result = subprocess.run(command, cwd=project, timeout=check["timeout"], check=False)
            if result.returncode:
                return result.returncode if result.returncode > 0 else 1
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.TimeoutExpired) as exc:
        print(f"VERIFICATION FAILED: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=Path, default=Path("."))
    parser.add_argument("--integration", action="store_true")
    args = parser.parse_args()
    sys.exit(verify(args.project.resolve(), args.integration))
