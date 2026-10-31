import ast
from pathlib import Path


def test_application_and_domain_have_no_framework_imports():
    root = Path(__file__).resolve().parents[2] / "paypong"
    forbidden = {"fastapi", "faststream", "sqlalchemy", "asyncpg", "httpx", "pydantic"}
    for layer in ("domain", "application"):
        for path in (root / layer).glob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                modules = []
                if isinstance(node, ast.Import):
                    modules = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    modules = [node.module]
                for module in modules:
                    assert module.split(".")[0] not in forbidden, (path, module)
                    assert not module.startswith(
                        ("paypong.infrastructure", "paypong.presentation")
                    ), (path, module)
