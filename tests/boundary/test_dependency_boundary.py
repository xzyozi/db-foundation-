"""依存境界テスト: 標準ライブラリのみ制約・層間依存方向・contrib 非依存を AST で検査する。"""

from __future__ import annotations

import ast
from pathlib import Path
import sys
import unittest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "storage_kernel"


def _layer_of(py_file: Path) -> str:
    rel = py_file.relative_to(SRC_ROOT)
    return rel.parts[0] if len(rel.parts) > 1 else "__root__"


def _resolve_absolute_import(py_file: Path, node: ast.ImportFrom) -> str | None:
    if node.level == 0:
        return node.module
    package_parts = list(py_file.relative_to(SRC_ROOT.parent).with_suffix("").parts)
    if py_file.name == "__init__.py":
        package_parts = package_parts[:-1]
    else:
        package_parts = package_parts[:-1]
    ascend = node.level - 1
    if ascend > len(package_parts):
        return None
    base_parts = package_parts[: len(package_parts) - ascend]
    if node.module:
        return ".".join([*base_parts, node.module])
    return ".".join(base_parts)


class TestDependencyBoundary(unittest.TestCase):
    """DBF-BD-001 §2.1 および DBF-ENV-001 §2（外部依存ゼロ・依存の向き）を検証する。"""

    def test_zero_third_party_dependencies(self) -> None:
        """src/storage_kernel 配下の全モジュールが Python 標準ライブラリと自パッケージのみを import すること。"""
        stdlib = sys.stdlib_module_names
        violations: list[str] = []

        for py_file in sorted(SRC_ROOT.rglob("*.py")):
            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        top = alias.name.split(".", 1)[0]
                        if top != "storage_kernel" and top not in stdlib:
                            violations.append(f"{py_file.relative_to(SRC_ROOT)}: third-party import {alias.name!r}")
                elif isinstance(node, ast.ImportFrom):
                    resolved = _resolve_absolute_import(py_file, node)
                    if resolved is None:
                        continue
                    top = resolved.split(".", 1)[0]
                    if top != "storage_kernel" and top not in stdlib:
                        violations.append(f"{py_file.relative_to(SRC_ROOT)}: third-party import {resolved!r}")

        self.assertEqual(violations, [])

    def test_layer_dependency_directions_and_no_contrib_imports(self) -> None:
        """core/runtime/ports/contracts が contrib や testing を import せず、依存方向を守ること。"""
        forbidden_by_layer: dict[str, set[str]] = {
            "contracts": {"ports", "core", "runtime", "testing", "contrib"},
            "ports": {"core", "runtime", "testing", "contrib"},
            "core": {"testing", "contrib"},
            "runtime": {"core", "testing", "contrib"},
            "testing": {"contrib"},
        }
        violations: list[str] = []

        for py_file in sorted(SRC_ROOT.rglob("*.py")):
            layer = _layer_of(py_file)
            forbidden = forbidden_by_layer.get(layer)
            if not forbidden:
                continue

            tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
            for node in ast.walk(tree):
                target: str | None = None
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("storage_kernel."):
                            target = alias.name.split(".", 2)[1]
                            if target in forbidden:
                                violations.append(f"{py_file.relative_to(SRC_ROOT)} imports {alias.name!r}")
                elif isinstance(node, ast.ImportFrom):
                    resolved = _resolve_absolute_import(py_file, node)
                    if resolved and resolved.startswith("storage_kernel."):
                        parts = resolved.split(".")
                        if len(parts) >= 2 and parts[1] in forbidden:
                            violations.append(f"{py_file.relative_to(SRC_ROOT)} imports {resolved!r}")

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
