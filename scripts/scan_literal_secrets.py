from __future__ import annotations

import ast
import re
import subprocess
import sys
import tokenize
from pathlib import Path


SENSITIVE_NAMES = ("api_key", "apikey", "secret", "token", "password", "webhook", "signature")
SAFE_MARKERS = ("example", "placeholder", "replace", "your_", "unknown", "injected", "do-not-log")


def assigned_names(node: ast.AST) -> list[str]:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        return [name for item in node.elts for name in assigned_names(item)]
    return []


def scan(path: Path) -> list[tuple[int, str]]:
    try:
        with tokenize.open(path) as handle:
            tree = ast.parse(handle.read(), filename=str(path))
    except (SyntaxError, UnicodeError):
        return []
    findings = []
    for node in ast.walk(tree):
        pairs = []
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            value = node.value
            pairs = [(name, value) for target in targets for name in assigned_names(target)]
        for name, value in pairs:
            normalized = name.casefold()
            if not any(marker in normalized for marker in SENSITIVE_NAMES):
                continue
            if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
                continue
            literal = value.value.strip()
            if len(literal) < 8 or any(marker in literal.casefold() for marker in SAFE_MARKERS):
                continue
            findings.append((node.lineno, name))
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if not (
                    isinstance(key, ast.Constant) and isinstance(key.value, str)
                    and isinstance(value, ast.Constant) and isinstance(value.value, str)
                ):
                    continue
                if not any(marker in key.value.casefold() for marker in SENSITIVE_NAMES):
                    continue
                literal = value.value.strip()
                if len(literal) >= 8 and not any(marker in literal.casefold() for marker in SAFE_MARKERS):
                    findings.append((node.lineno, f"dict:{key.value}"))
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            literal = node.value.strip()
            if re.search(r"https?://[^/\s:@]+:[^/\s@]+@", literal):
                findings.append((getattr(node, "lineno", 0), "credentialed_url"))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            positional = list(node.args.posonlyargs) + list(node.args.args)
            defaults = [None] * (len(positional) - len(node.args.defaults)) + list(node.args.defaults)
            defaults += list(node.args.kw_defaults)
            arguments = positional + list(node.args.kwonlyargs)
            for argument, value in zip(arguments, defaults):
                if not any(marker in argument.arg.casefold() for marker in SENSITIVE_NAMES):
                    continue
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    literal = value.value.strip()
                    if len(literal) >= 8 and not any(marker in literal.casefold() for marker in SAFE_MARKERS):
                        findings.append((node.lineno, f"default:{argument.arg}"))
    return findings


def deployment_python_files(root: Path) -> list[Path]:
    """Return versioned and deployable Python files, honoring .gitignore."""
    git_dir = root / ".version-history"
    if not git_dir.is_dir():
        return list(root.rglob("*.py"))
    result = subprocess.run(
        [
            "git",
            f"--git-dir={git_dir}",
            f"--work-tree={root}",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            "*.py",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return [root / line for line in result.stdout.splitlines() if line]


def main(roots: list[str]) -> int:
    matches = []
    selected_roots = roots or ["."]
    for root in selected_roots:
        path = Path(root)
        files = (
            [path]
            if path.is_file()
            else deployment_python_files(path.resolve())
            if not roots
            else path.rglob("*.py")
        )
        for file in files:
            if any(part in {".test-deps", ".venv", "node_modules", "__pycache__"} for part in file.parts):
                continue
            for line, name in scan(file):
                matches.append(f"{file}:{line}:{name}")
    if matches:
        print("\n".join(sorted(matches)))
        return 1
    print("No non-placeholder sensitive string assignments found.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
