"""集中本地门：合同、生成、测试及可选现代静态检查；不发布、不调用远端CI。"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--quality-python", help="独立现代质量工具解释器；省略则只执行运行门")
    parser.add_argument(
        "--contract-mode", choices=("internal", "public"), default="internal",
        help="显式选择合同及测试范围；默认internal，不因缺少内部资产自动降级",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    evidence = args.evidence.resolve()
    if root == evidence or root in evidence.parents:
        raise ValueError("evidence must be outside the source tree")
    evidence.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    env.pop("PYTHONHOME", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    # 保留系统临时目录位置，只消除macOS /var等别名；不放宽SDK逐层拒绝链接的检查。
    temporary_directory = str(Path(tempfile.gettempdir()).resolve())
    for variable in ("TMPDIR", "TEMP", "TMP"):
        env[variable] = temporary_directory
    excluded_tests = ["tests/test_contract_assets.py"] if args.contract_mode == "public" else []
    test_command = [
        sys.executable, "-m", "pytest", "tests", "-q", "-p", "no:cacheprovider",
        "--junitxml=" + str(evidence / "tests.xml"),
    ]
    test_command.extend("--ignore=" + path for path in excluded_tests)
    print("contract mode: " + args.contract_mode, flush=True)
    if excluded_tests:
        print("explicitly excluded internal tests: " + ", ".join(excluded_tests), flush=True)
    print("real Serve: not run (requires a separately supplied authorized fixture)", flush=True)
    commands = [
        ("environment", [sys.executable, str(root / "tools/environment_info.py")]),
        (
            "contract",
            [sys.executable, str(root / "tools/contract_check.py"), "--mode", args.contract_mode],
        ),
        (
            "generated",
            [sys.executable, str(root / "tools/generate_api.py"), "--mode", args.contract_mode, "--check"],
        ),
        ("persistence-generated", [sys.executable, str(root / "tools/generate_terminal_persistence.py"), "--check"]),
        ("tests", test_command),
    ]
    if args.quality_python:
        commands += [
            ("lint", [args.quality_python, "-m", "ruff", "check", "src", "demo", "tests", "integration", "tools"]),
            ("types", [args.quality_python, "-m", "mypy", "src/tansr_sdk"]),
        ]
    source = {}
    for directory in ("src", "demo", "tests", "integration", "tools", "contract", "contract-persistence", "requirements"):
        for path in sorted((root / directory).rglob("*")):
            if (
                path.is_file()
                and not any(part in ("__pycache__", ".pytest_cache", ".mypy_cache") for part in path.parts)
                and path.suffix not in (".pyc", ".pyo")
            ):
                source[str(path.relative_to(root)).replace("\\", "/")] = hashlib.sha256(path.read_bytes()).hexdigest()
    receipt = {
        "python": sys.version, "executable": sys.executable, "source": source, "gates": [],
        "contractMode": args.contract_mode, "excludedTestFiles": excluded_tests,
        "temporaryDirectory": temporary_directory,
        "realServe": "not-run: requires a separately supplied authorized fixture",
    }
    failed = False
    try:
        for name, command in commands:
            start = time.monotonic()
            with (evidence / (name + ".log")).open("wb") as log:
                result = subprocess.run(command, cwd=str(root), env=env, stdout=log, stderr=subprocess.STDOUT)
            receipt["gates"].append(
                {
                    "name": name,
                    "command": command,
                    "exitCode": result.returncode,
                    "seconds": round(time.monotonic() - start, 3),
                }
            )
            print(name + ": " + ("PASS" if result.returncode == 0 else "FAIL"), flush=True)
            failed = failed or result.returncode != 0
        changed = [
            name
            for name, digest in source.items()
            if not (root / name).is_file() or hashlib.sha256((root / name).read_bytes()).hexdigest() != digest
        ]
        receipt["sourceChangedDuringGate"] = changed
        failed = failed or bool(changed)
        receipt["status"] = "failed" if failed else "passed"
    finally:
        (evidence / "receipt.json").write_text(
            json.dumps(receipt, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
