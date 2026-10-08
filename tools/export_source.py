#!/usr/bin/env python3
"""逐文件白名单导出Python公开源码快照；无Git历史、私有合同或发布动作。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

sys.dont_write_bytecode = True
from contract_check import (  # noqa: E402 -- disable bytecode before local tool import
    LOCK_HASH, PUBLIC_CANDIDATES, PUBLIC_METADATA, check, read_asset, strict_json,
)
from contract_export import outside_source, public_payloads  # noqa: E402
from generate_api import generate, verify  # noqa: E402


# 逐文件评审范围，不按目录递归复制；新增源码/工具须显式更新此集合。
SOURCE_FILES = (
    ".gitattributes",
    ".github/workflows/ci.yml",
    ".github/workflows/publish.yml",
    ".gitignore",
    "LICENSE",
    "MANIFEST.in",
    "README.md",
    "demo/LICENSE",
    "demo/MANIFEST.in",
    "demo/README.md",
    "demo/pyproject.toml",
    "demo/src/tansr_demo/__init__.py",
    "demo/src/tansr_demo/archive.py",
    "demo/src/tansr_demo/async_chat.py",
    "demo/src/tansr_demo/chat.py",
    "demo/src/tansr_demo/common.py",
    "demo/src/tansr_demo/py.typed",
    "demo/src/tansr_demo/tools.py",
    "doc/guide.md",
    "doc/使用指南.md",
    "doc/发行说明.md",
    "integration/archive.py",
    "integration/demos.py",
    "integration/executor.py",
    "integration/host.py",
    "integration/session.py",
    "integration/session_extra.py",
    "integration/session_loss.py",
    "pyproject.toml",
    "requirements/README.md",
    "requirements/quality-modern.lock",
    "requirements/runtime-modern.lock",
    "requirements/runtime-py37.lock",
    "requirements/runtime-py38.lock",
    "requirements/runtime-py39.lock",
    "requirements/tools-modern.lock",
    "requirements/tools-py37.lock",
    "src/tansr_sdk/__init__.py",
    "src/tansr_sdk/_resolver.py",
    "src/tansr_sdk/api/__init__.py",
    "src/tansr_sdk/api/_schema_data.py",
    "src/tansr_sdk/api/client.py",
    "src/tansr_sdk/api/operations.py",
    "src/tansr_sdk/api/schema.py",
    "src/tansr_sdk/api/types.py",
    "src/tansr_sdk/archive/__init__.py",
    "src/tansr_sdk/archive/_validation.py",
    "src/tansr_sdk/archive/async_client.py",
    "src/tansr_sdk/archive/client.py",
    "src/tansr_sdk/archive/intent.py",
    "src/tansr_sdk/archive/material.py",
    "src/tansr_sdk/archive/store.py",
    "src/tansr_sdk/archive/sync.py",
    "src/tansr_sdk/archive/types.py",
    "src/tansr_sdk/canonical.py",
    "src/tansr_sdk/errors.py",
    "src/tansr_sdk/executor/__init__.py",
    "src/tansr_sdk/executor/_common.py",
    "src/tansr_sdk/executor/async_client.py",
    "src/tansr_sdk/executor/client.py",
    "src/tansr_sdk/executor/journal.py",
    "src/tansr_sdk/executor/output.py",
    "src/tansr_sdk/executor/runner.py",
    "src/tansr_sdk/executor/tool.py",
    "src/tansr_sdk/lifecycle.py",
    "src/tansr_sdk/py.typed",
    "src/tansr_sdk/session/__init__.py",
    "src/tansr_sdk/session/_validation.py",
    "src/tansr_sdk/session/async_client.py",
    "src/tansr_sdk/session/client.py",
    "src/tansr_sdk/session/events.py",
    "src/tansr_sdk/session/types.py",
    "src/tansr_sdk/sse.py",
    "src/tansr_sdk/storage/__init__.py",
    "src/tansr_sdk/storage/_posix.py",
    "src/tansr_sdk/storage/_windows.py",
    "src/tansr_sdk/storage/encrypted_store.py",
    "src/tansr_sdk/storage/private_directory.py",
    "src/tansr_sdk/strict_json.py",
    "src/tansr_sdk/transport.py",
    "tests/test_api.py",
    "tests/test_api_adversarial.py",
    "tests/test_archive.py",
    "tests/test_contract_public.py",
    "tests/test_contract_runtime.py",
    "tests/test_contract_schema.py",
    "tests/test_demo.py",
    "tests/test_executor.py",
    "tests/test_executor_adversarial.py",
    "tests/test_executor_node_vectors.py",
    "tests/test_json.py",
    "tests/test_public_api.py",
    "tests/test_session.py",
    "tests/test_source_export.py",
    "tests/test_sse.py",
    "tests/test_storage.py",
    "tests/test_storage_security.py",
    "tests/test_transport.py",
    "tools/check.py",
    "tools/contract_check.py",
    "tools/contract_export.py",
    "tools/environment_info.py",
    "tools/export_source.py",
    "tools/generate_api.py",
    "tools/package_check.py",
)
MANIFEST = "SOURCE-MANIFEST.json"
REPOSITORY = "https://github.com/tansrai/tansr-python"
PUBLIC_AGENTS = """# Tansr Python SDK contributor guide

This repository contains the Python 3.7+ SDK and three public API demos under MIT.
Serve owns the agent core; device tools must not silently fall back to its host.

- Preserve frozen UAPI revision 7 bytes and generated operation/schema tables.
  This public distribution contains 20 approved contract assets. Use explicit
  --mode public with tools/contract_check.py and tools/generate_api.py --check.
  Never infer a different mode from missing assets.
- Run tools/check.py --contract-mode public --evidence <outside-source-directory>
  and supply --quality-python <modern-python> for the pinned lint/type tools.
  Public CI runs the common/public tests and builds both packages. The internal
  39-asset maintenance test is not part of this distribution.
- The seven integration drivers require an explicitly supplied compatible Serve
  fixture and provenance. No private Serve bundle or credentials are included;
  public CI does not claim that those separate integration gates ran.
- Keep Python 3.7 syntax, sync/async ownership, cancellation, authorization,
  original write identities, durable ACK recovery and material boundaries intact.
- Read the bilingual guides for installation, TLS runtime prerequisites and
  separate SDK/Demo packages. Source tests do not prove installed-only consumption.
- Do not commit credentials, build outputs, logs or user journals. A source export
  is a reviewed snapshot, not proof of a published tag or PyPI release.
"""


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def expected_paths():
    if len(SOURCE_FILES) != len(set(SOURCE_FILES)):
        raise ValueError("duplicate reviewed source path")
    return set(SOURCE_FILES) | {
        "contract/" + name for name in PUBLIC_CANDIDATES + PUBLIC_METADATA
    } | {"AGENTS.md", MANIFEST}


def require_licenses(payloads):
    for name in ("LICENSE", "demo/LICENSE"):
        if b"MIT License" not in payloads[name]:
            raise ValueError("reviewed MIT license required: " + name)


def export_source(root, output, *, contract_mode="internal"):
    root, output = Path(root), Path(output)
    outside_source(root, output)
    expected_paths()
    contract = public_payloads(root / "contract", source_mode=contract_mode)
    verify(root, generate(root, mode=contract_mode))
    # 所有输入读完且核验后再创建输出，不默认外发未审文件或私有Git历史。
    payloads = {name: read_asset(root, name) for name in SOURCE_FILES}
    require_licenses(payloads)
    payloads.update({"contract/" + name: raw for name, raw in contract.items()})
    payloads["AGENTS.md"] = PUBLIC_AGENTS.encode("utf-8")
    manifest = {
        "format": "tansr-python-public-source-v1", "distribution": "public",
        "license": "MIT", "repository": REPOSITORY, "sourceLockSha256": LOCK_HASH,
        "files": [
            {"path": name, "bytes": len(raw), "sha256": digest(raw)}
            for name, raw in sorted(payloads.items())
        ],
    }
    payloads[MANIFEST] = (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    output.mkdir(exist_ok=False)
    for name, raw in sorted(payloads.items()):
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(raw)
    verify_export(output)
    return manifest


def verify_export(root):
    root = Path(root)
    allowed = expected_paths()
    expected_directories = {
        parent.as_posix() for name in allowed for parent in Path(name).parents if str(parent) != "."
    }
    # 先读manifest也验证根不是链接；逐层不跟随链接遍历，拒绝额外Git/私有目录。
    manifest = strict_json(read_asset(root, MANIFEST))
    files, directories = set(), set()
    for current, children, names in os.walk(str(root), followlinks=False):
        for name in children + names:
            path = Path(current) / name
            relative = path.relative_to(root).as_posix()
            attributes = path.lstat()
            if stat.S_ISLNK(attributes.st_mode) or getattr(attributes, "st_file_attributes", 0) & 0x400:
                raise ValueError("links/reparse points forbidden in public source")
            if stat.S_ISDIR(attributes.st_mode):
                directories.add(relative)
            elif stat.S_ISREG(attributes.st_mode):
                files.add(relative)
            else:
                raise ValueError("non-regular public source entry: " + relative)
    if files != allowed or directories != expected_directories:
        raise ValueError("public source inventory has missing or unexpected entries")
    fields = {"format", "distribution", "license", "repository", "sourceLockSha256", "files"}
    if (not isinstance(manifest, dict) or set(manifest) != fields
            or manifest["format"] != "tansr-python-public-source-v1"
            or manifest["distribution"] != "public" or manifest["license"] != "MIT"
            or manifest["repository"] != REPOSITORY or manifest["sourceLockSha256"] != LOCK_HASH):
        raise ValueError("public source manifest identity mismatch")
    entries = manifest["files"]
    if (not isinstance(entries, list) or len(entries) != len(allowed) - 1
            or any(not isinstance(item, dict) or set(item) != {"path", "bytes", "sha256"} for item in entries)
            or any(not isinstance(item["path"], str) for item in entries)
            or {item["path"] for item in entries} != allowed - {MANIFEST}):
        raise ValueError("public source manifest file inventory mismatch")
    for item in entries:
        raw = read_asset(root, item["path"])
        if type(item["bytes"]) is not int or item["bytes"] != len(raw) or item["sha256"] != digest(raw):
            raise ValueError("public source bytes differ: " + item["path"])
    if read_asset(root, "AGENTS.md") != PUBLIC_AGENTS.encode("utf-8"):
        raise ValueError("public contributor guide differs from reviewed text")
    require_licenses({name: read_asset(root, name) for name in ("LICENSE", "demo/LICENSE")})
    check(root / "contract", mode="public")
    verify(root, generate(root, mode="public"))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--contract-mode", choices=("internal", "public"), default="internal")
    command = parser.add_mutually_exclusive_group(required=True)
    command.add_argument("--output", type=Path, help="源树外尚不存在的新目录")
    command.add_argument("--verify", action="store_true", help="只检查干净公开快照，不修改")
    args = parser.parse_args()
    if args.verify:
        if args.contract_mode != "public":
            parser.error("--verify requires explicit --contract-mode public")
        manifest = verify_export(args.root)
    else:
        manifest = export_source(args.root, args.output, contract_mode=args.contract_mode)
    print("public-source: {} reviewed files plus manifest; no history, upload or release".format(
        len(manifest["files"])))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("public-source failed: " + str(error), file=sys.stderr)
        sys.exit(1)
