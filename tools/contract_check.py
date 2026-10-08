#!/usr/bin/env python3
"""校验Python冻结合同；模式必须显式指定，缺件不会自动降级。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import sys

COMMIT = "83c64b2c519623a79994c942a5132300eb8174d4"
SCHEMA_HASH = "b60e77ffcbf08d985a993dbdbd4cf610f12f7c7e70f090aee5ff8d523f70bb57"
MANIFEST_HASH = "f6255bc868b4de5cb2af759a55069ab4651027d4125ef86d37f14a91d3307288"
FILES_HASH = "e4d377f8a4c00be3f27c94670b4bc389fc4ba6ae62eeece8178391fe28d8ca77"
LOCK_HASH = "61fdcd48fca1629e99bca9c963937209ecac3abddee7fb133c870a01f25efdfe"

# 固定公开范围；实际授权以EXPORT-POLICY为准，不能仅凭此列表外发。
PUBLIC_CANDIDATES = (
    "api-error-map.json", "api-manifest.json", "archive-sync-v1.schema.json",
    "canonical-cross-vectors.json", "sdk2-archive-recovery-v1.golden.json",
    "sdk2-archive-recovery-v1.schema.json", "sdk2-cache-core-v1.schema.json",
    "sdk2-cache-v1.schema.json", "sdk2-ext-v1.schema.json", "sdk2-wire-v1.json",
    "terminal-observation-v1.golden.json", "terminal-observation-v1.schema.json",
    "terminal-profile-v1.golden.json", "terminal-profile-v1.schema.json",
    "terminal-services-v1.golden.json", "terminal-services-v1.schema.json",
    "terminal-shell-sandbox-v1.golden.json", "terminal-shell-sandbox-v1.schema.json",
    "unified-v1.golden.json", "unified-v1.schema.json",
)
METADATA = ("LOCK.json", "PROVENANCE.json", "DISTRIBUTION.json", "EXPORT-POLICY.json", "README.md")
PUBLIC_METADATA = METADATA


def strict_json(raw):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate JSON key: " + key)
            result[key] = value
        return result

    def invalid_constant(value):
        raise ValueError("non-finite JSON constant: " + value)

    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", "strict")
    return json.loads(raw, object_pairs_hook=unique_object, parse_constant=invalid_constant)


def checked_path(root, relative):
    if (not isinstance(relative, str) or not relative or "\\" in relative
            or ":" in relative or any(ord(char) < 32 for char in relative)
            or any(part in ("", ".", "..") for part in relative.split("/"))):
        raise ValueError("contract path must be canonical and relative")
    root = Path(root).resolve()
    path = root / relative
    resolved = path.resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("contract path escapes root")
    return path


def _no_link(path):
    attributes = path.lstat()
    if (stat.S_ISLNK(attributes.st_mode)
            or getattr(attributes, "st_file_attributes", 0) & 0x400):
        raise ValueError("contract links/reparse points are forbidden: " + str(path))
    return attributes


def read_asset(root, relative):
    root = Path(root)
    _no_link(root)
    path = checked_path(root, relative)
    current = root
    for part in relative.split("/"):
        current = current / part
        attributes = _no_link(current)
    if not stat.S_ISREG(attributes.st_mode):
        raise ValueError("contract asset is not a regular file: " + relative)
    return path.read_bytes()


def distribution(mode):
    if mode not in ("internal", "public"):
        raise ValueError("contract mode must be explicit internal or public")
    return {
        "format": "tansr-python-contract-distribution-v1", "scope": mode,
        "sourceLockSha256": LOCK_HASH, "sourceLockFileCount": 39,
        "contractFileCount": 39 if mode == "internal" else len(PUBLIC_CANDIDATES),
    }


def check_policy(root, require_approved=False):
    policy = strict_json(read_asset(root, "EXPORT-POLICY.json"))
    if (not isinstance(policy, dict)
            or set(policy) != {"format", "sourceCommit", "sourceLockSha256", "status", "files", "authorization"}
            or policy["format"] != "tansr-python-contract-export-policy-v1"
            or policy["sourceCommit"] != COMMIT or policy["sourceLockSha256"] != LOCK_HASH
            or policy["files"] != list(PUBLIC_CANDIDATES)):
        raise ValueError("export policy differs from the Python candidate scope")
    if policy["status"] == "pending":
        if policy["authorization"] is not None:
            raise ValueError("pending export policy must not claim authorization")
        if require_approved:
            raise ValueError("Python public contract use is not authorized: policy is pending")
    elif policy["status"] == "approved":
        if not isinstance(policy["authorization"], str) or not policy["authorization"].strip():
            raise ValueError("approved Python policy requires its explicit authorization record")
    else:
        raise ValueError("unknown Python export policy status")
    return policy


def _inventory(root):
    root = Path(root)
    _no_link(root)
    files, directories = set(), set()
    for current, children, names in os.walk(str(root), followlinks=False):
        for name in children + names:
            path = Path(current) / name
            attributes = _no_link(path)
            relative = path.relative_to(root).as_posix()
            if stat.S_ISDIR(attributes.st_mode):
                directories.add(relative)
            elif stat.S_ISREG(attributes.st_mode):
                files.add(relative)
            else:
                raise ValueError("non-regular contract entry: " + relative)
    return files, directories


def check(root, source=None, *, mode=None):
    """内部逐39项，公开逐批准20项；source另外核对全部39个原Git blob。"""
    if mode not in ("internal", "public"):
        raise ValueError("contract mode must be explicit internal or public")
    root = Path(root)
    lock_raw = read_asset(root, "LOCK.json")
    if hashlib.sha256(lock_raw).hexdigest() != LOCK_HASH:
        raise ValueError("Python frozen lock bytes changed")
    lock = strict_json(lock_raw)
    fields = ("sourceCommit", "manifestRevision", "schemaHash", "operationCount", "familyCount")
    if tuple(lock.get(key) for key in fields) != (COMMIT, 7, SCHEMA_HASH, 81, 11):
        raise ValueError("frozen baseline identity changed")
    pinned = json.dumps(lock["files"], sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if len(lock["files"]) != 39 or hashlib.sha256(pinned).hexdigest() != FILES_HASH:
        raise ValueError("frozen file identities changed")
    check_policy(root, require_approved=mode == "public")
    if strict_json(read_asset(root, "DISTRIBUTION.json")) != distribution(mode):
        raise ValueError("distribution declaration does not match explicit " + mode + " mode")
    selected = {item["path"] for item in lock["files"]} if mode == "internal" else set(PUBLIC_CANDIDATES)
    expected_files = selected | set(METADATA)
    expected_directories = set()
    for relative in expected_files:
        parts = relative.split("/")
        expected_directories.update("/".join(parts[:i]) for i in range(1, len(parts)))
    if _inventory(root) != (expected_files, expected_directories):
        raise ValueError(mode + " contract inventory has missing or unexpected entries")
    paths, sources, raw_assets = set(), set(), {}
    for item in lock["files"]:
        checked_path(root, item["path"])
        checked_path(root, item["source"])
        if item["path"] in paths or item["source"] in sources:
            raise ValueError("duplicate asset path or source")
        paths.add(item["path"])
        sources.add(item["source"])
        if item["path"] in selected:
            raw = read_asset(root, item["path"])
            if hashlib.sha256(raw).hexdigest() != item["sha256"]:
                raise ValueError("asset digest mismatch: " + item["path"])
            raw_assets[item["path"]] = raw
        if source is not None:
            original = subprocess.check_output(
                ["git", "show", COMMIT + ":" + item["source"]], cwd=str(source))
            if hashlib.sha256(original).hexdigest() != item["sha256"]:
                raise ValueError("frozen Git object differs: " + item["source"])
            if item["path"] in selected and original != raw_assets[item["path"]]:
                raise ValueError("asset differs from frozen Git object: " + item["path"])
    manifest = strict_json(raw_assets["api-manifest.json"])
    if (hashlib.sha256(raw_assets["api-manifest.json"]).hexdigest() != MANIFEST_HASH
            or len(manifest["operations"]) != 81 or len(manifest["families"]) != 11):
        raise ValueError("frozen operation/family catalogue changed")
    if len(strict_json(raw_assets["unified-v1.golden.json"])["vectors"]) != 165:
        raise ValueError("unified golden count changed")
    provenance = strict_json(read_asset(root, "PROVENANCE.json"))
    if (not isinstance(provenance, dict)
            or provenance.get("format") != "tansr-python-contract-provenance-v1"
            or provenance.get("consumer") != "tansr-python"
            or provenance.get("sourceRepo") != lock["sourceRepo"]
            or provenance.get("sourceBranch") != lock["sourceBranch"]
            or provenance.get("sourceCommit") != COMMIT
            or provenance.get("sourceLockSha256") != LOCK_HASH
            or provenance.get("files") != lock["files"]):
        raise ValueError("Python provenance differs from frozen baseline")
    return lock


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1] / "contract")
    parser.add_argument("--source", type=Path, help="本地CLI检出；只读核对冻结39个Git对象")
    parser.add_argument("--mode", choices=("internal", "public"), required=True,
                        help="必须显式选择；public还需要Python已批准策略，缺件不降级")
    args = parser.parse_args()
    check(args.root, args.source, mode=args.mode)
    count = 39 if args.mode == "internal" else len(PUBLIC_CANDIDATES)
    print("contract-check: mode={}, {}/{} frozen assets; source lock=39, r7, 81 operations".format(
        args.mode, count, count))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.CalledProcessError) as error:
        print("contract-check failed: " + str(error), file=sys.stderr)
        sys.exit(1)
