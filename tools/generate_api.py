#!/usr/bin/env python3
"""从冻结资产离线生成Python目录/schema；安装和运行不调用此工具。"""
import argparse
import copy
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from contract_check import check, read_asset, strict_json  # noqa: E402 -- disable bytecode before local tool import

UNFENCED = frozenset((
    "discovery.manifest", "discovery.capabilities", "discovery.session.capabilities", "session.capabilities",
))


def generate(root, *, mode=None):
    root = Path(root)
    contract = root / "contract"
    lock = check(contract, mode=mode)
    manifest = strict_json(read_asset(contract, "api-manifest.json"))
    families = {item["id"]: item for item in manifest["families"]}
    names = set()
    for operation in manifest["operations"]:
        name = operation["name"]
        if name in names:
            raise ValueError("duplicate frozen operation name")
        names.add(name)
        operation["fenced"] = name not in UNFENCED
        operation["requestIdPath"] = copy.deepcopy(families.get(operation["family"], {}).get("requestIdPath"))
    if len(names) != 81 or sum(item["fenced"] for item in manifest["operations"]) != 77:
        raise ValueError("frozen operation/fence count changed")
    errors = strict_json(read_asset(contract, "api-error-map.json"))
    encoded_manifest = json.dumps(manifest, ensure_ascii=True, separators=(",", ":"))
    encoded_errors = json.dumps(errors, ensure_ascii=True, separators=(",", ":"))
    lines = [
        '"""冻结r7生成目录；由tools/generate_api.py生成，请勿手改。"""',
        "import copy", "import json", "from typing import Any, Dict", "",
        "from ..errors import Error", "",
        "_MANIFEST = json.loads(" + repr(encoded_manifest) + ")",
        "_ERROR_MAP = json.loads(" + repr(encoded_errors) + ")",
        '_OPERATIONS = {item["name"]: item for item in _MANIFEST["operations"]}', "", "",
        "def manifest() -> Dict[str, Any]:",
        '    """返回目录的自有深拷贝，调用方修改不会改变后续请求。"""',
        "    return copy.deepcopy(_MANIFEST)", "", "",
        "def get_operation(name: str) -> Dict[str, Any]:",
        '    """只接受冻结目录已知操作，返回独立可修改的参数映射。"""',
        "    if not isinstance(name, str) or name not in _OPERATIONS:",
        '        raise Error("invalid_input", "unknown operation")',
        "    return copy.deepcopy(_OPERATIONS[name])", "", "",
        "def error_map() -> Dict[str, Any]:",
        '    """返回原错误目录的自有副本。"""',
        "    return copy.deepcopy(_ERROR_MAP)", "",
    ]
    outputs = {"src/tansr_sdk/api/operations.py": "\n".join(lines)}
    schemas = sorted(item["path"] for item in lock["files"]
                     if "/" not in item["path"] and item["path"].endswith(".schema.json"))
    if len(schemas) != 10:
        raise ValueError("frozen embedded schema count changed")
    lines = [
        '"""冻结schema原字节及对象；仅供共享验证器内部使用。"""',
        "import json", "from typing import Any, Dict", "",
        "SCHEMA_BYTES: Dict[str, bytes] = {",
    ]
    for relative in schemas:
        raw = read_asset(contract, relative)
        strict_json(raw)
        lines.append("    " + repr(relative[:-len(".schema.json")]) + ": " + repr(raw) + ",")
    lines.extend([
        "}", "",
        "SCHEMAS: Dict[str, Any] = {name: json.loads(raw.decode('utf-8')) for name, raw in SCHEMA_BYTES.items()}",
        "",
    ])
    outputs["src/tansr_sdk/api/_schema_data.py"] = "\n".join(lines)
    return outputs


def verify(root, outputs):
    for relative, content in outputs.items():
        path = Path(root) / relative
        if not path.is_file() or path.read_bytes() != content.encode("utf-8"):
            raise ValueError("generated output drift: " + relative)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--mode", choices=("internal", "public"), required=True,
                        help="明确合同模式，public必须获Python范围授权")
    parser.add_argument("--check", action="store_true", help="只检查生成物，不修改任何文件")
    args = parser.parse_args()
    outputs = generate(args.root, mode=args.mode)
    if args.check:
        verify(args.root, outputs)
    else:
        for relative, content in outputs.items():
            path = args.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content.encode("utf-8"))
    print("generate-api: 81 operations, 77 fences, error map and 10 schemas "
          + ("verified" if args.check else "written"))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("generate-api failed: " + str(error), file=sys.stderr)
        sys.exit(1)
