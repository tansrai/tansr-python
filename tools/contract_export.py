#!/usr/bin/env python3
"""按本库已批准策略导出20项公开合同；不提交、上传或发布。"""
import argparse
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True
from contract_check import (  # noqa: E402 -- disable bytecode before local tool import
    PUBLIC_CANDIDATES, PUBLIC_METADATA, check, check_policy, distribution, read_asset,
)


def outside_source(root, output):
    root, output = Path(root).resolve(), Path(output).resolve()
    if root == output or root in output.parents or output in root.parents:
        raise ValueError("public destination must be outside the source tree")


def public_payloads(root, *, source_mode="internal"):
    root = Path(root)
    check(root, mode=source_mode)
    check_policy(root, require_approved=True)
    # 先核验并读取全套输入；授权/原件失败不能产生看似完整的公开树。
    payloads = {
        name: read_asset(root, name)
        for name in PUBLIC_CANDIDATES + PUBLIC_METADATA if name != "DISTRIBUTION.json"
    }
    payloads["DISTRIBUTION.json"] = (json.dumps(distribution("public"), indent=2) + "\n").encode("utf-8")
    return payloads


def export_public(root, output, *, source_mode="internal"):
    root, output = Path(root), Path(output)
    outside_source(root, output)
    payloads = public_payloads(root, source_mode=source_mode)
    output.mkdir(exist_ok=False)
    # 仅写新目录；遇到IO错误保留失败件，不删除或覆盖调用方目录。
    for name, raw in sorted(payloads.items()):
        with (output / name).open("xb") as stream:
            stream.write(raw)
    check(output, mode="public")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1] / "contract")
    parser.add_argument("--mode", choices=("public",), required=True)
    parser.add_argument("--source-mode", choices=("internal", "public"), default="internal")
    parser.add_argument("--output", type=Path, required=True, help="源树外尚不存在的新目录")
    args = parser.parse_args()
    export_public(args.root, args.output, source_mode=args.source_mode)
    print("contract-export: approved public scope; no upload or release performed")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError) as error:
        print("contract-export failed: " + str(error), file=sys.stderr)
        sys.exit(1)
