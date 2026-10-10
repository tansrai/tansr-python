#!/usr/bin/env python3
"""仅生成批准的独立 profile，不覆盖旧合同锁或其生成输出。"""
import argparse
import hashlib
import json
from pathlib import Path

SCHEMA_SHA256 = "47387fb03308d00244e876a3bb429e24b81ac8d94d5f611aeda43e03b7c8b16c"
GOLDEN_SHA256 = "4b2c492ae590fda9447a5a53d13f3f7a935956c929cc23441a765947e3ef6503"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    for name, expected in (("schema", SCHEMA_SHA256), ("golden", GOLDEN_SHA256)):
        path = root / ("contract-persistence/terminal-persistence-v1." + name + ".json")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise SystemExit("independent persistence contract digest mismatch: " + path.name)
    value = json.loads((root / "contract-persistence/terminal-persistence-v1.schema.json").read_bytes())
    compact = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    source = ('"""由 tools/generate_terminal_persistence.py 生成，禁止手改。"""\n'
              'import json\n\nSCHEMA = json.loads(' + repr(compact) + ')\n')
    target = root / "src/tansr_sdk/terminal_persistence/_schema.py"
    if args.check:
        if target.read_bytes() != source.encode("utf-8"):
            raise SystemExit("persistence generated schema stale")
    else:
        target.write_bytes(source.encode("utf-8"))


if __name__ == "__main__":
    main()
