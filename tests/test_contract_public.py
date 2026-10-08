"""公开20项合同的显式模式、逐字节校验及生成验收；不读取内部reference。"""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import contract_check  # noqa: E402 -- repository-local tool after explicit path setup
import generate_api  # noqa: E402 -- repository-local tool after explicit path setup


def write_json(path, value):
    path.write_bytes((json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


class PublicContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="tansr-python-public-contract-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.contract = self.root / "contract"
        self.contract.mkdir()
        # 相同测试可在内部维护树及公开20项树运行，不读取未授权原文。
        for name in contract_check.PUBLIC_CANDIDATES + contract_check.METADATA:
            shutil.copyfile(str(ROOT / "contract" / name), str(self.contract / name))
        write_json(self.contract / "DISTRIBUTION.json", contract_check.distribution("public"))
        self.policy = json.loads((self.contract / "EXPORT-POLICY.json").read_bytes())
        self.policy["status"] = "approved"
        self.policy["authorization"] = "TEST FIXTURE ONLY: explicit Python public subset authorization"
        self.save_policy()

    def save_policy(self):
        write_json(self.contract / "EXPORT-POLICY.json", self.policy)

    def test_exact_public_subset_preserves_pinned_bytes_and_full_source_identity(self):
        lock = contract_check.check(self.contract, mode="public")
        self.assertEqual(len(lock["files"]), 39)
        self.assertEqual(lock["sourceCommit"], contract_check.COMMIT)
        self.assertEqual(len(contract_check.PUBLIC_CANDIDATES), 20)
        self.assertEqual(
            {path.name for path in self.contract.iterdir()},
            set(contract_check.PUBLIC_CANDIDATES + contract_check.METADATA),
        )
        self.assertFalse((self.contract / "reference").exists())
        self.assertFalse((self.contract / "sdk2-archive-recovery-v1.sqlite.sql").exists())
        pins = {item["path"]: item["sha256"] for item in lock["files"]}
        for name in contract_check.PUBLIC_CANDIDATES:
            with self.subTest(name=name):
                self.assertEqual(hashlib.sha256((self.contract / name).read_bytes()).hexdigest(), pins[name])

    def test_public_generation_reproduces_checked_in_runtime_without_private_sources(self):
        outputs = generate_api.generate(self.root, mode="public")
        self.assertEqual(set(outputs), {
            "src/tansr_sdk/api/operations.py", "src/tansr_sdk/api/_schema_data.py",
        })
        generate_api.verify(ROOT, outputs)
        for relative, content in outputs.items():
            target = self.root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content.encode("utf-8"))
        generate_api.verify(self.root, outputs)
        target = self.root / "src/tansr_sdk/api/operations.py"
        target.write_bytes(target.read_bytes() + b"# synthetic output drift\n")
        with self.assertRaisesRegex(ValueError, "generated output drift"):
            generate_api.verify(self.root, outputs)

    def test_mode_is_required_by_functions_and_command_line(self):
        with self.assertRaisesRegex(ValueError, "explicit"):
            contract_check.check(self.contract)
        with self.assertRaisesRegex(ValueError, "explicit"):
            generate_api.generate(self.root)
        for tool in ("contract_check.py", "generate_api.py"):
            result = subprocess.run(
                [sys.executable, "-B", str(ROOT / "tools" / tool)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn(b"--mode", result.stderr)

    def test_public_declaration_cannot_satisfy_internal_mode(self):
        with self.assertRaisesRegex(ValueError, "declaration"):
            contract_check.check(self.contract, mode="internal")
        write_json(self.contract / "DISTRIBUTION.json", contract_check.distribution("internal"))
        with self.assertRaisesRegex(ValueError, "inventory"):
            contract_check.check(self.contract, mode="internal")
        with self.assertRaisesRegex(ValueError, "declaration"):
            contract_check.check(self.contract, mode="public")

    def test_missing_public_asset_is_rejected_without_mode_downgrade(self):
        (self.contract / "api-error-map.json").unlink()
        with self.assertRaisesRegex(ValueError, "inventory"):
            contract_check.check(self.contract, mode="public")
        with self.assertRaisesRegex(ValueError, "inventory"):
            generate_api.generate(self.root, mode="public")

    def test_private_or_unlisted_payloads_are_rejected(self):
        for name in ("reference/synthetic.txt", "sdk2-archive-recovery-v1.sqlite.sql", "extra.json"):
            with self.subTest(name=name):
                target = self.contract / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"synthetic forbidden public payload")
                with self.assertRaisesRegex(ValueError, "inventory"):
                    contract_check.check(self.contract, mode="public")
                target.unlink()
                if target.parent != self.contract:
                    target.parent.rmdir()

    def test_single_byte_public_asset_change_is_rejected(self):
        target = self.contract / "unified-v1.schema.json"
        target.write_bytes(target.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "digest mismatch"):
            contract_check.check(self.contract, mode="public")

    def test_pending_authorization_never_inherits_repository_permission(self):
        self.policy["status"] = "pending"
        self.policy["authorization"] = None
        self.save_policy()
        with self.assertRaisesRegex(ValueError, "policy is pending"):
            contract_check.check(self.contract, mode="public")
        with self.assertRaisesRegex(ValueError, "policy is pending"):
            generate_api.generate(self.root, mode="public")

    def test_authorization_and_python_specific_exact_allowlist_are_required(self):
        original = dict(self.policy)
        mutations = (
            {"authorization": " "},
            {"format": "tansr-cpp-contract-export-policy-v1"},
            {"files": self.policy["files"][:-1]},
            {"files": self.policy["files"] + ["reference/synthetic.txt"]},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                self.policy = dict(original, **mutation)
                self.save_policy()
                with self.assertRaises(ValueError):
                    contract_check.check(self.contract, mode="public")

    def test_rewritten_source_lock_and_provenance_are_rejected(self):
        lock_file = self.contract / "LOCK.json"
        original = lock_file.read_bytes()
        lock_file.write_bytes(original + b"\n")
        with self.assertRaisesRegex(ValueError, "lock bytes changed"):
            contract_check.check(self.contract, mode="public")
        lock_file.write_bytes(original)
        provenance = json.loads((self.contract / "PROVENANCE.json").read_bytes())
        provenance["files"][0]["source"] = "wrong-origin"
        write_json(self.contract / "PROVENANCE.json", provenance)
        with self.assertRaisesRegex(ValueError, "provenance"):
            contract_check.check(self.contract, mode="public")

    def test_path_escapes_and_ambiguous_metadata_are_rejected(self):
        for path in ("../outside", "/absolute", "C:/absolute", "a\\b", "a/../b", "a//b", "a/./b", "a\x00b"):
            with self.subTest(path=path):
                with self.assertRaises(ValueError):
                    contract_check.checked_path(self.root, path)
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', '{"a":1}'.encode("utf-16"), b'\xef\xbb\xbf{}'):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    contract_check.strict_json(raw)


if __name__ == "__main__":
    unittest.main()
