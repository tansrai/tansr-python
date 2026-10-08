"""公开源码导出的本地合成授权正反例；不公开真实候选、不上传或提交。"""
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
import contract_check  # noqa: E402 -- repository-local tools after explicit path setup
import contract_export  # noqa: E402
import export_source  # noqa: E402


def write_json(path, value):
    path.write_bytes((json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


class SourceExportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="tansr-python-source-export-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.output = self.root / "public-output"
        self.source.mkdir()
        # 授权文件均为临时合成值，不将仓库当前pending状态当成已获许可。
        synthetic = {
            "LICENSE": b"MIT License\nTEST FIXTURE ONLY, not a publication authorization\n",
            "demo/LICENSE": b"MIT License\nTEST FIXTURE ONLY, not a publication authorization\n",
            ".github/workflows/publish.yml": b"name: synthetic publication fixture\n",
        }
        for name in export_source.SOURCE_FILES:
            target = self.source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            if name in synthetic:
                target.write_bytes(synthetic[name])
            else:
                shutil.copyfile(str(ROOT / name), str(target))
        contract = self.source / "contract"
        contract.mkdir()
        for name in contract_check.PUBLIC_CANDIDATES + contract_check.PUBLIC_METADATA:
            shutil.copyfile(str(ROOT / "contract" / name), str(contract / name))
        write_json(contract / "DISTRIBUTION.json", contract_check.distribution("public"))
        policy = json.loads((contract / "EXPORT-POLICY.json").read_bytes())
        policy["status"] = "approved"
        policy["authorization"] = "TEST FIXTURE ONLY: simulated Python public source authorization"
        write_json(contract / "EXPORT-POLICY.json", policy)

    def export(self):
        return export_source.export_source(self.source, self.output, contract_mode="public")

    def test_exact_source_inventory_omits_history_internal_docs_and_unknown_files(self):
        private_files = (
            ".git/config", ".env", "doc/开发进度.md", "tests/test_contract_assets.py",
            "artifacts/private-serve.bundle.cjs", "unreviewed-source.py",
        )
        for name in private_files:
            target = self.source / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"SYNTHETIC-PRIVATE-MUST-NOT-EXPORT")
        (self.source / "AGENTS.md").write_bytes(b"SYNTHETIC-INTERNAL-CONTRIBUTOR-GUIDE")
        manifest = self.export()
        self.assertEqual(export_source.verify_export(self.output), manifest)
        self.assertEqual(
            {path.relative_to(self.output).as_posix() for path in self.output.rglob("*") if path.is_file()},
            export_source.expected_paths(),
        )
        for name in private_files:
            self.assertFalse((self.output / name).exists(), name)
        self.assertFalse((self.output / "contract/reference").exists())
        self.assertFalse((self.output / "contract/sdk2-archive-recovery-v1.sqlite.sql").exists())
        self.assertEqual(len(list((self.output / "contract").iterdir())), 25)
        self.assertEqual((self.output / "AGENTS.md").read_bytes(), export_source.PUBLIC_AGENTS.encode("utf-8"))
        for name in export_source.SOURCE_FILES:
            self.assertEqual((self.source / name).read_bytes(), (self.output / name).read_bytes(), name)
        for name in contract_check.PUBLIC_CANDIDATES:
            self.assertEqual((self.source / "contract" / name).read_bytes(),
                             (self.output / "contract" / name).read_bytes(), name)

    def test_pending_policy_refuses_before_output_creation(self):
        path = self.source / "contract/EXPORT-POLICY.json"
        policy = json.loads(path.read_bytes())
        policy.update(status="pending", authorization=None)
        write_json(path, policy)
        with self.assertRaisesRegex(ValueError, "policy is pending"):
            self.export()
        self.assertFalse(self.output.exists())

    def test_missing_mit_license_in_either_package_refuses_before_output(self):
        for name in ("LICENSE", "demo/LICENSE"):
            with self.subTest(name=name):
                path = self.source / name
                original = path.read_bytes()
                path.write_bytes(b"Proprietary: synthetic unapproved fixture")
                with self.assertRaisesRegex(ValueError, "MIT license required"):
                    self.export()
                self.assertFalse(self.output.exists())
                path.write_bytes(original)

    def test_missing_reviewed_file_is_not_silently_dropped(self):
        (self.source / "integration/host.py").unlink()
        with self.assertRaises(OSError):
            self.export()
        self.assertFalse(self.output.exists())

    def test_existing_output_is_never_overwritten_or_deleted(self):
        self.output.mkdir()
        marker = self.output / "owned-by-caller"
        marker.write_bytes(b"keep")
        with self.assertRaises(FileExistsError):
            self.export()
        self.assertEqual(marker.read_bytes(), b"keep")
        self.assertEqual(list(self.output.iterdir()), [marker])

    def test_overlapping_destination_is_rejected(self):
        for target in (self.source, self.source / "public", self.root):
            with self.subTest(target=str(target)):
                with self.assertRaisesRegex(ValueError, "outside"):
                    export_source.export_source(self.source, target, contract_mode="public")
        self.assertFalse((self.source / "public").exists())

    def test_public_input_does_not_select_itself_as_default_mode(self):
        with self.assertRaisesRegex(ValueError, "declaration"):
            export_source.export_source(self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_generated_drift_refuses_before_output_creation(self):
        path = self.source / "src/tansr_sdk/api/operations.py"
        path.write_bytes(path.read_bytes() + b"# synthetic drift\n")
        with self.assertRaisesRegex(ValueError, "generated output drift"):
            self.export()
        self.assertFalse(self.output.exists())

    def test_public_snapshot_reexports_deterministically_without_git(self):
        manifest = self.export()
        second = self.root / "second-public-output"
        again = export_source.export_source(self.output, second, contract_mode="public")
        self.assertEqual(manifest, again)
        self.assertEqual((self.output / export_source.MANIFEST).read_bytes(),
                         (second / export_source.MANIFEST).read_bytes())

    def test_manifest_and_actual_bytes_both_have_to_match(self):
        self.export()
        path = self.output / "README.md"
        original = path.read_bytes()
        path.write_bytes(original + b"\n")
        with self.assertRaisesRegex(ValueError, "bytes differ"):
            export_source.verify_export(self.output)
        path.write_bytes(original)
        manifest_path = self.output / export_source.MANIFEST
        manifest = json.loads(manifest_path.read_bytes())
        manifest["repository"] = "https://example.invalid/unapproved"
        write_json(manifest_path, manifest)
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            export_source.verify_export(self.output)

    def test_verify_rejects_extra_file_empty_directory_and_git_metadata(self):
        self.export()
        for name, directory in (("unreviewed.txt", False), ("unreviewed-empty", True), (".git", True)):
            with self.subTest(name=name):
                target = self.output / name
                if directory:
                    target.mkdir()
                else:
                    target.write_bytes(b"synthetic")
                with self.assertRaisesRegex(ValueError, "inventory"):
                    export_source.verify_export(self.output)
                if directory:
                    target.rmdir()
                else:
                    target.unlink()

    def test_contract_only_export_has_exact_20_payloads_and_source_lock(self):
        contract_export.export_public(self.source / "contract", self.output, source_mode="public")
        contract_check.check(self.output, mode="public")
        self.assertEqual(len(list(self.output.iterdir())), 25)
        self.assertEqual(hashlib.sha256((self.output / "LOCK.json").read_bytes()).hexdigest(),
                         contract_check.LOCK_HASH)
        self.assertFalse((self.output / "reference").exists())
        with self.assertRaises(FileExistsError):
            contract_export.export_public(self.source / "contract", self.output, source_mode="public")


if __name__ == "__main__":
    unittest.main()
