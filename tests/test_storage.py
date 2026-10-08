"""实际私有介质、进程锁、提交故障与密码认证边界。"""
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

from tansr_sdk import strict_json
from tansr_sdk.errors import Error
from tansr_sdk.lifecycle import CancellationToken
from tansr_sdk.storage import EncryptedStore, PrivateDirectory, decode_bytes, encode_bytes


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="tansr-python-storage-")).resolve()
        self.path = self.root / "私有 space"
        self.directory = PrivateDirectory(self.path, create=True)
        self.resources = [self.directory]

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        shutil.rmtree(str(self.root))

    def opened(self, path=None, **options):
        directory = PrivateDirectory(path or self.path, **options)
        self.resources.append(directory)
        return directory

    def store(self, **options):
        store = EncryptedStore(self.directory, "archive", b"k" * 32, "key-1", **options)
        self.resources.append(store)
        return store

    def error(self, code, function, *args, **kwargs):
        with self.assertRaises(Error) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def child(self, code, *args):
        import tansr_sdk.storage
        env = dict(os.environ)
        env["PYTHONPATH"] = str(pathlib.Path(tansr_sdk.storage.__file__).parents[2])
        return subprocess.run([sys.executable, "-c", code] + list(args), env=env,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)

    def test_bytes_names_and_exclusive_creation(self):
        raw = b"\x00\xff\r\n\xe4\xb8"
        self.directory.write("材料.bin", raw, replace=False)
        self.assertEqual(self.directory.read("材料.bin"), raw)
        self.error("conflict", self.directory.write, "材料.bin", b"other", replace=False)
        self.error("capacity", self.directory.read, "材料.bin", max_bytes=2)
        self.assertFalse(self.directory.exists("absent"))
        with self.assertRaises(FileNotFoundError):
            self.directory.read("absent")
        for name in ("../escape", "a/b", "a\\b", "a:b", "a\x00b", ".", "..", "NUL",
                     "COM1.txt", "com¹", "a.", "a ", ".tansr-directory.lock"):
            self.error("permission", self.directory.write, name, b"private")
        self.error("invalid_argument", self.directory.write, "mutable", bytearray(b"private"))

    def test_real_same_and_cross_process_lock_and_reopen(self):
        other = self.opened()
        with self.directory.lock("business.lock"):
            self.error("conflict", other.lock("business.lock").__enter__)
            self.error("conflict", self.directory.write, "business.lock", b"overwrite")
            self.error("conflict", other.write, "business.lock", b"overwrite")
            self.error("conflict", other.remove, "business.lock")
            code = """import sys
from tansr_sdk.storage import PrivateDirectory
from tansr_sdk.errors import Error
with PrivateDirectory(sys.argv[1]) as d:
    try:
        with d.lock('business.lock'):
            sys.exit(9)
    except Error as exc:
        sys.exit(0 if exc.code == 'conflict' else 8)
"""
            result = self.child(code, str(self.path))
            self.assertEqual(result.returncode, 0, result.stderr.decode("utf-8", "replace"))
        with other.lock("business.lock"):
            pass
        self.assertTrue(self.directory.exists("business.lock"))

    def test_capacity_reserves_old_and_new_snapshot(self):
        limited = self.opened(max_file_bytes=128, max_total_bytes=200)
        limited.write("state", b"o" * 100)
        self.error("capacity", limited.write, "state", b"n" * 110)
        self.assertEqual(limited.read("state"), b"o" * 100)
        self.error("capacity", limited.write, "large", b"x" * 129)

    def test_file_count_reserves_atomic_temporary_and_keeps_existing_state(self):
        limited = self.opened(max_files=2)
        limited.write("state", b"old")
        self.error("capacity", limited.write, "state", b"new")
        self.error("capacity", limited.lock("another.lock").__enter__)
        self.assertEqual(limited.read("state"), b"old")
        self.assertFalse(limited.exists("another.lock"))

    def test_close_handle_error_does_not_strand_transaction_ownership(self):
        backend = self.directory._backend
        original = backend.close_file
        failed = [False]

        def close(handle):
            original(handle)
            if handle == self.directory._gate and not failed[0]:
                failed[0] = True
                raise OSError("injected close failure after OS release")

        backend.close_file = close
        try:
            with self.assertRaises(OSError):
                self.directory.check_access()
        finally:
            backend.close_file = original
        self.directory.write("state", b"continued")
        self.assertEqual(self.directory.read("state"), b"continued")

    def test_precommit_faults_preserve_original(self):
        self.directory.write("state", b"old")
        for phase in ("written", "file_synced", "before_replace"):
            def fault(stage):
                if stage == phase:
                    raise OSError("injected storage stage")
            self.error("io", self.directory.write, "state", b"new", hook=fault)
            self.assertEqual(self.directory.read("state"), b"old")
            self.assertFalse(any(p.name.startswith(".tansr-tmp-") for p in self.path.iterdir()))

    def test_postcommit_fault_freezes_until_cold_reopen(self):
        for phase in ("replaced", "directory_synced"):
            folder = self.root / phase
            directory = self.opened(folder, create=True)
            directory.write("state", b"old")
            def fault(stage):
                if stage == phase:
                    raise OSError("injected postcommit")
            self.error("unknown", directory.write, "state", b"new", hook=fault)
            self.assertTrue(directory.uncertain)
            self.error("unknown", directory.read, "state")
            directory.close()
            cold = self.opened(folder)
            self.assertEqual(cold.read("state"), b"new")

    def test_actual_process_crash_observes_complete_old_or_new(self):
        code = """import os,sys
from tansr_sdk.storage import PrivateDirectory
d=PrivateDirectory(sys.argv[1])
def stop(stage):
    if stage == sys.argv[2]:
        os._exit(87)
d.write('state', b'new', hook=stop)
"""
        for phase, expected in (("written", b"old"), ("replaced", b"new")):
            folder = self.root / ("crash-" + phase)
            directory = self.opened(folder, create=True)
            directory.write("state", b"old")
            directory.close()
            result = self.child(code, str(folder), phase)
            self.assertEqual(result.returncode, 87, result.stderr.decode("utf-8", "replace"))
            cold = self.opened(folder)
            self.assertEqual(cold.read("state"), expected)

    def test_cancel_before_commit_and_after_commit_are_distinct(self):
        self.directory.write("state", b"old")
        token = CancellationToken()
        def before(stage):
            if stage == "before_replace":
                token.cancel()
        self.error("cancelled", self.directory.write, "state", b"new", cancel=token, hook=before)
        self.assertEqual(self.directory.read("state"), b"old")
        token = CancellationToken()
        def after(stage):
            if stage == "replaced":
                token.cancel()
        self.directory.write("state", b"new", cancel=token, hook=after)
        self.assertEqual(self.directory.read("state"), b"new")

    def test_close_waits_for_real_disk_transaction(self):
        at_replace, release, closed = threading.Event(), threading.Event(), threading.Event()
        failures = []
        def hook(stage):
            if stage == "replaced":
                at_replace.set()
                if not release.wait(5):
                    raise RuntimeError("test barrier timeout")
        def writer():
            try:
                self.directory.write("state", b"committed", hook=hook)
            except BaseException as exc:
                failures.append(exc)
        thread = threading.Thread(target=writer)
        thread.start()
        self.assertTrue(at_replace.wait(5))
        def closer():
            self.directory.close()
            closed.set()
        closing = threading.Thread(target=closer)
        closing.start()
        try:
            self.assertFalse(closed.wait(0.1))
        finally:
            release.set()
            thread.join(5)
            closing.join(5)
        self.assertTrue(closed.is_set())
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(self.opened().read("state"), b"committed")

    def test_live_authorization_and_reentry(self):
        allowed = [True]
        directory = self.opened(check_access=lambda: allowed[0])
        directory.write("state", b"old")
        def revoke(stage):
            if stage == "before_replace":
                allowed[0] = False
        self.error("permission", directory.write, "state", b"new", hook=revoke)
        self.error("permission", directory.read, "state")
        allowed[0] = True
        self.assertEqual(directory.read("state"), b"old")
        def reenter(stage):
            if stage == "written":
                directory.close()
        self.error("reentrant", directory.write, "state", b"new", hook=reenter)
        self.assertEqual(directory.read("state"), b"old")

    def test_hardlinks_rejected_without_changing_external_bytes(self):
        self.directory.write("state", b"private")
        alias = self.path / "alias"
        os.link(str(self.path / "state"), str(alias))
        try:
            self.error("permission", self.directory.read, "state")
            self.error("permission", self.directory.write, "state", b"wrong")
            self.assertEqual(alias.read_bytes(), b"private")
        finally:
            alias.unlink()

    def test_encrypted_round_trip_nonce_lexemes_and_no_plaintext(self):
        store = self.store()
        self.assertIsNone(store.load())
        source = b'{"n":-0,"float":1.0,"exponent":1e0,"large":999999999999999999999}'
        state = strict_json.loads(source)
        state["body"] = encode_bytes(b"secret-marker-\x00\xff\r\n")
        store.save(state)
        first = self.directory.read("archive")
        self.assertNotIn(b"secret-marker", first)
        loaded = store.load()
        self.assertEqual(decode_bytes(loaded.pop("body")), b"secret-marker-\x00\xff\r\n")
        self.assertEqual(strict_json.dumps(loaded), source)
        store.save(state)
        self.assertNotEqual(first, self.directory.read("archive"))
        store.close()
        reopened = self.store()
        self.assertEqual(reopened.load()["body"], state["body"])

    def test_wrong_key_tamper_truncation_and_unknown_format_preserve_original(self):
        store = self.store()
        store.save({"secret": "private"})
        original = self.directory.read("archive")
        store.close()
        wrong = EncryptedStore(self.directory, "archive", b"w" * 32, "key-1")
        self.resources.append(wrong)
        self.error("integrity", wrong.load)
        self.error("integrity", wrong.save, {"reset": True})
        wrong.close()
        self.assertEqual(self.directory.read("archive"), original)
        for damaged in (original[:-1] + bytes([original[-1] ^ 1]), original[:8], b"Other-SDK/1\n"):
            self.directory.write("archive", damaged)
            reader = self.store()
            self.error("integrity", reader.load)
            reader.close()
            self.assertEqual(self.directory.read("archive"), damaged)

    def test_key_rotation_and_usage_limit_survive_cold_open(self):
        store = self.store(max_encryptions=2)
        store.save({"generation": 1})
        store.save({"generation": 2})
        store.close()
        cold = self.store(max_encryptions=2)
        self.error("capacity", cold.save, {"generation": 3})
        self.error("invalid_argument", cold.rotate_key, b"k" * 32, "key-2")
        cold.rotate_key(b"r" * 32, "key-2")
        cold.close()
        rotated = EncryptedStore(self.directory, "archive", b"r" * 32, "key-2", max_encryptions=2)
        self.resources.append(rotated)
        self.assertEqual(rotated.load(), {"generation": 2})
        rotated.save({"generation": 3})
        self.error("capacity", rotated.save, {"generation": 4})

    def test_base64_rejects_noncanonical_and_noncontiguous_buffers(self):
        for value in ("Zg", "Zh==", "Zg==\n", "é", "!!!!"):
            with self.assertRaises(Error):
                decode_bytes(value)
        for value in (bytearray(b"ab"), memoryview(b"abcd")[::2]):
            self.error("invalid_argument", encode_bytes, value)
            self.error("invalid_argument", EncryptedStore, self.directory, "bad", value, "key")
        self.assertEqual(decode_bytes(encode_bytes(b"\x00\xff")), b"\x00\xff")


if __name__ == "__main__":
    unittest.main()
