"""平台原生 ACL/reparse、链接与路径替换竞态。临时根始终位于 OS temp。"""
import ctypes
import os
import pathlib
import shutil
import struct
import tempfile
import threading
import unittest

from tansr_sdk.errors import Error
from tansr_sdk.storage import PrivateDirectory


def _windows_dacl(path, broad):
    from ctypes import wintypes as w
    from tansr_sdk.storage import _windows as native
    descriptor = native._PTR()
    if broad:
        if not native._sddl("D:P(A;;FA;;;OW)(A;;FR;;;WD)", 1, ctypes.byref(descriptor), None):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        descriptor = native._descriptor(os.path.isdir(str(path)))
    set_security = native._advapi.SetFileSecurityW
    set_security.argtypes = [w.LPCWSTR, w.DWORD, native._PTR]
    set_security.restype = w.BOOL
    try:
        if not set_security(str(path), 0x80000004, descriptor):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        native._local_free(descriptor)


def _junction(link, target):
    from ctypes import wintypes as w
    from tansr_sdk.storage import _windows as native
    os.mkdir(str(link))
    handle = native._create(str(link), 0x40000000, 0, None, 3, 0x02200000, None)
    if handle == native._INVALID:
        raise ctypes.WinError(ctypes.get_last_error())
    substitute = ("\\??\\" + str(target)).encode("utf-16-le")
    printed = str(target).encode("utf-16-le")
    paths = substitute + b"\x00\x00" + printed + b"\x00\x00"
    header = struct.pack("<LHHHHHH", 0xA0000003, 8 + len(paths), 0,
                         0, len(substitute), len(substitute) + 2, len(printed))
    buffer = ctypes.create_string_buffer(header + paths)
    returned = w.DWORD()
    control = native._kernel.DeviceIoControl
    control.argtypes = [w.HANDLE, w.DWORD, native._PTR, w.DWORD, native._PTR,
                        w.DWORD, ctypes.POINTER(w.DWORD), native._PTR]
    control.restype = w.BOOL
    try:
        if not control(handle, 0x900A4, buffer, len(header + paths), None, 0,
                       ctypes.byref(returned), None):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        native._close(handle)


class StorageSecurityTests(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp(prefix="tansr-python-storage-security-")).resolve()
        self.resources = []

    def tearDown(self):
        for resource in reversed(self.resources):
            resource.close()
        shutil.rmtree(str(self.root))

    def directory(self, name):
        directory = PrivateDirectory(self.root / name, create=True)
        self.resources.append(directory)
        return directory

    def test_existing_broad_directory_is_rejected_and_not_repaired(self):
        directory = self.directory("broad")
        directory.close()
        if os.name == "nt":
            _windows_dacl(directory.path, True)
        else:
            os.chmod(directory.path, 0o755)
        with self.assertRaises(Error):
            PrivateDirectory(directory.path, create=True)
        # 第二次仍拒绝，证明构造器没有悄悄覆盖原 ACL/mode。
        with self.assertRaises(Error):
            PrivateDirectory(directory.path)

    def test_native_junction_or_symlink_and_ancestor_are_rejected(self):
        target = self.directory("target")
        child = PrivateDirectory(pathlib.Path(target.path) / "child", create=True)
        child.close()
        link = self.root / "link"
        try:
            if os.name == "nt":
                _junction(link, pathlib.Path(target.path))
            else:
                os.symlink(target.path, str(link))
            for path in (link, link / "child"):
                with self.assertRaises((Error, OSError)):
                    PrivateDirectory(path)
        finally:
            # 显式只移除链接本身，兼容 Python3.7 rmtree 的历史 junction 行为。
            if os.path.lexists(str(link)):
                os.rmdir(str(link)) if os.name == "nt" else os.unlink(str(link))
        self.assertTrue(os.path.isdir(target.path))

    def test_native_file_permissions_revalidated_before_delivery(self):
        directory = self.directory("delivery")
        directory.write("state", b"private")
        original = directory._backend.read
        changed = [False]
        def read(handle, length):
            result = original(handle, length)
            if not changed[0]:
                changed[0] = True
                if os.name == "nt":
                    _windows_dacl(pathlib.Path(directory.path) / "state", True)
                else:
                    os.chmod(str(pathlib.Path(directory.path) / "state"), 0o644)
            return result
        directory._backend.read = read
        with self.assertRaises(Error) as caught:
            directory.read("state")
        self.assertEqual(caught.exception.code, "permission")
        self.assertTrue(changed[0])

    def test_directory_replacement_at_each_commit_barrier(self):
        for phase in ("written", "file_synced", "before_replace", "replaced", "directory_synced"):
            directory = self.directory("race-" + phase)
            directory.write("state", b"old")
            active = pathlib.Path(directory.path)
            moved = self.root / ("moved-" + phase)
            changed, failures = [], []
            def hook(stage):
                if stage != phase:
                    return
                def replace():
                    try:
                        try:
                            os.rename(str(active), str(moved))
                        except PermissionError:
                            changed.append(False)
                            return
                        changed.append(True)
                        with PrivateDirectory(active, create=True) as other:
                            other.write("state", b"foreign")
                    except BaseException as exc:
                        failures.append(exc)
                worker = threading.Thread(target=replace)
                worker.start()
                worker.join(5)
                self.assertFalse(worker.is_alive())
            if os.name == "nt":
                directory.write("state", b"new", hook=hook)
                self.assertEqual(changed, [False])
                self.assertEqual(directory.read("state"), b"new")
            else:
                with self.assertRaises(Error):
                    directory.write("state", b"new", hook=hook)
                self.assertEqual(changed, [True])
                self.assertEqual((active / "state").read_bytes(), b"foreign")
                expected = b"new" if phase in ("replaced", "directory_synced") else b"old"
                self.assertEqual((moved / "state").read_bytes(), expected)
                with self.assertRaises(Error):
                    directory.read("state")
            self.assertEqual(failures, [])

    def test_temporary_and_committed_leaf_substitution(self):
        foreign = self.directory("foreign")
        foreign.write("sentinel", b"foreign-original")
        for phase in ("before_replace", "directory_synced"):
            directory = self.directory("leaf-" + phase)
            directory.write("state", b"old")
            active = pathlib.Path(directory.path)
            changed = []
            def hook(stage):
                if stage != phase:
                    return
                target = active / "state"
                if stage == "before_replace":
                    target = next(p for p in active.iterdir() if p.name.startswith(".tansr-tmp-"))
                try:
                    os.rename(str(target), str(active / "held-original"))
                except PermissionError:
                    changed.append(False)
                    return
                changed.append(True)
                os.link(str(pathlib.Path(foreign.path) / "sentinel"), str(target))
            if os.name == "nt" and phase == "before_replace":
                directory.write("state", b"new", hook=hook)
                self.assertEqual(changed, [False])
            else:
                with self.assertRaises(Error):
                    directory.write("state", b"new", hook=hook)
                self.assertEqual(changed, [True])
            self.assertEqual((pathlib.Path(foreign.path) / "sentinel").read_bytes(), b"foreign-original")
            # 移除本测试已知别名，避免下一次迭代把外部原件变成多链接输入。
            if changed == [True]:
                alias = active / "state" if phase == "directory_synced" else next(
                    p for p in active.iterdir() if p.name.startswith(".tansr-tmp-"))
                alias.unlink()

    def test_another_directory_object_cannot_replace_or_remove_held_lock(self):
        first = self.directory("shared-lock")
        second = PrivateDirectory(first.path)
        self.resources.append(second)
        with first.lock("claim"):
            for operation in (lambda: second.write("claim", b"changed"), lambda: second.remove("claim")):
                with self.assertRaises(Error) as caught:
                    operation()
                self.assertEqual(caught.exception.code, "conflict")


if __name__ == "__main__":
    unittest.main()
