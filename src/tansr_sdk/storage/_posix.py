"""Unix 的目录描述符、nofollow、权限及 flock 适配。"""
import os
import stat
import sys

from ..errors import Error

assert sys.platform != "win32"


class Backend:
    def __init__(self, path, create):
        import fcntl
        self._fcntl = fcntl
        self.path = path
        self.fd = -1
        self._pid = os.getpid()
        if create:
            parent = self._walk(os.path.dirname(path), False)
            try:
                try:
                    os.mkdir(os.path.basename(path), 0o700, dir_fd=parent)
                except FileExistsError:
                    pass
                os.fsync(parent)
            finally:
                os.close(parent)
        self.fd = self._walk(path, True)
        try:
            self._identity = self.identity(self.fd)
        except BaseException:
            self.close()
            raise

    @staticmethod
    def _check(fd, directory=False, private=True):
        info = os.fstat(fd)
        valid = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
        if not valid or (not directory and info.st_nlink != 1):
            raise Error("permission", "private storage file type rejected")
        if private and (info.st_uid != os.geteuid() or info.st_mode & 0o077):
            raise Error("permission", "private storage ownership or mode rejected")
        return info

    @classmethod
    def _walk(cls, path, private):
        fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            for part in path.split("/"):
                if not part:
                    continue
                next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY |
                                  os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                os.close(fd)
                fd = next_fd
                cls._check(fd, True, False)
            cls._check(fd, True, private)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def verify(self):
        if os.getpid() != self._pid:
            raise Error("permission", "inherited storage must be reopened after fork")
        self._check(self.fd, True)
        current = self._walk(self.path, True)
        try:
            if self.identity(current) != self._identity:
                raise Error("permission", "private storage directory was replaced")
        finally:
            os.close(current)

    @staticmethod
    def identity(fd):
        info = os.fstat(fd)
        return info.st_dev, info.st_ino

    def open(self, name, create=False, exclusive=False, writable=False):
        flags = os.O_RDWR if writable else os.O_RDONLY
        flags |= os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
        if create:
            flags |= os.O_CREAT
        if exclusive:
            flags |= os.O_EXCL
        fd = os.open(name, flags, 0o600, dir_fd=self.fd)
        try:
            self._check(fd)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def check(self, fd):
        return self._check(fd)

    @staticmethod
    def close_file(fd):
        os.close(fd)

    @staticmethod
    def size(fd):
        return os.fstat(fd).st_size

    @staticmethod
    def read(fd, length):
        return os.read(fd, length)

    @staticmethod
    def write(fd, data):
        return os.write(fd, data)

    @staticmethod
    def sync_file(fd):
        os.fsync(fd)

    def sync_directory(self):
        os.fsync(self.fd)

    def names(self):
        with os.scandir(self.fd) as entries:
            for entry in entries:
                yield entry.name

    def replace(self, source, target, replace):
        if replace:
            os.replace(source, target, src_dir_fd=self.fd, dst_dir_fd=self.fd)
        else:
            # linkat 原子拒覆盖；原临时名字随后移除，提交后只保留一个硬链接。
            os.link(source, target, src_dir_fd=self.fd, dst_dir_fd=self.fd,
                    follow_symlinks=False)
            try:
                os.unlink(source, dir_fd=self.fd)
            except BaseException as exc:
                raise Error("unknown", "exclusive storage commit may have completed") from exc

    def remove(self, name):
        os.unlink(name, dir_fd=self.fd)

    remove_durable = remove

    def lock(self, fd):
        try:
            self._fcntl.flock(fd, self._fcntl.LOCK_EX | self._fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise Error("conflict", "private storage already locked") from exc

    def unlock(self, fd):
        if os.getpid() != self._pid:
            raise Error("permission", "forked process cannot unlock parent storage")
        self._fcntl.flock(fd, self._fcntl.LOCK_UN)

    def close(self):
        if self.fd >= 0:
            fd, self.fd = self.fd, -1
            os.close(fd)
