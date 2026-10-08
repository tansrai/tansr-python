"""Demo共用凭据、截止和资源归属；不在这里重建协议或网络实现。"""
import argparse
import asyncio
import codecs
import ctypes
import os
import re
import select
import signal
import ssl
import sys
import threading
import uuid
from pathlib import Path

from tansr_sdk import AuthToken, CancellationToken, Client, Error
from tansr_sdk import strict_json
from tansr_sdk.lifecycle import now_ms
from tansr_sdk.session import WriteOptions
from tansr_sdk.storage import PrivateDirectory
from tansr_sdk.transport import HttpTransport


def fail(code="invalid_input"):
    raise Error(code)


def safe(value):
    # 控制字符包括ESC、回车与终端C1控制；换行仅由程序自己的输出添加。
    text = str(value)
    return "".join(c if ord(c) >= 32 and not 127 <= ord(c) < 160 else "?" for c in text)


def emit_json(value):
    print(safe(strict_json.dumps(value).decode("utf-8")), flush=True)


def absolute_file(value):
    if (not isinstance(value, str) or not os.path.isabs(value) or
            any(part in (".", "..") for part in re.split(r"[/\\]", value))):
        raise argparse.ArgumentTypeError("requires an absolute path without dot segments")
    return Path(os.path.normpath(value))


def bounded_integer(low, high):
    def parse(value):
        if not re.fullmatch(r"[0-9]+", value) or not low <= int(value) <= high:
            raise argparse.ArgumentTypeError("expected integer {}..{}".format(low, high))
        return int(value)
    return parse


def parser(program, description):
    result = argparse.ArgumentParser(prog=program, description=description, allow_abbrev=False)
    result.add_argument("--base", default=os.environ.get("TANSR_BASE_URL", "http://127.0.0.1:8787"))
    result.add_argument("--family", choices=("sdk1", "sdk2-offload-v1"), default="sdk1")
    result.add_argument("--token-file", type=absolute_file, default=os.environ.get("TANSR_TOKEN_FILE"))
    result.add_argument("--scope-file", type=absolute_file, default=os.environ.get("TANSR_SCOPE_FILE"))
    result.add_argument("--ca-file", type=absolute_file, help="additional trusted CA PEM; verification remains enabled")
    result.add_argument("--timeout", type=bounded_integer(1, 86400), default=600,
                        help="whole local Demo lifetime in seconds (default: 600)")
    result.add_argument("--request-timeout", type=bounded_integer(1, 86400), default=30,
                        help="ordinary control-request limit in seconds (default: 30)")
    return result


def request_id():
    return uuid.uuid4().hex


def require_arguments(parser_, args, required=(), forbidden=()):
    for name in required:
        if getattr(args, name, None) is None:
            parser_.error("requires --" + name.replace("_", "-"))
    for name in forbidden:
        if getattr(args, name, None) is not None:
            parser_.error("this mode does not accept --" + name.replace("_", "-"))


class Credentials:
    """每次使用重读当前scope和token，旧档案里的身份不能恢复权限。"""
    def __init__(self, token_file, scope_file):
        if token_file is None or scope_file is None:
            fail("credentials_required")
        token_file, scope_file = absolute_file(str(token_file)), absolute_file(str(scope_file))
        if token_file.parent != scope_file.parent or token_file.name == scope_file.name:
            fail("credentials_layout")
        self.directory = PrivateDirectory(str(token_file.parent))
        self.token_name, self.scope_name = token_file.name, scope_file.name
        try:
            self.scope = self._scope()
        except BaseException:
            self.directory.close()
            raise

    def _scope(self):
        value = strict_json.loads(self.directory.read(self.scope_name, max_bytes=8192))
        keys = {"applicationScopeId", "endUserId", "authorizationRevision"}
        if (not isinstance(value, dict) or set(value) != keys or
                any(not isinstance(item, str) or not item or len(item) > 128 for item in value.values())):
            fail("invalid_scope")
        return value

    def check(self, cancel=None):
        if cancel is not None:
            cancel.check()
        if self._scope() != self.scope:
            fail("permission")

    def token(self, cancel):
        self.check(cancel)
        raw = self.directory.read(self.token_name, max_bytes=8192).strip(b"\r\n")
        if not raw or any(byte < 33 or byte > 126 for byte in raw):
            fail("invalid_token")
        self.check(cancel)
        return AuthToken(raw.decode("ascii"), strict_json.dumps(self.scope).decode("utf-8"))

    def close(self):
        self.directory.close()


class Host:
    """持有自己的Client、计时器和凭据；退出时等待实际资源关闭。"""
    def __init__(self, args):
        self.args = args
        self.cancel = CancellationToken()
        self.deadline_ms = now_ms() + args.timeout * 1000
        self.credentials = Credentials(args.token_file, args.scope_file)
        self._timer = None
        self._old_signal = None
        self.api = None
        transport = None
        try:
            # 凭据回调会重新读取scope；它不能重入同一目录的事务门锁。
            for name in ("prepare_create", "create_intent", "intent", "file"):
                value = getattr(args, name, None)
                if value is not None:
                    self.check_state_directory(Path(value).parent)
            if getattr(args, "journal", None) is not None:
                self.check_state_directory(args.journal)
            if args.ca_file is not None:
                context = ssl.create_default_context(cafile=str(args.ca_file))
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                transport = HttpTransport(ssl_context=context)
            self.api = Client(args.base, self.credentials.token, family=args.family,
                              transport=transport, timeout=args.request_timeout)
        except BaseException:
            if transport is not None:
                transport.close()
            self.credentials.close()
            raise

    def __enter__(self):
        self._timer = threading.Timer(self.args.timeout, self.cancel.cancel)
        self._timer.name = "tansr-demo-deadline"
        self._timer.start()
        if threading.current_thread() is threading.main_thread():
            self._old_signal = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, lambda signum, frame: self.cancel.cancel())
        return self

    def context(self, deadline_ms=None):
        self.cancel.check(self.deadline_ms)
        deadline = min(self.deadline_ms, deadline_ms or self.api.default_deadline_ms())
        return dict(cancel=self.cancel, deadline_ms=deadline)

    def write(self, deadline_ms=None, key=None):
        return WriteOptions(request_key=key or request_id(), **self.context(deadline_ms))

    def owner(self):
        self.credentials.check(self.cancel)
        return dict(base=self.api.base_url, family=self.args.family, scope=self.credentials.scope)

    def private_directory(self, path, create=False):
        self.check_state_directory(path)
        return PrivateDirectory(str(path), create=create,
                                check_access=lambda: self.credentials.check(self.cancel))

    def check_state_directory(self, path):
        credentials = self.credentials.directory.path
        same = os.path.normcase(os.path.abspath(str(path))) == os.path.normcase(credentials)
        if not same and os.path.exists(str(path)):
            same = os.path.samefile(str(path), credentials)
        if same:
            fail("credentials_state_overlap")

    def __exit__(self, *unused):
        self.cancel.cancel()
        if self._timer is not None:
            self._timer.cancel()
            self._timer.join()
        if self._old_signal is not None:
            signal.signal(signal.SIGINT, self._old_signal)
        if not self.api.close(timeout=30):
            # 不释放凭据、也不声称关闭完成。调用者仍持有Host。
            fail("not_quiescent")
        self.credentials.close()


def read_private(path, maximum=65536):
    path = absolute_file(str(path))
    with PrivateDirectory(str(path.parent)) as directory:
        return directory.read(path.name, max_bytes=maximum)


def save_owned(path, host, value):
    path = absolute_file(str(path))
    body = strict_json.dumps(dict(format="tansr-python-demo-owned-v1", owner=host.owner(), value=value))
    if len(body) > 2 << 20:
        fail("capacity")
    with host.private_directory(path.parent, create=True) as directory:
        directory.write(path.name, body, replace=False)


def load_owned(path, host):
    path = absolute_file(str(path))
    with host.private_directory(path.parent) as directory:
        body = strict_json.loads(directory.read(path.name, max_bytes=2 << 20))
    if (not isinstance(body, dict) or set(body) != {"format", "owner", "value"}
            or body["format"] != "tansr-python-demo-owned-v1" or body["owner"] != host.owner()):
        fail("permission")
    return body["value"]


def report(action):
    try:
        action()
        return 0
    except KeyboardInterrupt:
        print("cancelled: local observation stopped; remote outcome is unchanged", file=sys.stderr)
        return 130
    except asyncio.CancelledError:
        raise
    except Error as error:
        # 不打印message/detail或异常链；第三方服务返回的文本可能含秘密。
        print("error: {} (HTTP {}); inspect original identity before retry".format(
            safe(error.code), error.http_status), file=sys.stderr)
        return 1
    except (OSError, ValueError, TypeError, KeyError):
        print("error: invalid local data or unavailable resource; original files retained", file=sys.stderr)
        return 1
    except Exception:
        print("error: outcome unknown; retain original identities and local files", file=sys.stderr)
        return 1


class Console:
    """轮询借用的stdin，不为阻塞input留下不可回收线程。"""
    def __init__(self):
        self.ended = False
        self.buffer = ""
        self.decoder = codecs.getincrementaldecoder(sys.stdin.encoding or "utf-8")("strict")
        self._special = False

    def _bytes(self):
        if os.name != "nt":
            if not select.select([sys.stdin], [], [], 0)[0]:
                return None
            return os.read(sys.stdin.fileno(), 4096)
        import msvcrt
        if sys.stdin.isatty():
            chars = []
            while msvcrt.kbhit() and len(chars) < 4096:
                char = msvcrt.getwch()
                if self._special:
                    self._special = False
                elif char in ("\x00", "\xe0"):
                    self._special = True
                elif char == "\x1a":
                    self.ended = True
                    break
                elif char == "\x03":
                    raise KeyboardInterrupt
                elif char in ("\b", "\x7f"):
                    if chars:
                        chars.pop()
                        print("\b \b", end="", flush=True)
                    elif self.buffer:
                        self.buffer = self.buffer[:-1]
                        print("\b \b", end="", flush=True)
                else:
                    char = "\n" if char == "\r" else char
                    chars.append(char)
                    print(char, end="", flush=True)
            return "".join(chars)
        handle = ctypes.c_void_p(msvcrt.get_osfhandle(sys.stdin.fileno()))
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        if kernel.GetFileType(handle) == 1:  # 普通重定向文件可直接有界读取。
            return os.read(sys.stdin.fileno(), 4096)
        available = ctypes.c_ulong()
        if not kernel.PeekNamedPipe(handle, None, 0, None, ctypes.byref(available), None):
            if ctypes.get_last_error() in (109, 232):
                return b""
            fail("stdin_unavailable")
        if available.value == 0:
            return None
        return os.read(sys.stdin.fileno(), min(4096, available.value))

    def poll(self):
        if "\n" not in self.buffer and not self.ended:
            data = self._bytes()
            if data == b"":
                self.ended = True
                self.buffer += self.decoder.decode(b"", final=True)
            elif isinstance(data, bytes):
                self.buffer += self.decoder.decode(data)
            elif isinstance(data, str):
                self.buffer += data
        if len(self.buffer) > 65536:
            fail("capacity")
        if "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            return line.rstrip("\r")
        if self.ended and self.buffer:
            line, self.buffer = self.buffer, ""
            return line
        return None
