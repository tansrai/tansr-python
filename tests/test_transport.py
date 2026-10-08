import datetime
from contextlib import contextmanager
import ipaddress
import os
from pathlib import Path
import socket
import socketserver
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from tansr_sdk import _resolver
from tansr_sdk import AuthToken, Client
from tansr_sdk.errors import Error
from tansr_sdk.lifecycle import CancellationToken, now_ms
from tansr_sdk.transport import HttpRequest, HttpTransport, _tls_runtime_supported


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(3)
        self.server.accepted.set()
        raw = b""
        try:
            while b"\r\n\r\n" not in raw and len(raw) <= 65536:
                part = self.request.recv(4096)
                if not part:
                    self.server.peer_closed.set()
                    return
                raw += part
            route = raw.split(b" ", 2)[1]
            self.server.requests.append(raw)
            self.server.arrived.set()
            if route.startswith(b"/blocked-"):
                prefixes = {
                    b"/blocked-headers": b"HTTP/1.1 200 OK\r\nX-Pending: ",
                    b"/blocked-body": b"HTTP/1.1 200 OK\r\nContent-Length: 99999\r\n\r\nx",
                    b"/blocked-sse": b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\ndata:x\n\n",
                }
                self.request.sendall(prefixes[route])
                self.server.response_started.set()
                while not self.server.stop.is_set():
                    try:
                        if not self.request.recv(1024):
                            self.server.peer_closed.set()
                            break
                    except socket.timeout:
                        continue
                    except OSError:
                        self.server.peer_closed.set()
                        break
                return
            if route.endswith(b"/redirect-other"):
                self.request.sendall(
                    b"HTTP/1.1 302 Found\r\nLocation: "
                    + self.server.redirect_url.encode("ascii")
                    + b"\r\nContent-Length: 0\r\n\r\n"
                )
                return
            if route == b"/hang":
                self.server.stop.wait(5)
                return
            if route == b"/slow":
                for byte in b"HTTP/1.1 200 OK\r\nX-Slow: delayed\r\nContent-Length: 0\r\n\r\n":
                    self.request.sendall(bytes((byte,)))
                    if self.server.stop.wait(0.025):
                        return
                return
            if route == b"/stream":
                self.request.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\ndata:x\n\n"
                )
                self.server.stop.wait(5)
                return
            responses = {
                b"/ok": b'HTTP/1.1 200 OK\r\nContent-Length: 2\r\nETag: "1"\r\n\r\nok',
                b"/error": b'HTTP/1.1 429 Too Many Requests\r\nContent-Length: 7\r\nRetry-After: 2\r\n\r\n{"x":1}',
                b"/redirect": b"HTTP/1.1 302 Found\r\nLocation: /ok\r\nContent-Length: 0\r\n\r\n",
                b"/short": b"HTTP/1.1 200 OK\r\nContent-Length: 20\r\n\r\nabc",
                b"/duplicate": b'HTTP/1.1 200 OK\r\nETag: "1"\r\nETag: "2"\r\nContent-Length: 0\r\n\r\n',
                b"/ambiguous": b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
                b"/headers": b"HTTP/1.1 200 OK\r\nX-A: " + b"a" * 40000 + b"\r\nX-B: " + b"b" * 40000 + b"\r\n\r\n",
                b"/chunked": b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nok\r\n1\r\n!\r\n0\r\n\r\n",
            }
            self.request.sendall(responses[route])
        except (OSError, IndexError, KeyError):
            return


class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = False

    def __init__(self, context=None):
        self.context = context
        self.stop = threading.Event()
        self.arrived = threading.Event()
        self.accepted = threading.Event()
        self.response_started = threading.Event()
        self.peer_closed = threading.Event()
        self.requests = []
        super().__init__(("127.0.0.1", 0), _Handler)
        self.thread = threading.Thread(target=self.serve_forever, kwargs={"poll_interval": 0.01})
        self.thread.start()

    def get_request(self):
        sock, address = super().get_request()
        if self.context is not None:
            sock.settimeout(2)
            try:
                sock = self.context.wrap_socket(sock, server_side=True)
            except BaseException:
                sock.close()
                raise
        return sock, address

    def close(self):
        self.stop.set()
        self.shutdown()
        self.thread.join()
        self.server_close()


@contextmanager
def _certificate_chain(expired=False):
    """只在临时目录生成本地CA/叶证书；过期仅发生于受信CA签出的叶证书。"""
    with tempfile.TemporaryDirectory(prefix="tansr-python-cert-chain-") as directory:
        directory = Path(directory)
        stamp = datetime.datetime.now(datetime.timezone.utc)
        ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Tansr test local CA")])
        ca = (
            x509.CertificateBuilder()
            .subject_name(ca_name)
            .issuer_name(ca_name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(stamp - datetime.timedelta(days=7))
            .not_valid_after(stamp + datetime.timedelta(days=7))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), critical=True)
            .sign(ca_key, hashes.SHA256())
        )
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        leaf = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(stamp - datetime.timedelta(days=2))
            .not_valid_after(stamp + datetime.timedelta(days=-1 if expired else 1))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
                ),
                critical=False,
            )
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .sign(ca_key, hashes.SHA256())
        )
        ca_path, cert_path, key_path = (directory / name for name in ("ca.pem", "leaf.pem", "key.pem"))
        ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        cert_path.write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
        key_path.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(str(cert_path), str(key_path))
        yield str(ca_path), context


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.server = _Server()
        self.transport = HttpTransport()
        self.addCleanup(self.server.close)
        self.addCleanup(self.transport.close)

    def req(self, path="/ok", timeout=3000, cancel=None, cap=8388608, host="127.0.0.1"):
        return HttpRequest(
            "GET",
            "http://{}:{}{}".format(host, self.server.server_address[1], path),
            {},
            None,
            now_ms() + timeout,
            cancel or CancellationToken(),
            cap,
        )

    def test_real_body_non2xx_and_no_redirect(self):
        response = self.transport.request(self.req())
        self.assertEqual((200, b"ok", '"1"'), (response.status, response.body, response.headers["etag"]))
        response = self.transport.request(self.req("/error"))
        self.assertEqual((429, b'{"x":1}'), (response.status, response.body))
        response = self.transport.request(self.req("/redirect"))
        self.assertEqual(302, response.status)
        self.assertEqual(3, len(self.server.requests))
        self.assertTrue(self.transport.close())

    def test_chunked_and_body_caps_and_truncation(self):
        self.assertEqual(b"ok!", self.transport.request(self.req("/chunked")).body)
        for path, cap, code in (
            ("/ok", 1, "resource_limit"),
            ("/chunked", 2, "resource_limit"),
            ("/short", 100, "network"),
        ):
            with self.subTest(path=path), self.assertRaises(Error) as cm:
                self.transport.request(self.req(path, cap=cap))
            self.assertEqual(code, cm.exception.code)
        self.assertTrue(self.transport.close())

    def test_headers_rejected_before_followup_or_body(self):
        for path, code in (("/duplicate", "contract"), ("/ambiguous", "contract"), ("/headers", "resource_limit")):
            with self.subTest(path=path), self.assertRaises(Error) as cm:
                self.transport.request(self.req(path))
            self.assertEqual(code, cm.exception.code)

    def test_invalid_request_has_no_network_effect(self):
        for headers in (
            {"X-Test": "a\r\nInjected: x"},
            {"Host": "elsewhere"},
            {"X-A": "a", "x-a": "b"},
            {"Transfer-Encoding": "chunked"},
            {"Content-Length": "2"},
        ):
            req = self.req()
            req.headers.update(headers)
            with self.assertRaises(Error):
                self.transport.request(req)
        self.assertEqual([], self.server.requests)

    def test_header_deadline_survives_slow_trickle(self):
        start = time.monotonic()
        with self.assertRaises(Error) as cm:
            self.transport.request(self.req("/slow", timeout=180))
        self.assertEqual("timeout", cm.exception.code)
        self.assertLess(time.monotonic() - start, 1.5)
        self.assertTrue(self.transport.close())

    def test_external_cancel_interrupts_blocking_headers(self):
        cancel = CancellationToken()
        failures = []

        def run():
            try:
                self.transport.request(self.req("/hang", cancel=cancel))
            except Error as exc:
                failures.append(exc.code)

        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(self.server.arrived.wait(2))
        cancel.cancel()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(["cancelled"], failures)
        self.assertTrue(self.transport.close())

    def test_stream_close_and_cancel_dont_deliver_buffered_data(self):
        cancel = CancellationToken()
        stream = self.transport.stream(self.req("/stream", cancel=cancel))
        iterator = stream.iter_bytes()
        self.assertEqual(b"data:x\n\n", next(iterator))
        cancel.cancel()
        with self.assertRaises(Error) as cm:
            next(iterator)
        self.assertEqual("cancelled", cm.exception.code)
        self.assertTrue(self.transport.close())

    def test_transport_close_wakes_blocking_stream_and_rejects_new_work(self):
        stream = self.transport.stream(self.req("/stream"))
        iterator = stream.iter_bytes()
        next(iterator)
        failures = []

        def run():
            try:
                next(iterator)
            except Error as exc:
                failures.append(exc.code)

        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(self.transport.close(timeout=2))
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(["cancelled"], failures)
        with self.assertRaises(Error):
            self.transport.request(self.req())

    def test_real_hostname_resolution(self):
        self.assertEqual(b"ok", self.transport.request(self.req(host="localhost")).body)

    def test_cancel_during_tls_handshake_against_unresponsive_peer(self):
        token = CancellationToken()
        failures = []
        req = HttpRequest(
            "GET", "https://127.0.0.1:{}/".format(self.server.server_address[1]), {}, None, now_ms() + 3000, token
        )
        if not _tls_runtime_supported():
            # 实际旧解释器必须在连接前拒绝，不能将此分支记为真实TLS握手通过。
            with self.assertRaises(Error) as cm:
                self.transport.request(req)
            self.assertEqual("unsupported_tls_runtime", cm.exception.code)
            self.assertFalse(self.server.accepted.is_set())
            return

        def run():
            try:
                self.transport.request(req)
            except Error as exc:
                failures.append(exc.code)

        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(self.server.accepted.wait(2))
        token.cancel()
        worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(["cancelled"], failures)
        self.assertTrue(self.transport.close())

    @unittest.skipUnless(_tls_runtime_supported(), "legacy runtime TLS is separately tested as explicitly rejected")
    def test_original_deadline_during_real_tls_handshake_closes_peer(self):
        request = HttpRequest(
            "GET",
            "https://127.0.0.1:{}/".format(self.server.server_address[1]),
            {},
            None,
            now_ms() + 200,
            CancellationToken(),
        )
        with self.assertRaises(Error) as expired:
            self.transport.request(request)
        self.assertEqual(expired.exception.code, "timeout")
        self.assertTrue(self.server.accepted.is_set(), "TLS handshake never reached the real peer")
        self.assertTrue(self.server.peer_closed.wait(2), "TLS peer did not observe socket closure")
        self.assertTrue(self.transport.close(2))

    def test_capacity_wait_keeps_original_deadline(self):
        transport = HttpTransport(max_connections=2, max_streams=1)
        self.addCleanup(transport.close)
        first = transport.stream(self.req("/stream"))
        with self.assertRaises(Error) as cm:
            transport.stream(self.req(timeout=100))
        self.assertEqual("timeout", cm.exception.code)
        # 已饱和的长流仍给短控制请求保留连接槽。
        self.assertEqual(b"ok", transport.request(self.req()).body)
        first.close()
        self.assertEqual(b"ok", transport.request(self.req()).body)

    def test_single_connection_disables_streams_explicitly(self):
        with HttpTransport(max_connections=1) as transport:
            self.assertEqual(b"ok", transport.request(self.req()).body)
            with self.assertRaises(Error) as cm:
                transport.stream(self.req("/stream"))
            self.assertEqual("resource_limit", cm.exception.code)
        with self.assertRaises(Error):
            HttpTransport(max_connections=2, max_streams=2)

    def test_resolver_exception_releases_owned_transport_state(self):
        class BrokenResolver:
            def resolve(self, *args):
                raise RuntimeError("synthetic-secret")

        with HttpTransport(resolver=BrokenResolver()) as transport:
            with self.assertRaises(Error) as cm:
                transport.request(self.req())
            self.assertEqual("network", cm.exception.code)
            self.assertNotIn("synthetic-secret", str(cm.exception))
            self.assertTrue(transport.close())

    def test_real_cross_origin_redirect_rejected_without_forwarding_token(self):
        destination = _Server()
        self.addCleanup(destination.close)
        self.server.redirect_url = "http://127.0.0.1:{}/ok".format(destination.server_address[1])
        with Client(
            "http://127.0.0.1:{}".format(self.server.server_address[1]),
            lambda token: AuthToken("synthetic-sensitive-token", "test-principal"),
            transport=self.transport,
        ) as client:
            with self.assertRaises(Error) as rejected:
                client.call("session.get", parameters={"id": "redirect-other"})
            self.assertEqual(rejected.exception.code, "contract")
        self.assertEqual(len(self.server.requests), 1)
        self.assertIn(b"synthetic-sensitive-token", self.server.requests[0])
        self.assertEqual(destination.requests, [])
        self.assertFalse(destination.accepted.is_set())


class NetworkPhaseTests(unittest.TestCase):
    def exercise(self, context=None, ca_file=None):
        for phase in ("headers", "body", "sse"):
            for action in ("cancel", "deadline"):
                with self.subTest(phase=phase, action=action, tls=context is not None):
                    server = _Server(context)
                    transport = HttpTransport(ca_file=ca_file)
                    token = CancellationToken()
                    errors = []
                    first_chunk = threading.Event()
                    # 各阶段使用有合法SAN的数值地址，避免把DNS启动成本冒充本阶段超时。
                    scheme, host = ("https" if context is not None else "http"), "127.0.0.1"
                    req = HttpRequest(
                        "GET",
                        "{}://{}:{}/blocked-{}".format(scheme, host, server.server_address[1], phase),
                        {},
                        None,
                        now_ms() + (1000 if action == "deadline" else 5000),
                        token,
                    )

                    def request():
                        try:
                            if phase == "sse":
                                with transport.stream(req) as stream:
                                    for unused in stream.iter_bytes():
                                        first_chunk.set()
                            else:
                                transport.request(req)
                        except Error as error:
                            errors.append(error.code)

                    worker = threading.Thread(target=request, name="test-real-network-phase")
                    worker.start()
                    try:
                        self.assertTrue(
                            server.response_started.wait(3),
                            "request never reached selected wire phase: " + repr(errors),
                        )
                        if phase == "sse":
                            self.assertTrue(first_chunk.wait(2), "SSE body never reached consumer")
                        if action == "cancel":
                            token.cancel()
                        worker.join(3)
                        self.assertFalse(worker.is_alive(), "request worker survives cancellation/deadline")
                        self.assertEqual(errors, ["cancelled" if action == "cancel" else "timeout"])
                        self.assertTrue(server.peer_closed.wait(2), "peer did not observe socket closure")
                        self.assertTrue(transport.close(2), "transport claimed work still active")
                    finally:
                        token.cancel()
                        transport.close(2)
                        worker.join(3)
                        server.close()

    def test_real_http_headers_body_sse_cancel_and_original_deadline_close_peer(self):
        self.exercise()

    @unittest.skipUnless(_tls_runtime_supported(), "legacy runtime TLS is separately tested as explicitly rejected")
    def test_real_tls_headers_body_sse_cancel_and_original_deadline_close_peer(self):
        with _certificate_chain() as (ca_file, context):
            self.exercise(context, ca_file)


class ResolverTests(unittest.TestCase):
    def test_forbidden_child_creation_is_explicit_without_thread_fallback(self):
        resolver = _resolver.Resolver()
        with mock.patch.object(_resolver.subprocess, "Popen", side_effect=PermissionError("sandbox denied")):
            with self.assertRaises(Error) as denied:
                resolver.resolve("localhost", 80, now_ms() + 1000, CancellationToken())
        self.assertEqual(denied.exception.code, "network")
        self.assertEqual(resolver._processes, set())
        self.assertTrue(resolver.close())

    def test_dns_original_deadline_reaps_owned_child_without_stopping_unrelated_process(self):
        resolver = _resolver.Resolver()
        original = subprocess.Popen
        unrelated = original(
            [sys.executable, "-I", "-S", "-c", "import time; time.sleep(30)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=_resolver._environment(),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        processes = []

        def spawn(*args, **kwargs):
            child = original(*args, **kwargs)
            processes.append(child)
            return child

        try:
            with mock.patch.object(_resolver, "_WORKER", "import time; time.sleep(30)"), mock.patch.object(
                _resolver.subprocess, "Popen", spawn
            ):
                with self.assertRaises(Error) as expired:
                    resolver.resolve("localhost", 80, now_ms() + 300, CancellationToken())
            self.assertEqual(expired.exception.code, "timeout")
            self.assertEqual(len(processes), 1)
            self.assertIsNotNone(processes[0].poll())
            self.assertTrue(processes[0].stdout.closed)
            self.assertEqual(resolver._processes, set())
            self.assertTrue(resolver.close())
            self.assertIsNone(unrelated.poll(), "resolver stopped a process it does not own")
        finally:
            resolver.close()
            unrelated.terminate()
            unrelated.wait(3)

    def test_numeric_bypasses_subprocess_and_secrets_not_in_child_environment(self):
        resolver = _resolver.Resolver()
        with mock.patch.object(_resolver.subprocess, "Popen", side_effect=AssertionError("unexpected spawn")):
            self.assertEqual(
                socket.AF_INET, resolver.resolve("127.0.0.1", 80, now_ms() + 1000, CancellationToken())[0][0]
            )
        with mock.patch.dict(os.environ, {"TANSR_TEST_SECRET": "synthetic-secret", "PYTHONPATH": "untrusted"}):
            self.assertNotIn("TANSR_TEST_SECRET", _resolver._environment())
            self.assertNotIn("PYTHONPATH", _resolver._environment())
        self.assertTrue(resolver.close())

    def test_blocked_real_child_cancel_is_joined(self):
        resolver = _resolver.Resolver(1)
        token = CancellationToken()
        processes = []
        started = threading.Event()
        failures = []
        original = subprocess.Popen

        def spawn(*args, **kwargs):
            child = original(*args, **kwargs)
            processes.append(child)
            started.set()
            return child

        def run():
            try:
                resolver.resolve("localhost", 80, now_ms() + 3000, token)
            except Error as exc:
                failures.append(exc.code)

        with mock.patch.object(_resolver, "_WORKER", "import time; time.sleep(30)"), mock.patch.object(
            _resolver.subprocess, "Popen", spawn
        ):
            worker = threading.Thread(target=run)
            worker.start()
            self.assertTrue(started.wait(2))
            token.cancel()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(["cancelled"], failures)
        self.assertIsNotNone(processes[0].poll())
        self.assertTrue(processes[0].stdout.closed)
        self.assertTrue(resolver.close())

    def test_bad_and_oversized_child_reply_is_rejected_and_reaped(self):
        for script, code in (
            ("print('not-json')", "network"),
            ("import sys; sys.stdout.buffer.write(b'x'*100000);sys.stdout.buffer.flush()", "resource_limit"),
        ):
            resolver = _resolver.Resolver()
            with mock.patch.object(_resolver, "_WORKER", script), self.assertRaises(Error) as cm:
                resolver.resolve("localhost", 80, now_ms() + 3000, CancellationToken())
            self.assertEqual(code, cm.exception.code)
            self.assertTrue(resolver.close())
            self.assertEqual(set(), resolver._processes)


class TlsTests(unittest.TestCase):
    @unittest.skipUnless(_tls_runtime_supported(), "legacy runtime TLS is separately tested as explicitly rejected")
    def test_expired_leaf_from_explicit_trusted_ca_rejected_before_business_http(self):
        with _certificate_chain(expired=True) as (ca_file, context):
            server = _Server(context)
            try:
                with HttpTransport(ca_file=ca_file) as transport:
                    request = HttpRequest(
                        "GET",
                        "https://localhost:{}/ok".format(server.server_address[1]),
                        {"Authorization": "Bearer synthetic-secret"},
                        None,
                        now_ms() + 3000,
                        CancellationToken(),
                    )
                    with self.assertRaises(Error) as expired:
                        transport.request(request)
                    self.assertEqual(expired.exception.code, "tls")
                    self.assertTrue(transport.close(2))
                self.assertEqual(server.requests, [], "expired certificate received business token/body")
            finally:
                server.close()

    def test_affected_runtime_or_unverified_provider_rejected_before_dns(self):
        class NoDns:
            def resolve(self, *args):
                raise AssertionError("unsafe TLS must not resolve or connect")

        for brand, version, allowed in (
            ("OpenSSL 1.1.1g", 0x1010107F, False),
            ("OpenSSL 1.1.1m", 0x101010DF, False),
            ("OpenSSL 1.1.1n", 0x101010EF, True),
            ("OpenSSL 3.0.1", 0x30000010, False),
            ("OpenSSL 3.0.2", 0x30000020, True),
            ("OpenSSL 3.5.9", 0x30500090, True),
            ("LibreSSL 2.8.3", 0x30500090, False),
        ):
            with mock.patch.object(ssl, "OPENSSL_VERSION", brand), mock.patch.object(
                ssl, "OPENSSL_VERSION_NUMBER", version
            ):
                self.assertEqual(allowed, _tls_runtime_supported())
                if not allowed:
                    with HttpTransport(resolver=NoDns()) as transport:
                        with self.assertRaises(Error) as cm:
                            transport.request(
                                HttpRequest(
                                    "GET", "https://example.invalid/", {}, None, now_ms() + 1000, CancellationToken()
                                )
                            )
                        self.assertEqual("unsupported_tls_runtime", cm.exception.code)

    def test_ca_and_hostname_verification_and_insecure_context_rejected(self):
        with tempfile.TemporaryDirectory(prefix="tansr-python-tls-") as directory:
            directory = Path(directory).resolve()
            key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
            stamp = datetime.datetime.now(datetime.timezone.utc)
            cert = (
                x509.CertificateBuilder()
                .subject_name(name)
                .issuer_name(name)
                .public_key(key.public_key())
                .serial_number(x509.random_serial_number())
                .not_valid_before(stamp - datetime.timedelta(days=1))
                .not_valid_after(stamp + datetime.timedelta(days=1))
                .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
                .sign(key, hashes.SHA256())
            )
            cert_path, key_path = directory / "cert.pem", directory / "key.pem"
            cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
            key_path.write_bytes(
                key.private_bytes(
                    serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
                )
            )
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(str(cert_path), str(key_path))
            server = _Server(context)
            try:
                for host, ca, code in (
                    ("localhost", str(cert_path), None),
                    ("localhost", None, "tls"),
                    ("127.0.0.1", str(cert_path), "tls"),
                ):
                    if not _tls_runtime_supported():
                        code = "unsupported_tls_runtime"
                    with HttpTransport(ca_file=ca) as transport:
                        req = HttpRequest(
                            "GET",
                            "https://{}:{}/ok".format(host, server.server_address[1]),
                            {},
                            None,
                            now_ms() + 3000,
                            CancellationToken(),
                        )
                        if code is None:
                            self.assertEqual(b"ok", transport.request(req).body)
                        else:
                            with self.assertRaises(Error) as cm:
                                transport.request(req)
                            self.assertEqual(code, cm.exception.code)
                context = ssl._create_unverified_context()
                with self.assertRaises(Error):
                    HttpTransport(ssl_context=context)
            finally:
                server.close()


if __name__ == "__main__":
    unittest.main()
