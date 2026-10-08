# Python SDK guide

[中文](https://github.com/tansrai/tansr-python/blob/main/doc/%E4%BD%BF%E7%94%A8%E6%8C%87%E5%8D%97.md) · [Project overview](https://github.com/tansrai/tansr-python/blob/main/README.md) · [Release notes](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md)

This guide describes version `0.1.0` and its three executable Demos. The SDK and Demo use the [MIT license](https://github.com/tansrai/tansr-python/blob/main/LICENSE), with PyPI as the package distribution channel. See the [release notes](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md) for installation, tested environments and platform limits.

## Installation and responsibilities

Runtime source supports CPython 3.7+, excluding 3.9.0/3.9.1. Compatibility does not restore upstream maintenance of older interpreters. Serve owns the run loop, context, memory, permissions and tool scheduling. The Python host supplies identity, explicitly registered business tools and private local archives. The SDK does not provide a general Shell/PTY, audio recorder/player or multi-device synchronization.

In a virtual environment, install the SDK with `python -m pip install tansr-sdk==0.1.0`, and optionally run `python -m pip install tansr-sdk-demo==0.1.0`. For offline use, install verified matching wheels, replacing these absolute paths. The separate Demo package requires exactly the same SDK version:

```text
python -m pip install /absolute/artifacts/tansr_sdk-0.1.0-py3-none-any.whl /absolute/artifacts/tansr_sdk_demo-0.1.0-py3-none-any.whl
python -m pip check
tansr-py-chat --help
tansr-py-tools --help
tansr-py-archive --help
```

Equivalent module entries are `python -m tansr_demo.chat`, `python -m tansr_demo.tools` and `python -m tansr_demo.archive`. Help does not read credentials or contact Serve. To build source in a prepared modern environment, run `python -m build --wheel --sdist` and `python -m build --wheel --sdist demo` from the repository root. See [environment and packaging notes](https://github.com/tansrai/tansr-python/blob/main/requirements/README.md) for dependency locks and independent wheel/sdist consumption. Source import, successful builds and clean installed consumption are separate checks.

Demos connect to an existing Serve; they neither download nor start it. The host must configure Serve, a model provider, terminal identity and any required archive source. The default contract family is `sdk1`; pass `--family sdk2-offload-v1` to every relevant command when using offload.

## Wiring your application

Install only `tansr-sdk` when embedding it in your own application. `tansr-sdk-demo` is a separate consumer, not a runtime dependency or required authentication framework.

| Owner | Responsibility |
| --- | --- |
| Serve and its trusted host | Model access, run loop, context assembly/compression, memory, permissions, adjudication, tool routing and usage |
| Application backend | Issue terminal credentials and stable application/user scope, configure authorized capabilities and archive Sources |
| Python host | Supply `AuthToken` through a provider, display events/approvals/questions, explicitly register business tools and protect local state |
| User interface | Submit approval/question answers, distinguish received/consumed/completed states and show unknown outcomes honestly |

Control operations use HTTP; live events use SSE. WebSocket is not required. `base_url` is the Serve origin, such as `https://serve.example.com`, without an `/api` suffix, credentials, query or fragment. The SDK refuses redirects rather than forwarding credentials to another origin. The integrated Node/Electron SDK keeps its existing execution model.

| Family | Setup and storage | Recovery |
| --- | --- | --- |
| `sdk1` (default) | Existing full session/history persistence path on Serve | Attach or explicitly resume a trusted existing ID; do not assume a lost creation is safe to repeat |
| `sdk2-offload-v1` | Serve must configure an authorized Source and offload host; the terminal maintains archives/materials | Retain the original creation requestId/body/key/deadline; restore the same intent or known session |

Choose the family when constructing `Client`; it is not negotiated into a different family behind your back. `SessionClient.create()` starts a session, `attach()` reads an existing session's metadata and `resume()` explicitly requests restoration. Fork/checkpoint and input capabilities follow the selected contract; do not assume every operation is valid for both families. Send only declared tools and use actual capabilities returned by Serve. A missing terminal executor must not cause device operations to fall back to the Serve machine.

An `AuthToken` contains a token and stable principal. Keep the principal identical when rotating a token for the same application/user/authorization scope. A different principal requires a new Client and reauthorization; a saved session ID or local file cannot authorize a different user.

## Credentials and shared options

Token and scope must be different files in the same private directory. Paths must be absolute with no `.` or `..` components. The scope document has exactly these three fields, each a nonempty host-issued string of at most 128 characters. Do not invent identity values:

```json
{"applicationScopeId":"issued-application","endUserId":"issued-user","authorizationRevision":"issued-revision"}
```

Keep the credentials directory separate from transaction state roots. For example, use tansr-demo-credentials/ for token/scope, another tansr-demo-state/ for creation/material intents and archive files, and tansr-demo-journal/ for the executor journal. The parent of prepare-create/create-intent/intent/file and the journal directory itself must not equal the credentials directory. The Demo rejects this before creating a Client with credentials_state_overlap, instead of failing later on a native directory lock. This separation retains current-scope checks on storage operations; it neither caches authorization nor weakens underlying verification.

For example, run this local script to enter already issued credentials. Its parent directory must exist; existing files are never overwritten:

```python
from getpass import getpass
from pathlib import Path
from tansr_sdk import strict_json
from tansr_sdk.storage import PrivateDirectory

path = Path.home() / "tansr-demo-credentials"
scope = {key: input(key + ": ") for key in
         ("applicationScopeId", "endUserId", "authorizationRevision")}
token = getpass("Issued Serve token: ").encode("ascii")
with PrivateDirectory(str(path), create=True) as directory:
    directory.write("scope.json", strict_json.dumps(scope), replace=False)
    directory.write("token", token, replace=False)
```

PowerShell:

```powershell
$env:TANSR_BASE_URL = 'http://127.0.0.1:8787'
$env:TANSR_TOKEN_FILE = 'C:\Users\YOUR_USER\tansr-demo-credentials\token'
$env:TANSR_SCOPE_FILE = 'C:\Users\YOUR_USER\tansr-demo-credentials\scope.json'
```

Linux/macOS:

```sh
export TANSR_BASE_URL=http://127.0.0.1:8787
export TANSR_TOKEN_FILE="$HOME/tansr-demo-credentials/token"
export TANSR_SCOPE_FILE="$HOME/tansr-demo-credentials/scope.json"
```

| Option | Default and meaning |
| --- | --- |
| `--base` | `TANSR_BASE_URL`, otherwise `http://127.0.0.1:8787` |
| `--family` | `sdk1`; alternative: `sdk2-offload-v1` |
| `--token-file` / `--scope-file` | Corresponding environment variables; both required |
| `--ca-file` | Optional absolute path to trusted CA PEM; TLS verification stays enabled |
| `--timeout` | Entire local Demo lifetime: 600 seconds, range 1..86400 |
| `--request-timeout` | Ordinary control request limit: 30 seconds, range 1..86400 |

Before HTTPS DNS resolution or connection, the default HttpTransport checks the stdlib SSL runtime. The recognized minimum is OpenSSL 1.1.1n+ in the 1.x branch or 3.0.2+ in the 3.x branch. Earlier versions and unverified alternative providers fail with unsupported_tls_runtime; there is no bypass flag. This addresses a known certificate-parsing risk and does not restore upstream maintenance of old OpenSSL. The separate OpenSSL used by cryptography cannot satisfy the stdlib SSL check.

Official Windows CPython 3.7.9 includes OpenSSL 1.1.1g, so its default HTTPS remains unavailable. Its pre-connection rejection is retained; loopback HTTP cannot replace a positive TLS result. A separate isolated Anaconda CPython 3.7.9 `h60c2a47_0` + OpenSSL 1.1.1w `h2bbff1b_0` combination has now passed with the installed SDK wheel: default-trust HTTPS 200, CA/hostname/expired-certificate rejection, real handshakes and SSE cancellation/deadlines. This applies to that exact combination, not every Python 3.7 environment.

The Python language/ABI floor and SSL runtime baseline are separate requirements. Exact official packages/hashes and isolated installation/`micromamba run -p ... python` steps are in the [release notes](https://github.com/tansrai/tansr-python/blob/main/doc/%E5%8F%91%E8%A1%8C%E8%AF%B4%E6%98%8E.md). This supplies a working legacy 3.7.9 route, not restored upstream maintenance for Python 3.7 or OpenSSL 1.1.1. Prefer maintained modern runtimes for new applications. Hosts may instead inject an independently verified transport; Demos have no such switch. Do not replace global DLLs or disable verification. `--ca-file` is neither a guard bypass nor a guarantee that all system roots are additionally retained.

SDK subprocesses and newly created venvs must also inherit the prefix's DLL search paths. In some tested launch contexts, the initial `micromamba run` interpreter could load SSL while its children lacked Library/bin in PATH. The release notes provide the verified child-environment launcher; it adds only process-local paths, without changing global PATH. Do not directly launch prefix/python.exe or reuse a venv created by the old interpreter as evidence that the new SSL environment is active.

On macOS, also verify the interpreter build's actual directory-descriptor capabilities. A tested Conda CPython 3.7.12 build targeting macOS 10.9 supports TLS but has empty `os.supports_dir_fd` and lacks the required `*at` build capabilities, so it cannot use SDK private directories, archives or journals. A `3.7+` version string alone does not establish full compatibility. The SDK does not downgrade to unchecked path access. The alternative isolated x86_64 build from official CPython 3.7.17 **source**, targeting 10.13 and linking OpenSSL 1.1.1w, passed actual directory capabilities, SDK/Demo sdist installation, the full unit suite, default HTTPS, real asynchronous sessions in both families and Demos. This is a local Rosetta build, not an official binary distribution or native arm64 Python 3.7; see the release notes for the exact scope.

The new interpreter remains linked to the existing OpenSSL prefix. The old Conda interpreter's private-storage failure does not make its linked OpenSSL libraries disposable. Retain the runtime, that dependency prefix and the new venv created from the runtime. Copying only the venv/python executable or removing those still-linked libraries breaks this environment.

The Demo rechecks current scope whenever obtaining a token or accessing private state. Scope changes invalidate the current instance; archived identity cannot restore authorization. Tokens can rotate within the same scope. Saved intents retain their original absolute deadline: a longer new process timeout does not extend it. Ctrl+C stops local work; only explicit `/interrupt` requests remote interruption.

Exit 0 means the stage stated by that command succeeded. Exit 1 means runtime failure, cancellation or unknown outcome; argument errors exit 2, and directly caught `KeyboardInterrupt` exits 130. Host usually turns Ctrl+C into cancellation and exit 1. Errors expose only sanitized error codes and HTTP status, never tokens, server detail or exception chains. Conversation text and explicitly requested query results are displayed in the terminal.

## Chat Demo

```text
tansr-py-chat --message "Hello"
tansr-py-chat --attach SESSION_ID
tansr-py-chat --resume SESSION_ID
```

Omit `--message` for interactive use. Attach observes an existing session; resume explicitly calls the resume API. Both are mutually exclusive with each other and the two creation-intent modes below. Attach/resume reject `--model` and `--request-id`. New sessions optionally accept model; offload creation requires a stable request-id, while sdk1 forbids it. `--last-event-id` is the original delivered decimal-string cursor, not a turn ID or archive coverage watermark.

The SSE stream opens before sending. Only a matching terminal outcome completes the current turn: 202, EOF or an older turn's completion does not. A running session rejects `--message`. Interactive attachment obtains a trusted current turn or replays from 0 when required; an arbitrary nonzero cursor cannot replace that replay. A replay gap, EOF without terminal outcome, failed or aborted turn exits nonzero. Reconcile the original session; the Demo does not automatically create another session or resend a message.

| Interactive input | Behavior |
| --- | --- |
| Plain text | Starts a new turn only while idle |
| `/interrupt` | Requests remote interruption, then observes the actual terminal event |
| `/allow TICKET` / `/deny TICKET` | Answers an observed, still-open permission ticket using its original digest |
| `/answers TICKET JSON_ARRAY` | Answers the current question; each item has questionId and optionally selectedOptionIds/freeText |
| `/target` | Reads current input capabilities and a trusted target |
| `/insert JSON_OBJECT` | Submits original inputId/target/content.text and optional ack; acknowledgement is not consumption |
| `/history` | Calls history(0, 0) for the history summary/count |
| `/quit` | Stops local observation without remote interruption |

Use actual historyEpoch/turnId values from a fresh target query, not these placeholders:

```text
/insert {"inputId":"my-original-input","target":{"historyEpoch":"REAL_EPOCH","turnId":"REAL_TURN"},"content":{"text":"More context"}}
```

Noninteractive permission/question requests exit nonzero and retain the session so it can be attached interactively. Inline tool requests are not automatically executed; use the explicit executor described below.

### Offload creation with a lost response

Persist the original intent before submission:

```text
tansr-py-chat --family sdk2-offload-v1 --request-id create-one --prepare-create ABSOLUTE_PRIVATE_FILE --timeout 600
tansr-py-chat --family sdk2-offload-v1 --create-intent ABSOLUTE_PRIVATE_FILE
tansr-py-chat --family sdk2-offload-v1 --attach RETURNED_SESSION_ID --message "Hello"
```

Prepare creates no session. Create-intent retains the original body, requestId, requestKey, deadline and owner; it prints the session ID and exits without a turn. Both intent modes reject message/cursor; submitting a saved intent also rejects model/requestId overrides. After a lost response, reuse the same file within its original deadline. Expiry leaves an unknown outcome; a fresh ID is not recovery. sdk1 has no such requestId recovery contract and an unknown creation must not be blindly retried.

## Business tools and live output Demo

```text
tansr-py-tools --journal ABSOLUTE_PRIVATE_DIRECTORY --require-output --run-once
```

For a new offload session, add `--family sdk2-offload-v1 --request-id tools-one`. Executor ID defaults to `python-demo` and can be set with `--executor`. `--session SESSION_ID` attaches to a session already declaring the same DemoOrderStatus tool and rejects request-id. The Demo refreshes capability closures, initializes and binds an executor, and requires the tool to be effectively available. It does not edit an existing session's declarations.

After `ready`, attach chat in another terminal using the printed command and ask for order DEMO-001. This tool returns synthetic sample data: DEMO-001 is awaiting shipment; other IDs produce a definite business error. It is not a real order service. The deterministic repository fixture uses GO-TOOL as a test trigger; this is fixture behavior, not general SDK syntax.

Require-output requires output negotiation and seal ACK. The handler emits its first stdout block, cooperatively waits 0.75 seconds, then emits stderr; the first block precedes handler completion. It does not replace process-global stdout/stderr. Cancellation after output retains unknown outcome rather than asserting no side effects. Runner handles lease renewal, current authorization and durable original claims/business receipts. Output sealing is independently confirmed and does not rewrite the business result.

Run-once stops after one actual submitted business receipt, never a successful empty poll. A non-completed business receipt or unconfirmed output exits nonzero. Retain the journal and do not rerun the handler to repair a seal. Without run-once the executor continues taking work; Ctrl+C stops it locally.

### Register tools in your own application

Follow the [tools Demo](https://github.com/tansrai/tansr-python/blob/main/demo/src/tansr_demo/tools.py): `Tool(declaration, handler)` → `Tool.registration()` → `ExecutorClient.register()` → `initialize()` / `bind()` → optional `negotiate_output()` → `Runner`. Initialization declares the platform. Effective tools and binding must be confirmed by Serve; locally supported tools are not automatically authorized. Use the current capability closure, not a closure, binding or connectionRevision copied from an old log.

The handler signature is `handler(context: ToolContext, arguments: dict)`. `context.check()` checks the original deadline and cooperative cancellation. When output is negotiated, use `context.output.capture("stdout", b"...")` or the stderr byte channel. Tool declarations and ordinary business JSON follow their own contract; do not apply control canonicalization to rewrite business floats. The SDK maintains per-operation output sequences, original UTF-8 bytes and digests.

A successful result is `{"status":"ok","content":[{"t":"text","text":"..."}]}`. A definite business error can return `{"status":"error","message":"..."}`. `Rejected(code)` is only for a refusal known to have no side effects. Ordinary exceptions, invalid results and cancellation with uncertain side effects must preserve unknown outcome. Read `ExecutionOutcome.receipt`, `output_confirmed` and `output_error` separately. Reconcile lost output using its original status/blocks, without rerunning the business function.

`adapt_async_handler` runs the coroutine on a separate loop in that handler's worker thread, not the application's main loop. Synchronous handlers still cooperate through `context`; the SDK cannot forcibly kill arbitrary Python threads. Authorization callbacks, handlers and on_receipt callbacks must not block indefinitely.

Before creation, the journal saves original body/key/deadline in demo-session.intent.json; demo-session.result.json stores the known session ID. Restarting the same directory attaches that ID. Offload lost creation reuses the original intent; sdk1 with only a pending intent refuses another creation. If a trusted source recovers the sdk1 ID, explicitly pass session to attach and record it. Deleting a journal or changing directories is not recovery of the original operation.

## Local archive Demo

Serve's host owns archive source registration and identity. Query target first. An already bound offload session cannot be forced into an unbound creation path. sdk1 manual creation uses prepare/create.

| Mode | Required specific options | Successful stage |
| --- | --- | --- |
| target | --session | Reads binding target |
| prepare-create | --session --source --request-id --intent | Persists original intent; creates nothing |
| create | --intent | Submits original intent and returns binding ID |
| creation-status | --intent | Original operation is completed |
| status | --binding | Reads state/publishedThroughSequence/acknowledgedCoverage |
| sync | --binding --file --key-id | All pages synchronized; optional max-pages |
| recover | --binding --file --key-id --request-id | Existing pending ACK recovered and confirmed completed |
| materials | --binding --file --key-id --request-id --intent | Supplies one actual material request and reports ingress state |
| material-submit | --intent | Submits original material response and reports ingress state |
| material-status | --intent | Material state is core-consumed |

Pass the mode with `--mode`. Options belonging to other modes are rejected. Intent/file paths must be absolute private file paths. The three store modes additionally require `--key-file` or TANSR_ARCHIVE_KEY_FILE. This file contains 64 hex characters, optionally followed by a newline: a host-managed 32-byte AES-256-GCM key. Do not embed, discard or store the key within the archive. Key-id identifies the key; it contains no secret material.

Example of generating a new private key file, only for a new archive. Never overwrite the original key needed for recovery:

```python
import secrets
from pathlib import Path
from tansr_sdk.storage import PrivateDirectory

with PrivateDirectory(str(Path.home() / "tansr-demo-keys"), create=True) as directory:
    directory.write("archive.key", secrets.token_hex(32).encode("ascii"), replace=False)
```

```text
tansr-py-archive --mode target --session SESSION_ID
tansr-py-archive --mode sync --binding BINDING_ID --file ABSOLUTE_ARCHIVE_FILE --key-id local-key-1 --key-file ABSOLUTE_KEY_FILE
tansr-py-archive --mode recover --binding BINDING_ID --file ABSOLUTE_ARCHIVE_FILE --key-id local-key-1 --key-file ABSOLUTE_KEY_FILE --request-id ORIGINAL_RECOVERY_ID
```

Sync defaults to 64 pages; max-pages accepts 1..1024. Reaching that bound without completion fails. Pages and chain continuity are verified, and data plus pending coverage are durably saved before ACK. Event cursors, receipt/head, pending coverage, ACK and material consumption are separate states: a saved file does not prove ACK confirmation. Recover requires the original archive and repairs only pending ACK; it does not claim all remaining pages are synchronized. Only a stale revision with the explicit `if_match_stale` reason permits explicit rebase, retaining the original operationEpoch. Arbitrary 412, busy, expiry or revocation does not authorize a new-epoch retry.

### Material blocks and cold recovery

Start the listener before requesting the real archived material through Serve:

```text
tansr-py-archive --mode materials --binding BINDING_ID --file ABSOLUTE_ARCHIVE_FILE --key-id local-key-1 --key-file ABSOLUTE_KEY_FILE --request-id response-one --intent ABSOLUTE_RESPONSE_FILE
tansr-py-archive --mode material-status --intent ABSOLUTE_RESPONSE_FILE
```

The SDK supplies only named material from verified records. The Demo first saves .request and its .owner, fixing the original remaining TTL as an absolute deadline. Before uploading blocks, .identity persists the original response requestId, operationEpoch and deadline. Finally it saves the exact response intent and its owner. Cold recovery reuses these files, identity and deadline; it obtains no fresh TTL and does not replace the key or overwrite the old intent.

Materials/material-submit may exit 0 at received. This is ingress acceptance, not core use. Material-status exits 0 only at core-consumed; received, expiry and unknown state are not consumption completion. After intent expiry, status modes can still query original facts; expiry does not authorize another write.

### Archives, memory and cache are different capabilities

`FileStore(path, key, key_id, identity, check_access)` manages a single terminal's encrypted archive. Derive its identity from actual binding/status through `identity_from_binding`. Every access still checks current authorization; ciphertext, an old ticket or a backup grants no authority. A custom `ArchiveStore` may use developer-provided storage, but must preserve the same validation, atomic durability, pending/coverage separation and cancellation/commit boundaries. Python storage files are not interchangeable with other SDKs' private storage formats.

The Python host retains long history and supplies named materials; Serve decides what to use, compress and assemble into context. This version has no dedicated `MemoryClient` or complete memory publication/synchronization host. Generic `Client.call` can consume the frozen `terminal.memory.read`, `terminal.memory.command` and `terminal.memory.receipt` operations when authorized by Serve; `FileStore` is not a complete memory database. Likewise, `cache.*` are frozen lower-level operations, not a promise of automatic high-level cache continuity in this package.

For an existing session, read memory state using `api.call("terminal.memory.read", CallOptions(parameters={"id": session_id}))`; import `CallOptions` from `tansr_sdk`. Writes require the original schema, actual revision/requestId and authorization context. Do not copy another session's data. Availability of configuration, memory and cache depends on Serve's host configuration and negotiation, not a client-side unlock.

## Sync, asyncio and resource lifetimes

The three console commands use synchronous public APIs. An additional executable async single-turn example is provided:

```text
python -m tansr_demo.async_chat --message "Hello"
python -m tansr_demo.async_chat --attach ID --message "Hello"
```

It requires an idle session and message, rejects creation-intent modes and directs permission/question handling to interactive attachment. Inside an existing event loop, await SDK methods directly instead of nesting asyncio.run. This complete script requires only the SDK, not the Demo package. It uses the issued private token/scope files described above and a trusted `TANSR_SESSION_ID`. For offload, also set `TANSR_SESSION_FAMILY=sdk2-offload-v1`; this variable is read by this example, not implicitly by Client:

```python
import asyncio
import os
from pathlib import Path
from tansr_sdk import AuthToken, Client, Error, strict_json
from tansr_sdk.session import AsyncSessionClient, SessionClient
from tansr_sdk.storage import PrivateDirectory

def token_provider(cancel):
    cancel.check()
    token_file = Path(os.environ["TANSR_TOKEN_FILE"])
    scope_file = Path(os.environ["TANSR_SCOPE_FILE"])
    if token_file.parent != scope_file.parent:
        raise Error("invalid_input")
    with PrivateDirectory(str(token_file.parent)) as directory:
        scope = strict_json.loads(directory.read(scope_file.name, max_bytes=8192))
        fields = {"applicationScopeId", "endUserId", "authorizationRevision"}
        if (not isinstance(scope, dict) or set(scope) != fields or
                any(not isinstance(value, str) or not 1 <= len(value) <= 128
                    for value in scope.values())):
            raise Error("invalid_input")
        token = directory.read(token_file.name, max_bytes=8192).strip(b"\r\n").decode("ascii")
        if strict_json.loads(directory.read(scope_file.name, max_bytes=8192)) != scope:
            raise Error("permission")
    cancel.check()
    principal = strict_json.dumps({key: scope[key] for key in sorted(scope)}).decode("utf-8")
    return AuthToken(token, principal)

async def read_async(api, session_id):
    async with AsyncSessionClient(api) as sessions:
        session = await sessions.attach(session_id)
        return await session.history(0, 0)

with Client(os.environ["TANSR_BASE_URL"], token_provider,
            family=os.environ.get("TANSR_SESSION_FAMILY", "sdk1")) as api:
    session_id = os.environ["TANSR_SESSION_ID"]
    print(SessionClient(api).attach(session_id).history(0, 0))
    print(asyncio.run(read_async(api, session_id)))
```

The provider derives a stable principal from trusted scope. Client checks it each time it reads a new token and rejects reuse after an identity change. Production hosts may replace the function with their own authentication system; they need not copy Demo file management. Never distribute the Serve provider's upstream model key to terminals.

For synchronous streams, call `session.events(last_event_id, cancel=..., deadline_ms=...)` and use `with stream`. Read `session.meta().last_seq` before sending and construct `TurnTracker(floor)`. After submission is accepted, pass events to `tracker.observe(event)` and inspect the matching `Outcome.status`. The asynchronous form is `async with await session.events(...) as stream` followed by `async for event in stream`; the initial await is required. `TurnTracker.from_replay(floor)` / `TurnTracker.resume(floor, turn_id)` handle an already-running original turn. Reconcile replay gaps instead of accepting an older terminal event as the new turn's success. Complete executable flows are in [sync chat](https://github.com/tansrai/tansr-python/blob/main/demo/src/tansr_demo/chat.py) and [async chat](https://github.com/tansrai/tansr-python/blob/main/demo/src/tansr_demo/async_chat.py).

Client defaults to max_pending=64 total in-flight operations, including providers and streams; accepted range is 1..4096, and exceeding capacity raises capacity. AsyncClient defaults to 4 workers/16 pending for control and 4/8 for streams. AsyncSessionClient defaults to control 4/16 and streams 2/8. The default HTTP transport permits 16 connections and 8 long streams, reserving room for control requests. Demo event delivery has a queue limit of 16. These independent budgets are not additive guarantees of server concurrency.

`AsyncArchiveClient(client, bridge=None, *, stream_workers=2, max_stream_pending=8, storage_workers=2, max_storage_pending=8)` separates short control operations, long streams and storage work. Its own control bridge defaults to 4/16; stream opening/reading uses 2/8; `prepare_materials`, `prepare_materials_before`, `sync_once` and `recover_pending` use the separate 2/8 storage pool. An injected `bridge` is borrowed only for short control calls. `aclose()` leaves that borrowed bridge open, closes the instance's own stream/storage pools and waits for its own work to end. Larger pools do not alter authorization, original deadlines or durable commit order.

Each async bridge submission captures the calling task's `contextvars.Context`. A callback's ContextVar assignments do not leak into the host or the next request. Runner observation/heartbeat threads inherit the current execution context; OutputWriter retains the owner context from construction, so later producers cannot replace output ownership through their thread identity. This is Python's standard shallow context copy, not a deep copy of mutable values. Providers/authorization callbacks must still verify current scope every time; context propagation is neither a frozen authorization grant nor a substitute for revocation checks.

Client takes ownership of an injected transport and closes it. AsyncClient(base_url, provider) owns its new Client; AsyncClient(client=existing_client) borrows the supplied Client and closes only its own bridge, streams and request tokens. High-level AsyncSessionClient, AsyncExecutorClient and AsyncArchiveClient borrow their synchronous entry points; closing them does not close the host's shared Client or send interrupt.

Use with/async with or explicitly close streams. A False return from close(timeout)/aclose(timeout) means work is still pending; retain dependencies until actual quiescence. Cancelling a Future does not kill a Python thread, revoke a remote request or roll back durable local facts. Business handlers must cooperate with cancellation, and async instances cannot cross event loops.

Executor APIs also include AsyncRunner and adapt_async_handler; archives expose AsyncArchiveClient. Sessions additionally offer multimodal send_blocks, input status, checkpoint byte import/export, compact/cwd and transcribe/speak requests. Text Demos do not exercise the entire API. Audio requests do not automatically record, play or send transcription as chat, and HTTP 200 does not replace response-content validation.

`Session.close()` / `await AsyncSession.close()` is a write that **ends the remote session**. `Client.close()`, async facade `aclose()`, event-stream close and `Runner.close()` reclaim local resources; these operations are not interchangeable. Stop new work, close streams and runners/facades, wait for actual disk/handler completion, then release journals, archives and finally Client. `AsyncRunner.aclose()` also stops its wrapped synchronous Runner, so pass only a Runner it is allowed to stop.

## Errors and troubleshooting

Catch public `tansr_sdk.Error`. Local validation/capacity/cancellation errors use `code`. Unified service errors normally use `code="http"`; also inspect `http_status`, `wire_code`, `retry_action`, `request_id` and `retry_after_ms`. `detail` may retain domainCode/reason for control flow, but should not be logged without redaction. HTTP status alone cannot establish whether side effects occurred.

| Symptom | Check first | Next step |
| --- | --- | --- |
| ModuleNotFoundError / command missing | Full venv interpreter path, `python -m pip show tansr-sdk tansr-sdk-demo`, separate Demo installation | Use that interpreter for `python -m tansr_demo.chat --help`; do not add the development tree to production PYTHONPATH |
| No matching wheel / dependency conflict | Python patch, CPU/OS, dependency markers, matching SDK/Demo versions | Use a verified target wheelhouse; do not disable dependency checks or assume native crypto dependencies are pure Python |
| unsupported_tls_runtime / TLS failure | Actual interpreter's `ssl.OPENSSL_VERSION`, trusted distribution, hostname and CA | Preserve the guard and use the verified isolated runtime described in release notes; never disable certificate verification |
| credentials_state_overlap / permission | Credentials versus state directories, current scope, private ACL/owner and path replacement | Separate the directories; create a new Client for a different identity; do not make the directory world-writable |
| http + 401/403 | Current short token, stable principal, application capabilities and service authorization | Reauthenticate or restore authorization; refreshing a same-principal token does not authorize retrying an unknown write |
| 202 without text / stream EOF / replay gap | Original session/turn, SSE proxy buffering/idle limits, cursor and service state | Reconnect to the original session and reconcile history/status; do not automatically create another session or resend |
| capacity / not_quiescent / close returns False | Unclosed streams, slow handlers/providers, active disk transactions and separate pending budgets | Stop new work, cancel cooperatively and wait for actual completion; do not claim a thread or transaction has rolled back |
| Network/timeout/result_unknown/commit_unknown | Original request key/body/deadline, journal/intent and remote status | Query the original outcome; do not change keys, delete directories or extend the original TTL to repeat business work |
| Stale revision / 412 | Actual domainCode/reason and original binding/epoch | Use explicit `recover_pending` rebase only when allowed; ordinary sync does not silently migrate |
| received but not core-consumed | Original material request, state and core scheduling | Retain the original archive/response intent and query material-status; do not advance consumption prematurely |
| Ciphertext authentication/key-id/format failure | Original key/file/owner, format version and backup source | Preserve the original and recover the correct key/file; do not erase or rekey it as recovery |

`Client.call` does not retry automatically. `retry_same_request(operation, options, previous)` accepts only an original error from the **same Client**, an explicit same-request hint and the same request fingerprint. It retains the original key/body/preconditions/absolute deadline. Altered error evidence, identity changes and deadline extensions are rejected. Query unknown/commit_unknown outcomes rather than resubmitting blindly; an ordinary 401 does not automatically refresh and replay a write.

Omitted fields and explicit null have different meanings. Do not turn every unspecified field into None. SDK `strict_json` retains numeric lexemes, but parsing and encoding does not preserve every original whitespace byte. Keep and reuse the original bytes whenever the contract requires byte-exact replay; do not replace archive/replay/signed control bodies with `json.dumps` or another encoding. Private journals, intents and archives are not freely editable configuration files.

## Verification boundaries

Runtime uses embedded generated operations and schemas; applications do not need contract source files or the generator. Public source checks use explicit `--mode public` and verify the repository's public contract manifest. Private service fixtures and development history are not runtime package content. Verify the version, source and hashes of release artifacts.

Local Demo tests use synthetic sessions, events and storage to cover arguments, identity, lost responses, terminal outcomes, blocks and received semantics. Separate integration runs exercise the actual Serve core with synthetic authentication, platform and model fixtures; they do not claim paid-model acceptance. Full regression, tested Windows/Linux/macOS combinations, independent wheel/sdist consumption and publication are separate checks. The release notes state the verified combinations and limits, without extending them to untested environments.
