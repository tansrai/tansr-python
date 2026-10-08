"""原创建意图、耐久ACK恢复与原TTL材料供给；不自动放弃旧身份。"""
import os
import re

from tansr_sdk import strict_json
from tansr_sdk.archive import ArchiveClient, FileStore, SavedIntent, identity_from_binding
from tansr_sdk.lifecycle import now_ms
from . import common

MODES = ("target", "prepare-create", "create", "creation-status", "status", "sync", "recover",
         "materials", "material-submit", "material-status")
MODE_ARGUMENTS = {
    "target": ("session",),
    "prepare-create": ("session", "source", "request_id", "intent"),
    "create": ("intent",), "creation-status": ("intent",), "status": ("binding",),
    "sync": ("binding", "file", "key_id"),
    "recover": ("binding", "file", "key_id", "request_id"),
    "materials": ("binding", "file", "key_id", "request_id", "intent"),
    "material-submit": ("intent",), "material-status": ("intent",),
}


def build_parser():
    parser = common.parser("tansr-py-archive", "Private archive, explicit recovery and material delivery through the SDK.")
    parser.add_argument("--mode", choices=MODES, required=True)
    for name in ("session", "source", "request-id", "binding", "key-id"):
        parser.add_argument("--" + name)
    for name in ("intent", "file", "key-file"):
        parser.add_argument("--" + name, type=common.absolute_file)
    parser.add_argument("--max-pages", type=common.bounded_integer(1, 1024),
                        help="sync page bound, default 64; bound reached is not completion")
    parser.epilog = ("Store modes use --key-file or TANSR_ARCHIVE_KEY_FILE (64 hex digits). "
                     "Keep original files, keys and identities on failure. Source registration belongs "
                     "to the Serve host. received is not core-consumed.")
    return parser


def validate_args(parser, args):
    required = MODE_ARGUMENTS[args.mode]
    allowed = set(required)
    if args.mode in ("sync", "recover", "materials"):
        allowed.add("key_file")
    if args.mode == "sync":
        allowed.add("max_pages")
    fields = {"session", "source", "request_id", "binding", "key_id", "intent", "file", "key_file", "max_pages"}
    common.require_arguments(parser, args, required, fields - allowed)


def save_intent(path, host, kind, body, deadline):
    owner_path = path.with_name(path.name + ".owner")
    with host.private_directory(path.parent, create=True) as directory:
        if directory.exists(owner_path.name):
            if common.load_owned(owner_path, host) != {"kind": kind}:
                common.fail("permission")
        else:
            common.save_owned(owner_path, host, {"kind": kind})
        # owner先落盘；中断留下owner时只允许原owner继续，不能覆盖旧意图。
        return SavedIntent.save(directory, path.name, kind, body, deadline)


def load_intent(path, host, kind=None):
    owner = common.load_owned(path.with_name(path.name + ".owner"), host)
    with host.private_directory(path.parent) as directory:
        intent = SavedIntent.load(directory, path.name)
    if owner != {"kind": intent.kind} or (kind is not None and kind != intent.kind):
        common.fail("invalid_intent")
    return intent


def exists(path, host):
    with host.private_directory(path.parent, create=True) as directory:
        return directory.exists(path.name)


def read_key(args):
    value = args.key_file or os.environ.get("TANSR_ARCHIVE_KEY_FILE")
    if not value:
        common.fail("archive_key_required")
    raw = common.read_private(common.absolute_file(str(value)), 256).strip(b"\r\n")
    if not re.fullmatch(b"[0-9a-fA-F]{64}", raw):
        common.fail("invalid_archive_key")
    return bytes.fromhex(raw.decode("ascii"))


def require_completed(receipt):
    if receipt.get("state") != "completed":
        common.fail("outcome_unknown")


def materials(client, store, host, args):
    response_path = args.intent
    request_path = response_path.with_name(response_path.name + ".request")
    identity_path = response_path.with_name(response_path.name + ".identity")
    if exists(response_path, host):
        saved = load_intent(response_path, host, "material-response")
        if saved.body["bindingId"] != args.binding or saved.body["request"]["requestId"] != args.request_id:
            common.fail("conflict")
    else:
        if exists(request_path, host):
            received = load_intent(request_path, host, "material-request")
        else:
            binding = client.binding(args.binding, **host.context())
            print("waiting for one material request; only named verified records are supplied", flush=True)
            received = None
            with client.events(binding, **host.context(host.deadline_ms)) as stream:
                for event in stream:
                    host.cancel.check(host.deadline_ms)
                    raw = strict_json.loads(event.data)["raw"]
                    if raw["eventType"] != "material.request":
                        continue
                    request = raw["payload"]
                    ttl = request["remainingTtlMs"]
                    if isinstance(ttl, bool) or not isinstance(ttl, int) or not 0 < ttl <= 86400000:
                        common.fail("contract")
                    end = min(now_ms() + ttl, host.deadline_ms)
                    received = save_intent(request_path, host, "material-request", request, end)
                    break
            if received is None:
                common.fail("outcome_unknown")
        if received.body["bindingId"] != args.binding:
            common.fail("conflict")
        host.cancel.check(received.deadline_ms)
        if exists(identity_path, host):
            saved_identity = common.load_owned(identity_path, host)
            if (not isinstance(saved_identity, dict) or set(saved_identity) != {"identity", "deadlineMs"}
                    or saved_identity["deadlineMs"] != received.deadline_ms):
                common.fail("invalid_intent")
            identity = saved_identity["identity"]
        else:
            binding = client.binding(args.binding, **host.context(received.deadline_ms))
            epoch = binding["operationEpoch"]
            if epoch is None:
                common.fail("contract")
            identity = dict(requestId=args.request_id, operationEpoch=epoch["id"])
            common.save_owned(identity_path, host, dict(identity=identity, deadlineMs=received.deadline_ms))
        if identity.get("requestId") != args.request_id:
            common.fail("conflict")
        # 原响应身份在分块网络写入前耐久保存，冷恢复不新造键/epoch或TTL。
        body = client.prepare_materials(store, received, identity, **host.context(received.deadline_ms))
        saved = save_intent(response_path, host, "material-response", body, received.deadline_ms)
    receipt = client.submit_materials(saved, **host.context(saved.deadline_ms))
    print("material state: {}; original response retained; use material-status for consumption".format(
        common.safe(receipt["state"])), flush=True)


def run(args):
    with common.Host(args) as host:
        client = ArchiveClient(host.api)
        mode = args.mode
        if mode == "target":
            target = client.binding_target(args.session, **host.context())
            common.emit_json(target)
            return
        if mode == "prepare-create":
            body = client.prepare_create(args.session, args.source, args.request_id,
                                         **host.context(host.deadline_ms))
            save_intent(args.intent, host, "binding-create", body, host.deadline_ms)
            print("binding intent saved; no binding was created; reuse create before the original deadline", flush=True)
            return
        if mode in ("create", "creation-status", "material-submit", "material-status"):
            kind = "binding-create" if mode in ("create", "creation-status") else "material-response"
            intent = load_intent(args.intent, host, kind)
            if mode == "create":
                binding = client.create_binding(intent, **host.context(intent.deadline_ms))
                print("binding: " + common.safe(binding["bindingId"]), flush=True)
            elif mode == "creation-status":
                receipt = client.creation_operation(intent.body["target"]["sessionId"], intent.body["request"],
                                                    **host.context())
                print("creation state: {}; binding: {}".format(common.safe(receipt["state"]),
                                                               common.safe(receipt.get("bindingId", ""))), flush=True)
                require_completed(receipt)
            else:
                receipt = (client.submit_materials(intent, **host.context(intent.deadline_ms))
                           if mode == "material-submit" else client.material_status(
                               intent.body["bindingId"], intent.body["materialRequestId"], **host.context()))
                print("material state: {} (received is not core-consumed)".format(common.safe(receipt["state"])), flush=True)
                if mode == "material-status" and receipt["state"] != "core-consumed":
                    common.fail("consumption_unconfirmed")
            return
        if mode == "status":
            status = client.status(args.binding, **host.context())
            common.emit_json({key: status[key] for key in ("state", "publishedThroughSequence", "acknowledgedCoverage")})
            return
        key = read_key(args)
        with host.private_directory(args.file.parent, create=mode == "sync") as directory:
            if mode != "sync" and not directory.exists(args.file.name):
                common.fail("original_archive_required")
        binding = client.binding(args.binding, **host.context())
        status = client.status(args.binding, **host.context())
        identity = identity_from_binding(binding, status)
        scope = host.credentials.scope
        if any(identity[key] != scope[key] for key in ("applicationScopeId", "endUserId")):
            common.fail("permission")

        def access(actual):
            host.credentials.check()
            if actual != identity:
                common.fail("permission")

        with FileStore(str(args.file), key, args.key_id, identity, access) as store:
            if mode == "recover":
                result = client.recover_pending(store, args.request_id, **host.context(host.deadline_ms))
                if not result.recovered or result.receipt is None:
                    common.fail("outcome_unknown")
                require_completed(result.receipt)
                print("pending ACK confirmed; recovery did not synchronize all remaining pages", flush=True)
            elif mode == "materials":
                materials(client, store, host, args)
            else:
                for page in range(args.max_pages or 64):
                    result = client.sync_once(store, common.request_id(), **host.context(host.deadline_ms))
                    print("page {}: verified records={} complete={} recovered={}".format(
                        page + 1, result.records, str(result.complete).lower(), str(result.recovered).lower()), flush=True)
                    if result.complete:
                        print("archive synchronized; coverage is separate from event cursor and material consumption", flush=True)
                        return
                common.fail("capacity")


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)
    return common.report(lambda: run(args))


if __name__ == "__main__":
    raise SystemExit(main())
