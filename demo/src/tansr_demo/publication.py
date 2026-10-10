"""显式记忆存储host；消费现有受信配置，不创建Source或模型工具。"""
import re

from tansr_sdk import CancellationToken, Error, strict_json
from tansr_sdk.executor import EncryptedJournal, ExecutorClient, FileJournal, Runner, current_platform
from tansr_sdk.memory_publication import FileStore, Host
from tansr_sdk.storage import PrivateDirectory
from . import common


def build_parser():
    parser = common.parser("python -m tansr_demo.publication", "Encrypted terminal MemoryPublication host.")
    parser.add_argument("--profile", choices=("memory-publication", "terminal-persistence-v1"), default="memory-publication",
                        help="new profile requires independent storage path; legacy media is never converted implicitly")
    parser.add_argument("--config", required=True, type=common.absolute_file,
                        help="private host-supplied identity, session, binding, connection and workspace JSON")
    parser.add_argument("--file", required=True, type=common.absolute_file)
    parser.add_argument("--journal-file", required=True, type=common.absolute_file)
    parser.add_argument("--key-file", required=True, type=common.absolute_file)
    parser.add_argument("--journal-key-file", required=True, type=common.absolute_file)
    parser.add_argument("--key-id", required=True)
    parser.add_argument("--journal-key-id", required=True)
    parser.add_argument("--mode", required=True, choices=("create", "reopen", "capacity", "copy-publication", "copy-journal", "migrate-journal"))
    parser.add_argument("--target-file", type=common.absolute_file, help="new create-only snapshot path")
    parser.add_argument("--target-key-file", type=common.absolute_file, help="private explicit target key")
    parser.add_argument("--target-key-id")
    parser.add_argument("--legacy-journal", type=common.absolute_file, help="existing plaintext journal directory")
    parser.add_argument("--operations-file", type=common.absolute_file, help="private complete original operation JSON array")
    parser.add_argument("--max-transfers", type=common.bounded_integer(1, 1048576), default=4096)
    parser.add_argument("--max-records", type=common.bounded_integer(1, 1048576), default=16384)
    parser.epilog = ("Serve host must install the source provider and binding using the original protocol. "
                     "Keep original paths, keys and transfer IDs on failure. This module does not publish or infer memories.")
    return parser


def read_key(path):
    raw = common.read_private(path, 256).strip(b"\r\n")
    if not re.fullmatch(b"[0-9a-fA-F]{64}", raw):
        common.fail("invalid_storage_key")
    return bytes.fromhex(raw.decode("ascii"))


def run(args):
    config = strict_json.loads(common.read_private(args.config, 32768))
    if (not isinstance(config, dict) or set(config) !=
            {"identity", "sessionId", "binding", "connection", "workspace"}):
        common.fail("invalid_publication_config")
    persistence = args.profile == "terminal-persistence-v1"
    store_type, host_type = FileStore, Host
    identity = config["identity"]
    store_options = dict(max_transfers=args.max_transfers)
    if persistence:
        from tansr_sdk.terminal_persistence import FileStore as PersistenceStore, Host as PersistenceHost
        from tansr_sdk.terminal_persistence._state import DEFAULT_LIMITS
        store_type, host_type = PersistenceStore, PersistenceHost
        if args.mode not in ("create", "reopen", "capacity", "copy-publication"):
            common.fail("new_profile_rejects_legacy_migration")
        identity = dict(**identity["scope"], **{key: identity[key] for key in ("sourceId", "sourceGeneration", "domainKey")})
        store_options = dict(limits=dict(DEFAULT_LIMITS, transferFacts=args.max_transfers))
    key, journal_key = read_key(args.key_file), read_key(args.journal_key_file)
    if key == journal_key:
        common.fail("independent_storage_keys_required")
    with common.Host(args) as host:
        host.check_state_directory(args.journal_file.parent)
        def context():
            host.credentials.check()
            return host.credentials.scope
        if args.mode in ("copy-publication", "copy-journal", "migrate-journal"):
            if not all((args.target_file, args.target_key_file, args.target_key_id)):
                common.fail("explicit_copy_target_and_key_required")
            host.check_state_directory(args.target_file.parent)
            target_key = read_key(args.target_key_file)
            journal_identity = dict(scope=config["identity"]["scope"], executorId=config["connection"]["executorId"])
            if args.mode == "copy-publication":
                if target_key == journal_key:
                    common.fail("independent_storage_keys_required")
                with store_type(str(args.file), key, args.key_id, identity, context,
                                mode="reopen", **store_options) as source:
                    result = source.copy_to(str(args.target_file), target_key, args.target_key_id)
                    if persistence:
                        common.emit_json(result)
            elif args.mode == "copy-journal":
                if target_key == key:
                    common.fail("independent_storage_keys_required")
                with EncryptedJournal(str(args.journal_file), journal_key, args.journal_key_id,
                        journal_identity, context, mode="reopen", max_records=args.max_records) as source:
                    source.copy_to(str(args.target_file), target_key, args.target_key_id)
            else:
                if not args.legacy_journal or not args.operations_file or target_key == key:
                    common.fail("explicit_legacy_inventory_and_independent_key_required")
                host.check_state_directory(args.legacy_journal)
                operations = strict_json.loads(common.read_private(args.operations_file, 32 << 20), max_bytes=32 << 20)
                with PrivateDirectory(str(args.legacy_journal)) as directory, FileJournal(directory) as source:
                    source.copy_to_encrypted(str(args.target_file), target_key, args.target_key_id,
                        journal_identity, context, operations=operations, max_records=args.max_records)
            print("read-only copy verified; source retained; cutover pending" if persistence else
                  "complete encrypted snapshot copied; source retained; reopen target and reconcile original keys")
            return
        mode = "reopen" if args.mode == "capacity" else args.mode
        with store_type(str(args.file), key, args.key_id, identity, context,
                        mode=mode, **store_options) as store:
            if persistence and store.copy_verified_cutover_pending and args.mode != "capacity":
                common.fail("read_only_copy_cutover_pending")
            with EncryptedJournal(str(args.journal_file), journal_key, args.journal_key_id,
                    dict(scope=config["identity"]["scope"], executorId=config["connection"]["executorId"]),
                    context, mode=mode, max_records=args.max_records) as journal:
                if args.mode == "capacity":
                    print(strict_json.dumps(store.capacity()).decode("utf-8"))
                    return
                adapter = host_type(store)
                registration = dict(protocol="sdk2-ext-v1", executorId=config["connection"]["executorId"],
                    platform=current_platform(), workspaces=[config["workspace"]], operations=["tool.invoke"],
                    tools=[adapter.registration()])
                def authorize(operation, cancel):
                    host.credentials.check(cancel)
                    if (operation["scope"] != context() or operation["sessionId"] != config["sessionId"] or
                            operation["binding"] != config["binding"] or operation["toolName"] != "MemoryPublication"):
                        common.fail("permission")
                client = ExecutorClient(host.api, context())
                run_cancel = CancellationToken()
                unlink = host.cancel.register(run_cancel.cancel)
                uncertain = []
                def receipt(operation, outcome):
                    request = strict_json.loads(operation["request"]["args"]["argsJson"])
                    print("publication receipt: " + common.safe(outcome.receipt["status"]), flush=True)
                    common.emit_json(dict(operationId=operation["operationId"], digest=operation["digest"],
                        action=request["action"], transferId=request.get("transferId"),
                        status=outcome.receipt["status"]))
                    if outcome.receipt["status"] == "unknown":
                        uncertain.append(operation["operationId"])
                        run_cancel.cancel()
                try:
                    adapter_option = {"terminal_persistence" if persistence else "memory_publication": adapter}
                    with Runner(client, registration, {}, journal, authorize,
                                connection=config["connection"], on_receipt=receipt, **adapter_option) as runner:
                        print("encrypted publication host ready; original source and binding retained", flush=True)
                        try:
                            runner.run(cancel=run_cancel)
                        except Error as error:
                            if error.code == "cancelled" and uncertain:
                                common.fail("outcome_unknown")
                            raise
                finally:
                    unlink()


def main(argv=None):
    args = build_parser().parse_args(argv)
    return common.report(lambda: run(args))


if __name__ == "__main__":
    raise SystemExit(main())
