"""显式只读演示业务；Runner持有续租、耐久事实和独立输出确认。"""
from tansr_sdk import CancellationToken, Error
from tansr_sdk.executor import ExecutorClient, FileJournal, Rejected, Runner, Tool, current_platform
from tansr_sdk.lifecycle import now_ms
from tansr_sdk.session import CreateOptions, SessionClient
from tansr_sdk.storage import PrivateDirectory
from . import common

TOOL_NAME = "DemoOrderStatus"


def declaration():
    return {
        "name": TOOL_NAME, "description": "Read the status of sample order DEMO-001; this is demonstration data.",
        "parameters": {"orderId": {"type": "string", "description": "Sample order ID: DEMO-001"}},
        "readOnly": True,
    }


def lookup(context, arguments):
    if (not isinstance(arguments, dict) or set(arguments) != {"orderId"}
            or not isinstance(arguments["orderId"], str)):
        raise Rejected("invalid_order_arguments")
    if context.cancelled:
        raise Rejected("cancelled_before_lookup")
    if context.output is not None:
        context.output.stdout.write(b"order lookup started\n")
        # 首块在handler结束前交付；输出之后取消必须保unknown，不能称零副作用。
        if context.cancel.wait(0.75):
            raise Error("outcome_unknown")
        context.check()
        context.output.stderr.write(b"order lookup completed\n")
    if arguments["orderId"] != "DEMO-001":
        return {"status": "error", "message": "Sample order not found / 演示订单不存在"}
    return {"status": "ok", "content": [{"t": "text", "text":
            "DEMO-001: awaiting shipment (sample data) / 待发货（演示数据）"}]}


def build_parser():
    parser = common.parser("tansr-py-tools", "Explicit DemoOrderStatus business tool; no shell/file executor.")
    parser.add_argument("--journal", required=True, type=common.absolute_file,
                        help="absolute private directory; retain original claims/receipts")
    parser.add_argument("--session", help="attach to an existing session declaring DemoOrderStatus")
    parser.add_argument("--executor", default="python-demo")
    parser.add_argument("--request-id", help="stable identity required for a new offload session")
    parser.add_argument("--require-output", action="store_true", help="require stdout/stderr negotiation and seal ACK")
    parser.add_argument("--run-once", action="store_true", help="stop after one submitted business receipt")
    return parser


def validate_args(parser, args):
    if args.session is not None:
        common.require_arguments(parser, args, forbidden=("request_id",))
    elif args.family == "sdk2-offload-v1":
        common.require_arguments(parser, args, required=("request_id",))
    if args.family == "sdk1" and args.request_id is not None:
        parser.error("sdk1 has no creation requestId recovery contract")


def session_for(args, host, directory):
    sessions = SessionClient(host.api)
    result_path = args.journal / "demo-session.result.json"
    intent_path = args.journal / "demo-session.intent.json"
    if directory.exists(result_path.name):
        saved = common.load_owned(result_path, host)
        if not isinstance(saved, dict) or set(saved) != {"sessionId"}:
            common.fail("invalid_intent")
        if args.session is not None and args.session != saved["sessionId"]:
            common.fail("conflict")
        if args.request_id is not None and directory.exists(intent_path.name):
            original = common.load_owned(intent_path, host)
            if original["body"].get("requestId") != args.request_id:
                common.fail("conflict")
        return sessions.attach(saved["sessionId"], **host.context())
    if args.session is not None:
        current = sessions.attach(args.session, **host.context())
    else:
        replay = directory.exists(intent_path.name)
        if replay:
            saved = common.load_owned(intent_path, host)
            if args.family == "sdk1":
                print("sdk1 creation outcome is unknown; no automatic new session or replay", flush=True)
                common.fail("outcome_unknown")
        else:
            body = dict(clientTools=[declaration()])
            if args.request_id is not None:
                body["requestId"] = args.request_id
            saved = dict(body=body, requestKey=common.request_id(), deadlineMs=host.deadline_ms)
            common.save_owned(intent_path, host, saved)
        body = saved.get("body") if isinstance(saved, dict) else None
        expected_keys = {"clientTools", "requestId"} if args.family == "sdk2-offload-v1" else {"clientTools"}
        if (not isinstance(saved, dict) or set(saved) != {"body", "requestKey", "deadlineMs"}
                or not isinstance(body, dict) or set(body) != expected_keys
                or body["clientTools"] != [declaration()] or body.get("requestId") != args.request_id):
            common.fail("invalid_intent")
        if isinstance(saved["deadlineMs"], bool) or not isinstance(saved["deadlineMs"], int):
            common.fail("invalid_intent")
        if saved["deadlineMs"] <= now_ms():
            common.fail("timeout")
        current = sessions.create(CreateOptions(request_id=body.get("requestId"), client_tools=body["clientTools"],
                                  write=host.write(saved["deadlineMs"], saved["requestKey"])))
    common.save_owned(result_path, host, dict(sessionId=current.id))
    return current


def run(args):
    with common.Host(args) as host:
        # 确定业务结果在本地取消后仍须落盘；存储授权回调不借用已取消的token。
        with PrivateDirectory(str(args.journal), create=True, check_access=host.credentials.check) as directory:
            current = session_for(args, host, directory)
            print("session: " + common.safe(current.id), flush=True)
            client = ExecutorClient(host.api, host.credentials.scope)
            tool = Tool(declaration(), lookup)
            workspace = dict(workspaceId="python-business", revision="1")
            registration = dict(protocol="sdk2-ext-v1", executorId=args.executor,
                                platform=current_platform(), workspaces=[workspace],
                                operations=["tool.invoke"], tools=[tool.registration()])
            connection = client.register(registration, **host.context())
            closure = current.capabilities(**host.context())
            initialized = client.initialize(current.id, registration["platform"], [TOOL_NAME],
                                            closure.closure_id, **host.context())
            closure = current.capabilities(**host.context())
            capabilities = client.bind(current.id, connection, workspace, initialized["capabilityRevision"],
                                       closure.closure_id, **host.context())
            binding = capabilities["binding"]
            if binding is None or not any(item["name"] == TOOL_NAME and item["available"]
                                          for item in capabilities["effectiveTools"]):
                common.fail("tool_unavailable")

            def authorize(operation, cancel):
                host.credentials.check(cancel)
                if (operation["scope"] != host.credentials.scope or operation["sessionId"] != current.id
                        or operation["toolName"] != TOOL_NAME or operation["request"]["operation"] != "tool.invoke"
                        or operation["binding"] != binding):
                    common.fail("permission")

            terminal = None
            if args.require_output:
                terminal = client.negotiate_output(dict(sessionContract=args.family, sessionId=current.id),
                                                   binding, common.request_id(), **host.context())
            run_cancel = CancellationToken()
            unlink = host.cancel.register(run_cancel.cancel)
            outcomes = []

            def receipt(operation, outcome):
                print("business receipt: {}; output confirmed={}".format(
                    common.safe(outcome.receipt["status"]), str(outcome.output_confirmed).lower()), flush=True)
                if args.run_once or outcome.receipt["status"] != "completed" or not outcome.output_confirmed:
                    outcomes.append(outcome)
                    run_cancel.cancel()

            journal = FileJournal(directory)
            runner = Runner(client, registration, {TOOL_NAME: tool}, journal, authorize,
                            connection=connection, terminal=terminal, require_output=args.require_output,
                            on_receipt=receipt)
            print("ready: attach tansr-py-chat --family {} --attach {} in another terminal".format(
                args.family, common.safe(current.id)), flush=True)
            print("Ask for DEMO-001. Ctrl+C stops this executor locally, not the Serve turn.", flush=True)
            try:
                try:
                    runner.run(cancel=run_cancel)
                except Error as error:
                    if error.code != "cancelled" or not outcomes or host.cancel.cancelled:
                        raise
            finally:
                unlink()
                if not runner.close(timeout=30):
                    common.fail("not_quiescent")
                journal.close()
            if not outcomes:
                common.fail("cancelled")
            outcome = outcomes[-1]
            if not outcome.output_confirmed:
                print("business receipt retained; output seal is unconfirmed; do not rerun the handler", flush=True)
                common.fail("output_incomplete")
            if outcome.receipt["status"] != "completed":
                common.fail("outcome_" + outcome.receipt["status"])


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)
    return common.report(lambda: run(args))


if __name__ == "__main__":
    raise SystemExit(main())
