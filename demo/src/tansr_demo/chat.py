"""创建、恢复与观察真实会话；受理或EOF不代表本轮完成。"""
import queue
import re
import threading

from tansr_sdk import Error, strict_json
from tansr_sdk.lifecycle import now_ms
from tansr_sdk.session import (
    Answer, CreateOptions, Input, InputContent, InputTarget, SessionClient, TurnTracker,
)
from . import common


def build_parser():
    parser = common.parser("tansr-py-chat", "Chat through the public SDK; no automatic permission approval.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resume", help="explicitly resume an existing session")
    mode.add_argument("--attach", help="attach without creating or resuming")
    mode.add_argument("--prepare-create", type=common.absolute_file,
                      help="save original offload creation intent; send no mutation")
    mode.add_argument("--create-intent", type=common.absolute_file,
                      help="create/replay original offload intent, print ID, send no turn")
    parser.add_argument("--model")
    parser.add_argument("--message", help="one noninteractive turn; absent means interactive")
    parser.add_argument("--last-event-id", help="original delivered decimal-string cursor")
    parser.add_argument("--request-id", help="stable offload creation identity")
    parser.epilog = ("Commands: /interrupt, /allow TICKET, /deny TICKET, /answers TICKET JSON_ARRAY, "
                     "/insert JSON_OBJECT, /target, /history, /quit. Ctrl+C and /quit stop local "
                     "observation; only /interrupt requests remote interruption.")
    return parser


def validate_args(parser, args):
    if args.prepare_create is not None or args.create_intent is not None:
        if args.family != "sdk2-offload-v1":
            parser.error("creation intent modes require --family sdk2-offload-v1")
        common.require_arguments(parser, args, forbidden=("message", "last_event_id"))
        if args.prepare_create is not None:
            common.require_arguments(parser, args, required=("request_id",))
        else:
            common.require_arguments(parser, args, forbidden=("request_id", "model"))
    elif args.resume is not None or args.attach is not None:
        common.require_arguments(parser, args, forbidden=("request_id", "model"))
    elif args.family == "sdk2-offload-v1":
        common.require_arguments(parser, args, required=("request_id",))
    if args.family == "sdk1" and args.request_id is not None:
        parser.error("sdk1 creation has no requestId recovery contract")


def creation_options(saved, host):
    if not isinstance(saved, dict) or set(saved) != {"body", "write"}:
        common.fail()
    body, write = saved["body"], saved["write"]
    if (not isinstance(body, dict) or set(body) - {"requestId", "model"}
            or not isinstance(body.get("requestId"), str)
            or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", body["requestId"])
            or ("model" in body and not isinstance(body["model"], str))
            or not isinstance(write, dict) or set(write) != {"requestKey", "deadlineMs"}
            or not isinstance(write["requestKey"], str)
            or not re.fullmatch(r"[!-~]{1,128}", write["requestKey"])
            or isinstance(write["deadlineMs"], bool) or not isinstance(write["deadlineMs"], int)):
        common.fail()
    if write["deadlineMs"] <= now_ms():
        common.fail("timeout")
    return CreateOptions(request_id=body["requestId"], model=body.get("model"),
                         write=host.write(write["deadlineMs"], write["requestKey"]))


def prepare_creation(args, host):
    body = dict(requestId=args.request_id)
    if args.model is not None:
        body["model"] = args.model
    saved = dict(body=body, write=dict(requestKey=common.request_id(), deadlineMs=host.deadline_ms))
    creation_options(saved, host)
    common.save_owned(args.prepare_create, host, saved)
    print("session creation intent saved; no session was created; reuse --create-intent before its original deadline")


class Events:
    """一个读者、有界交付；控制请求不排在长SSE后面。"""
    def __init__(self, stream):
        self.stream = stream
        self.stop = threading.Event()
        self.queue = queue.Queue(maxsize=16)
        self.worker = threading.Thread(target=self._read, name="tansr-demo-events")
        try:
            self.worker.start()
        except BaseException:
            self.stream.close()
            raise

    def _put(self, value):
        while not self.stop.is_set():
            try:
                self.queue.put(value, timeout=0.05)
                return
            except queue.Full:
                pass

    def _read(self):
        try:
            for event in self.stream:
                if self.stop.is_set():
                    return
                self._put(("event", event))
            self._put(("eof", None))
        except Error as error:
            self._put(("error", error))
        except Exception:
            self._put(("error", Error("unknown")))
        finally:
            self.stream.close()

    def poll(self):
        try:
            kind, value = self.queue.get_nowait()
        except queue.Empty:
            return None
        if kind == "error":
            raise value
        if kind == "eof":
            common.fail("outcome_unknown")
        return value

    def close(self):
        self.stop.set()
        self.stream.close()
        self.worker.join(timeout=30)
        if self.worker.is_alive():
            common.fail("not_quiescent")

    def __enter__(self):
        return self

    def __exit__(self, *unused):
        self.close()


class Tickets:
    def __init__(self):
        self.permissions = {}
        self.questions = set()

    def display(self, event):
        raw, kind = event.raw, event.kind
        if kind == "msg.text.delta":
            print(common.safe(raw.get("text", "")), end="", flush=True)
            return
        ticket = raw.get("requestId")
        if kind == "server.permission.request":
            if not isinstance(ticket, str) or not ticket or not isinstance(raw.get("digest"), str):
                common.fail("contract")
            self.permissions[ticket] = raw["digest"]
            print("\n[permission {}] {} {}".format(common.safe(ticket), common.safe(raw.get("name", "")),
                                                  common.safe(raw.get("summary", ""))), flush=True)
            print("/allow TICKET or /deny TICKET; no default approval", flush=True)
        elif kind == "server.permission.closed":
            self.permissions.pop(ticket, None)
        elif kind == "server.question.request":
            if not isinstance(ticket, str) or not ticket or not isinstance(raw.get("questions"), list):
                common.fail("contract")
            self.questions.add(ticket)
            print("\n[question {}]".format(common.safe(ticket)), flush=True)
            common.emit_json(raw["questions"])
            print("/answers TICKET JSON_ARRAY", flush=True)
        elif kind == "server.question.closed":
            self.questions.discard(ticket)
        elif kind == "server.tool.request":
            common.fail("explicit_executor_required")
        elif not kind.startswith("msg."):
            print("\n[event: {}]".format(common.safe(kind)), flush=True)


def answers_from(value):
    if not isinstance(value, list):
        common.fail()
    result = []
    for item in value:
        if (not isinstance(item, dict) or "questionId" not in item
                or set(item) - {"questionId", "selectedOptionIds", "freeText"}):
            common.fail()
        result.append(Answer(item["questionId"], item.get("selectedOptionIds", []), item.get("freeText")))
    return result


def input_from(value):
    if (not isinstance(value, dict) or not {"inputId", "target", "content"}.issubset(value)
            or set(value) - {"inputId", "target", "content", "ack"}
            or not isinstance(value["target"], dict) or set(value["target"]) != {"historyEpoch", "turnId"}
            or not isinstance(value["content"], dict) or set(value["content"]) != {"text"}):
        common.fail()
    return Input(value["inputId"], InputTarget(value["target"]["historyEpoch"], value["target"]["turnId"]),
                 InputContent(text=value["content"]["text"]), value.get("ack"))


def command(line, session, tickets, host):
    word, _, rest = line.partition(" ")
    rest = rest.strip()
    if word == "/quit":
        print("local observation stopped; no remote interrupt requested", flush=True)
        return "quit"
    if word == "/interrupt":
        session.interrupt(host.write())
        print("interruption accepted; waiting for terminal event", flush=True)
    elif word in ("/allow", "/deny"):
        if rest not in tickets.permissions:
            print("ticket absent or closed", flush=True)
        else:
            session.permission(rest, tickets.permissions[rest], word[1:], host.write())
            tickets.permissions.pop(rest)
    elif word == "/answers":
        ticket, _, body = rest.partition(" ")
        if ticket not in tickets.questions:
            print("question absent or closed", flush=True)
        else:
            session.answer(ticket, answers_from(strict_json.loads(body)), host.write())
            tickets.questions.remove(ticket)
    elif word == "/insert":
        result = session.submit_input(input_from(strict_json.loads(rest)), host.write())
        common.emit_json(result)
        print("input acknowledged; acknowledgement is not core consumption", flush=True)
    elif word == "/target":
        common.emit_json(session.input_capabilities(**host.context()))
    elif word == "/history":
        common.emit_json(session.history(0, 0, **host.context()))
    else:
        print("unknown command; see --help", flush=True)
    return None


def chat(session, host, message=None, cursor=None):
    meta = session.meta(**host.context())
    if not meta.live or meta.status == "ended":
        common.fail("session_not_live")
    active = meta.status == "running"
    if active and message is not None:
        common.fail("turn_running")
    floor = meta.last_seq
    tracker = TurnTracker(floor)
    replaying = False
    if active:
        target = session.input_capabilities(**host.context()).get("target")
        turn = target.get("turnId") if isinstance(target, dict) else None
        if turn:
            tracker = TurnTracker.resume(floor, turn)
        else:
            latest = session.meta(**host.context())
            if latest.live and latest.status == "idle":
                active, floor, tracker = False, latest.last_seq, TurnTracker(latest.last_seq)
            elif latest.live and latest.status == "running":
                tracker, replaying = TurnTracker.from_replay(floor), True
            else:
                common.fail("outcome_unknown")
    if replaying and cursor not in (None, "0"):
        common.fail("replay_from_zero_required")
    stream = session.events(cursor if cursor is not None else ("0" if active else str(floor)),
                            **host.context(host.deadline_ms))
    console, tickets = common.Console(), Tickets()
    with Events(stream) as events:
        if message is not None:
            session.send(message, host.write())
            active = True
        while True:
            host.cancel.check(host.deadline_ms)
            event = events.poll()
            if event is not None:
                if event.kind == "server.replay.gap":
                    common.fail("reconciliation_required")
                tickets.display(event)
                if message is not None and event.kind in ("server.permission.request", "server.question.request"):
                    print("noninteractive run needs a reply; attach interactively to this same session", flush=True)
                    common.fail("interactive_reply_required")
                outcome = tracker.observe(event)
                if replaying and int(event.envelope["eventId"]) >= floor:
                    replaying = False
                    if tracker.active_turn_id is None and outcome is None:
                        latest = session.meta(**host.context())
                        if latest.live and latest.status == "idle":
                            active, tracker, tickets = False, TurnTracker(latest.last_seq), Tickets()
                        else:
                            common.fail("outcome_unknown")
                if outcome is not None and active:
                    active, tickets = False, Tickets()
                    print("\n[turn {}]".format(common.safe(outcome.status)), flush=True)
                    if outcome.status != "completed":
                        common.fail("outcome_" + outcome.status)
                    if message is not None or console.ended:
                        return
            if message is None:
                line = console.poll()
                if console.ended and line is None and not active:
                    return
                if line:
                    if line.startswith("/"):
                        if command(line, session, tickets, host) == "quit":
                            return
                    elif active:
                        print("turn is running; use /insert or wait", flush=True)
                    else:
                        before = session.meta(**host.context())
                        if not before.live or before.status != "idle":
                            common.fail("turn_running")
                        tracker = TurnTracker(before.last_seq)
                        session.send(line, host.write())
                        active = True
            host.cancel.wait(0.01)


def run(args):
    with common.Host(args) as host:
        if args.prepare_create is not None:
            prepare_creation(args, host)
            return
        sessions = SessionClient(host.api)
        if args.resume is not None:
            current = sessions.resume(args.resume, host.write())
        elif args.attach is not None:
            current = sessions.attach(args.attach, **host.context())
        else:
            options = creation_options(common.load_owned(args.create_intent, host), host) if args.create_intent else (
                CreateOptions(request_id=args.request_id, model=args.model, write=host.write()))
            current = sessions.create(options)
        print("session: " + common.safe(current.id), flush=True)
        if args.create_intent is None:
            chat(current, host, args.message, args.last_event_id)


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_args(parser, args)
    return common.report(lambda: run(args))


if __name__ == "__main__":
    raise SystemExit(main())
