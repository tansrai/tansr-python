"""可执行的asyncio单轮示例；已有loop中的宿主可直接await observe。"""
import asyncio

from tansr_sdk.session import AsyncSessionClient, CreateOptions, TurnTracker
from . import chat, common


async def observe(host, args):
    async with AsyncSessionClient(host.api) as sessions:
        if args.resume is not None:
            current = await sessions.resume(args.resume, host.write())
        elif args.attach is not None:
            current = await sessions.attach(args.attach, **host.context())
        else:
            current = await sessions.create(CreateOptions(request_id=args.request_id, model=args.model,
                                                          write=host.write()))
        print("session: " + common.safe(current.id), flush=True)
        meta = await current.meta(**host.context())
        if not meta.live or meta.status != "idle":
            common.fail("turn_running")
        tracker = TurnTracker(meta.last_seq)
        stream = await current.events(args.last_event_id or str(meta.last_seq),
                                      **host.context(host.deadline_ms))
        tickets = chat.Tickets()
        try:
            async with stream:
                await current.send(args.message, host.write())
                async for event in stream:
                    host.cancel.check(host.deadline_ms)
                    if event.kind == "server.replay.gap":
                        common.fail("reconciliation_required")
                    tickets.display(event)
                    if event.kind in ("server.permission.request", "server.question.request"):
                        print("reply required: attach interactively to this same session", flush=True)
                        common.fail("interactive_reply_required")
                    outcome = tracker.observe(event)
                    if outcome is not None:
                        print("\n[turn {}]".format(common.safe(outcome.status)), flush=True)
                        if outcome.status != "completed":
                            common.fail("outcome_" + outcome.status)
                        return
            common.fail("outcome_unknown")
        except asyncio.CancelledError:
            # Python3.7的CancelledError属于Exception；不能被业务catch吞掉。
            await stream.aclose()
            raise


def main(argv=None):
    parser = chat.build_parser()
    parser.prog = "python -m tansr_demo.async_chat"
    args = parser.parse_args(argv)
    chat.validate_args(parser, args)
    common.require_arguments(parser, args, required=("message",), forbidden=("prepare_create", "create_intent"))

    def run():
        with common.Host(args) as host:
            asyncio.run(observe(host, args))
    return common.report(run)


if __name__ == "__main__":
    raise SystemExit(main())
