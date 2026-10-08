import unittest

from tansr_sdk.errors import Error
from tansr_sdk.sse import Frame, frames


class SseTests(unittest.TestCase):
    def test_every_byte_split_utf8_bom_crlf_and_multiple_data(self):
        raw = b"\xef\xbb\xbf: heartbeat\r\nid: 12\revent: update\ndata: " + "你好".encode() + b"\ndata:\nretry: 20\n\n"
        expected = [Frame("update", "你好\n", "12", 20)]
        self.assertEqual(expected, list(frames(bytes((b,)) for b in raw)))
        for offset in range(len(raw) + 1):
            self.assertEqual(expected, list(frames([raw[:offset], raw[offset:]])))

    def test_no_data_has_no_event_and_absent_vs_empty_id(self):
        self.assertEqual([], list(frames([b"id: cursor\nretry: 3\n\n: ping\n\n"])))
        self.assertEqual(
            [Frame(None, "", "", None), Frame(None, "ok", None, None)], list(frames([b"id:\ndata:\n\ndata: ok\n\n"]))
        )

    def test_retry_uint64_and_nul_id(self):
        self.assertEqual(
            [Frame(None, "x", None, 18446744073709551615)],
            list(frames([b"id: invalid\x00id\nretry: 18446744073709551615\nretry: 18446744073709551616\ndata:x\n\n"])),
        )
        self.assertEqual([Frame(None, "x", None, 0)], list(frames([b"retry: " + b"0" * 5000 + b"\ndata: x\n\n"])))

    def test_incomplete_frame_and_invalid_utf8_rejected(self):
        for value in (b"data: x", b"data: x\n", b"unknown", b"\xef\xbb", b"data: \xed\xa0\x80\n\n", b": \xff\n\n"):
            with self.subTest(value=value), self.assertRaises(Error) as cm:
                list(frames([value]))
            self.assertEqual("contract", cm.exception.code)
        self.assertEqual([], list(frames([b"id: final\n"])))

    def test_byte_limit_includes_comments_and_line_delimiters(self):
        raw = b"data: x\n\n"
        self.assertEqual(1, len(list(frames([raw], len(raw)))))
        for value in (raw, b":" + b"x" * 16):
            with self.assertRaises(Error) as cm:
                list(frames([value], len(raw) - 1))
            self.assertEqual("resource_limit", cm.exception.code)
        self.assertEqual(2, len(list(frames([raw + raw], len(raw)))))

    def test_closing_parser_closes_owned_source(self):
        closed = []

        def source():
            try:
                yield b"data:x\n\ndata:y\n\n"
            finally:
                closed.append(True)

        parser = frames(source())
        self.assertEqual("x", next(parser).data)
        parser.close()
        self.assertEqual([True], closed)


if __name__ == "__main__":
    unittest.main()
