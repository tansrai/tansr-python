"""严格字节、控制 canonical 与 Python 数值/可变对象边界。"""
import hashlib
import math
import unittest
from copy import deepcopy

from tansr_sdk import canonical, strict_json
from tansr_sdk.errors import Error


class StrictJsonTests(unittest.TestCase):
    def reject(self, raw, **limits):
        with self.assertRaises(Error):
            strict_json.loads(raw, **limits)

    def test_original_number_lexemes_survive_snapshot_and_encoding(self):
        raw = b'{"values":[-0,1.0,1e0,-1.5,1e400,9007199254740993],"flag":true}'
        value = strict_json.loads(raw)
        self.assertEqual([x.lexeme for x in value["values"]],
                         ["-0", "1.0", "1e0", "-1.5", "1e400", "9007199254740993"])
        self.assertEqual(strict_json.dumps(value), raw)
        self.assertEqual(strict_json.dumps(strict_json.snapshot(value)), raw)
        self.assertEqual(strict_json.dumps(deepcopy(value)), raw)
        self.assertTrue(math.isinf(value["values"][4]))
        self.assertIs(value["flag"], True)
        self.assertEqual(int(value["values"][-1]), 9007199254740993)

    def test_duplicates_are_rejected_before_dictionary_collapse(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":1,"\\u0061":2}',
                    b'{"nested":{"x":true,"x":false}}'):
            with self.subTest(raw=raw):
                self.reject(raw)

    def test_utf8_and_surrogate_boundaries(self):
        for raw in (b'"\xff"', b'"\xc0\xaf"', b'"\xed\xa0\x80"',
                    b'"\xf4\x90\x80\x80"', b'"\\ud800"', b'"\\udc00"',
                    b'"\\ud800\\u0041"', b'\xef\xbb\xbf{}',
                    '{}'.encode('utf-16'), '{}'.encode('utf-32')):
            with self.subTest(raw=raw):
                self.reject(raw)
        self.assertEqual(strict_json.loads(b'"\\ud83d\\ude00"'), "😀")
        self.assertEqual(strict_json.utf16_length("中😀"), 3)
        self.assertEqual(strict_json.loads('"é"'), 'é')
        self.assertNotEqual(strict_json.loads('"é"'), strict_json.loads('"é"'))

    def test_grammar_and_tail_are_strict(self):
        for raw in (b"", b" ", b"NaN", b"Infinity", b"-Infinity", b"01", b"1.",
                    b"+1", b"1e", b".1", b"trueX", b"null null", b"[1,]", b'{"a":1,}',
                    b"{1:0}", b'"\x00"', b'"\\x20"', b'"\\uZZZZ"'):
            with self.subTest(raw=raw):
                self.reject(raw)
        self.assertEqual(strict_json.loads(b" \r\n[0, false, null]\t"), [0, False, None])

    def test_bounds_are_applied_while_building_the_tree(self):
        self.assertEqual(strict_json.loads(b"[[0]]", max_depth=2, max_nodes=3), [[0]])
        self.reject(b"[[0]]", max_depth=1)
        self.reject(b"[0,0]", max_nodes=2)
        self.reject(b"[" * 2000 + b"]" * 2000)
        self.assertEqual(strict_json.loads('"😀"', max_bytes=6), "😀")
        self.reject('"😀"', max_bytes=5)
        self.assertEqual(strict_json.loads(b"1234", max_number_chars=4), 1234)
        self.reject(b"12345", max_number_chars=4)
        self.reject(b"9" * 4097)
        self.assertEqual(len(strict_json.dumps({"body": "x" * 300000}, max_bytes=400000)), 300011)
        with self.assertRaises(Error):
            strict_json.dumps({"body": "x" * 300000}, max_bytes=300000)

    def test_python_values_do_not_smuggle_types_cycles_or_nonfinite_numbers(self):
        for value in ({1: "numeric-key"}, {True: "boolean-key"}, {None: 1}, float("nan"),
                      float("inf"), float("-inf"), (1, 2), b"raw", object(), "\ud800"):
            with self.subTest(kind=type(value).__name__):
                with self.assertRaises(Error):
                    strict_json.dumps(value)
        cyclic = []
        cyclic.append(cyclic)
        with self.assertRaises(Error):
            strict_json.dumps(cyclic)
        with self.assertRaises(Error):
            strict_json.dumps(1 << 20000)
        value = {"nested": [{"value": 3}]}
        copied = strict_json.snapshot(value)
        value["nested"][0]["value"] = 4
        self.assertEqual(copied["nested"][0]["value"], 3)

    def test_lexeme_and_limits_cannot_be_changed_or_misdeclared(self):
        value = strict_json.JsonInt("-0")
        with self.assertRaises(AttributeError):
            value.lexeme = "1"
        with self.assertRaises(Error):
            strict_json.JsonInt(True)
        for limits in ({"max_bytes": True}, {"max_depth": 129}, {"max_nodes": 0},
                       {"max_number_chars": 4097}):
            with self.subTest(limits=limits):
                with self.assertRaises(Error):
                    strict_json.loads(b"0", **limits)


class CanonicalTests(unittest.TestCase):
    def test_control_numbers_are_not_python_numeric_coercions(self):
        self.assertEqual(canonical.encode({"n": True, "m": 0}), b'{"m":0,"n":true}')
        for raw in (b"-0", b"-1", b"1.0", b"1e0", b"9007199254740992", b"1e400"):
            with self.subTest(raw=raw):
                with self.assertRaises(Error):
                    canonical.encode(strict_json.loads(raw))
        with self.assertRaises(Error):
            canonical.encode({"x": 1.0})
        self.assertEqual(canonical.encode({"x": 9007199254740991}), b'{"x":9007199254740991}')

    def test_ascii_key_order_unicode_values_and_exact_strict_mode(self):
        value = {"9": 1, "10": 2, "z": "中😀", "a": "\b\t\n\f\r\x01\"\\/"}
        encoded = canonical.encode(value)
        self.assertTrue(encoded.startswith(b'{"10":2,"9":1,"a":'))
        self.assertIn("中😀".encode("utf-8"), encoded)
        self.assertEqual(canonical.encode({"slash": "/"}), b'{"slash":"/"}')
        self.assertEqual(canonical.encode(canonical.parse_strict(encoded)), encoded)
        for raw in (b'{"b":0,"a":0}', b' {"a":0}', b'{"a":0}\n'):
            canonical.decode(raw)
            with self.assertRaises(Error):
                canonical.parse_strict(raw)
        for key in ("", " ", "中", "\x7f"):
            with self.assertRaises(Error):
                canonical.encode({key: 0})

    def test_digest_domains_and_path_bytes(self):
        body = b'{"n":0}'
        self.assertEqual(canonical.digest_bytes("x", body), hashlib.sha256(b"x\0" + body).hexdigest())
        self.assertNotEqual(canonical.digest_bytes("x", body), canonical.digest_bytes("y", body))
        self.assertNotEqual(canonical.digest_bytes("x", body), canonical.digest_bytes("x", body + b" "))
        for domain in ("", "x\x00y", "\ud800"):
            with self.assertRaises(Error):
                canonical.digest_bytes(domain, body)
        self.assertEqual(canonical.encode_path_segment("a/b%2F+中😀"),
                         "a%2Fb%252F%2B%E4%B8%AD%F0%9F%98%80")
        self.assertEqual(canonical.encode_path_segment("-_.!~*'()"), "-_.!~*'()")


if __name__ == "__main__":
    unittest.main()
