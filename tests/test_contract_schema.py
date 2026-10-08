"""直接消费冻结金样；结构断言不冒充真实授权/执行/存储行为。"""
import base64
import copy
import hashlib
import json
from pathlib import Path
import unittest

from tansr_sdk import canonical, strict_json
from tansr_sdk.api.schema import validate_sandbox, validate_wire
from tansr_sdk.errors import Error

CONTRACT = Path(__file__).resolve().parents[1] / "contract"


def read(name):
    return strict_json.loads((CONTRACT / name).read_bytes(), max_depth=64, max_nodes=200000)


def patch(value, edit):
    if not edit["path"]:
        return copy.deepcopy(edit["value"])
    parts = [part.replace("~1", "/").replace("~0", "~") for part in edit["path"][1:].split("/")]
    parent = value
    for part in parts[:-1]:
        parent = parent[int(part)] if isinstance(parent, list) else parent[part]
    key = (len(parent) if parts[-1] == "-" else int(parts[-1])) if isinstance(parent, list) else parts[-1]
    if edit["op"] == "remove":
        del parent[key]
    elif edit["op"] == "add" and isinstance(parent, list):
        parent.insert(key, copy.deepcopy(edit["value"]))
    else:
        parent[key] = copy.deepcopy(edit["value"])
    return value


def materialize(vector, vectors, depth=0):
    if depth > 32:
        raise AssertionError("cyclic golden")
    if "valueFile" in vector:
        if vector["valueFile"] != "packages/server/contract/api-manifest.json":
            raise AssertionError("unexpected valueFile")
        return read("api-manifest.json")
    if "base" in vector:
        source = next(item for item in vectors if item["name"] == vector["base"])
        value = materialize(source, vectors, depth + 1)
        for edit in vector["patch"]:
            value = patch(value, edit)
        return value
    return copy.deepcopy(vector["value"])


class FrozenSchemaTests(unittest.TestCase):
    def verify(self, family, definition, value, accepted):
        if accepted:
            self.assertIsNone(validate_wire(family, definition, value))
        else:
            with self.assertRaises(Error):
                validate_wire(family, definition, value)

    def test_all_165_unified_vectors(self):
        vectors = read("unified-v1.golden.json")["vectors"]
        self.assertEqual(len(vectors), 165)
        self.assertEqual(sum(v["expect"] == "valid" for v in vectors), 41)
        for vector in vectors:
            with self.subTest(name=vector["name"]):
                self.verify("unified-v1", vector["definition"], materialize(vector, vectors),
                            vector["expect"] == "valid")

    def test_all_92_terminal_vectors(self):
        count = 0
        for family in ("terminal-services-v1", "terminal-observation-v1", "terminal-profile-v1"):
            source = read(family + ".golden.json")
            for group in ("positive", "negative"):
                for vector in source[group]:
                    with self.subTest(family=family, group=group, name=vector["id"]):
                        self.verify(family, vector["definition"], vector["value"], group == "positive")
                    count += 1
        self.assertEqual(count, 92)

    def test_all_32_sandbox_semantic_vectors(self):
        cases = read("terminal-shell-sandbox-v1.golden.json")["cases"]
        self.assertEqual(len(cases), 32)
        for case in cases:
            with self.subTest(name=case["id"]):
                if case["accept"]:
                    self.assertIsNone(validate_sandbox(case["kind"], case["input"], request=case.get("request")))
                else:
                    with self.assertRaises(Error):
                        validate_sandbox(case["kind"], case["input"], request=case.get("request"))

    def test_unknown_family_definition_and_python_type_smuggling(self):
        with self.assertRaises(Error) as unknown:
            validate_wire("agent-session-v1", "Id", "id")
        self.assertEqual(unknown.exception.code, "invalid_input")
        for path in CONTRACT.glob("*.schema.json"):
            with self.subTest(family=path.name):
                with self.assertRaises(Error):
                    validate_wire(path.name[:-12], "NoSuchDefinition", None)
        for value in (True, 1, -1, 1.0, None, "01", "9223372036854775808", "1\n", "١"):
            self.verify("unified-v1", "Sequence", value, False)
        self.verify("unified-v1", "Sequence", "9007199254740993", True)
        self.verify("unified-v1", "Sequence", "9223372036854775807", True)
        self.verify("unified-v1", "Id", "value\n", False)


class FrozenCanonicalTests(unittest.TestCase):
    def test_all_127_cross_vectors(self):
        vectors = json.loads((CONTRACT / "canonical-cross-vectors.json").read_bytes())["vectors"]
        self.assertEqual(len(vectors), 127)
        for vector in vectors:
            with self.subTest(name=vector["id"]):
                if "generator" in vector:
                    generator = vector["generator"]
                    self.assertEqual(generator["kind"], "flat-array")
                    elements = [generator["element"]] * generator["count"]
                    if "tail" in generator:
                        elements.append(generator["tail"])
                    raw = ("[" + ",".join(elements) + "]").encode("utf-8")
                elif vector["inputKind"] == "bytes":
                    raw = base64.b64decode(vector["input"])
                else:
                    raw = vector["input"].encode("utf-8")
                limit = vector.get("maxBytes", canonical.DEFAULT_MAX_BYTES)
                if vector["expect"] == "reject":
                    with self.assertRaises(Error):
                        canonical.decode(raw, max_bytes=limit)
                else:
                    value = canonical.decode(raw, max_bytes=limit)
                    encoded = canonical.encode(value, max_bytes=limit)
                    if "canonicalHex" in vector:
                        self.assertEqual(encoded.hex(), vector["canonicalHex"])
                    else:
                        self.assertEqual(hashlib.sha256(encoded).hexdigest(), vector["canonicalSha256"])

    def test_wire_metadata_paths_sse_and_original_ir(self):
        wire = read("sdk2-wire-v1.json")
        for item in wire["metadata"]:
            self.assertEqual(canonical.encode(item["value"]), item["utf8"].encode("utf-8"))
        for item in wire["pathIds"]:
            self.assertEqual(canonical.encode_path_segment(item["id"]), item["segment"])
        for raw in wire["invalidMetadata"]:
            with self.assertRaises(Error):
                canonical.decode(raw)
        event = wire["sse"]["frame"]
        encoded = "id: {}\nevent: {}\ndata: {}\n\n".format(
            event["cursor"], event["eventType"], canonical.encode(event).decode("utf-8"))
        self.assertEqual(encoded, wire["sse"]["utf8"])
        raw = wire["opaqueIr"].encode("utf-8")
        self.assertIn(b'"negativeZero":-0', raw)
        self.assertIn(b"1e-7", raw)
        strict_json.loads(raw)
        self.assertEqual(canonical.digest_bytes("opaque", raw), hashlib.sha256(b"opaque\0" + raw).hexdigest())


if __name__ == "__main__":
    unittest.main()
