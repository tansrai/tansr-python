"""公共与内部源码共用的运行目录、schema原字节及可变性验收。"""
import importlib
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


class RuntimeCatalogueTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.operations = importlib.import_module("tansr_sdk.api.operations")
        cls.schemas = importlib.import_module("tansr_sdk.api._schema_data")
        cls.error = importlib.import_module("tansr_sdk.errors").Error

    def test_81_original_operations_and_all_fields_are_preserved(self):
        original = json.loads((ROOT / "contract/api-manifest.json").read_bytes())
        generated = self.operations.manifest()
        families = {family["id"]: family for family in original["families"]}
        self.assertEqual(len(generated["operations"]), 81)
        self.assertEqual(sum(item["fenced"] for item in generated["operations"]), 77)
        for expected, actual in zip(original["operations"], generated["operations"]):
            self.assertEqual({key: actual[key] for key in expected}, expected)
            self.assertEqual(set(actual), set(expected) | {"fenced", "requestIdPath"})
            self.assertEqual(actual["requestIdPath"], families.get(expected["family"], {}).get("requestIdPath"))
            self.assertEqual(self.operations.get_operation(expected["name"]), actual)
        generated["operations"] = original["operations"]
        self.assertEqual(generated, original)

    def test_callers_cannot_mutate_shared_manifest_or_operations(self):
        baseline = self.operations.manifest()
        caller = self.operations.manifest()
        caller["families"][0]["domains"].append("injected")
        caller["operations"][0]["apiPath"] = "/wrong"
        caller["operations"].clear()
        self.assertEqual(self.operations.manifest(), baseline)
        for expected in baseline["operations"]:
            operation = self.operations.get_operation(expected["name"])
            operation["query"].append("injected")
            if operation["expectedRevision"] is not None:
                operation["expectedRevision"]["path"].append("injected")
            if operation["requestIdPath"] is not None:
                operation["requestIdPath"].append("injected")
            self.assertEqual(self.operations.get_operation(expected["name"]), expected)

    def test_unknown_and_nonstring_operations_raise_sdk_error(self):
        for name in ("unknown.operation", "", None, [], {}, 1, True):
            with self.assertRaises(self.error) as failure:
                self.operations.get_operation(name)
            self.assertEqual(failure.exception.code, "invalid_input")

    def test_all_10_schema_original_bytes_and_objects_are_embedded(self):
        originals = sorted((ROOT / "contract").glob("*.schema.json"))
        self.assertEqual(len(originals), 10)
        self.assertEqual(len(self.schemas.SCHEMAS), 10)
        for path in originals:
            name = path.name[:-len(".schema.json")]
            raw = path.read_bytes()
            self.assertEqual(self.schemas.SCHEMA_BYTES[name], raw)
            self.assertEqual(self.schemas.SCHEMAS[name], json.loads(raw))

    def test_error_catalogue_is_exact_and_returned_as_owned_data(self):
        expected = json.loads((ROOT / "contract/api-error-map.json").read_bytes())
        self.assertEqual(self.operations.error_map(), expected)
        self.assertEqual(len(expected["unifiedCodes"]), 17)
        unified = self.schemas.SCHEMAS["unified-v1"]["definitions"]["UnifiedCode"]["enum"]
        self.assertEqual(len(unified), 19)
        self.assertEqual(set(unified), set(expected["unifiedCodes"]) | {"precondition_failed", "not_canonical"})
        self.assertEqual(len(expected["retryActions"]), 6)
        caller = self.operations.error_map()
        caller["unifiedCodes"].clear()
        self.assertEqual(self.operations.error_map(), expected)



if __name__ == "__main__":
    unittest.main()
