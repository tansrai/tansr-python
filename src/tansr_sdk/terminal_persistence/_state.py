"""共同根、永久双键索引与原票据的机械状态变换；不解释回执业务。"""
import base64
from collections import OrderedDict
import copy
import hashlib
from typing import Any, Dict, List, Set

from .. import canonical, strict_json
from ..errors import Error

CONTRACT = "terminal-persistence-v1"
FORMAT = "tansr-python-terminal-persistence-v1"
BLOCK = 12288
METADATA_RESERVE = 262144
SEQUENCE_MAX = 9223372036854775807
DEFAULT_LIMITS = dict(activeTransfers=8, stagingBytes=16 << 20, receiptEntries=8192,
                      transferFacts=4096, objects=16384, retainedBytes=32 << 20)


class StorageError(Error):
    """已经确定的存储拒绝，不代表不确定的磁盘提交。"""


def need(value, code="integrity_mismatch"):
    if not value:
        raise StorageError(code)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return canonical.encode(value)


def hash_value(value):
    return digest(encoded(value))


def object_key(kind, sha256):
    return kind + ":" + sha256


def bitset(values):
    raw = bytearray((len(values) + 7) // 8)
    for index, ready in enumerate(values):
        if ready:
            raw[index // 8] |= 1 << (index % 8)
    return base64.b64encode(raw).decode("ascii")


def decode(value):
    from ..storage.encrypted_store import decode_bytes
    return decode_bytes(value, max_bytes=BLOCK)


def initial(identity, limits):
    return dict(format=FORMAT, identity=copy.deepcopy(identity), limits=copy.deepcopy(limits), root=None,
                objects={}, transfers={}, primary={}, secondary={})


class Measurements:
    """本实例有界 canonical 长度缓存；完整不可变树为键，不缓存授权或结果。"""
    def __init__(self):
        self._cache = OrderedDict()

    def size(self, value):
        def freeze(item):
            kind = type(item)
            if kind is dict:
                return ("object", tuple((key, freeze(item[key])) for key in sorted(item)))
            if kind is list:
                return ("array", tuple(freeze(child) for child in item))
            if kind in (int, strict_json.JsonInt):
                return ("integer", strict_json.number_lexeme(item))
            if kind is str:
                return ("string", item)
            if kind is bool or item is None:
                return ("boolean" if kind is bool else "null", item)
            raise StorageError("integrity_mismatch")
        key = freeze(value)
        found = self._cache.get(key)
        if found is not None:
            self._cache.move_to_end(key)
            return found
        result = len(encoded(value))
        self._cache[key] = result
        if len(self._cache) > 4096:
            self._cache.popitem(last=False)
        return result

    def clear(self):
        self._cache.clear()


class Engine:
    """每次写者锁内的一份自有候选；调用者只以一次原子快照提交它。"""
    def __init__(self, state, validate, measurements=None):
        self.measurements = measurements if measurements is not None else Measurements()
        self.state = state
        self.validate = validate
        self.changed = False
        self.error = None

    def _object(self, kind, sha256):
        key = object_key(kind, sha256)
        row = self.state["objects"].get(key)
        need(row is not None)
        raw = decode(row["base64"])
        need(row["kind"] == kind and row["sha256"] == sha256 and
             len(raw) == row["byteLength"] and digest(raw) == sha256)
        return raw

    def _page(self, kind, index, sha256):
        raw = self._object(kind, sha256)
        page = canonical.parse_strict(raw, max_bytes=BLOCK)
        field = "refs" if kind == "body-page" else "entries"
        need(set(page) == {"version", "kind", "index", field} and page["version"] == 1 and
             page["kind"] == kind and page["index"] == index and isinstance(page[field], list))
        return page[field]

    def _body_refs(self, root):
        if root is None:
            return []
        body = root["body"]
        refs = []
        for index, sha256 in enumerate(body["pageHashes"]):
            values = self._page("body-page", index, sha256)
            need(len(values) == min(64, body["blockCount"] - index * 64))
            refs.extend(values)
        need(len(refs) == body["blockCount"])
        for index, ref in enumerate(refs):
            self.validate("Ref", ref)
            need(ref["byteLength"] == min(BLOCK, body["byteLength"] - index * BLOCK))
        return refs

    def _plan(self, row):
        begin = row["begin"]
        refs: List[Any] = []
        entries: List[Any] = []
        complete = True
        for kind, plan, field, cap, total in (
                ("body-page", begin["body"], "refs", 64, begin["body"]["blockCount"]),
                ("index-page", begin["index"], "entries", 32, begin["index"]["entryCount"])):
            target = refs if field == "refs" else entries
            for index, sha256 in enumerate(plan["pageHashes"]):
                key = object_key(kind, sha256)
                count = min(cap, total - index * cap)
                if key not in row["accepted"]:
                    target.extend([None] * count)
                    complete = False
                    continue
                values = self._page(kind, index, sha256)
                need(len(values) == count)
                for value in values:
                    self.validate("Ref" if field == "refs" else "Entry", value)
                target.extend(values)
        for index, ref in enumerate(refs):
            if ref is not None:
                need(ref["byteLength"] == min(BLOCK, begin["body"]["byteLength"] - index * BLOCK))
        known = [entry for entry in entries if entry is not None]
        keys = [entry["primaryKey"] for entry in known]
        need(keys == sorted(set(keys)) and len({entry["secondaryKey"] for entry in known}) == len(known))
        return refs, entries, complete

    def _accept(self, row, kind, sha256, source):
        key = object_key(kind, sha256)
        if key not in row["accepted"]:
            row["accepted"][key] = source

    def _reuse(self, row):
        begin, base = row["begin"], row["baseRoot"]
        if base is not None:
            for index, sha256 in enumerate(begin["body"]["pageHashes"]):
                if index < len(base["body"]["pageHashes"]) and base["body"]["pageHashes"][index] == sha256:
                    self._page("body-page", index, sha256)
                    self._accept(row, "body-page", sha256, "reused")
        refs, entries, unused = self._plan(row)
        base_refs = {object_key("body-block", ref["sha256"]): ref for ref in self._body_refs(base)}
        for ref in refs:
            if ref is not None and base_refs.get(object_key("body-block", ref["sha256"])) == ref:
                self._object("body-block", ref["sha256"])
                self._accept(row, "body-block", ref["sha256"], "reused")
        for entry in entries:
            if entry is None:
                continue
            old = self.state["primary"].get(entry["primaryKey"])
            if old is not None and old["entry"] == entry and self.state["secondary"].get(entry["secondaryKey"]) == entry["primaryKey"]:
                self._object("receipt-value", entry["value"]["sha256"])
                self._accept(row, "receipt-value", entry["value"]["sha256"], "reused")

    def _progress(self, row):
        refs, entries, unused = self._plan(row)
        accepted = row["accepted"]
        begin = row["begin"]
        return dict(pagesReady=bitset([object_key(kind, sha256) in accepted
                    for kind, plan in (("body-page", begin["body"]), ("index-page", begin["index"]))
                    for sha256 in plan["pageHashes"]]),
                    bodyReady=bitset([ref is not None and object_key("body-block", ref["sha256"]) in accepted for ref in refs]),
                    valuesReady=bitset([entry is not None and object_key("receipt-value", entry["value"]["sha256"]) in accepted for entry in entries]),
                    receivedBytes=sum(self.state["objects"][key]["byteLength"] for key, source in accepted.items() if source == "received"))

    def _planned_objects(self, row):
        refs, entries, complete = self._plan(row)
        if not complete:
            return None
        objects: Dict[str, int] = {}
        def add(kind, sha256, length):
            key = object_key(kind, sha256)
            need(key not in objects or objects[key] == length, "invalid_request")
            objects[key] = length
        for kind, plan in (("body-page", row["begin"]["body"]), ("index-page", row["begin"]["index"])):
            for sha256 in plan["pageHashes"]:
                add(kind, sha256, len(self._object(kind, sha256)))
        for ref in refs:
            add("body-block", ref["sha256"], ref["byteLength"])
        for entry in entries:
            add("receipt-value", entry["value"]["sha256"], entry["value"]["byteLength"])
        need(len(objects) == row["begin"]["declared"]["objects"] and
             sum(objects.values()) == row["begin"]["declared"]["bytes"], "invalid_request")
        return objects

    def capacity(self):
        s = self.state
        retained = sum(obj["byteLength"] + self.measurements.size({key: obj[key] for key in ("kind", "sha256", "byteLength")}) for obj in s["objects"].values())
        retained += sum(self.measurements.size(value) for value in s["primary"].values())
        retained += self.measurements.size(s["root"]) if s["root"] is not None else 0
        used = dict(activeTransfers=0, stagingBytes=0, receiptEntries=len(s["primary"]),
                    transferFacts=len(s["transfers"]), objects=len(s["objects"]), retainedBytes=0,
                    reservedBytes=0, reservedObjects=0, reservedReceiptEntries=0)
        for row in s["transfers"].values():
            metadata = self.measurements.size({key: row[key] for key in ("begin", "owner", "baseRoot", "transfer")})
            retained += metadata
            if row["transfer"]["status"] != "staging":
                continue
            accepted = row["accepted"]
            actual = sum(s["objects"][key]["byteLength"] for key in accepted)
            descriptor = sum(self.measurements.size({field: s["objects"][key][field] for field in ("kind", "sha256", "byteLength")}) for key in accepted)
            remaining = row["begin"]["declared"]["bytes"] - actual
            need(remaining >= 0 and metadata + descriptor <= METADATA_RESERVE, "capacity_exceeded")
            used["activeTransfers"] += 1
            used["stagingBytes"] += remaining + row["transfer"]["progress"]["receivedBytes"]
            used["reservedBytes"] += remaining + METADATA_RESERVE - metadata - descriptor
            need(row["begin"]["declared"]["objects"] >= len(accepted), "invalid_request")
            used["reservedObjects"] += row["begin"]["declared"]["objects"] - len(accepted)
            used["reservedReceiptEntries"] += row["begin"]["index"]["addedCount"]
        used["retainedBytes"] = retained
        return dict(limits=copy.deepcopy(s["limits"]), used=used)

    def check_capacity(self):
        capacity = self.capacity()
        used, limits = capacity["used"], capacity["limits"]
        for name, maximum in limits.items():
            reserve = {"objects": "reservedObjects", "receiptEntries": "reservedReceiptEntries", "retainedBytes": "reservedBytes"}.get(name)
            need(used[name] + (used[reserve] if reserve else 0) <= maximum, "capacity_exceeded")
        return capacity

    def _matches(self, expected):
        root = self.state["root"]
        return expected is None if root is None else expected == dict(commitRoot=root["commitRoot"],
            generation=root["generation"], bodyEtag=root["body"]["sha256"],
            indexRoot=root["index"]["root"], indexCount=root["index"]["count"])

    def _reject(self, row, code):
        row["transfer"].update(status="rejected", progress=None, result=None,
                              rejection=dict(code=code, observedRoot=copy.deepcopy(self.state["root"])))
        row["baseRoot"] = None
        row["accepted"] = {}
        self._collect()
        self.error = code
        self.changed = True

    def _collect(self):
        # 只在持有同一写者锁的候选快照中回收；永久值和所有未决引用均保留。
        live: Set[str] = set()
        def protect(root):
            if root is not None:
                live.update(object_key("body-page", value) for value in root["body"]["pageHashes"])
                live.update(object_key("body-block", ref["sha256"]) for ref in self._body_refs(root))
        protect(self.state["root"])
        for row in self.state["transfers"].values():
            if row["transfer"]["status"] == "staging":
                protect(row["baseRoot"])
                live.update(row["accepted"])
        live.update(object_key("receipt-value", row["entry"]["value"]["sha256"]) for row in self.state["primary"].values())
        self.state["objects"] = {key: value for key, value in self.state["objects"].items() if key in live}

    def execute(self, request, owner, authorize_recovery=None):
        action = request["action"]
        response = {key: request[key] for key in ("contract", "sourceId", "sourceGeneration", "domainKey", "action")}
        root = self.state["root"]
        if action == "head":
            response.update(root=copy.deepcopy(root), capacity=self.check_capacity())
            return response
        if action in ("read", "lookup"):
            need(root is not None and root["commitRoot"] == request["commitRoot"], "revision_conflict")
            if action == "lookup":
                key = request["key"]["digest"]
                if request["key"]["kind"] == "secondary":
                    key = self.state["secondary"].get(key)
                record = self.state["primary"].get(key)
                entry = None
                if record is not None:
                    entry = copy.deepcopy(record["entry"])
                    raw = self._object("receipt-value", entry["value"]["sha256"])
                    need(len(raw) == entry["value"]["byteLength"])
                    entry["base64"] = base64.b64encode(raw).decode("ascii")
                response.update(commitRoot=root["commitRoot"], indexRoot=root["index"]["root"],
                                indexCount=root["index"]["count"], entry=entry)
            elif request["part"] == "body-page":
                index = request["pageIndex"]
                need(index < len(root["body"]["pageHashes"]), "invalid_request")
                raw = self._object("body-page", root["body"]["pageHashes"][index])
                response.update(part="body-page", commitRoot=root["commitRoot"], pageIndex=index,
                                byteLength=len(raw), base64=base64.b64encode(raw).decode("ascii"), payloadDigest=digest(raw))
            else:
                offset = request["offset"]
                need(offset <= root["body"]["byteLength"], "invalid_request")
                end = min(root["body"]["byteLength"], offset + request["length"])
                refs = self._body_refs(root)
                result = bytearray()
                for index in range(offset // BLOCK, (end + BLOCK - 1) // BLOCK):
                    raw = self._object("body-block", refs[index]["sha256"])
                    result.extend(raw[max(0, offset - index * BLOCK):min(len(raw), end - index * BLOCK)])
                raw = bytes(result)
                response.update(part="body", commitRoot=root["commitRoot"], bodyEtag=root["body"]["sha256"],
                                offset=offset, byteLength=len(raw), base64=base64.b64encode(raw).decode("ascii"),
                                payloadDigest=digest(raw), nextOffset=end, complete=end == root["body"]["byteLength"])
            return response
        transfer_id = request["transferId"]
        row = self.state["transfers"].get(transfer_id)
        if row is not None:
            need(row["begin"]["intentSha256"] == request["intentSha256"], "request_conflict")
            if row["owner"] != owner:
                proof = dict(identity=copy.deepcopy(self.state["identity"]), transferId=transfer_id,
                             originalOwner=copy.deepcopy(row["owner"]), currentOwner=copy.deepcopy(owner))
                need(action == "query" and authorize_recovery is not None and authorize_recovery(proof) is True,
                     "request_conflict")
        if action == "query" or row is None and action != "begin":
            response["transfer"] = copy.deepcopy(row["transfer"]) if row is not None else dict(
                transferId=transfer_id, intentSha256=request["intentSha256"], status="unknown",
                progress=None, result=None, rejection=None)
            return response
        if action == "begin":
            need(hash_value({key: value for key, value in request.items() if key != "intentSha256"}) == request["intentSha256"], "invalid_request")
            body, index = request["body"], request["index"]
            need(index["addedCount"] <= index["entryCount"], "invalid_request")
            need(body["blockCount"] == (body["byteLength"] + BLOCK - 1) // BLOCK and
                 len(body["pageHashes"]) == (body["blockCount"] + 63) // 64 and
                 len(index["pageHashes"]) == (index["entryCount"] + 31) // 32, "invalid_request")
            if body["byteLength"] == 0:
                need(body["sha256"] == digest(b""), "invalid_request")
            if row is None:
                row = dict(begin=copy.deepcopy(request), owner=copy.deepcopy(owner), baseRoot=copy.deepcopy(root),
                           accepted={}, transfer=dict(transferId=transfer_id, intentSha256=request["intentSha256"],
                           status="staging", progress=None, result=None, rejection=None))
                self.state["transfers"][transfer_id] = row
                if not self._matches(request["expected"]):
                    self.check_capacity_for_rejection(row)
                    self._reject(row, "revision_conflict")
                else:
                    self._reuse(row)
                    row["transfer"]["progress"] = self._progress(row)
                    self.check_capacity()
                    self.changed = True
            else:
                need(row["begin"] == request, "request_conflict")
        elif row["transfer"]["status"] == "staging" and action == "put":
            self._put(row, request)
        elif row["transfer"]["status"] == "staging" and action == "commit":
            self._commit(row)
        if row["transfer"]["status"] == "rejected":
            self.error = row["transfer"]["rejection"]["code"]
        response["transfer"] = copy.deepcopy(row["transfer"])
        return response

    def check_capacity_for_rejection(self, row):
        row["transfer"]["progress"] = dict(pagesReady="", bodyReady="", valuesReady="", receivedBytes=0)
        need(len(self.state["transfers"]) <= self.state["limits"]["transferFacts"], "capacity_exceeded")
        # 持久终态仍占一个原键及其完整元数据，不能借拒绝绕过保留字节帽。
        needed = self.measurements.size({key: row[key] for key in ("begin", "owner", "baseRoot", "transfer")})
        need(self.capacity()["used"]["retainedBytes"] + METADATA_RESERVE - needed <= self.state["limits"]["retainedBytes"], "capacity_exceeded")

    def _put(self, row, request):
        kind, sha256 = request["kind"], request["sha256"]
        raw = decode(request["base64"])
        need(len(raw) == request["byteLength"] and digest(raw) == sha256)
        if kind in ("body-page", "index-page"):
            plan = row["begin"]["body" if kind == "body-page" else "index"]
            need(sha256 in plan["pageHashes"], "invalid_request")
        else:
            refs, entries, unused = self._plan(row)
            candidates = refs if kind == "body-block" else [entry["value"] if entry else None for entry in entries]
            need(any(ref is not None and ref["sha256"] == sha256 and ref["byteLength"] == len(raw) for ref in candidates), "invalid_request")
        key = object_key(kind, sha256)
        obj = dict(kind=kind, sha256=sha256, byteLength=len(raw), base64=request["base64"])
        need(key not in self.state["objects"] or self.state["objects"][key] == obj)
        if key in row["accepted"]:
            return
        self.state["objects"][key] = obj
        self._accept(row, kind, sha256, "received")
        try:
            self._reuse(row)
            self._planned_objects(row)
        except Error as error:
            self._reject(row, "invalid_request" if error.code == "invalid_request" else "integrity_mismatch")
            return
        row["transfer"]["progress"] = self._progress(row)
        self.check_capacity()
        self.changed = True

    def _commit(self, row):
        planned = self._planned_objects(row)
        need(planned is not None and all(key in row["accepted"] for key in planned), "invalid_request")
        need(all(self.state["objects"][key]["byteLength"] == length for key, length in planned.items()))
        if not self._matches(row["begin"]["expected"]):
            self._reject(row, "revision_conflict")
            return
        refs, entries, unused = self._plan(row)
        check = hashlib.sha256()
        for ref in refs:
            check.update(self._object("body-block", ref["sha256"]))
        if check.hexdigest() != row["begin"]["body"]["sha256"]:
            self._reject(row, "integrity_mismatch")
            return
        root = self.state["root"]
        count = int(root["index"]["count"]) if root is not None else 0
        previous = root["index"]["root"] if root is not None else hash_value(["TPV1-INDEX", self.state["identity"]])
        added = []
        for entry in entries:
            primary, secondary = entry["primaryKey"], entry["secondaryKey"]
            old = self.state["primary"].get(primary)
            other = self.state["secondary"].get(secondary)
            if old is not None or other is not None:
                if old is None or old["entry"] != entry or other != primary:
                    self._reject(row, "request_conflict")
                    return
            else:
                added.append(entry)
        if len(added) != row["begin"]["index"]["addedCount"]:
            self._reject(row, "request_conflict")
            return
        generation = int(root["generation"]) + 1 if root is not None else 1
        need(generation <= SEQUENCE_MAX and count + len(added) <= SEQUENCE_MAX, "capacity_exceeded")
        for entry in added:
            count += 1
            previous = hash_value(["TPV1-ENTRY", previous, str(count), entry])
            self.state["primary"][entry["primaryKey"]] = dict(ordinal=str(count), entry=copy.deepcopy(entry))
            self.state["secondary"][entry["secondaryKey"]] = entry["primaryKey"]
        result = dict(generation=str(generation), body=copy.deepcopy(row["begin"]["body"]),
                      index=dict(root=previous, count=str(count)))
        result["commitRoot"] = hash_value(["TPV1-ROOT", self.state["identity"], result])
        self.state["root"] = result
        row["transfer"].update(status="committed", progress=None, result=copy.deepcopy(result), rejection=None)
        row["baseRoot"] = None
        row["accepted"] = {}
        self._collect()
        self.check_capacity()
        self.changed = True

    def audit(self, identity, limits):
        """冷读先核完整已认证快照；缺对象/索引损坏绝不解释成不存在。"""
        s = self.state
        need(type(s) is dict and set(s) == {"format", "identity", "limits", "root", "objects", "transfers", "primary", "secondary"})
        need(s["format"] == FORMAT and s["identity"] == identity and s["limits"] == limits)
        for name in ("objects", "transfers", "primary", "secondary"):
            need(type(s[name]) is dict)
        for key, obj in s["objects"].items():
            need(type(obj) is dict and set(obj) == {"kind", "sha256", "byteLength", "base64"})
            need(obj["kind"] in ("body-page", "index-page", "body-block", "receipt-value"))
            self.validate("Ref", {field: obj[field] for field in ("sha256", "byteLength")})
            need(key == object_key(obj["kind"], obj["sha256"]))
            self._object(obj["kind"], obj["sha256"])
        chain = {0: hash_value(["TPV1-INDEX", identity])}
        ordered = []
        for key, item in s["primary"].items():
            need(type(item) is dict and set(item) == {"ordinal", "entry"})
            self.validate("Sequence", item["ordinal"])
            self.validate("Entry", item["entry"])
            entry = item["entry"]
            need(entry["primaryKey"] == key and s["secondary"].get(entry["secondaryKey"]) == key)
            need(len(self._object("receipt-value", entry["value"]["sha256"])) == entry["value"]["byteLength"])
            ordered.append((int(item["ordinal"]), entry))
        need(len(s["secondary"]) == len(ordered))
        for ordinal, (number, entry) in enumerate(sorted(ordered, key=lambda item: item[0]), 1):
            need(number == ordinal)
            chain[ordinal] = hash_value(["TPV1-ENTRY", chain[ordinal - 1], str(ordinal), entry])

        def root_check(root, material=False):
            if root is None:
                return
            self.validate("Root", root)
            need(int(root["generation"]) > 0)
            body = root["body"]
            need(body["blockCount"] == (body["byteLength"] + BLOCK - 1) // BLOCK and
                 len(body["pageHashes"]) == (body["blockCount"] + 63) // 64)
            need(root["index"]["root"] == chain.get(int(root["index"]["count"])))
            need(root["commitRoot"] == hash_value(["TPV1-ROOT", identity,
                 {key: root[key] for key in ("generation", "body", "index")}]))
            if material:
                h = hashlib.sha256()
                for ref in self._body_refs(root):
                    raw = self._object("body-block", ref["sha256"])
                    need(len(raw) == ref["byteLength"])
                    h.update(raw)
                need(h.hexdigest() == body["sha256"])

        root_check(s["root"], True)
        need((s["root"] is None and not ordered) or
             s["root"] is not None and int(s["root"]["index"]["count"]) == len(ordered))
        generations = {}
        for transfer_id, row in s["transfers"].items():
            need(type(row) is dict and set(row) == {"begin", "owner", "baseRoot", "accepted", "transfer"})
            begin, transfer = row["begin"], row["transfer"]
            self.validate("BeginRequest", begin)
            self.validate("Owner", row["owner"])
            self.validate("Transfer", transfer)
            need(begin["transferId"] == transfer_id == transfer["transferId"] and
                 begin["intentSha256"] == transfer["intentSha256"] == hash_value(
                     {key: value for key, value in begin.items() if key != "intentSha256"}))
            need(all(begin[key] == identity[key] for key in ("sourceId", "sourceGeneration", "domainKey")))
            need(all(row["owner"]["scope"][key] == identity[key] for key in ("applicationScopeId", "endUserId")))
            body, index = begin["body"], begin["index"]
            need(index["addedCount"] <= index["entryCount"])
            need(body["blockCount"] == (body["byteLength"] + BLOCK - 1) // BLOCK and
                 len(body["pageHashes"]) == (body["blockCount"] + 63) // 64 and
                 len(index["pageHashes"]) == (index["entryCount"] + 31) // 32)
            need(body["byteLength"] != 0 or body["sha256"] == digest(b""))
            need(type(row["accepted"]) is dict)
            if transfer["status"] == "staging":
                base = row["baseRoot"]
                root_check(base, True)
                expected = None if base is None else dict(commitRoot=base["commitRoot"], generation=base["generation"],
                    bodyEtag=base["body"]["sha256"], indexRoot=base["index"]["root"], indexCount=base["index"]["count"])
                need(begin["expected"] == expected)
                refs, entries, unused = self._plan(row)
                allowed = {object_key(kind, sha) for kind, plan in (("body-page", body), ("index-page", index)) for sha in plan["pageHashes"]}
                allowed.update(object_key("body-block", ref["sha256"]) for ref in refs if ref is not None)
                allowed.update(object_key("receipt-value", entry["value"]["sha256"]) for entry in entries if entry is not None)
                base_blocks = {object_key("body-block", ref["sha256"]) for ref in self._body_refs(base)}
                reusable_pages = {object_key("body-page", sha) for i, sha in enumerate(body["pageHashes"])
                    if base is not None and i < len(base["body"]["pageHashes"]) and base["body"]["pageHashes"][i] == sha}
                reusable_values = {object_key("receipt-value", entry["value"]["sha256"]) for entry in entries
                    if entry is not None and s["primary"].get(entry["primaryKey"], {}).get("entry") == entry}
                for key, source in row["accepted"].items():
                    need(key in allowed and key in s["objects"] and source in ("received", "reused"))
                    need(source != "reused" or key in base_blocks | reusable_pages | reusable_values)
                need(transfer["progress"] == self._progress(row))
                self._planned_objects(row)
            else:
                need(row["baseRoot"] is None and not row["accepted"])
                if transfer["status"] == "committed":
                    result = transfer["result"]
                    root_check(result)
                    need(result["body"] == begin["body"] and result["generation"] not in generations)
                    generations[result["generation"]] = result
                else:
                    need(transfer["status"] == "rejected")
                    root_check(transfer["rejection"]["observedRoot"])
        if s["root"] is not None:
            need(generations.get(s["root"]["generation"]) == s["root"])
            need(len(generations) == int(s["root"]["generation"]))
        else:
            need(not generations)
        self.check_capacity()
