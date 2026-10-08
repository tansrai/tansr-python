"""在归档内构建四个分发物，再以空 venv 消费；不发布、不使用 editable。"""

import argparse
import configparser
import email.parser
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name, parse_wheel_filename


COMMANDS = ("tansr-py-chat", "tansr-py-tools", "tansr-py-archive")
PACKAGES = (("sdk", "tansr-sdk", "tansr_sdk"), ("demo", "tansr-sdk-demo", "tansr_demo"))


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path, value):
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def clean_environment():
    # 不输出继承环境；移除源码导入、pip 配置覆盖和本 SDK 凭据。
    env = {key: value for key, value in os.environ.items() if not key.upper().startswith(("PYTHON", "PIP_", "TANSR_"))}
    env["PYTHONNOUSERSITE"] = "1"
    env["PIP_CONFIG_FILE"] = os.devnull
    env["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    return env


class Runner:
    def __init__(self, evidence):
        self.evidence = evidence
        self.commands = []

    def run(self, label, args, cwd, env=None, timeout=900):
        log = self.evidence / "logs" / (label + ".log")
        record = {"label": label, "args": [str(arg) for arg in args], "cwd": str(cwd), "log": str(log)}
        self.commands.append(record)
        with log.open("wb") as output:
            try:
                result = subprocess.run(
                    record["args"],
                    cwd=str(cwd),
                    env=env or clean_environment(),
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=timeout,
                    check=False,
                )
                record["exit_code"] = result.returncode
            except subprocess.TimeoutExpired:
                record["timeout_seconds"] = timeout
                write_json(self.evidence / "commands.json", self.commands)
                raise RuntimeError(label + " timed out; original log retained")
        write_json(self.evidence / "commands.json", self.commands)
        if result.returncode:
            raise RuntimeError(label + " failed; see " + str(log))
        return log.read_text(encoding="utf-8", errors="replace")


def inspect_locks(directory):
    rows = []
    for path in sorted(directory.glob("*.lock")):
        joined = path.read_text(encoding="utf-8").replace("\\\n", " ")
        names = set()
        packages = []
        for line in joined.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("--hash=sha256:")
            if len(parts) < 2:
                raise RuntimeError(str(path) + ": requirement lacks SHA256")
            requirement = Requirement(parts[0].strip())
            pins = list(requirement.specifier)
            if requirement.url or len(pins) != 1 or pins[0].operator != "==" or "*" in pins[0].version:
                raise RuntimeError(str(path) + ": requirement must have an exact version")
            name = re.sub(r"[-_.]+", "-", requirement.name).lower()
            if name in names or requirement.extras:
                raise RuntimeError(str(path) + ": duplicate name or unreviewed extra")
            names.add(name)
            hashes = [part.strip() for part in parts[1:]]
            if any(re.fullmatch(r"[0-9a-f]{64}", value) is None for value in hashes):
                raise RuntimeError(str(path) + ": malformed SHA256")
            packages.append({"requirement": str(requirement), "wheel_hash_count": len(hashes), "wheel_sha256": hashes})
        if not packages:
            raise RuntimeError(str(path) + ": empty lock")
        rows.append({"file": path.name, "sha256": digest(path), "packages": packages})
    if not rows:
        raise RuntimeError("No requirement locks found")
    return rows


def inspect_wheelhouses(sources, locks):
    allowed = {}
    for lock in locks:
        for row in lock["packages"]:
            requirement = Requirement(row["requirement"])
            version = next(iter(requirement.specifier)).version
            key = (canonicalize_name(requirement.name), version)
            allowed.setdefault(key, set()).update(row["wheel_sha256"])
    rows = []
    for source in sources:
        if not source.is_dir():
            raise RuntimeError("Local wheelhouse missing: " + str(source))
        wheels = sorted(source.glob("*.whl"))
        if not wheels:
            raise RuntimeError("Local wheelhouse is empty: " + str(source))
        for path in wheels:
            if path.is_symlink():
                raise RuntimeError("Local wheel must not be a symlink: " + str(path))
            name, version, _, _ = parse_wheel_filename(path.name)
            value = digest(path)
            if value not in allowed.get((str(name), str(version)), set()):
                raise RuntimeError("Local wheel SHA256 is not in the exact locks: " + str(path))
            rows.append({"path": str(path), "name": str(name), "version": str(version), "sha256": value})
    return rows


def stage_project(source, destination):
    destination.mkdir(parents=True)
    required = ("pyproject.toml", "README.md", "src")
    for name in required:
        if not (source / name).exists():
            raise RuntimeError("Missing package input: " + str(source / name))
    files = [path for path in (source / "src").rglob("*") if path.is_file() and "__pycache__" not in path.parts]
    files += [
        source / name
        for name in ("pyproject.toml", "README.md", "MANIFEST.in", "LICENSE", "LICENSE.md", "LICENSE.txt")
        if (source / name).is_file()
    ]
    rows = []
    for path in sorted(files):
        relative = path.relative_to(source)
        if path.is_symlink() or any(parent.is_symlink() for parent in path.parents if parent != source.parent):
            raise RuntimeError("Package input contains a symlink: " + str(path))
        if path.suffix in (".pyc", ".pyo"):
            continue
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(str(path), str(target))
        rows.append({"path": relative.as_posix(), "sha256": digest(target)})
    return rows


def safe_member(name):
    parts = PurePosixPath(name).parts
    return (
        bool(parts) and not name.startswith(("/", "\\")) and ".." not in parts and "\\" not in name and ":" not in name
    )


def inspect_artifacts(directory, package_name, import_name, version):
    wheels = list(directory.glob("*.whl"))
    sources = list(directory.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sources) != 1:
        raise RuntimeError("Each project must produce exactly one wheel and one sdist")
    wheel = wheels[0]
    if not wheel.name.endswith("-py3-none-any.whl"):
        raise RuntimeError("SDK/Demo wheel must be py3-none-any")
    with zipfile.ZipFile(str(wheel)) as archive:
        names = archive.namelist()
        if any(not safe_member(name) for name in names):
            raise RuntimeError("Unsafe wheel member")
        if any(name.endswith((".pyd", ".so", ".dll", ".dylib")) for name in names):
            raise RuntimeError("Own wheel unexpectedly contains a native binary")
        if import_name + "/py.typed" not in names:
            raise RuntimeError("Typed package marker missing")
        other = "tansr_demo/" if import_name == "tansr_sdk" else "tansr_sdk/"
        if any(name.startswith(other) for name in names):
            raise RuntimeError("SDK and Demo packages must not duplicate each other")
        metadata_paths = [name for name in names if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise RuntimeError("Ambiguous wheel metadata")
        metadata = email.parser.Parser().parsestr(archive.read(metadata_paths[0]).decode("utf-8"))
        if metadata["Name"] != package_name or metadata["Version"] != version:
            raise RuntimeError("Wheel identity mismatch")
        if SpecifierSet(metadata.get("Requires-Python", "")) != SpecifierSet(">=3.7,!=3.9.0,!=3.9.1"):
            raise RuntimeError("Wheel Requires-Python differs from the approved baseline")
        if import_name == "tansr_demo":
            requires = [Requirement(value) for value in metadata.get_all("Requires-Dist", [])]
            if not any(req.name == "tansr-sdk" and str(req.specifier) == "==" + version for req in requires):
                raise RuntimeError("Demo must require the exact same SDK version")
            entry_path = metadata_paths[0].rsplit("/", 1)[0] + "/entry_points.txt"
            entry = configparser.ConfigParser()
            entry.read_string(archive.read(entry_path).decode("utf-8"))
            if set(entry["console_scripts"]) != set(COMMANDS):
                raise RuntimeError("Demo console script set mismatch")
    with tarfile.open(str(sources[0]), "r:gz") as archive:
        members = archive.getmembers()
        if any(not safe_member(member.name) or not (member.isfile() or member.isdir()) for member in members):
            raise RuntimeError("Unsafe sdist member")
        roots = {PurePosixPath(member.name).parts[0] for member in members}
        if len(roots) != 1 or any(".git" in PurePosixPath(member.name).parts for member in members):
            raise RuntimeError("sdist must have one root and no Git metadata")
    return {"wheel": wheel, "sdist": sources[0]}


def interpreter_lane(runner, python, empty, label):
    value = runner.run(
        label, [python, "-I", "-c", "import json,sys; print(json.dumps(list(sys.version_info[:3])))"], empty
    )
    version = json.loads(value)
    if version[:2] == [3, 7]:
        return "py37", version
    if version[0] == 3 and version[1] in (12, 13, 14):
        return "modern", version
    raise RuntimeError("No verified complete tool lock for consumer " + str(version))


def pip(runner, label, python, args, cwd):
    return runner.run(label, [python, "-I", "-m", "pip", "--isolated", "--disable-pip-version-check"] + args, cwd)


def download_locked(runner, label, python, lane, requirements, wheelhouse, cwd, wheelhouse_sources=()):
    wheelhouse.mkdir(parents=True)
    args = [
        "download",
        "--no-cache-dir",
        "--no-deps",
        "--require-hashes",
        "--only-binary=:all:",
        "--dest",
        wheelhouse,
    ]
    if wheelhouse_sources:
        args.append("--no-index")
        for source in wheelhouse_sources:
            args.extend(["--find-links", source])
    else:
        args.extend(["--index-url", "https://pypi.org/simple"])
    for kind in ("runtime", "tools"):
        args.extend(["-r", requirements / (kind + "-" + lane + ".lock")])
    if lane == "py37":
        # 旧解释器只参与离线消费，下载使用现代解释器已维护的 TLS。
        code = (
            "import json; from packaging.tags import sys_tags; "
            "tags=list(sys_tags()); "
            "print(json.dumps({'platforms':list(dict.fromkeys(t.platform for t in tags if t.platform!='any'))}))"
        )
        target = json.loads(runner.run(label + "-tags", [python, "-I", "-c", code], cwd))
        args.extend(["--python-version", "3.7", "--implementation", "cp"])
        for abi in ("cp37m", "abi3", "none"):
            args.extend(["--abi", abi])
        for platform_tag in target["platforms"]:
            args.extend(["--platform", platform_tag])
        python = sys.executable
    pip(runner, label, python, args, cwd)


def consume(runner, label, interpreter, requirements, artifacts, evidence, version, wheelhouse_sources=()):
    lane, pyversion = interpreter_lane(runner, interpreter, evidence / "empty", label + "-version")
    wheelhouse = evidence / "wheelhouse" / label
    download_locked(
        runner, label + "-download", interpreter, lane, requirements, wheelhouse, evidence / "empty", wheelhouse_sources
    )
    result = {"python": pyversion, "modes": {}}
    for mode in ("wheel", "sdist"):
        root = evidence / "consumers" / label / mode
        root.mkdir(parents=True)
        empty = root / "empty"
        empty.mkdir()
        venv = root / "venv"
        runner.run(label + "-" + mode + "-venv", [interpreter, "-I", "-m", "venv", venv], empty)
        bindir = venv / ("Scripts" if os.name == "nt" else "bin")
        python = bindir / ("python.exe" if os.name == "nt" else "python")
        args = ["install", "--no-index", "--find-links", wheelhouse, "--require-hashes", "--only-binary=:all:"]
        for kind in ("runtime", "tools"):
            args.extend(["-r", requirements / (kind + "-" + lane + ".lock")])
        pip(runner, label + "-" + mode + "-dependencies", python, args, empty)
        # sdist 保留默认 PEP517 隔离；离线 wheelhouse 只提供已核哈希的同轮工具。
        args = ["install", "--no-cache-dir", "--no-index", "--find-links", wheelhouse, "--no-deps"]
        args.extend(artifacts[key][mode] for key, _, _ in PACKAGES)
        pip(runner, label + "-" + mode + "-install", python, args, empty)
        pip(runner, label + "-" + mode + "-pip-check", python, ["check"], empty)
        code = (
            "import json,pathlib,sys,tansr_sdk,tansr_demo; "
            "from tansr_sdk import Client,AsyncClient,AuthToken,CallOptions,ApiResponse,Error,CancellationToken; "
            "root=pathlib.Path(sys.prefix).resolve(); "
            "mods=[tansr_sdk,tansr_demo]; "
            "paths=[pathlib.Path(m.__file__).resolve() for m in mods]; "
            "[p.relative_to(root) for p in paths]; "
            "assert tansr_sdk.__version__ == " + repr(version) + "; "
            "print(json.dumps({'prefix':str(root),'modules':[str(p) for p in paths],'sdk_version':tansr_sdk.__version__}))"
        )
        imported = runner.run(label + "-" + mode + "-imports", [python, "-I", "-c", code], empty)
        for command in COMMANDS:
            executable = bindir / (command + (".exe" if os.name == "nt" else ""))
            runner.run(label + "-" + mode + "-" + command, [executable, "--help"], empty, timeout=30)
        # 旧解释器的弃用告警仍留在原日志；最后一行是探针 JSON。
        result["modes"][mode] = {
            "imports": json.loads(imported.splitlines()[-1]),
            "pip_check": "pass",
            "demo_help": list(COMMANDS),
        }
    return result


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True, help="必须为不存在的新归档目录")
    parser.add_argument("--consumer-python", action="append", default=[], help="可重复；完整解释器路径")
    parser.add_argument(
        "--wheelhouse-source",
        type=Path,
        action="append",
        default=[],
        help="可重复；已归档 wheelhouse，本轮重核哈希并完全离线复用",
    )
    parser.add_argument("--version", default="0.1.0")
    parser.add_argument("--check-locks-only", action="store_true")
    args = parser.parse_args()
    source = args.source.resolve()
    evidence = args.evidence.resolve()
    if source == evidence or source in evidence.parents:
        raise RuntimeError("Evidence must be outside the product source tree")
    evidence.mkdir(parents=True, exist_ok=False)
    (evidence / "logs").mkdir()
    (evidence / "empty").mkdir()
    locks = inspect_locks(source / "requirements")
    write_json(evidence / "locks.json", locks)
    wheelhouse_sources = [path.resolve() for path in args.wheelhouse_source]
    if wheelhouse_sources:
        verified = inspect_wheelhouses(wheelhouse_sources, locks)
        write_json(evidence / "wheelhouse-sources.json", verified)
    if args.check_locks_only:
        print("Exact version/SHA256 lock structure passed: " + str(len(locks)))
        return
    runner = Runner(evidence)
    result = {"source": str(source), "version": args.version, "status": "running", "projects": {}, "consumers": {}}
    try:
        build_lane, build_version = interpreter_lane(runner, sys.executable, evidence / "empty", "build-version")
        if build_lane != "modern":
            raise RuntimeError("Build gate must run with the prepared modern interpreter")
        result["build_python"] = build_version
        wheelhouse = evidence / "wheelhouse" / "build"
        download_locked(
            runner,
            "build-download",
            sys.executable,
            build_lane,
            source / "requirements",
            wheelhouse,
            evidence / "empty",
            wheelhouse_sources,
        )
        artifacts = {}
        for key, name, import_name in PACKAGES:
            original = source if key == "sdk" else source / "demo"
            stage = evidence / "stage" / key
            inputs = stage_project(original, stage)
            output = evidence / "dist" / key
            output.mkdir(parents=True)
            env = clean_environment()
            env["PIP_NO_INDEX"] = "1"
            env["PIP_FIND_LINKS"] = wheelhouse.as_uri()
            runner.run(
                "build-" + key,
                [sys.executable, "-I", "-m", "build", "--wheel", "--sdist", "--outdir", output, stage],
                evidence / "empty",
                env=env,
            )
            artifacts[key] = inspect_artifacts(output, name, import_name, args.version)
            result["projects"][key] = {
                "inputs": inputs,
                "artifacts": {
                    kind: {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}
                    for kind, path in artifacts[key].items()
                },
            }
        for i, interpreter in enumerate(args.consumer_python or [sys.executable]):
            label = "consumer-" + str(i + 1)
            result["consumers"][label] = consume(
                runner,
                label,
                str(Path(interpreter).resolve()),
                source / "requirements",
                artifacts,
                evidence,
                args.version,
                wheelhouse_sources,
            )
        for key, _, _ in PACKAGES:
            original = source if key == "sdk" else source / "demo"
            for row in result["projects"][key]["inputs"]:
                if digest(original / row["path"]) != row["sha256"]:
                    raise RuntimeError("Source input changed during package gate: " + row["path"])
        result["status"] = "pass"
    except Exception as error:
        result["status"] = "failed"
        result["error"] = str(error)
        raise
    finally:
        write_json(evidence / "receipt.json", result)
    print("Four artifacts and all isolated consumers passed: " + str(evidence))


if __name__ == "__main__":
    main()
