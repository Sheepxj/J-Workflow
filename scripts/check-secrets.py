import argparse
import hashlib
import json
import math
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
TOKEN = re.compile(rb"[A-Za-z0-9_+/-]{16,}")
PROVIDER_TOKEN = re.compile(
    rb"\b(?:sk-[A-Za-z0-9_-]{20,}|gh[pousr]_[A-Za-z0-9]{20,}|"
    rb"github_pat_[A-Za-z0-9_]{30,}|AKIA[A-Z0-9]{16})\b"
)
ASSIGNMENT = re.compile(
    rb"(?i)(?:api[_-]?key|api[_-]?secret|access[_-]?token|"
    rb"client[_-]?secret|secret[_-]?key)"
    rb"[\"']?\s*[:=]\s*[\"']?([A-Za-z0-9_+/=-]{24,})(?=[\s\"'\\,;}\]]|$)"
)
FALLBACK = re.compile(
    rb"\$\{[A-Z0-9_]*(?:KEY|SECRET|TOKEN|PASSWORD)[A-Z0-9_]*:([^}\r\n]{24,})\}"
)
PRIVATE_HEADER = re.compile(
    rb"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----"
)
PLACEHOLDERS = (
    b"your",
    b"example",
    b"placeholder",
    b"replace_with",
    b"xxxx",
    b"test_",
    b"useyouapikey",
)
FRONTEND_ENV = {
    "console/frontend/.env.development",
    "console/frontend/.env.production",
    "console/frontend/.env.test",
}


def git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=str(ROOT))


def placeholder(value: bytes) -> bool:
    return any(word in value.lower() for word in PLACEHOLDERS)


def suspicious(value: bytes) -> bool:
    if placeholder(value) or re.fullmatch(rb"[A-Z_]+", value):
        return False
    counts = Counter(value)
    entropy = -sum(
        (n / len(value)) * math.log2(n / len(value)) for n in counts.values()
    )
    return entropy >= 3.0


def findings(
    path: str, data: bytes, fingerprints: set[str]
) -> list[tuple[str, int, str]]:
    name = Path(path).name
    if (
        path.startswith(".local/")
        or "/load-test/results/" in path
        or name == "config.env"
        or (
            name.startswith(".env")
            and not name.endswith(".example")
            and path not in FRONTEND_ENV
        )
        or name.endswith((".pem", ".key", ".p12", ".pfx"))
    ):
        return [(path, 1, "private file must not be tracked")]
    normalized = data.replace(b'\\"', b'"')
    result = set()
    for number, line in enumerate(normalized.splitlines(), 1):
        if any(
            hashlib.sha256(m.group()).hexdigest() in fingerprints
            for m in TOKEN.finditer(line)
        ):
            result.add((path, number, "known local credential"))
        if any(
            not placeholder(m.group()) for m in PROVIDER_TOKEN.finditer(line)
        ):
            result.add((path, number, "provider token"))
        dummy_key = b"PRIVATE KEY-----xxx-----END PRIVATE KEY" in line.replace(
            b"\\n", b""
        ).replace(b"\\", b"")
        if PRIVATE_HEADER.search(line) and not dummy_key:
            result.add((path, number, "private key"))
        for pattern in (ASSIGNMENT, FALLBACK):
            if any(suspicious(m.group(1)) for m in pattern.finditer(line)):
                result.add((path, number, "possible hardcoded credential"))
    return sorted(result)


def entries(ref: Optional[str] = None) -> list[tuple[str, str]]:
    if ref is None:
        raw = git("ls-files", "--stage", "-z")
        return [
            (item.split(b"\t", 1)[1].decode("utf-8"), item.split()[1].decode())
            for item in raw.split(b"\0")
            if item and item.split()[2] == b"0"
        ]
    raw = git("ls-tree", "-r", "-z", ref)
    return [
        (item.split(b"\t", 1)[1].decode("utf-8"), item.split()[2].decode())
        for item in raw.split(b"\0")
        if item and item.split()[1] == b"blob"
    ]


def scan(
    entry_list: list[tuple[str, str]],
    fingerprints: set[str],
    seen: set[tuple[str, str]],
) -> list[tuple[str, int, str]]:
    selected = [
        (path, sha) for path, sha in entry_list if (path, sha) not in seen
    ]
    seen.update(selected)
    if not selected:
        return []
    process = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        cwd=str(ROOT),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    output, _ = process.communicate(
        "".join(sha + "\n" for _, sha in selected).encode()
    )
    if process.returncode:
        raise RuntimeError("Cannot read Git objects")
    offset = 0
    result = []
    for path, sha in selected:
        end = output.index(b"\n", offset)
        header = output[offset:end].split()
        if len(header) != 3 or header[1] != b"blob":
            raise RuntimeError("Unexpected Git object: " + sha)
        size = int(header[2])
        data = output[end + 1 : end + 1 + size]
        offset = end + 2 + size
        result.extend(findings(path, data, fingerprints))
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--staged", action="store_true")
    mode.add_argument("--history", metavar="REF")
    mode.add_argument("--push", action="store_true")
    args = parser.parse_args()
    fingerprints = set(
        json.loads((ROOT / ".secret-fingerprints.json").read_text())["sha256"]
    )
    seen = set()
    result = []
    if args.staged:
        result = scan(entries(), fingerprints, seen)
    else:
        refs = [args.history] if args.history else []
        if args.push:
            for line in sys.stdin:
                fields = line.split()
                if len(fields) != 4:
                    raise RuntimeError("Invalid pre-push input")
                if set(fields[1]) != {"0"}:
                    refs.append(fields[1])
        for ref in refs:
            for commit in git("rev-list", ref).decode().splitlines():
                result.extend(scan(entries(commit), fingerprints, seen))
    if result:
        print(
            "Secret check blocked this operation. "
            "Only paths and line numbers are shown.",
            file=sys.stderr,
        )
        for path, line, reason in sorted(set(result)):
            print("{}:{}: {}".format(path, line, reason), file=sys.stderr)
        return 1
    print("Secret check passed ({} unique file versions).".format(len(seen)))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (
        OSError,
        ValueError,
        RuntimeError,
        subprocess.CalledProcessError,
    ) as error:
        print("Secret check failed: {}".format(error), file=sys.stderr)
        sys.exit(2)
