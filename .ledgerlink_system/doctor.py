from __future__ import annotations
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class Check:
    name: str
    ok: bool
    info: str


def run(cmd: List[str], timeout: int = 20) -> tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        shell=False,
    )
    return p.returncode, p.stdout.strip(), p.stderr.strip()


def main() -> int:
    root = Path.cwd()
    results: List[Check] = []

    # 1) Git repo integrity
    rc, out, err = run(["git", "rev-parse", "--show-toplevel"])
    if rc != 0:
        print("FAIL git_repo: Not inside a git repo.")
        return 2

    results.append(Check("git_repo", True, out))

    # 2) Disk I/O probe
    try:
        probe = root / ".ledgerlink_system" / "doctor_probe.tmp"
        probe.parent.mkdir(exist_ok=True)
        probe.write_text("ok", encoding="utf-8")
        assert probe.read_text(encoding="utf-8") == "ok"
        probe.unlink(missing_ok=True)
        results.append(Check("disk_io", True, "OK"))
    except Exception as e:
        results.append(Check("disk_io", False, str(e)))
        return report(results)

    # 3) Free disk space (>5GB recommended)
    usage = shutil.disk_usage(str(root))
    free_gb = usage.free / (1024 ** 3)
    results.append(Check("disk_free", free_gb > 5, f"{free_gb:.2f} GB free"))

    # 4) Python venv exists
    py = root / ".venv" / "Scripts" / "python.exe"
    if not py.exists():
        results.append(Check("python_venv", False, "Missing .venv python"))
        return report(results)

    rc, out, err = run([str(py), "-V"])
    results.append(Check("python_version", rc == 0, out or err))

    # 5) Validator exists + blocks numbers
    validator = root / ".ledgerlink_system" / "validator.py"
    if not validator.exists():
        results.append(Check("validator_present", False, "validator.py missing"))
        return report(results)

    payload = json.dumps({"text": "Total is $123", "channel": "doctor"})
    p = subprocess.Popen(
        [str(py), str(validator)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    out, err = p.communicate(payload, timeout=10)

    try:
        data = json.loads(out)
        blocked = data.get("status") == "BLOCK"
        results.append(Check("validator_blocks_numbers", blocked, out))
    except Exception:
        results.append(Check("validator_blocks_numbers", False, out or err))

    # 6) Node present
    rc, out, err = run(["node", "-v"])
    results.append(Check("node_version", rc == 0, out or err))

    # 7) Run log rotation once (safe)
    rot = root / ".ledgerlink_system" / "log_rotate.ps1"
    if rot.exists():
        rc, out, err = run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(rot)],
            timeout=60,
        )
        results.append(Check("log_rotation", rc == 0, "OK" if rc == 0 else err))
    else:
        results.append(Check("log_rotation", False, "log_rotate.ps1 missing"))

    return report(results)


def report(results: List[Check]) -> int:
    print("LedgerLink Doctor Report")
    print("=" * 28)
    ok_all = True
    for r in results:
        status = "OK  " if r.ok else "FAIL"
        print(f"{status} {r.name}: {r.info}")
        if not r.ok:
            ok_all = False
    return 0 if ok_all else 2


if __name__ == "__main__":
    sys.exit(main())
