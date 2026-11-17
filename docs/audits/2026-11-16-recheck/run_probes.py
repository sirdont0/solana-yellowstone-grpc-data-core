#!/usr/bin/env python3
"""Run five WAL regression expectations through a temporary Go overlay.

All five tests are expected to pass against the current implementation. A
nonzero exit indicates a regression or a changed fault-injection point that
needs review. The overlay never edits runtime sources; its only injected
runtime fault is an error after CURRENT rename and before directory fsync.
This focused regression probe is not the complete WAL fault/load acceptance
suite described in the surrounding audit.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile

evidence = Path(__file__).resolve().parent
root = evidence.parents[2]
source = root / "internal/ingest/wal_v2.go"
needle = "\treturn syncWALV2Dir(dir)\n"
code = source.read_text()
if code.count(needle) != 1:
    raise SystemExit("WAL implementation changed; review the fault injection point")
fault = ('\tif filepath.Base(path) == "CURRENT" && '
         'os.Getenv("CORE_AUDIT_FAIL_AFTER_CURRENT_RENAME") == "1" '
         '{ return errors.New("injected directory fsync failure after CURRENT rename") }\n')
with tempfile.TemporaryDirectory(prefix="core-wal-audit-") as directory:
    temp = Path(directory)
    injected = temp / "wal_v2_injected.go"
    injected.write_text(code.replace(needle, fault + needle))
    overlay = temp / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {
        str(source): str(injected),
        str(root / "internal/ingest/audit_probe_test.go"):
            str(evidence / "regression_probes.go.txt"),
    }}))
    env = dict(os.environ)
    env.pop("CORE_AUDIT_FAIL_AFTER_CURRENT_RENAME", None)
    result = subprocess.run([
        "go", "test", "-race", "-count=1", "-overlay", str(overlay),
        "./internal/ingest", "-run", "^TestAudit", "-v",
    ], cwd=root, env=env)
    raise SystemExit(result.returncode)
