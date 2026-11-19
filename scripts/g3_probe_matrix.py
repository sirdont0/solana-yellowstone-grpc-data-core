#!/usr/bin/env python3
"""Run bounded, redacted Yellowstone provider capability probes.

This delegates wire-level observations to `corectl probe`. It intentionally
does not capture raw updates, credentials, or private endpoint URLs. A missing
requested update kind remains UNKNOWN, even if the subscription was accepted.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone


PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PROFILES = (
    ("finalized_kinds", "finalized", "all", "reopen"),
    ("confirmed_replay", "confirmed", "slots,blocks_meta", "reopen"),
    ("processed_replay", "processed", "slots,blocks_meta", "reopen"),
    ("finalized_retention", "finalized", "slots", "retention"),
    # Fresh gRPC connections distinguish unsupported filters from a provider
    # that stops servicing rapid sequential streams on a shared connection.
    ("finalized_blocks_meta_isolated", "finalized", "blocks_meta", "reopen"),
    ("finalized_accounts_isolated", "finalized", "accounts", "reopen"),
    ("finalized_footer_isolated", "finalized", "block_footer", "reopen"),
)


def load_local_env(path: Path, env: dict[str, str]) -> None:
    """Read three known dotenv keys; do not evaluate shell syntax."""
    if not path.is_file():
        raise RuntimeError("the requested credentials file is missing")
    accepted = {"YELLOWSTONE_ENDPOINT", "YELLOWSTONE_X_TOKEN", "YELLOWSTONE_AUTH_HEADER"}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key not in accepted:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        env.setdefault(key, value)


def run_profile(corectl: str, name: str, commitment: str, kinds: str,
                behavior: str, env: dict[str, str], args: argparse.Namespace) -> dict:
    command = [
        corectl, "probe", "--provider-id", args.provider_id,
        "--token-env", "YELLOWSTONE_TOKEN", "--token-header", env["YELLOWSTONE_AUTH_HEADER"],
        "--commitment", commitment, "--kinds", kinds,
        "--program", PROGRAM, "--sample", args.sample,
        "--timeout", args.timeout, "--max-updates", str(args.max_updates),
        "--core-revision", args.core_revision,
    ]
    if behavior == "reopen":
        command.append("--reopen-overlap")
    if behavior == "retention":
        command.append("--retention-boundary")
    # Bound the complete process independently of its per-RPC deadlines.
    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True,
                                timeout=args.process_timeout, check=False)
    except subprocess.TimeoutExpired:
        return {"probe_error": "PROCESS_TIMEOUT", "profile": name}
    if result.returncode != 0:
        # Never serialize stderr: lower-level libraries may include endpoint details.
        return {"probe_error": "EXIT_NONZERO", "exit_code": result.returncode,
                "profile": name}
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"probe_error": "INVALID_JSON", "profile": name}
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corectl", required=True, help="path to a built corectl executable")
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--env-file", type=Path, help="optional local .env, read only; not copied")
    parser.add_argument("--provider-id", default="publicnode")
    parser.add_argument("--sample", default="4s")
    parser.add_argument("--timeout", default="5s")
    parser.add_argument("--max-updates", type=int, default=8)
    parser.add_argument("--process-timeout", type=int, default=120)
    parser.add_argument("--core-revision", default="")
    parser.add_argument("--summarize-only", action="store_true",
                        help="rebuild manifest from existing redacted JSON; no provider calls")
    parser.add_argument("--profiles", default=",".join(p[0] for p in PROFILES[:4]),
                        help="comma-separated subset of profile names")
    args = parser.parse_args()
    available = {p[0]: p for p in PROFILES}
    selected = [name.strip() for name in args.profiles.split(",") if name.strip()]
    if not selected or any(name not in available for name in selected):
        parser.error("--profiles must select one or more known profiles")
    if args.max_updates <= 0 or args.max_updates > 100:
        parser.error("--max-updates must be in [1, 100] for this bounded audit")

    env = dict(os.environ)
    if not args.summarize_only:
        if args.env_file:
            load_local_env(args.env_file, env)
        if "YELLOWSTONE_TOKEN" not in env and "YELLOWSTONE_X_TOKEN" in env:
            env["YELLOWSTONE_TOKEN"] = env["YELLOWSTONE_X_TOKEN"]
        env.setdefault("YELLOWSTONE_AUTH_HEADER", "x-token")
        if not env.get("YELLOWSTONE_ENDPOINT") or not env.get("YELLOWSTONE_TOKEN"):
            print("required endpoint/token not configured; no probes run", file=sys.stderr)
            return 2
        args.out_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(args.out_dir, 0o700)
    elif not args.out_dir.is_dir():
        print("output directory not found for summarize-only", file=sys.stderr)
        return 2

    manifest = {
        "schema_version": 1,
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "core_revision": args.core_revision,
        "provider_id": args.provider_id,
        "scope": "bounded wire observations, not canonical bank/account proof",
        "profiles": {},
    }
    any_error = False
    if not args.summarize_only:
        for name in selected:
            _, commitment, kinds, behavior = available[name]
            result = run_profile(args.corectl, name, commitment, kinds, behavior, env, args)
            path = args.out_dir / f"{name}.json"
            path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            os.chmod(path, 0o600)
            any_error |= "probe_error" in result

    # Rebuild from every existing named report so separate bounded runs do not
    # erase earlier findings from the manifest.
    revisions = set()
    for name, (_, commitment, kinds, _) in available.items():
        path = args.out_dir / f"{name}.json"
        if not path.is_file():
            continue
        raw = path.read_bytes()
        result = json.loads(raw)
        if result.get("core_revision"):
            revisions.add(result["core_revision"])
        streams = result.get("streams", {})
        statuses = {
            kind: {
                "initial": profile["first"]["status"],
                "reopen": profile.get("reopen_overlap", {}).get("status", "NOT_RUN"),
                "retention": profile.get("retention_boundary", {}).get("status", "NOT_RUN"),
            }
            for kind, profile in streams.items()
        }
        manifest["profiles"][name] = {
            "file": path.name,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "probe_error": result.get("probe_error"),
            "kinds": kinds,
            "commitment": commitment,
            "started_at_utc": result.get("started_at_utc"),
            "stream_statuses": statuses,
        }
        print(f"{name}: {statuses if statuses else result.get('probe_error')}")
    manifest["observed_core_revisions"] = sorted(revisions)
    manifest_path = args.out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(manifest_path, 0o600)
    return 1 if any_error else 0


if __name__ == "__main__":
    sys.exit(main())
