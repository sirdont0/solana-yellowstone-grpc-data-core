#!/usr/bin/env python3
"""Offline A/B Yellowstone WAL parity + independent finalized Solana RPC witness.

This is a bounded diagnostic, not a proof of complete account/config history.
Reads only closed source WALs; no Yellowstone credential is read or transmitted.
Requires a previously built corectl binary, e.g.:
  go build -o ./data/corectl ./cmd/corectl
"""
from __future__ import annotations
import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import random
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


class AuditError(Exception):
    pass


def source_transactions(corectl: str, data_dir: str, source: str, max_records: int) -> dict[str, tuple[int, int, str]]:
    """Read a verified *closed* source WAL without loading protobuf payloads."""
    base = [corectl, "verify", "--data-dir", data_dir, "--source", source]
    verify = subprocess.run(base, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if verify.returncode:
        raise AuditError(f"WAL verification failed for {source} (code {verify.returncode})")
    command = [corectl, "replay", "--data-dir", data_dir, "--source", source,
               "--kind", "transaction", "--omit-payload"]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    observations: dict[str, tuple[int, int, str]] = {}
    count = 0
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            count += 1
            if count > max_records:
                raise AuditError(f"{source}: max_records exceeded; narrow the capture")
            row = json.loads(line)
            signature = row.get("signature")
            slot = row.get("slot")
            index = row.get("tx_index", 0)
            digest = row.get("digest")
            if not isinstance(signature, str) or not signature or not isinstance(slot, int):
                raise AuditError(f"{source}: transaction record lacks slot/signature")
            if not isinstance(index, int):
                raise AuditError(f"{source}: transaction index invalid")
            if not isinstance(digest, str) or not digest:
                raise AuditError(f"{source}: transaction digest absent")
            value = (slot, index, digest)
            prior = observations.get(signature)
            if prior is not None and prior != value:
                raise AuditError(f"{source}: conflicting positions for one signature")
            observations[signature] = value
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    if proc.wait() != 0:
        raise AuditError(f"{source}: WAL replay failed")
    return observations


def bounded_pairs(records: dict[str, tuple[int, int, str]], lo: int, hi: int) -> dict[str, tuple[int, int, str]]:
    return {sig: position for sig, position in records.items() if lo <= position[0] <= hi}


def compare_source_sets(a: dict[str, tuple[int, int, str]], b: dict[str, tuple[int, int, str]]) -> dict:
    aa, bb = set(a), set(b)
    common = aa & bb
    position_conflicts = sum(a[sig][:2] != b[sig][:2] for sig in common)
    digest_conflicts = sum(len(a[sig]) > 2 and len(b[sig]) > 2 and
                           a[sig][2] != b[sig][2] for sig in common)
    return {
        "a_count": len(aa), "b_count": len(bb), "intersection": len(common),
        "a_only": len(aa - bb), "b_only": len(bb - aa),
        "index_or_slot_conflicts": position_conflicts,
        "content_digest_conflicts": digest_conflicts,
        "status": "MATCH" if aa == bb and position_conflicts == 0 and digest_conflicts == 0 else "MISMATCH",
    }


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def rpc_call(url: str, method: str, params: list, retries: int = 4) -> object:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    req = urllib.request.Request(url, data=body,
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "DualFinalizedAudit/1.0"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=25) as response:
                payload = json.load(response)
            if "error" in payload:
                code = payload["error"].get("code", "unknown")
                raise AuditError(f"RPC {method} returned error code {code}")
            return payload.get("result")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, AuditError) as error:
            if attempt == retries - 1:
                if isinstance(error, urllib.error.HTTPError):
                    raise AuditError(f"RPC {method} HTTP {error.code}") from error
                if isinstance(error, AuditError):
                    raise
                raise AuditError(f"RPC {method} network {type(error).__name__}") from error
            time.sleep(min(2 ** attempt, 8))
    raise AuditError("RPC retry exhausted")


def rpc_signature_witness(
    url: str, program: str, lo: int, hi: int, max_pages: int, page_pause: float = 0.1
) -> tuple[dict[str, int], dict]:
    head = rpc_call(url, "getSlot", [{"commitment": "finalized"}])
    if not isinstance(head, int) or head < hi:
        return {}, {"status": "PENDING", "finalized_slot": head}
    discovered: dict[str, int] = {}
    before = None
    pages = 0
    reached_start = False
    previous_slot = None
    ordering_error = False
    for _ in range(max_pages):
        options = {"limit": 1000, "commitment": "finalized"}
        if before:
            options["before"] = before
        page = rpc_call(url, "getSignaturesForAddress", [program, options])
        pages += 1
        if not isinstance(page, list):
            raise AuditError("RPC getSignaturesForAddress returned non-list")
        if not page:
            # An empty page may mean retention/pruning or no further history.
            # Without an observed result below lo, this is not complete proof.
            reached_start = previous_slot is not None and previous_slot < lo
            break
        for item in page:
            signature = item.get("signature")
            slot = item.get("slot")
            if not isinstance(signature, str) or not isinstance(slot, int):
                raise AuditError("RPC signature item incomplete")
            if previous_slot is not None and slot > previous_slot:
                ordering_error = True
            previous_slot = slot
            if lo <= slot <= hi:
                discovered[signature] = slot
        if min(item["slot"] for item in page) < lo:
            reached_start = True
            break
        if before == page[-1]["signature"]:
            break  # non-progressing cursor: UNKNOWN, not a false complete audit
        before = page[-1]["signature"]
        time.sleep(page_pause)
    status = "COMPLETE" if reached_start and not ordering_error else "UNKNOWN"
    return discovered, {"status": status, "pages": pages, "finalized_slot": head,
                        "reached_start": reached_start, "ordering_error": ordering_error}


def compare_with_oracle(
    a: dict[str, tuple[int, int, str]], b: dict[str, tuple[int, int, str]], oracle: dict[str, int]
) -> dict:
    sa, sb, so = set(a), set(b), set(oracle)
    return {
        "oracle_count": len(so), "a_missing": len(so - sa), "b_missing": len(so - sb),
        "both_missing": len(so - (sa | sb)),
        "a_extras": len(sa - so), "b_extras": len(sb - so),
        "slot_conflicts_a": sum(a[sig][0] != oracle[sig] for sig in sa & so),
        "slot_conflicts_b": sum(b[sig][0] != oracle[sig] for sig in sb & so),
        "missing_signature_hashes": [fingerprint(s)[:16] for s in sorted(so - (sa | sb))[:10]],
    }


def sample_finalized_blocks(
    url: str, program: str, lo: int, hi: int,
    a: dict[str, tuple[int, int, str]], b: dict[str, tuple[int, int, str]], count: int, seed: int
) -> dict:
    if count == 0:
        return {"status": "NOT_RUN", "blocks": []}
    if hi - lo > 100_000:
        return {"status": "UNKNOWN", "reason": "range_too_large", "blocks": []}
    available = rpc_call(url, "getBlocks", [lo, hi, {"commitment": "finalized"}])
    if not isinstance(available, list):
        raise AuditError("getBlocks returned non-list")
    sample = sorted(random.Random(seed).sample(available, min(count, len(available))))
    blocks = []
    for slot in sample:
        block = rpc_call(url, "getBlock", [slot, {
            "encoding": "json", "transactionDetails": "full", "rewards": False,
            "commitment": "finalized", "maxSupportedTransactionVersion": 1
        }])
        if not isinstance(block, dict):
            blocks.append({"slot": slot, "status": "UNKNOWN", "reason": "block_unavailable"})
            continue
        expected = {}
        for index, tx in enumerate(block.get("transactions", [])):
            tr = tx.get("transaction") or {}
            message = tr.get("message") or {}
            keys = [k if isinstance(k, str) else k.get("pubkey")
                    for k in message.get("accountKeys", [])]
            loaded = (tx.get("meta") or {}).get("loadedAddresses") or {}
            keys.extend(loaded.get("writable", []))
            keys.extend(loaded.get("readonly", []))
            if program in keys:
                signatures = tr.get("signatures") or []
                if signatures:
                    expected[signatures[0]] = (slot, index)
        record = {"slot": slot, "total_transactions": len(block.get("transactions", [])),
                  "selected_transactions": len(expected),
                  "blockhash_sha256": fingerprint(block.get("blockhash", "")),
                  "parent_slot": block.get("parentSlot")}
        matches = True
        for src, observed in (("a", a), ("b", b)):
            missing = set(expected) - set(observed)
            extras = {sig for sig, pos in observed.items() if pos[0] == slot} - set(expected)
            conflicts = sum(observed[sig][:2] != pos for sig, pos in expected.items() if sig in observed)
            record[src] = {"missing": len(missing), "extras": len(extras), "position_conflicts": conflicts}
            matches &= not missing and not extras and conflicts == 0
        record["status"] = "MATCH" if matches else "MISMATCH"
        blocks.append(record)
    status = "MATCH" if blocks and all(b["status"] == "MATCH" for b in blocks) else "UNKNOWN" if not blocks or any(b["status"] == "UNKNOWN" for b in blocks) else "MISMATCH"
    return {"status": status, "sample_seed": seed, "produced_blocks": len(available), "blocks": blocks}


def run(args: argparse.Namespace) -> dict:
    parsed = urllib.parse.urlsplit(args.rpc_url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise AuditError("RPC URL must be HTTPS without URL credentials/query/fragment")
    a_all = source_transactions(args.corectl, args.data_dir, args.source_a, args.max_records)
    b_all = source_transactions(args.corectl, args.data_dir, args.source_b, args.max_records)
    if not a_all or not b_all:
        raise AuditError("one or both WALs contain no transaction observations")
    lo = max(min(v[0] for v in a_all.values()), min(v[0] for v in b_all.values()))
    hi = min(max(v[0] for v in a_all.values()), max(v[0] for v in b_all.values()))
    if args.start_slot is not None:
        lo = max(lo, args.start_slot)
    else:
        lo += args.trim_slots
    if args.end_slot is not None:
        hi = min(hi, args.end_slot)
    else:
        hi -= args.trim_slots
    if lo > hi:
        raise AuditError("no comparable window after boundary trim")
    a = bounded_pairs(a_all, lo, hi)
    b = bounded_pairs(b_all, lo, hi)
    parity = compare_source_sets(a, b)
    report = {
        "schema_version": 1, "time_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "program": args.program, "commitment": "finalized",
        "a": args.source_a, "b": args.source_b, "failure_domains": 1,
        "rpc_endpoint_sha256": fingerprint(args.rpc_url),
        "range": [lo, hi], "source_parity": parity,
        "signature_witness": {"status": "UNKNOWN"},
        "block_samples": {"status": "NOT_RUN", "blocks": []},
        "account_history": "UNVERIFIED", "config_history": "UNVERIFIED",
        "ordered_effects": "UNVERIFIED",
        "limitations": [
            "Two PublicNode connections share one operator/failure domain and do not prove geographic separation.",
            "getSignaturesForAddress validates an independent RPC signature index, not every account write.",
            "Sampled finalized blocks do not establish continuous complete-bank or config history.",
            "The tool reads closed WAL only; it is not a running production auditor."
        ]
    }
    try:
        oracle, witness = rpc_signature_witness(args.rpc_url, args.program, lo, hi, args.max_pages)
        report["signature_witness"] = witness
        if witness["status"] == "COMPLETE":
            comparisons = compare_with_oracle(a, b, oracle)
            report["signature_witness"].update(comparisons)
            if any(comparisons[k] for k in ("a_missing", "b_missing", "both_missing",
                                          "a_extras", "b_extras",
                                          "slot_conflicts_a", "slot_conflicts_b")):
                report["signature_witness"]["status"] = "MISMATCH"
            else:
                report["signature_witness"]["status"] = "MATCH"
    except AuditError as exc:
        report["rpc_error_class"] = type(exc).__name__
        report["rpc_error"] = str(exc)
        report["signature_witness"]["status"] = "UNKNOWN"
    # Full block sampling is independently useful even when signature pagination
    # hit its budget. It must not convert the unsampled interval into MATCH.
    if report["signature_witness"]["status"] != "PENDING":
        try:
            report["block_samples"] = sample_finalized_blocks(
                args.rpc_url, args.program, lo, hi, a, b, args.sample_blocks, args.seed)
        except AuditError as exc:
            report["block_samples"] = {"status": "UNKNOWN", "blocks": [], "error": str(exc)}
    statuses = (parity["status"], report["signature_witness"]["status"],
                report["block_samples"]["status"])
    report["result"] = ("MISMATCH" if "MISMATCH" in statuses else
                        "BOUNDED_CORROBORATED" if statuses == ("MATCH", "MATCH", "MATCH") else "UNKNOWN")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corectl", required=True, help="path to built corectl binary")
    parser.add_argument("--data-dir", required=True, help="root of closed source WALs")
    parser.add_argument("--source-a", default="publicnode_a")
    parser.add_argument("--source-b", default="publicnode_b")
    parser.add_argument("--program", required=True, help="Solana program ID")
    parser.add_argument("--rpc-url", default="https://api.mainnet-beta.solana.com")
    parser.add_argument("--start-slot", type=int)
    parser.add_argument("--end-slot", type=int)
    parser.add_argument("--trim-slots", type=int, default=8)
    parser.add_argument("--max-pages", type=int, default=250)
    parser.add_argument("--max-records", type=int, default=500_000)
    parser.add_argument("--sample-blocks", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20261119)
    parser.add_argument("--output", required=True, help="local redacted JSON report path")
    args = parser.parse_args()
    if args.max_pages < 1 or args.max_records < 1 or args.trim_slots < 0 or not (0 <= args.sample_blocks <= 20):
        parser.error("invalid resource bounds")
    try:
        report = run(args)
    except (AuditError, OSError, json.JSONDecodeError) as exc:
        print(f"audit failed closed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 3
    dest = pathlib.Path(args.output)
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Never overwrite previous evidence; restrict access even with a permissive umask.
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as out:
        out.write(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(json.dumps({"result": report["result"], "range": report["range"],
                      "source_parity": report["source_parity"]["status"],
                      "signature_witness": report["signature_witness"]["status"],
                      "block_samples": report["block_samples"]["status"]}, sort_keys=True))
    return 0 if report["result"] == "BOUNDED_CORROBORATED" else 2 if report["result"] == "MISMATCH" else 3


if __name__ == "__main__":
    raise SystemExit(main())
