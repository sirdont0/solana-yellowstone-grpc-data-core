#!/usr/bin/env python3
"""Bounded public Solana JSON-RPC getBlocks/getBlock feasibility probe.

This is evidence tooling, not a production backfill worker. It never sends
credentials and intentionally samples only a small finalized slot window.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import urllib.request

DEFAULT_ENDPOINT = "https://api.mainnet-beta.solana.com"


def rpc(endpoint: str, method: str, params: list[object], timeout: float) -> object:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    request = urllib.request.Request(
        endpoint,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "solana-core-g3-repair-probe/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        decoded = json.load(response)
    if "error" in decoded:
        raise RuntimeError(f"{method}: {decoded['error']}")
    return decoded.get("result")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--range-slots", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--out", help="write redacted JSON evidence to this path")
    args = parser.parse_args()
    if args.range_slots < 1 or args.range_slots > 100:
        parser.error("--range-slots must be in [1, 100]")
    if args.timeout <= 0 or args.timeout > 30:
        parser.error("--timeout must be in (0, 30]")

    try:
        current = int(rpc(args.endpoint, "getSlot", [{"commitment": "finalized"}], args.timeout))
        start = max(0, current - args.range_slots)
        slots = rpc(args.endpoint, "getBlocks", [start, current, {"commitment": "finalized"}], args.timeout)
        if not isinstance(slots, list) or not slots:
            raise RuntimeError("getBlocks returned no finalized slots in the bounded range")
        sample_slot = int(slots[-1])
        block = rpc(
            args.endpoint,
            "getBlock",
            [sample_slot, {
                "commitment": "finalized",
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": False,
                "maxSupportedTransactionVersion": 1,
            }],
            args.timeout,
        )
        if not isinstance(block, dict):
            raise RuntimeError(f"getBlock returned no block object for slot {sample_slot}")
        result = {
            "status": "PASS",
            "checked_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "provider": "Solana public mainnet JSON-RPC",
            "endpoint": args.endpoint.removeprefix("https://").removeprefix("http://").rstrip("/"),
            "commitment": "finalized",
            "observed_finalized_slot": current,
            "range_start": start,
            "range_end": current,
            "getBlocks_count": len(slots),
            "getBlocks_slots": slots,
            "sample_block_slot": sample_slot,
            "blockhash": block.get("blockhash"),
            "previousBlockhash": block.get("previousBlockhash"),
            "parentSlot": block.get("parentSlot"),
            "transaction_count": len(block.get("transactions", [])),
            "max_supported_transaction_version": 1,
            "limitations": [
                "bounded one-shot RPC probe, not a sustained backfill test",
                "does not return complete validator account-write/config history",
                "public endpoint quotas and availability are not guaranteed",
                "endpoint operator/backend independence from PublicNode is not attested",
            ],
        }
    except Exception as exc:  # noqa: BLE001 - evidence tool reports the exact bounded failure.
        result = {
            "status": "FAIL",
            "checked_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "provider": "Solana public mainnet JSON-RPC",
            "endpoint": args.endpoint.removeprefix("https://").removeprefix("http://").rstrip("/"),
            "error": str(exc),
        }
        print(json.dumps(result, indent=2))
        return 1

    rendered = json.dumps(result, indent=2) + "\n"
    if args.out:
        with open(args.out, "w", encoding="utf-8") as target:
            target.write(rendered)
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main())
