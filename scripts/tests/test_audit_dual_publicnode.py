"""Regression tests for the bounded finalized A/B + external RPC witness."""
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import audit_dual_publicnode as audit


class DualAuditTests(unittest.TestCase):
    def test_equal_sources_are_only_parity_not_completeness(self):
        a = {"sig-1": (100, 1), "sig-2": (101, 2)}
        b = dict(a)
        self.assertEqual(audit.compare_source_sets(a, b)["status"], "MATCH")
        external = {"sig-1": 100, "sig-2": 101, "sig-3": 102}
        checked = audit.compare_with_oracle(a, b, external)
        self.assertEqual(checked["both_missing"], 1)
        self.assertEqual(checked["a_missing"], 1)
        self.assertEqual(checked["b_missing"], 1)

    def test_one_source_missing_vs_union(self):
        a = {"sig-1": (100, 1), "sig-2": (100, 2)}
        b = {"sig-1": (100, 1)}
        oracle = {"sig-1": 100, "sig-2": 100}
        self.assertEqual(audit.compare_source_sets(a, b)["a_only"], 1)
        checked = audit.compare_with_oracle(a, b, oracle)
        self.assertEqual(checked["both_missing"], 0)
        self.assertEqual(checked["b_missing"], 1)

    def test_same_signature_different_position_is_mismatch(self):
        a = {"sig-1": (100, 3)}
        b = {"sig-1": (100, 4)}
        checked = audit.compare_source_sets(a, b)
        self.assertEqual(checked["status"], "MISMATCH")
        self.assertEqual(checked["index_or_slot_conflicts"], 1)

    def test_same_signature_and_position_but_different_content_is_mismatch(self):
        a = {"sig-1": (100, 3, "digest-a")}
        b = {"sig-1": (100, 3, "digest-b")}
        checked = audit.compare_source_sets(a, b)
        self.assertEqual(checked["status"], "MISMATCH")
        self.assertEqual(checked["index_or_slot_conflicts"], 0)
        self.assertEqual(checked["content_digest_conflicts"], 1)

    def test_bounded_pairs_excludes_boundary(self):
        records = {"a": (99, 0), "b": (100, 1), "c": (110, 2), "d": (111, 3)}
        self.assertEqual(set(audit.bounded_pairs(records, 100, 110)), {"b", "c"})

    def test_stale_rpc_returns_pending_not_match(self):
        with mock.patch.object(audit, "rpc_call", return_value=99):
            signatures, status = audit.rpc_signature_witness("https://example.org", "pubkey", 100, 105, 3)
        self.assertEqual(signatures, {})
        self.assertEqual(status["status"], "PENDING")

    def test_empty_historical_page_without_lower_boundary_is_unknown(self):
        pages = [200, [{"signature": "sig-1", "slot": 150}], []]
        with mock.patch.object(audit, "rpc_call", side_effect=pages):
            signatures, status = audit.rpc_signature_witness("https://example.org", "pubkey", 100, 160, 3, page_pause=0)
        self.assertEqual(signatures, {"sig-1": 150})
        self.assertEqual(status["status"], "UNKNOWN")
        self.assertFalse(status["reached_start"])

    def test_block_sampling_catches_shared_missing_transaction(self):
        program = "program"
        block = {"blockhash": "blockhash", "parentSlot": 199, "transactions": [
            {"transaction": {"signatures": ["sig-a"], "message": {"accountKeys": [program]}}}
        ]}
        def fake_rpc(_url, method, _params):
            if method == "getBlocks":
                return [200]
            if method == "getBlock":
                return block
            self.fail("unexpected RPC")
        with mock.patch.object(audit, "rpc_call", side_effect=fake_rpc):
            result = audit.sample_finalized_blocks("https://example.org", program, 200, 200, {}, {}, 1, 1)
        self.assertEqual(result["status"], "MISMATCH")
        self.assertEqual(result["blocks"][0]["a"]["missing"], 1)
        self.assertEqual(result["blocks"][0]["b"]["missing"], 1)

    def test_block_sampling_includes_loaded_addresses(self):
        program = "program"
        block = {"blockhash": "blockhash", "parentSlot": 199, "transactions": [
            {"transaction": {"signatures": ["sig-a"], "message": {"accountKeys": ["other"]}},
             "meta": {"loadedAddresses": {"writable": [program], "readonly": []}}}
        ]}
        def fake_rpc(_url, method, _params):
            return [200] if method == "getBlocks" else block
        with mock.patch.object(audit, "rpc_call", side_effect=fake_rpc):
            result = audit.sample_finalized_blocks("https://example.org", program, 200, 200,
                                                   {"sig-a": (200, 0)}, {"sig-a": (200, 0)}, 1, 1)
        self.assertEqual(result["status"], "MATCH")


if __name__ == "__main__":
    unittest.main()
