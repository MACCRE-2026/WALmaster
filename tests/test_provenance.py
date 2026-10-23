"""Tests for Subjective Provenance and SLSA Level 3 Attestations.

Verification matrix:
1. Subjective Logic opinion tuple creation and mathematical invariants.
2. Jøsang consensus operator fusion and uncertainty reduction.
3. Edge cases: dogmatic consensus, vacuous opinions.
4. SLSA Level 3 Attestation data model, binary receipts, and Merkle root calculation.
5. Persistent ledger append/read for attestations.jsonl.
6. Quarantine cordoning ledger for quarantine_diffs.jsonl.
"""
from __future__ import annotations

import hashlib
import uuid
from pathlib import Path

import pytest

from core.provenance import (
    BinaryReceipt,
    QuarantineRecord,
    SLSAAttestation,
    SubjectiveOpinion,
    append_attestation,
    append_quarantine_record,
    compute_merkle_root,
    compute_payload_hash,
    fuse_opinions,
    read_attestations,
    read_quarantine_records,
)


def test_subjective_opinion_creation_valid() -> None:
    """Verify valid opinion construction and expected value calculation."""
    op = SubjectiveOpinion(b=0.7, d=0.2, u=0.1, a=0.5)
    assert op.b == pytest.approx(0.7, abs=1e-6)
    assert op.d == pytest.approx(0.2, abs=1e-6)
    assert op.u == pytest.approx(0.1, abs=1e-6)
    assert op.a == pytest.approx(0.5, abs=1e-6)

    # Expected value E = b + a * u
    # 0.7 + 0.5 * 0.1 = 0.75
    assert op.expected_value() == pytest.approx(0.75, abs=1e-6)

    # Serialization roundtrip
    d = op.as_dict()
    assert d == {"b": op.b, "d": op.d, "u": op.u, "a": op.a}
    restored = SubjectiveOpinion.from_dict(d)
    assert restored == op


def test_subjective_opinion_validation_invalid() -> None:
    """Verify that opinion invariants are strictly enforced."""
    # Sum != 1.0
    with pytest.raises(ValueError, match="must sum to 1.0"):
        SubjectiveOpinion(b=0.5, d=0.5, u=0.5, a=0.5)

    # Negative component
    with pytest.raises(ValueError, match="must be non-negative"):
        SubjectiveOpinion(b=-0.1, d=0.8, u=0.3, a=0.5)

    # Base rate out of bounds
    with pytest.raises(ValueError, match="Base rate 'a' must be in"):
        SubjectiveOpinion(b=0.8, d=0.1, u=0.1, a=1.5)


def test_fuse_opinions_decreases_uncertainty() -> None:
    """Verify that corroborating opinions strictly decrease uncertainty (u_fused < min(u_a, u_b))."""
    # Two agents observe agreeing evidence with some uncertainty
    omega_a = SubjectiveOpinion(b=0.6, d=0.1, u=0.3, a=0.5)
    omega_b = SubjectiveOpinion(b=0.7, d=0.1, u=0.2, a=0.5)

    fused = fuse_opinions(omega_a, omega_b)

    # Mathematical invariant: u_fused must be strictly lower than both inputs
    assert fused.u < omega_a.u
    assert fused.u < omega_b.u
    assert fused.u < min(omega_a.u, omega_b.u)

    # Sum must remain 1.0
    assert (fused.b + fused.d + fused.u) == pytest.approx(1.0, abs=1e-6)

    # Belief should increase
    assert fused.b > max(omega_a.b, omega_b.b)


def test_fuse_opinions_dogmatic_edge_case() -> None:
    """Verify consensus fusion when uncertainty is zero (denominator == 0)."""
    # Dogmatic opinions: u_a = 0, u_b = 0
    omega_a = SubjectiveOpinion(b=1.0, d=0.0, u=0.0, a=0.5)
    omega_b = SubjectiveOpinion(b=0.0, d=1.0, u=0.0, a=0.5)

    # Equal relative weight average
    fused = fuse_opinions(omega_a, omega_b, dogmatic_weight=0.5)

    assert fused.u == pytest.approx(0.0, abs=1e-6)
    assert fused.b == pytest.approx(0.5, abs=1e-6)
    assert fused.d == pytest.approx(0.5, abs=1e-6)
    assert (fused.b + fused.d + fused.u) == pytest.approx(1.0, abs=1e-6)


def test_fuse_opinions_vacuous_prior() -> None:
    """Verify fusion with a vacuous opinion (u=1.0) preserves the informed opinion."""
    informed = SubjectiveOpinion(b=0.75, d=0.15, u=0.10, a=0.5)
    vacuous = SubjectiveOpinion(b=0.0, d=0.0, u=1.0, a=0.5)

    fused = fuse_opinions(informed, vacuous)

    assert fused.b == pytest.approx(informed.b, abs=1e-6)
    assert fused.d == pytest.approx(informed.d, abs=1e-6)
    assert fused.u == pytest.approx(informed.u, abs=1e-6)


def test_binary_receipt_and_attestation() -> None:
    """Verify BinaryReceipt creation and SLSAAttestation serialization."""
    sample_stdout = "Execution completed successfully with 0 errors\n"
    expected_digest = hashlib.sha256(sample_stdout.encode("utf-8")).hexdigest()

    receipt = BinaryReceipt.from_execution(
        exit_code=0,
        tool_name="git_diff_engine",
        stdout=sample_stdout,
    )
    assert receipt.exit_code == 0
    assert receipt.tool_name == "git_diff_engine"
    assert receipt.stdout_digest == expected_digest

    op = SubjectiveOpinion(b=0.9, d=0.05, u=0.05, a=0.5)
    attestation = SLSAAttestation(
        actor_id="cloud_runner_worker_01",
        runtime_hash="sha256:7c9e0a2b5d4e1f8a",
        prev_merkle_root="root_alpha",
        next_merkle_root="root_beta",
        timestamp=1730000000.0,
        opinion=op,
        binary_receipt=receipt,
    )

    digest_1 = attestation.compute_digest()
    digest_2 = attestation.compute_digest()
    assert digest_1 == digest_2
    assert len(digest_1) == 64

    # Dict roundtrip
    data = attestation.as_dict()
    restored = SLSAAttestation.from_dict(data)
    assert restored.actor_id == attestation.actor_id
    assert restored.opinion == attestation.opinion
    assert restored.binary_receipt == attestation.binary_receipt
    assert restored.compute_digest() == digest_1


def test_attestation_ledger_persistence(test_artifacts_dir: Path) -> None:
    """Verify append and read operations for attestations.jsonl."""
    ledger_file = test_artifacts_dir / f"test_attestations_{uuid.uuid4().hex[:8]}.jsonl"

    try:
        receipt = BinaryReceipt(exit_code=0, tool_name="pytest", stdout_digest="abc123digest")
        op = SubjectiveOpinion(b=0.85, d=0.05, u=0.10, a=0.5)

        attestations = [
            SLSAAttestation(
                actor_id=f"agent_{i}",
                runtime_hash=f"hash_{i}",
                prev_merkle_root=f"prev_{i}",
                next_merkle_root=f"next_{i}",
                timestamp=1730000000.0 + i,
                opinion=op,
                binary_receipt=receipt,
            )
            for i in range(3)
        ]

        for att in attestations:
            append_attestation(ledger_file, att)

        loaded = read_attestations(ledger_file)
        assert len(loaded) == 3
        for original, restored in zip(attestations, loaded, strict=True):
            assert original.actor_id == restored.actor_id
            assert original.compute_digest() == restored.compute_digest()
    finally:
        if ledger_file.exists():
            ledger_file.unlink(missing_ok=True)


def test_quarantine_cordon_ledger(test_artifacts_dir: Path) -> None:
    """Verify recording and querying quarantine cordons in quarantine_diffs.jsonl."""
    quarantine_file = test_artifacts_dir / f"test_quarantine_{uuid.uuid4().hex[:8]}.jsonl"

    try:
        record = QuarantineRecord(
            quarantine_id="quar_9876",
            timestamp=1730000100.0,
            reason="Unresolvable concurrent branch divergence at page 42",
            actor_id="desktop_antigravity",
            payload={"divergent_keys": ["step_count", "lamport_clock"]},
            opinion=SubjectiveOpinion(b=0.1, d=0.8, u=0.1, a=0.5),
            diagnostics={"trace": "Branch diverged from root commit 0a1b2c"},
        )

        append_quarantine_record(quarantine_file, record)

        entries = read_quarantine_records(quarantine_file)
        assert len(entries) == 1
        assert entries[0]["quarantine_id"] == "quar_9876"
        assert entries[0]["reason"] == "Unresolvable concurrent branch divergence at page 42"
        assert entries[0]["opinion"]["d"] == pytest.approx(0.8, abs=1e-6)
    finally:
        if quarantine_file.exists():
            quarantine_file.unlink(missing_ok=True)


def test_merkle_root_computation() -> None:
    """Verify deterministic Merkle root calculation."""
    leaves = [
        compute_payload_hash({"step": 1, "action": "read"}),
        compute_payload_hash({"step": 2, "action": "write"}),
        compute_payload_hash({"step": 3, "action": "commit"}),
    ]

    root = compute_merkle_root(leaves)
    assert isinstance(root, str)
    assert len(root) == 64

    # Empty leaves returns deterministic SHA-256 of empty bytes
    empty_root = compute_merkle_root([])
    assert empty_root == hashlib.sha256(b"").hexdigest()

    # Identical leaves produce identical root
    root2 = compute_merkle_root(leaves)
    assert root == root2
