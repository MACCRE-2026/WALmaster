"""Subjective Provenance & SLSA Level 3 Attestation Engine.

Mathematical Foundations:
Subjective Logic consensus operator (Jøsang, 2016) over opinion tuples
omega = (b, d, u, a) where b + d + u = 1.0 and a in [0.0, 1.0].
SLSA Level 3 cryptographically-chained attestations with Merkle roots
and quarantine cordoning for unresolvable branch divergences.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

OPINION_TOLERANCE: Final[float] = 1e-6


@dataclass(slots=True, frozen=True)
class SubjectiveOpinion:
    """Subjective Logic opinion tuple omega = (b, d, u, a).

    Attributes:
        b: Belief mass [0.0, 1.0].
        d: Disbelief mass [0.0, 1.0].
        u: Uncertainty mass [0.0, 1.0].
        a: Prior base rate [0.0, 1.0] (default 0.5).

    Invariants:
        b >= 0.0, d >= 0.0, u >= 0.0, 0.0 <= a <= 1.0
        abs((b + d + u) - 1.0) <= 1e-6
    """

    b: float = 1.0
    d: float = 0.0
    u: float = 0.0
    a: float = 0.5

    def __post_init__(self) -> None:
        """Validate bounds and epistemic normalization invariants."""
        if self.b < -OPINION_TOLERANCE or self.d < -OPINION_TOLERANCE or self.u < -OPINION_TOLERANCE:
            raise ValueError(
                f"Opinion components must be non-negative: b={self.b}, d={self.d}, u={self.u}"
            )
        if self.a < -OPINION_TOLERANCE or self.a > 1.0 + OPINION_TOLERANCE:
            raise ValueError(f"Base rate 'a' must be in [0.0, 1.0]: a={self.a}")

        total: float = self.b + self.d + self.u
        if abs(total - 1.0) > OPINION_TOLERANCE:
            raise ValueError(
                f"Opinion components (b + d + u) must sum to 1.0 within {OPINION_TOLERANCE} tolerance, got {total}"
            )

        # Sanitize clamped values to guard against tiny float drift
        object.__setattr__(self, "b", max(0.0, float(self.b)))
        object.__setattr__(self, "d", max(0.0, float(self.d)))
        object.__setattr__(self, "u", max(0.0, float(self.u)))
        object.__setattr__(self, "a", min(1.0, max(0.0, float(self.a))))

    def expected_value(self) -> float:
        """Calculate subjective probability expectation E = b + a * u."""
        return self.b + self.a * self.u

    def as_dict(self) -> dict[str, float]:
        """Convert opinion to structured dictionary."""
        return {"b": self.b, "d": self.d, "u": self.u, "a": self.a}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SubjectiveOpinion:
        """Deserialize an opinion tuple from dictionary representation."""
        return cls(
            b=float(data.get("b", 1.0)),
            d=float(data.get("d", 0.0)),
            u=float(data.get("u", 0.0)),
            a=float(data.get("a", 0.5)),
        )

    @classmethod
    def dogmatic_true(cls) -> SubjectiveOpinion:
        """Construct dogmatic true opinion (b=1, d=0, u=0)."""
        return cls(b=1.0, d=0.0, u=0.0, a=0.5)

    @classmethod
    def dogmatic_false(cls) -> SubjectiveOpinion:
        """Construct dogmatic false opinion (b=0, d=1, u=0)."""
        return cls(b=0.0, d=1.0, u=0.0, a=0.5)

    @classmethod
    def vacuous(cls, a: float = 0.5) -> SubjectiveOpinion:
        """Construct vacuous prior opinion with zero evidence (u=1)."""
        return cls(b=0.0, d=0.0, u=1.0, a=a)


def fuse_opinions(
    omega_a: SubjectiveOpinion,
    omega_b: SubjectiveOpinion,
    dogmatic_weight: float = 0.5,
) -> SubjectiveOpinion:
    """Fuse two subjective opinions using Jøsang's consensus operator (cumulative fusion).

    Formula:
        numerator_b = b_a * u_b + b_b * u_a
        numerator_d = d_a * u_b + d_b * u_a
        numerator_u = u_a * u_b
        denominator = u_a + u_b - u_a * u_b

    Edge cases:
        When denominator == 0 (dogmatic opinions where u_a == 0 and u_b == 0):
        The opinions are fused using a relative weight average:
            b = dogmatic_weight * b_a + (1 - dogmatic_weight) * b_b
            d = dogmatic_weight * d_a + (1 - dogmatic_weight) * d_b
            u = 0.0
            a = dogmatic_weight * a_a + (1 - dogmatic_weight) * a_b

    Invariants:
        Corroborating non-dogmatic opinions strictly decrease uncertainty:
        u_fused < min(u_a, u_b).
    """
    numerator_b: float = omega_a.b * omega_b.u + omega_b.b * omega_a.u
    numerator_d: float = omega_a.d * omega_b.u + omega_b.d * omega_a.u
    numerator_u: float = omega_a.u * omega_b.u
    denominator: float = omega_a.u + omega_b.u - (omega_a.u * omega_b.u)

    if abs(denominator) < 1e-12:
        # Both opinions have zero uncertainty (dogmatic edge case)
        b_fused = dogmatic_weight * omega_a.b + (1.0 - dogmatic_weight) * omega_b.b
        d_fused = dogmatic_weight * omega_a.d + (1.0 - dogmatic_weight) * omega_b.d
        u_fused = 0.0
        a_fused = dogmatic_weight * omega_a.a + (1.0 - dogmatic_weight) * omega_b.a
    else:
        b_fused = numerator_b / denominator
        d_fused = numerator_d / denominator
        u_fused = numerator_u / denominator

        # Fuse base rate a
        denominator_a: float = omega_a.u + omega_b.u - 2.0 * (omega_a.u * omega_b.u)
        if abs(denominator_a) < 1e-9:
            a_fused = (omega_a.a + omega_b.a) / 2.0
        else:
            numerator_a: float = (
                omega_a.a * omega_b.u
                + omega_b.a * omega_a.u
                - (omega_a.a + omega_b.a) * (omega_a.u * omega_b.u)
            )
            a_fused = numerator_a / denominator_a
            a_fused = max(0.0, min(1.0, a_fused))

    # Normalize components to prevent minute floating-point rounding errors
    total_mass: float = b_fused + d_fused + u_fused
    if total_mass > 0.0:
        b_fused /= total_mass
        d_fused /= total_mass
        u_fused /= total_mass

    return SubjectiveOpinion(b=b_fused, d=d_fused, u=u_fused, a=a_fused)


@dataclass(slots=True, frozen=True)
class BinaryReceipt:
    """Execution receipt capturing proof of tool/runner invocation."""

    exit_code: int
    tool_name: str
    stdout_digest: str

    def as_dict(self) -> dict[str, Any]:
        """Convert binary receipt to dictionary."""
        return {
            "exit_code": self.exit_code,
            "tool_name": self.tool_name,
            "stdout_digest": self.stdout_digest,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BinaryReceipt:
        """Deserialize binary receipt from dictionary."""
        return cls(
            exit_code=int(data["exit_code"]),
            tool_name=str(data["tool_name"]),
            stdout_digest=str(data["stdout_digest"]),
        )

    @classmethod
    def from_execution(cls, exit_code: int, tool_name: str, stdout: str | bytes) -> BinaryReceipt:
        """Create receipt computing SHA-256 digest over stdout payload."""
        payload_bytes = stdout.encode("utf-8") if isinstance(stdout, str) else stdout
        digest = hashlib.sha256(payload_bytes).hexdigest()
        return cls(exit_code=exit_code, tool_name=tool_name, stdout_digest=digest)


@dataclass(slots=True, frozen=True)
class SLSAAttestation:
    """SLSA Level 3 Attestation data structure with subjective provenance grounding."""

    actor_id: str
    runtime_hash: str
    prev_merkle_root: str
    next_merkle_root: str
    timestamp: float
    opinion: SubjectiveOpinion
    binary_receipt: BinaryReceipt

    def compute_digest(self) -> str:
        """Compute deterministic SHA-256 digest over canonical JSON representation."""
        payload: str = json.dumps(self.as_dict(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def as_dict(self) -> dict[str, Any]:
        """Convert attestation to structured dictionary."""
        return {
            "actor_id": self.actor_id,
            "runtime_hash": self.runtime_hash,
            "prev_merkle_root": self.prev_merkle_root,
            "next_merkle_root": self.next_merkle_root,
            "timestamp": self.timestamp,
            "opinion": self.opinion.as_dict(),
            "binary_receipt": self.binary_receipt.as_dict(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SLSAAttestation:
        """Deserialize attestation from dictionary representation."""
        opinion_raw = data.get("opinion", {})
        opinion = (
            opinion_raw
            if isinstance(opinion_raw, SubjectiveOpinion)
            else SubjectiveOpinion.from_dict(opinion_raw)
        )
        receipt_raw = data.get("binary_receipt", {})
        receipt = (
            receipt_raw
            if isinstance(receipt_raw, BinaryReceipt)
            else BinaryReceipt.from_dict(receipt_raw)
        )
        return cls(
            actor_id=str(data["actor_id"]),
            runtime_hash=str(data["runtime_hash"]),
            prev_merkle_root=str(data["prev_merkle_root"]),
            next_merkle_root=str(data["next_merkle_root"]),
            timestamp=float(data["timestamp"]),
            opinion=opinion,
            binary_receipt=receipt,
        )


def append_attestation(ledger_path: Path | str, attestation: SLSAAttestation) -> None:
    """Append a validated SLSA attestation to the specified JSONL ledger.

    Enforces deterministic context management and fsync for durability.
    """
    path = Path(ledger_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    line: str = json.dumps(attestation.as_dict(), sort_keys=True) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def read_attestations(ledger_path: Path | str) -> list[SLSAAttestation]:
    """Read all SLSA attestations from a JSONL ledger."""
    path = Path(ledger_path)
    if not path.exists():
        return []
    attestations: list[SLSAAttestation] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            data = json.loads(line_str)
            attestations.append(SLSAAttestation.from_dict(data))
    return attestations


@dataclass(slots=True, frozen=True)
class QuarantineRecord:
    """Forensic record for cordoned unresolvable diffs or malformed records."""

    quarantine_id: str
    timestamp: float
    reason: str
    actor_id: str
    payload: Any
    opinion: SubjectiveOpinion | None = None
    diagnostics: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        """Convert quarantine record to dictionary."""
        return {
            "quarantine_id": self.quarantine_id,
            "timestamp": self.timestamp,
            "reason": self.reason,
            "actor_id": self.actor_id,
            "payload": self.payload,
            "opinion": self.opinion.as_dict() if self.opinion is not None else None,
            "diagnostics": self.diagnostics or {},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QuarantineRecord:
        """Deserialize quarantine record from dictionary."""
        op_data = data.get("opinion")
        op = SubjectiveOpinion.from_dict(op_data) if op_data is not None else None
        return cls(
            quarantine_id=str(data["quarantine_id"]),
            timestamp=float(data["timestamp"]),
            reason=str(data["reason"]),
            actor_id=str(data.get("actor_id", "unknown")),
            payload=data.get("payload"),
            opinion=op,
            diagnostics=data.get("diagnostics"),
        )


def append_quarantine_record(
    quarantine_path: Path | str,
    record: QuarantineRecord | dict[str, Any],
) -> None:
    """Append an unresolvable diff or malformed record to quarantine_diffs.jsonl.

    Enforces deterministic context management and fsync.
    """
    path = Path(quarantine_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = record.as_dict() if isinstance(record, QuarantineRecord) else record
    line: str = json.dumps(data, sort_keys=True) + "\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(line)
        f.flush()
        os.fsync(f.fileno())


def read_quarantine_records(quarantine_path: Path | str) -> list[dict[str, Any]]:
    """Read all cordoned quarantine records from quarantine_diffs.jsonl."""
    path = Path(quarantine_path)
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if not line_str:
                continue
            records.append(json.loads(line_str))
    return records


def compute_payload_hash(payload: Any) -> str:
    """Compute deterministic SHA-256 hash of any JSON-serializable payload or bytes."""
    if isinstance(payload, bytes):
        raw = payload
    elif isinstance(payload, str):
        raw = payload.encode("utf-8")
    else:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def compute_merkle_root(leaf_hashes: list[str]) -> str:
    """Compute Merkle root from a sequence of leaf hashes (SHA-256)."""
    if not leaf_hashes:
        return hashlib.sha256(b"").hexdigest()
    current = list(leaf_hashes)
    while len(current) > 1:
        next_level: list[str] = []
        for i in range(0, len(current), 2):
            left = current[i]
            right = current[i + 1] if i + 1 < len(current) else current[i]
            combined = hashlib.sha256((left + right).encode("utf-8")).hexdigest()
            next_level.append(combined)
        current = next_level
    return current[0]
