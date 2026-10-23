# RULE 01: ENTERPRISE EPISTEMIC PROVENANCE & AI SAFE² ALIGNMENT
**Standard:** AI SAFE² v3.0 Deterministic Alignment & Multi-Tier Sovereign Execution  
**Compliance Mappings:** NIST AI RMF 1.0 (SP 1270) | ISO/IEC 42001 | SLSA v1.0 Level 3  

---

## 1. Dynamic Epistemic Provenance (Subjective Logic Consensus)

The archaic Biba Low-Water-Mark model ($W_{\text{final}} = \min_i(W_{\text{source}_i})$) is superseded. In modern multi-agent collaborative systems, rigid scalar minimization causes unrecoverable trust decay when reconciling disparate sources.

WALmaster mandates **Subjective Epistemic Fusion**:
1. Every state assertion, frame commit, and projection carries an epistemic opinion tuple:
   $$\omega = (b, d, u, a)$$
   where $b$ is belief, $d$ is disbelief, $u$ is uncertainty, and $a$ is the base-rate prior ($b + d + u = 1$).
2. **Corroborative Evidence Fusion:** When independent agents or verification runs corroborate a state, their opinions fuse via Subjective Consensus:
   $$\omega_{A \oplus B} = \omega_A \oplus \omega_B$$
   Independent verification **reduces uncertainty ($u \downarrow$)**, elevating confidence rather than decaying it.
3. **Provenance Attestation (SLSA Level 3):** Epistemic ratings must never exist as unverified assertions. Every state transition must link to a deterministic proof bundle:
   * Source Identity (Actor / Subagent Conversation ID / Runtime Hash).
   * Input Merkle Root $\text{Hash}(S_{\text{prev}})$.
   * Output Merkle Root $\text{Hash}(S_{\text{next}})$.
   * Binary OS Execution Receipt.

---

## 2. Mechanical Work Proofs (Counted Work Invariant)

1. **Zero Trust in Self-Reported Prose:** An LLM agent stating in conversational prose that *"all tests pass"* or *"42 items were verified"* has zero epistemic standing ($u = 1.0$).
2. **Binary Exit Receipt Mandate:** A status of `CLEAN`, `VERIFIED`, or `SUCCESS` is strictly conditional on an attached machine-verifiable binary receipt:
   * Exit Code $0$ from an external interpreter (`pytest`, `ruff`, `mypy`).
   * Physical line-count or diff-manifest hash from the OS filesystem.
   * Direct output from `PRAGMA integrity_check` returning literal string `"ok"`.
3. **Ambiguity Handling:** Any unverified or partially scanned state MUST be reported as `UNVERIFIED`, `INCOMPLETE`, or `CORDONED`, never `CLEAN`.

---

## 3. Atomic Multi-File Storage & Zero-Torn Handover

1. **Storage Set Invariant:** In SQLite Write-Ahead Logging (WAL) mode, the database file (`.db`), write-ahead log (`.db-wal`), and shared memory index (`.db-shm`) constitute a single atomic unit. Checkpoint barriers (`PRAGMA wal_checkpoint(TRUNCATE)`) must drain all pending frames to zero before file handles or leases are transferred.
2. **Fail-Closed Gatekeeper Rule:** If a safety hook, verification script, or lease arbiter encounters an unexpected exception, missing argument, or process crash, the system MUST **fail closed** (rejecting execution/handover) rather than failing open.
