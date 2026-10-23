# RULE 04: EMPIRICAL RED/GREEN TDD & VERIFICATION MANDATE

## 1. Verified Means Reproduced
User bug reports, torn-page errors, and database corruption leads are hypotheses, not conclusions. An issue must be reproduced with an automated crash or concurrency injector before code is designed or committed.

## 2. Red/Green TDD Lifecycle
Every bug fix, lease arbiter mechanism, or projection feature must follow this strict sequence:
1. **Red Test Authoring:** Write an automated test simulating the failure condition (e.g., competing process writing during handover, or process abort during WAL append).
2. **Empirical Red Traceback:** Execute the test and demonstrate the failure traceback (e.g., `PRAGMA integrity_check` returning malformed).
3. **Minimal Implementation:** Implement the minimal VFS or arbiter code to resolve the failure.
4. **Green Verification:** Re-execute the test suite and confirm a clean 0 exit code across the entire codebase.

## 3. Whole-Codebase Verification Mandate
Scoped checks on single files are prohibited. Verification sweeps must evaluate all modules (`core/`, `tests/`, `telemetry/`) to prevent silent cross-module interface regressions.
