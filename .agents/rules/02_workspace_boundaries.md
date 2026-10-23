# RULE 02: WORKSPACE BOUNDARIES & HOST STORAGE PROTECTION

## 1. The Sovereign Workspace Rule
Agents must act strictly within the designated repository root: `B:\WALmaster`. Never mutate, write to, or touch files in external or sibling repositories without explicit operator authorization.

## 2. Drive B Sovereignty & Storage Blacklist on C:\
* Host systems possess limited space on `C:\`. All virtual environments (`.venv`), temporary caches, build artifacts, crash dumps, and test sandboxes MUST be directed strictly to `B:\WALmaster\.development`.
* Writing scratch files or temporary test databases to `C:\` is strictly forbidden.
* Only root-level user-facing markdown planning artifacts are permitted in the conversation directory on `C:\`. All canonical project artifacts must reside in `B:\WALmaster\.development\artifacts\planning/`.
