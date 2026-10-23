# RULE 05: DRIVE B SOVEREIGNTY & ABSOLUTE PYTHON 3.11+ TYPING

## 1. Resource Hygiene & Absolute Typing
* Every function signature, return type, and class attribute in `WALmaster` MUST have explicit, strict Python 3.11+ type hints (`from __future__ import annotations`).
* Every SQLite connection, Win32 handle, network socket, and subprocess must be managed via explicit context managers (`with`) or `try...finally` teardown blocks.

## 2. Ephemeral Storage Directives
All ephemeral execution artifacts must be routed exclusively into `B:\WALmaster\.development`:
* Cache: `B:\WALmaster\.development\cache`
* Scratch: `B:\WALmaster\.development\artifacts\scratch`
* Tests: `B:\WALmaster\.development\artifacts\tests`
