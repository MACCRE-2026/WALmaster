"""WALmaster Tier-3 Protobuf & Relational Registry Synchronization Engine.

Maintains bidirectional, deterministic synchronization across Antigravity's
three-tier persistence architecture:
  Tier 1: <id>.db table trajectory_metadata_blob
  Tier 2: conversation_summaries.db
  Tier 3: agyhub_summaries_proto.pb
"""
from __future__ import annotations

import datetime
import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("walmaster.summary_sync")

DEFAULT_SUMMARIES_DB = Path.home() / ".gemini" / "antigravity" / "conversation_summaries.db"
DEFAULT_AGYHUB_PROTO = Path.home() / ".gemini" / "antigravity" / "agyhub_summaries_proto.pb"
DEFAULT_CONV_DIR = Path.home() / ".gemini" / "antigravity" / "conversations"
DEFAULT_BRAIN_DIR = Path.home() / ".gemini" / "antigravity" / "brain"

# Known canonical workspace URIs to project IDs
WORKSPACE_TO_PROJECT_ID: dict[str, str] = {
    "file:///b:/CRSDL-MACCRE": "5213403d-0b3b-425f-8d93-930e20114c4e",
    "file:///b%3A/CRSDL-MACCRE": "5213403d-0b3b-425f-8d93-930e20114c4e",
    "file:///b:/EXO_GANS": "8a9183fa-2e72-414b-bd2c-c0259245a6fa",
    "file:///b%3A/EXO_GANS": "8a9183fa-2e72-414b-bd2c-c0259245a6fa",
    "file:///b:/OmniTD": "a311d681-cd0e-4ce9-96e8-e960e6c80d2b",
    "file:///b%3A/OmniTD": "a311d681-cd0e-4ce9-96e8-e960e6c80d2b",
    "file:///b:/ProjectAvocado": "130124a4-3973-4ff8-8326-01a5c3b46475",
    "file:///b%3A/ProjectAvocado": "130124a4-3973-4ff8-8326-01a5c3b46475",
    "file:///b:/WALmaster": "3de8a35e-8d05-4f91-bd4c-0a785b017993",
    "file:///b%3A/WALmaster": "3de8a35e-8d05-4f91-bd4c-0a785b017993",
    "file:///b:/GemTrader": "d0cbed28-f5d2-41a3-8b79-d0260e283229",
    "file:///b%3A/GemTrader": "d0cbed28-f5d2-41a3-8b79-d0260e283229",
    "file:///b:/MACCREv2": "27bcfea3-abcb-44b1-b327-4a6180d5141c",
    "file:///b%3A/MACCREv2": "27bcfea3-abcb-44b1-b327-4a6180d5141c",
    "file:///b:/Lychrel": "429bdf01-62b8-4a1c-a484-cab654ae0d78",
    "file:///b%3A/Lychrel": "429bdf01-62b8-4a1c-a484-cab654ae0d78",
    "file:///b:/CPJA-AutoTime": "877d9015-8e44-45b3-a98e-370d286bb5bf",
    "file:///b%3A/CPJA-AutoTime": "877d9015-8e44-45b3-a98e-370d286bb5bf",
    "file:///b:/EHD-ResearchTests": "1906f50a-34b0-4f51-9c44-3e75163233a2",
    "file:///b%3A/EHD-ResearchTests": "1906f50a-34b0-4f51-9c44-3e75163233a2",
    "file:///b:/MACCRE/NewsCast": "4ba90247-714e-4c5e-bdf7-ecb73e6a3261",
    "file:///b%3A/MACCRE/NewsCast": "4ba90247-714e-4c5e-bdf7-ecb73e6a3261",
    "file:///b:/MACCRE/ANTIGRAVITY_DevEnv/Project_Prism": "a70b45ad-3714-4da8-a62b-ed17f832b628",
    "file:///b%3A/MACCRE/ANTIGRAVITY_DevEnv/Project_Prism": "a70b45ad-3714-4da8-a62b-ed17f832b628",
    "file:///b:/MACCRE/ANTIGRAVITY_DevEnv/Project_Prism/PRISM_Nexus": "5464a29e-351e-423a-9e69-94e6fdc40a9c",
    "file:///b%3A/MACCRE/ANTIGRAVITY_DevEnv/Project_Prism/PRISM_Nexus": "5464a29e-351e-423a-9e69-94e6fdc40a9c",
}


def make_proto_varint(val: int) -> bytes:
    """Encode an integer as a standard Protobuf varint."""
    out = bytearray()
    while val > 0x7F:
        out.append((val & 0x7F) | 0x80)
        val >>= 7
    out.append(val & 0x7F)
    return bytes(out)


def make_proto_tag(field_num: int, wire_type: int) -> bytes:
    """Encode a Protobuf field tag (field_number << 3 | wire_type)."""
    return make_proto_varint((field_num << 3) | wire_type)


def make_timestamp_proto(unix_seconds: int, nanos: int = 0) -> bytes:
    """Encode a google.protobuf.Timestamp submessage."""
    p = bytearray()
    p.extend(make_proto_tag(1, 0))
    p.extend(make_proto_varint(unix_seconds))
    if nanos > 0:
        p.extend(make_proto_tag(2, 0))
        p.extend(make_proto_varint(nanos))
    return bytes(p)


def normalize_workspace_uri(ws_uri: str) -> str:
    """Normalize workspace URI so that Windows drive letter colons are encoded as %3A matching project configs."""
    if not ws_uri:
        return "file:///b%3A/CRSDL-MACCRE"
    clean = ws_uri.replace("%3A", ":")
    return re.sub(r"(file:///[a-zA-Z]):", r"\1%3A", clean)


def make_workspace_proto(ws_uri: str) -> bytes:
    """Encode a Field 9 workspace descriptor submessage matching canonical Antigravity schema."""
    uri_b = ws_uri.encode("utf-8")
    sub = bytearray()
    sub.extend(make_proto_tag(1, 2))
    sub.extend(make_proto_varint(len(uri_b)))
    sub.extend(uri_b)
    sub.extend(make_proto_tag(2, 2))
    sub.extend(make_proto_varint(len(uri_b)))
    sub.extend(uri_b)
    sub.extend(make_proto_tag(3, 2))
    sub.extend(make_proto_varint(0))
    sub.extend(make_proto_tag(4, 2))
    sub.extend(make_proto_varint(4))
    sub.extend(b"main")
    return bytes(sub)


def build_full_trajectory_summary(
    session_id: str,
    title: str,
    step_count: int,
    root_trajectory_id: str,
    ws_uri: str,
    project_id: str,
    created_at_sec: int,
    last_modified_sec: int,
    last_user_input_step_index: int = -1,
    metadata_blob: bytes | None = None,
) -> bytes:
    """Construct a complete, canonical CascadeTrajectorySummary Protobuf message matching d7cf."""
    clean_ws = ws_uri.replace("%3A", ":")
    encoded_ws = normalize_workspace_uri(ws_uri)

    ws_msg = make_workspace_proto(clean_ws)
    ts_mod = make_timestamp_proto(last_modified_sec)
    ts_cre = make_timestamp_proto(created_at_sec)

    effective_metadata_blob = metadata_blob
    if not effective_metadata_blob:
        meta_fields: dict[int, list[tuple[str, Any]]] = {
            1: [("bytes", ws_msg)],
            2: [("bytes", ts_cre)],
            3: [("bytes", root_trajectory_id.encode("ascii", errors="ignore"))],
            6: [("bytes", session_id.encode("ascii", errors="ignore"))],
            7: [("bytes", encoded_ws.encode("utf-8"))],
        }
        if project_id:
            meta_fields[18] = [("bytes", project_id.encode("ascii", errors="ignore"))]
        effective_metadata_blob = serialize_proto_fields(meta_fields)
    else:
        meta_f = parse_proto_fields(effective_metadata_blob)
        if 18 not in meta_f and project_id:
            meta_f[18] = [("bytes", project_id.encode("ascii", errors="ignore"))]
        if 7 not in meta_f:
            meta_f[7] = [("bytes", encoded_ws.encode("utf-8"))]
        if 1 not in meta_f:
            meta_f[1] = [("bytes", ws_msg)]
        effective_metadata_blob = serialize_proto_fields(meta_f)

    disp_fields: dict[int, list[tuple[str, Any]]] = {
        1: [("bytes", title.encode("utf-8"))],
        7: [("bytes", ts_mod)],
    }
    disp_bytes = serialize_proto_fields(disp_fields)

    input_step_idx = (
        last_user_input_step_index
        if last_user_input_step_index >= 0
        else max(0, step_count - 1)
    )

    top_fields: dict[int, list[tuple[str, Any]]] = {
        1: [("bytes", title.encode("utf-8"))],
        2: [("varint", step_count)],
        3: [("bytes", ts_mod)],
        4: [("bytes", root_trajectory_id.encode("utf-8"))],
        5: [("varint", 1)],
        7: [("bytes", ts_cre)],
        9: [("bytes", ws_msg)],
        10: [("bytes", ts_mod)],
        15: [("bytes", disp_bytes)],
        16: [("varint", input_step_idx)],
        17: [("bytes", effective_metadata_blob)],
        22: [("varint", 4)],
        28: [("varint", 4)],
    }
    return serialize_proto_fields(top_fields)


def parse_proto_fields(data: bytes) -> dict[int, list[tuple[str, Any]]]:
    """Parse raw bytes into Protobuf field mappings."""
    idx = 0
    fields: dict[int, list[tuple[str, Any]]] = {}
    while idx < len(data):
        tag_val = 0
        shift = 0
        while idx < len(data):
            b = data[idx]
            idx += 1
            tag_val |= (b & 0x7F) << shift
            if not (b & 0x80):
                break
            shift += 7

        wire_type = tag_val & 0x07
        field_num = tag_val >> 3

        if wire_type == 0:  # varint
            val = 0
            shift = 0
            while idx < len(data):
                b = data[idx]
                idx += 1
                val |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            fields.setdefault(field_num, []).append(("varint", val))
        elif wire_type == 2:  # length-delimited
            length = 0
            shift = 0
            while idx < len(data):
                b = data[idx]
                idx += 1
                length |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            val_bytes = data[idx : idx + length]
            idx += length
            fields.setdefault(field_num, []).append(("bytes", val_bytes))
        elif wire_type == 1:  # fixed64
            val_64 = data[idx : idx + 8]
            idx += 8
            fields.setdefault(field_num, []).append(("fixed64", val_64))
        elif wire_type == 5:  # fixed32
            val_32 = data[idx : idx + 4]
            idx += 4
            fields.setdefault(field_num, []).append(("fixed32", val_32))
        else:
            break
    return fields


def serialize_proto_fields(fields_dict: dict[int, list[tuple[str, Any]]]) -> bytes:
    """Serialize field mappings into deterministic Protobuf bytes."""
    out = bytearray()
    for field_num in sorted(fields_dict.keys()):
        for wire_type_name, val in fields_dict[field_num]:
            if wire_type_name == "varint":
                out.extend(make_proto_tag(field_num, 0))
                out.extend(make_proto_varint(int(val)))
            elif wire_type_name == "bytes":
                out.extend(make_proto_tag(field_num, 2))
                out.extend(make_proto_varint(len(val)))
                out.extend(val)
            elif wire_type_name == "fixed64":
                out.extend(make_proto_tag(field_num, 1))
                out.extend(val)
            elif wire_type_name == "fixed32":
                out.extend(make_proto_tag(field_num, 5))
                out.extend(val)
    return bytes(out)


def patch_raw_summary(
    raw: bytes,
    ws_uri: str,
    unix_seconds: int,
    metadata_blob: bytes | None = None,
    *,
    session_id: str | None = None,
    title: str | None = None,
    step_count: int | None = None,
    root_trajectory_id: str | None = None,
    project_id: str | None = None,
    created_at_sec: int | None = None,
    last_user_input_step_index: int = -1,
) -> bytes:
    """Patch or synthesize a conversation's raw_summary Protobuf payload with valid workspace, timestamps, and metadata."""
    fields = parse_proto_fields(raw) if raw else {}

    # If raw is empty or missing essential fields (title, trajectory_id), synthesize the full message
    if not fields or 1 not in fields or 4 not in fields:
        s_id = session_id or (
            fields.get(4, [("", b"")])[0][1].decode("latin1", errors="ignore")
            if 4 in fields
            else "restored-conversation"
        )
        t_title = title or (
            fields.get(1, [("", b"")])[0][1].decode("utf-8", errors="ignore")
            if 1 in fields
            else "Restored Conversation"
        )
        s_count = step_count or (fields.get(2, [("", 0)])[0][1] if 2 in fields else 1)
        r_traj = root_trajectory_id or s_id
        p_id = project_id or WORKSPACE_TO_PROJECT_ID.get(ws_uri, "")
        c_sec = created_at_sec or unix_seconds

        return build_full_trajectory_summary(
            session_id=s_id,
            title=t_title,
            step_count=s_count,
            root_trajectory_id=r_traj,
            ws_uri=ws_uri,
            project_id=p_id,
            created_at_sec=c_sec,
            last_modified_sec=unix_seconds,
            last_user_input_step_index=last_user_input_step_index,
            metadata_blob=metadata_blob,
        )

    # Replace negative year 0001 (-62B varint) timestamps with valid current/transcript timestamps
    bad_marker = b"\x80\x92\xb8\xc3\x98"
    ts_bytes = make_timestamp_proto(unix_seconds)

    if 3 not in fields or any(bad_marker in val for _, val in fields.get(3, [])):
        fields[3] = [("bytes", ts_bytes)]

    if 10 not in fields or any(bad_marker in val for _, val in fields.get(10, [])):
        fields[10] = [("bytes", ts_bytes)]

    clean_ws = ws_uri.replace("%3A", ":")
    fields[9] = [("bytes", make_workspace_proto(clean_ws))]

    # Ensure Field 17 has the metadata blob containing the project ID binding
    if metadata_blob and len(metadata_blob) > 0:
        fields[17] = [("bytes", metadata_blob)]
    elif project_id and 17 in fields:
        meta_sub = parse_proto_fields(fields[17][0][1])
        if 18 not in meta_sub:
            meta_sub[18] = [("bytes", project_id.encode("ascii", errors="ignore"))]
            fields[17] = [("bytes", serialize_proto_fields(meta_sub))]

    return serialize_proto_fields(fields)


def deserialize_agyhub_proto_state(data: bytes) -> tuple[list[tuple[str, bytes]], dict[str, int]]:
    """Parse agyhub_summaries_proto.pb into (summaries, file_mod_times)."""
    top = parse_proto_fields(data)
    items = top.get(1, [])
    parsed_summaries: list[tuple[str, bytes]] = []
    for _, item_bytes in items:
        f = parse_proto_fields(item_bytes)
        cid = f.get(1, [("", b"")])[0][1].decode("utf-8", errors="ignore")
        raw = f.get(2, [("", b"")])[0][1]
        if cid:
            parsed_summaries.append((cid, raw))

    parsed_mod_times: dict[str, int] = {}
    time_items = top.get(2, [])
    for _, item_bytes in time_items:
        f = parse_proto_fields(item_bytes)
        cid = f.get(1, [("", b"")])[0][1].decode("utf-8", errors="ignore")
        ts_blob = f.get(2, [("", b"")])[0][1]
        if cid and ts_blob:
            ts_f = parse_proto_fields(ts_blob)
            if 1 in ts_f and ts_f[1][0][0] == "varint":
                parsed_mod_times[cid] = ts_f[1][0][1]

    return parsed_summaries, parsed_mod_times


def deserialize_agyhub_proto(data: bytes) -> list[tuple[str, bytes]]:
    """Parse agyhub_summaries_proto.pb into a list of (conversation_id, raw_summary) pairs."""
    summaries, _ = deserialize_agyhub_proto_state(data)
    return summaries


def serialize_agyhub_proto(
    entries: list[tuple[str, bytes]],
    mod_times: dict[str, int] | None = None,
) -> bytes:
    """Serialize a list of (conversation_id, raw_summary) pairs and mod_times into agyhub_summaries_proto.pb format."""
    out = bytearray()
    for cid, raw in entries:
        item = bytearray()
        cid_b = cid.encode("utf-8")
        item.extend(make_proto_tag(1, 2))
        item.extend(make_proto_varint(len(cid_b)))
        item.extend(cid_b)
        item.extend(make_proto_tag(2, 2))
        item.extend(make_proto_varint(len(raw)))
        item.extend(raw)

        out.extend(make_proto_tag(1, 2))
        out.extend(make_proto_varint(len(item)))
        out.extend(item)

    if mod_times:
        for cid, ts in mod_times.items():
            item = bytearray()
            cid_b = cid.encode("utf-8")
            item.extend(make_proto_tag(1, 2))
            item.extend(make_proto_varint(len(cid_b)))
            item.extend(cid_b)
            ts_blob = make_timestamp_proto(ts)
            item.extend(make_proto_tag(2, 2))
            item.extend(make_proto_varint(len(ts_blob)))
            item.extend(ts_blob)

            out.extend(make_proto_tag(2, 2))
            out.extend(make_proto_varint(len(item)))
            out.extend(item)

    return bytes(out)


def resolve_project_name(ws_uri: str, proj_id: str) -> str:
    """Resolve human-readable project folder name for conversations_fts."""
    if ws_uri:
        name = ws_uri.rstrip("/").split("/")[-1]
        if name:
            return name
    p_json = Path.home() / ".gemini" / "config" / "projects" / f"{proj_id}.json"
    if p_json.exists():
        try:
            with open(p_json, "r", encoding="utf-8") as f:
                d = json.load(f)
                cand = d.get("name")
                if cand:
                    return str(cand)
        except (json.JSONDecodeError, OSError):
            pass
    return "WALmaster"


def resolve_conversation_workspace(
    cid: str,
    *,
    conv_dir: Path | None = None,
    brain_dir: Path | None = None,
    summaries_db: Path | None = None,
) -> tuple[str, str, int]:
    """Resolve the canonical workspace URI, project ID, and last timestamp for a conversation.

    Returns:
        tuple of (workspace_uri, project_id, unix_timestamp_seconds)
    """
    c_dir = conv_dir or DEFAULT_CONV_DIR
    b_dir = brain_dir or DEFAULT_BRAIN_DIR

    ws_uri = "file:///b%3A/CRSDL-MACCRE"
    proj_id = "5213403d-0b3b-425f-8d93-930e20114c4e"
    ts_seconds = int(time.time())

    # 1. Probe .bak or .db trajectory_metadata_blob
    for suffix in (".db.corrupt.bak", ".db"):
        candidate_db = c_dir / f"{cid}{suffix}"
        if candidate_db.exists():
            try:
                with sqlite3.connect(f"file:{candidate_db.as_posix()}?mode=ro", uri=True) as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT data FROM trajectory_metadata_blob WHERE id='main'")
                    r = cur.fetchone()
                    if r and r[0] and len(r[0]) > 0:
                        raw_blob = r[0]
                        f = parse_proto_fields(raw_blob)
                        meta_dict = parse_proto_fields(f[17][0][1]) if (17 in f and 18 not in f) else f
                        if 18 in meta_dict and meta_dict[18] and meta_dict[18][0][1]:
                            cand_pid = meta_dict[18][0][1].decode("ascii", errors="ignore")
                            if cand_pid:
                                proj_id = cand_pid
                        if 7 in meta_dict and meta_dict[7] and meta_dict[7][0][1]:
                            cand_ws = meta_dict[7][0][1].decode("utf-8", errors="ignore")
                            if cand_ws:
                                ws_uri = cand_ws
                                if cand_ws in WORKSPACE_TO_PROJECT_ID and not proj_id:
                                    proj_id = WORKSPACE_TO_PROJECT_ID[cand_ws]
                                break

                        # Fallback to string search if proto fields didn't match
                        m = re.findall(rb"file:///[a-zA-Z0-9_/.:%-]+", raw_blob)
                        if m:
                            matched = m[0].decode("latin1").removesuffix("z")
                            clean_matched = matched.replace("%3A", ":")
                            canonical_ws = clean_matched.replace(":", "%3A", 1)
                            if clean_matched in WORKSPACE_TO_PROJECT_ID:
                                ws_uri = canonical_ws
                                proj_id = WORKSPACE_TO_PROJECT_ID[clean_matched]
                                break
            except (sqlite3.Error, OSError):
                pass

    # 2. Probe transcript for timestamp and workspace hints
    transcript_file = b_dir / cid / ".system_generated" / "logs" / "transcript.jsonl"
    if transcript_file.exists():
        try:
            with open(transcript_file, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                if lines:
                    last_obj = json.loads(lines[-1].strip())
                    created_at_str = last_obj.get("created_at")
                    if created_at_str:
                        clean_iso = created_at_str.replace("Z", "+00:00")
                        dt = datetime.datetime.fromisoformat(clean_iso)
                        ts_seconds = int(dt.timestamp())
        except (json.JSONDecodeError, OSError, ValueError):
            pass

    # Ensure canonical %3A URL encoding for project matching
    ws_uri = normalize_workspace_uri(ws_uri)

    return ws_uri, proj_id, ts_seconds



def sync_conversation_metadata(
    session_id: str,
    *,
    summaries_db: Path | None = None,
    agyhub_proto: Path | None = None,
    conv_dir: Path | None = None,
    brain_dir: Path | None = None,
) -> dict[str, Any]:
    """Synchronize a single conversation across all 3 persistence tiers."""
    s_db = summaries_db or DEFAULT_SUMMARIES_DB
    proto_p = agyhub_proto or DEFAULT_AGYHUB_PROTO
    c_dir = conv_dir or DEFAULT_CONV_DIR
    b_dir = brain_dir or DEFAULT_BRAIN_DIR

    ws_uri, proj_id, ts_sec = resolve_conversation_workspace(
        session_id, conv_dir=c_dir, brain_dir=b_dir, summaries_db=s_db
    )

    # 1. Ensure Tier 1: trajectory_metadata_blob in <cid>.db is populated
    tier1_updated = False
    meta_blob: bytes | None = None
    root_traj_id = session_id
    live_db = c_dir / f"{session_id}.db"
    bak_db = c_dir / f"{session_id}.db.corrupt.bak"

    if live_db.exists():
        try:
            with sqlite3.connect(live_db, timeout=5.0) as conn:
                cur = conn.cursor()
                # Read root trajectory id from trajectory_meta
                try:
                    cur.execute(
                        "SELECT trajectory_id FROM trajectory_meta WHERE cascade_id = ? AND trajectory_type = 4 LIMIT 1",
                        (session_id,),
                    )
                    r_meta = cur.fetchone()
                    if r_meta and r_meta[0]:
                        root_traj_id = r_meta[0]
                except sqlite3.Error:
                    pass

                cur.execute("SELECT data FROM trajectory_metadata_blob WHERE id='main'")
                r = cur.fetchone()
                if r and r[0] and len(r[0]) > 0:
                    meta_blob = r[0]

                if (not meta_blob or len(meta_blob) == 0) and bak_db.exists():
                    with sqlite3.connect(f"file:{bak_db.as_posix()}?mode=ro", uri=True) as b_conn:
                        b_cur = b_conn.cursor()
                        b_cur.execute("SELECT data FROM trajectory_metadata_blob WHERE id='main'")
                        b_r = b_cur.fetchone()
                        if b_r and b_r[0] and len(b_r[0]) > 0:
                            meta_blob = b_r[0]
                            cur.execute(
                                "INSERT OR REPLACE INTO trajectory_metadata_blob (id, data) VALUES ('main', ?)",
                                (meta_blob,),
                            )
                            conn.commit()
                            tier1_updated = True
        except (sqlite3.Error, OSError) as e:
            logger.debug("Tier 1 blob sync notice for %s: %s", session_id, e)

    # 2. Update Tier 2: conversation_summaries.db (columns and raw_summary)
    tier2_updated = False
    patched_raw = b""
    if s_db.exists():
        try:
            with sqlite3.connect(s_db, timeout=10.0) as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT title, step_count, raw_summary FROM conversation_summaries WHERE conversation_id = ?",
                    (session_id,),
                )
                row = cur.fetchone()
                if row:
                    _title, _step_count, orig_raw = row
                    patched_raw = patch_raw_summary(
                        orig_raw or b"",
                        ws_uri,
                        ts_sec,
                        metadata_blob=meta_blob,
                        session_id=session_id,
                        title=_title or "Restored Conversation",
                        step_count=_step_count or 1,
                        root_trajectory_id=root_traj_id,
                        project_id=proj_id,
                        created_at_sec=ts_sec,
                    )

                    dt_obj = datetime.datetime.fromtimestamp(ts_sec, tz=datetime.timezone.utc)
                    clean_dt_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S") + f".{dt_obj.microsecond:07d}+00:00"

                    cur.execute(
                        """
                        UPDATE conversation_summaries
                        SET project_id = ?,
                            workspace_uris = ?,
                            last_modified_time = ?,
                            raw_summary = ?
                        WHERE conversation_id = ?
                        """,
                        (proj_id, json.dumps([ws_uri]), clean_dt_str, patched_raw, session_id),
                    )

                    # Update conversations_fts if table exists
                    try:
                        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversations_fts'")
                        if cur.fetchone():
                            proj_name = resolve_project_name(ws_uri, proj_id)
                            cur.execute(
                                """
                                UPDATE conversations_fts
                                SET workspace = ?,
                                    project = ?
                                WHERE chunk_index = -1 AND conversation_id = ?
                                """,
                                (proj_name, proj_name, session_id),
                            )
                    except sqlite3.Error as e:
                        logger.debug("conversations_fts notice for %s: %s", session_id, e)

                    conn.commit()
                    tier2_updated = True
        except (sqlite3.Error, OSError) as e:
            logger.debug("Tier 2 summaries DB notice for %s: %s", session_id, e)

    # 3. Update Tier 3: agyhub_summaries_proto.pb
    tier3_updated = False
    if proto_p.exists() and patched_raw:
        try:
            with open(proto_p, "rb") as f:
                proto_data = f.read()

            entries, mod_times = deserialize_agyhub_proto_state(proto_data)
            new_entries: list[tuple[str, bytes]] = []
            found = False
            for entry_cid, entry_raw in entries:
                if entry_cid == session_id:
                    new_entries.append((entry_cid, patched_raw))
                    found = True
                else:
                    new_entries.append((entry_cid, entry_raw))

            if not found:
                new_entries.append((session_id, patched_raw))

            mod_times[session_id] = ts_sec
            new_proto_data = serialize_agyhub_proto(new_entries, mod_times=mod_times)
            tmp_proto_p = proto_p.with_suffix(".tmp")
            with open(tmp_proto_p, "wb") as f:
                f.write(new_proto_data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_proto_p, proto_p)
            tier3_updated = True
        except OSError as e:
            logger.debug("Tier 3 proto sync notice for %s: %s", session_id, e)

    return {
        "session_id": session_id,
        "workspace_uri": ws_uri,
        "project_id": proj_id,
        "timestamp_seconds": ts_sec,
        "tier1_updated": tier1_updated,
        "tier2_updated": tier2_updated,
        "tier3_updated": tier3_updated,
    }


def sync_all_summaries(
    *,
    summaries_db: Path | None = None,
    agyhub_proto: Path | None = None,
    conv_dir: Path | None = None,
    brain_dir: Path | None = None,
    target_cids: list[str] | None = None,
) -> dict[str, Any]:
    """Sweep all conversations in conversation_summaries.db and align all 3 tiers."""
    s_db = summaries_db or DEFAULT_SUMMARIES_DB
    proto_p = agyhub_proto or DEFAULT_AGYHUB_PROTO
    c_dir = conv_dir or DEFAULT_CONV_DIR
    b_dir = brain_dir or DEFAULT_BRAIN_DIR

    results: list[dict[str, Any]] = []

    if not s_db.exists():
        return {"total_checked": 0, "synchronized": 0, "results": []}

    cids_to_sync: list[str] = []
    if target_cids is not None:
        cids_to_sync = list(target_cids)
    else:
        with sqlite3.connect(s_db, timeout=10.0) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT conversation_id FROM conversation_summaries
                WHERE project_id = ''
                   OR project_id IS NULL
                   OR last_modified_time LIKE '0001-01-01%'
                   OR length(raw_summary) < 300
                UNION
                SELECT conversation_id FROM conversations_fts
                WHERE chunk_index = -1
                  AND (project = '' OR project IS NULL)
                """
            )
            cids_to_sync = [r[0] for r in cur.fetchall()]

    for cid in cids_to_sync:
        res = sync_conversation_metadata(
            cid,
            summaries_db=s_db,
            agyhub_proto=proto_p,
            conv_dir=c_dir,
            brain_dir=b_dir,
        )
        results.append(res)

    return {
        "total_checked": len(cids_to_sync),
        "synchronized": len(results),
        "results": results,
    }
