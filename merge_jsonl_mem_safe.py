#!/usr/bin/env python3
import argparse
import json
import sqlite3
import os
import sys

def open_sqlite(db_path: str):
    """
    Open (or create) an on‐disk SQLite database to hold:
      table merged (
        merge_key   TEXT   PRIMARY KEY,
        merged_json TEXT   -- JSON‐serialized merged document
      )
    """
    new_db = not os.path.exists(db_path)
    conn = sqlite3.connect(db_path, isolation_level=None)
    cursor = conn.cursor()
    if new_db:
        cursor.execute("""
            CREATE TABLE merged (
                merge_key   TEXT   PRIMARY KEY,
                merged_json TEXT
            )
        """)
    return conn

def load_existing(conn: sqlite3.Connection, key: str):
    """
    Fetch the current merged JSON for `key` from SQLite. Returns:
      - dict parsed from JSON if row exists
      - None if no row exists yet
    """
    c = conn.cursor()
    c.execute("SELECT merged_json FROM merged WHERE merge_key = ?", (key,))
    row = c.fetchone()
    if row is None:
        return None
    return json.loads(row[0])

def write_merged(conn: sqlite3.Connection, key: str, merged_doc: dict):
    """
    Upsert the merged_doc (serialized as JSON) under merge_key=key.
    """
    j = json.dumps(merged_doc, separators=(',', ':'))
    c = conn.cursor()
    c.execute("""
        REPLACE INTO merged (merge_key, merged_json)
        VALUES (?, ?)
    """, (key, j))

def merge_two_dicts_append_unique(existing: dict, new: dict, merge_key: str):
    """
    Merge two JSON objects so that:
      - merge_key is carried once (they must match).
      - All other fields are “appended” into an array of unique values.
    """
    if existing is None:
        result = { merge_key: new[merge_key] }
        for fld, val in new.items():
            if fld == merge_key:
                continue
            if isinstance(val, list):
                seen = set()
                unique = []
                for x in val:
                    if x not in seen:
                        seen.add(x)
                        unique.append(x)
                result[fld] = unique
            else:
                result[fld] = [val]
        return result

    merged = { merge_key: existing[merge_key] }

    for fld in (set(existing.keys()) | set(new.keys())) - {merge_key}:
        v_exist = existing.get(fld, None)
        v_new   = new.get(fld, None)

        if isinstance(v_exist, list):
            list_exist = v_exist
        elif v_exist is None:
            list_exist = []
        else:
            list_exist = [v_exist]

        if isinstance(v_new, list):
            list_new = v_new
        elif v_new is None:
            list_new = []
        else:
            list_new = [v_new]

        combined = list_exist + list_new
        seen = set()
        unique_list = []
        for x in combined:
            if x not in seen:
                seen.add(x)
                unique_list.append(x)

        merged[fld] = unique_list

    return merged

def stream_merge_jsonl_append_unique(input_path: str, output_path: str, merge_key: str):
    """
    Reads input JSONL line by line, merges rows by merge_key
    into an on‐disk SQLite store (appending and deduping every field),
    then exports merged rows to output JSONL.
    """
    tmp_db = output_path + ".db"
    if os.path.exists(tmp_db):
        os.remove(tmp_db)
    conn = open_sqlite(tmp_db)

    with open(input_path, "r", encoding="utf-8") as fin:
        for line_num, raw in enumerate(fin, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON on line {line_num}: {e}", file=sys.stderr)
                continue

            if merge_key not in obj:
                print(f"Warning: no key '{merge_key}' on line {line_num}; skipping.", file=sys.stderr)
                continue

            key_val = obj[merge_key]
            existing = load_existing(conn, key_val)
            merged = merge_two_dicts_append_unique(existing, obj, merge_key)
            write_merged(conn, key_val, merged)

    with open(output_path, "w", encoding="utf-8") as fout:
        c = conn.cursor()
        for row in c.execute("SELECT merged_json FROM merged"):
            fout.write(row[0] + "\n")

    conn.close()
    # Optionally: os.remove(tmp_db)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Stream‐merge a large JSONL by a given key, "
                    "appending every field into a unique list."
    )
    parser.add_argument("--input",  "-i", required=True,
                        help="Path to input JSONL file.")
    parser.add_argument("--output", "-o", required=True,
                        help="Path where merged JSONL will be written.")
    parser.add_argument("--key",    "-k", required=True,
                        help="Name of the top‐level key to merge on (e.g. 'user_id').")
    args = parser.parse_args()

    stream_merge_jsonl_append_unique(args.input, args.output, args.key)