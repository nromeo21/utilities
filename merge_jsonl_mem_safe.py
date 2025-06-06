#!/usr/bin/env python3
import argparse
import json
import sqlite3
import os
import sys
import hashlib
import tempfile
from typing import Any, Dict, List, Optional, Set, Union, TextIO
import gc
import urllib.parse

try:
from smart_open import open as smart_open
SMART_OPEN_AVAILABLE = True
except ImportError:
SMART_OPEN_AVAILABLE = False
smart_open = None

def serialize_value(value: Any) -> str:
“”“Serialize any JSON value to a consistent string representation for hashing.”””
return json.dumps(value, sort_keys=True, separators=(’,’, ‘:’))

def deserialize_value(value_str: str) -> Any:
“”“Deserialize a string back to its original JSON value.”””
return json.loads(value_str)

def hash_value(value: Any) -> str:
“”“Create a hash of a value for deduplication.”””
serialized = serialize_value(value)
return hashlib.sha256(serialized.encode(‘utf-8’)).hexdigest()

def open_sqlite(db_path: str) -> sqlite3.Connection:
“””
Open SQLite database optimized for large-scale operations.
Creates tables for merged documents and field values.
“””
new_db = not os.path.exists(db_path)
conn = sqlite3.connect(db_path, isolation_level=None)

# Optimize SQLite for bulk operations
conn.execute("PRAGMA journal_mode = WAL")
conn.execute("PRAGMA synchronous = NORMAL")
conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
conn.execute("PRAGMA temp_store = MEMORY")
conn.execute("PRAGMA mmap_size = 268435456")  # 256MB mmap

cursor = conn.cursor()

if new_db:
    # Main table for merge keys
    cursor.execute("""
        CREATE TABLE merge_keys (
            merge_key TEXT PRIMARY KEY,
            key_value TEXT NOT NULL
        )
    """)
    
    # Table for field values with deduplication
    cursor.execute("""
        CREATE TABLE field_values (
            merge_key TEXT,
            field_name TEXT,
            value_hash TEXT,
            value_data TEXT,
            PRIMARY KEY (merge_key, field_name, value_hash),
            FOREIGN KEY (merge_key) REFERENCES merge_keys(merge_key)
        )
    """)
    
    # Indexes for performance
    cursor.execute("CREATE INDEX idx_field_values_merge_key ON field_values(merge_key)")
    cursor.execute("CREATE INDEX idx_field_values_field ON field_values(field_name)")

return conn

def insert_field_values_batch(conn: sqlite3.Connection, batch_data: List[tuple]) -> None:
“”“Insert field values in batches for better performance.”””
if not batch_data:
return

cursor = conn.cursor()
cursor.executemany("""
    INSERT OR IGNORE INTO field_values (merge_key, field_name, value_hash, value_data)
    VALUES (?, ?, ?, ?)
""", batch_data)

def process_document(conn: sqlite3.Connection, doc: Dict[str, Any], merge_key_field: str,
batch_data: List[tuple], batch_size: int = 1000) -> List[tuple]:
“””
Process a single document, adding its field values to the batch.
Returns updated batch_data.
“””
if merge_key_field not in doc:
return batch_data

merge_key_value = doc[merge_key_field]
merge_key_str = serialize_value(merge_key_value)

# Insert or update merge key
cursor = conn.cursor()
cursor.execute("""
    INSERT OR REPLACE INTO merge_keys (merge_key, key_value)
    VALUES (?, ?)
""", (merge_key_str, serialize_value(merge_key_value)))

# Process each field
for field_name, field_value in doc.items():
    if field_name == merge_key_field:
        continue
        
    # Handle list fields
    if isinstance(field_value, list):
        for item in field_value:
            item_hash = hash_value(item)
            item_data = serialize_value(item)
            batch_data.append((merge_key_str, field_name, item_hash, item_data))
    else:
        # Handle single values
        value_hash = hash_value(field_value)
        value_data = serialize_value(field_value)
        batch_data.append((merge_key_str, field_name, value_hash, value_data))

# Flush batch if it's getting large
if len(batch_data) >= batch_size:
    insert_field_values_batch(conn, batch_data)
    batch_data.clear()
    gc.collect()  # Force garbage collection

return batch_data

def export_merged_data(conn: sqlite3.Connection, output_file: TextIO, merge_key_field: str) -> None:
“”“Export merged data from SQLite to JSONL format.”””
cursor = conn.cursor()


# Get all unique merge keys
cursor.execute("SELECT merge_key, key_value FROM merge_keys ORDER BY merge_key")

for merge_key_str, key_value_str in cursor.fetchall():
    # Reconstruct the merged document
    merged_doc = {merge_key_field: deserialize_value(key_value_str)}
    
    # Get all field values for this merge key
    field_cursor = conn.cursor()
    field_cursor.execute("""
        SELECT field_name, value_data 
        FROM field_values 
        WHERE merge_key = ? 
        ORDER BY field_name, value_data
    """, (merge_key_str,))
    
    current_field = None
    field_values = []
    
    for field_name, value_data in field_cursor.fetchall():
        if current_field != field_name:
            # Save previous field if exists
            if current_field is not None:
                if len(field_values) == 1:
                    merged_doc[current_field] = field_values[0]
                else:
                    merged_doc[current_field] = field_values
            
            # Start new field
            current_field = field_name
            field_values = []
        
        field_values.append(deserialize_value(value_data))
    
    # Handle last field
    if current_field is not None:
        if len(field_values) == 1:
            merged_doc[current_field] = field_values[0]
        else:
            merged_doc[current_field] = field_values
    
    # Write to output
    json.dump(merged_doc, output_file, separators=(',', ':'))
    output_file.write('\n')


def is_s3_uri(path: str) -> bool:
“”“Check if path is an S3 URI.”””
return path.startswith(‘s3://’)

def is_stdin_stdout(path: str) -> bool:
“”“Check if path represents stdin/stdout.”””
return path == “-”

def validate_s3_support():
“”“Validate that smart_open is available for S3 operations.”””
if not SMART_OPEN_AVAILABLE:
print(“Error: smart_open library is required for S3 support.”, file=sys.stderr)
print(“Install it with: pip install smart_open[s3]”, file=sys.stderr)
sys.exit(1)

def get_s3_transport_params(kms_key_id: Optional[str] = None) -> Dict[str, Any]:
“”“Get S3 transport parameters including KMS encryption.”””
params = {
‘multipart_upload’: True,
‘multipart_upload_kwargs’: {}
}


if kms_key_id:
    params['multipart_upload_kwargs'].update({
        'ServerSideEncryption': 'aws:kms',
        'SSEKMSKeyId': kms_key_id
    })

return params


def get_input_stream(input_path: str) -> TextIO:
“”“Get input stream - stdin, file, or S3.”””
if is_stdin_stdout(input_path):
return sys.stdin
elif is_s3_uri(input_path):
validate_s3_support()
print(f”Opening S3 input: {input_path}”, file=sys.stderr)
return smart_open(input_path, ‘r’, encoding=‘utf-8’)
else:
return open(input_path, “r”, encoding=“utf-8”)

def get_output_stream(output_path: str, kms_key_id: Optional[str] = None) -> TextIO:
“”“Get output stream - stdout, file, or S3 with optional KMS encryption.”””
if is_stdin_stdout(output_path):
return sys.stdout
elif is_s3_uri(output_path):
validate_s3_support()
transport_params = get_s3_transport_params(kms_key_id)
print(f”Opening S3 output: {output_path}”, file=sys.stderr)
if kms_key_id:
print(f”Using KMS key: {kms_key_id}”, file=sys.stderr)
return smart_open(output_path, ‘w’, encoding=‘utf-8’,
transport_params=transport_params)
else:
return open(output_path, “w”, encoding=“utf-8”)

def get_temp_db_path(output_path: str) -> str:
“”“Get temporary database path.”””
if is_stdin_stdout(output_path) or is_s3_uri(output_path):
# Use system temp directory for stdout or S3
return os.path.join(tempfile.gettempdir(), f”jsonl_merge_{os.getpid()}.db”)
return output_path + “.tmp.db”

def get_file_size_mb(file_path: str) -> float:
“”“Get file size in MB for progress reporting. Returns 0 for stdin/S3.”””
if is_stdin_stdout(file_path) or is_s3_uri(file_path):
return 0.0
return os.path.getsize(file_path) / (1024 * 1024)

def get_progress_message(input_path: str, output_path: str) -> str:
“”“Get appropriate progress message based on input/output types.”””
if is_stdin_stdout(input_path):
return “Processing from stdin…”
elif is_s3_uri(input_path):
return f”Processing from S3: {input_path}…”
else:
file_size = get_file_size_mb(input_path)
return f”Processing {input_path} ({file_size:.2f} MB)…”

def get_completion_message(output_path: str) -> str:
“”“Get appropriate completion message based on output type.”””
if is_stdin_stdout(output_path):
return “Merge complete. Output written to stdout”
elif is_s3_uri(output_path):
return f”Merge complete. Output written to S3: {output_path}”
else:
return f”Merge complete. Output written to {output_path}”

def stream_merge_jsonl_optimized(input_path: str, output_path: str, merge_key_field: str,
batch_size: int = 1000, progress_interval: int = 10000,
kms_key_id: Optional[str] = None) -> None:
“””
Optimized stream merger for very large JSONL files.
Supports stdin/stdout via ‘-’ parameter and S3 URIs with KMS encryption.
“””
tmp_db = get_temp_db_path(output_path)


# Clean up any existing temp DB
if os.path.exists(tmp_db):
    os.remove(tmp_db)

input_stream = None
output_stream = None

try:
    conn = open_sqlite(tmp_db)
    batch_data = []
    processed_lines = 0
    
    # Show progress message
    print(get_progress_message(input_path, output_path), file=sys.stderr)
    
    # Open input stream
    input_stream = get_input_stream(input_path)
    
    for line_num, raw_line in enumerate(input_stream, start=1):
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        
        try:
            doc = json.loads(raw_line)
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON on line {line_num}: {e}", file=sys.stderr)
            continue
        
        if merge_key_field not in doc:
            print(f"Warning: no key '{merge_key_field}' on line {line_num}; skipping.", 
                  file=sys.stderr)
            continue
        
        batch_data = process_document(conn, doc, merge_key_field, batch_data, batch_size)
        processed_lines += 1
        
        if processed_lines % progress_interval == 0:
            print(f"Processed {processed_lines} lines...", file=sys.stderr)
    
    # Close input stream if it's not stdin
    if not is_stdin_stdout(input_path):
        input_stream.close()
    input_stream = None
    
    # Flush any remaining batch data
    if batch_data:
        insert_field_values_batch(conn, batch_data)
    
    print(f"Processed {processed_lines} total lines. Exporting merged data...", file=sys.stderr)
    
    # Open output stream and export merged data
    output_stream = get_output_stream(output_path, kms_key_id)
    export_merged_data(conn, output_stream, merge_key_field)
    
    # Close output stream if it's not stdout
    if not is_stdin_stdout(output_path):
        output_stream.close()
    output_stream = None
    
    print(get_completion_message(output_path), file=sys.stderr)
    
except KeyboardInterrupt:
    print("\nOperation cancelled by user", file=sys.stderr)
    sys.exit(1)
except BrokenPipeError:
    # Handle broken pipe gracefully (e.g., when piping to head)
    sys.exit(0)
except Exception as e:
    print(f"Error: {e}", file=sys.stderr)
    sys.exit(1)
finally:
    # Clean up streams
    if input_stream and not is_stdin_stdout(input_path):
        try:
            input_stream.close()
        except:
            pass
    if output_stream and not is_stdin_stdout(output_path):
        try:
            output_stream.close()
        except:
            pass
    
    # Clean up database connection
    if 'conn' in locals():
        try:
            conn.close()
        except:
            pass
    
    # Clean up temp database
    if os.path.exists(tmp_db):
        try:
            os.remove(tmp_db)
        except:
            pass


def get_file_size_mb(file_path: str) -> float:
“”“Get file size in MB for progress reporting. Returns 0 for stdin/S3.”””
if is_stdin_stdout(file_path) or is_s3_uri(file_path):
return 0.0
return os.path.getsize(file_path) / (1024 * 1024)

if **name** == “**main**”:
parser = argparse.ArgumentParser(
description=“Optimized stream-merge for very large JSONL files. “
“Merges records by key, appending all fields into unique lists. “
“Supports stdin/stdout (’-’) and S3 URIs with KMS encryption.”
)
parser.add_argument(”–input”, “-i”, required=True,
help=“Path to input JSONL file (use ‘-’ for stdin, ‘s3://bucket/key’ for S3).”)
parser.add_argument(”–output”, “-o”, required=True,
help=“Path where merged JSONL will be written (use ‘-’ for stdout, ‘s3://bucket/key’ for S3).”)
parser.add_argument(”–key”, “-k”, required=True,
help=“Name of the top-level key to merge on (e.g. ‘user_id’).”)
parser.add_argument(”–batch-size”, “-b”, type=int, default=1000,
help=“Batch size for database operations (default: 1000).”)
parser.add_argument(”–progress-interval”, “-p”, type=int, default=10000,
help=“Progress reporting interval in lines (default: 10000).”)
parser.add_argument(”–kms-key-id”, type=str,
help=“AWS KMS key ID for S3 encryption (optional). Can be key ID, ARN, or alias.”)


args = parser.parse_args()

# Validate S3 support if needed
if is_s3_uri(args.input) or is_s3_uri(args.output):
    validate_s3_support()

# Report file size (only for actual files)
if not is_stdin_stdout(args.input) and not is_s3_uri(args.input):
    file_size = get_file_size_mb(args.input)
    print(f"Input file size: {file_size:.2f} MB", file=sys.stderr)

stream_merge_jsonl_optimized(
    args.input, 
    args.output, 
    args.key,
    args.batch_size,
    args.progress_interval,
    args.kms_key_id
)

"""
TODO: Implement gzip input and output handling
import argparse
import json
import sqlite3
import os
import sys
import hashlib
import tempfile
from typing import Any, Dict, List, Optional, Set, Union, TextIO
import gc
import urllib.parse

# ← NEW IMPORTS:
import gzip
import io

try:
    from smart_open import open as smart_open
    SMART_OPEN_AVAILABLE = True
except ImportError:
    SMART_OPEN_AVAILABLE = False
    smart_open = None


def is_s3_uri(path: str) -> bool:
    """Check if path is an S3 URI."""
    return path.startswith("s3://")


def is_stdin_stdout(path: str) -> bool:
    """Check if path represents stdin/stdout."""
    return path == "-"


def validate_s3_support():
    """Validate that smart_open is available for S3 operations."""
    if not SMART_OPEN_AVAILABLE:
        print("Error: smart_open library is required for S3 support.", file=sys.stderr)
        print("Install it with: pip install smart_open[s3]", file=sys.stderr)
        sys.exit(1)


def get_s3_transport_params(kms_key_id: Optional[str] = None) -> Dict[str, Any]:
    """Get S3 transport parameters including KMS encryption."""
    params = {
        "multipart_upload": True,
        "multipart_upload_kwargs": {}
    }
    if kms_key_id:
        params["multipart_upload_kwargs"].update({
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": kms_key_id
        })
    return params


def get_input_stream(input_path: str) -> TextIO:
    """
    Get input stream – stdin, local file, gzip‐wrapped local file,
    or (possibly gzipped) S3 object.
    """
    # 1) stdin
    if is_stdin_stdout(input_path):
        return sys.stdin

    # 2) S3 URI
    elif is_s3_uri(input_path):
        validate_s3_support()
        # We’ll need binary mode if it’s “.gz”, otherwise text mode is fine.
        if input_path.endswith(".gz"):
            # Open raw bytes from S3, then wrap in gzip + TextIO
            transport_params = get_s3_transport_params()  # pass no KMS for input
            raw_stream = smart_open(input_path, "rb", transport_params=transport_params)
            gzip_stream = gzip.GzipFile(fileobj=raw_stream, mode="rb")
            return io.TextIOWrapper(gzip_stream, encoding="utf-8")
        else:
            # Regular (uncompressed) text from S3
            transport_params = get_s3_transport_params()  # still pass transport_params if needed
            return smart_open(input_path, "r", encoding="utf-8", transport_params=transport_params)

    # 3) Local path
    else:
        if input_path.endswith(".gz"):
            # Open local gzip in text mode
            return gzip.open(input_path, "rt", encoding="utf-8")
        else:
            # Regular local file
            return open(input_path, "r", encoding="utf-8")


def get_output_stream(output_path: str, kms_key_id: Optional[str] = None) -> TextIO:
    """
    Get output stream – stdout, local file, gzip‐wrapped local file,
    or (possibly gzipped) S3 object (with optional KMS).
    """
    # 1) stdout
    if is_stdin_stdout(output_path):
        return sys.stdout

    # 2) S3 URI
    elif is_s3_uri(output_path):
        validate_s3_support()
        transport_params = get_s3_transport_params(kms_key_id)
        if output_path.endswith(".gz"):
            # Open raw binary to S3, then wrap in gzip + TextIO
            raw_stream = smart_open(output_path, "wb", transport_params=transport_params)
            gzip_stream = gzip.GzipFile(fileobj=raw_stream, mode="wb")
            return io.TextIOWrapper(gzip_stream, encoding="utf-8")
        else:
            # Regular (uncompressed) text‐mode S3
            return smart_open(output_path, "w", encoding="utf-8", transport_params=transport_params)

    # 3) Local path
    else:
        if output_path.endswith(".gz"):
            # Open local gzip in write‐text mode
            return gzip.open(output_path, "wt", encoding="utf-8")
        else:
            # Regular local file
            return open(output_path, "w", encoding="utf-8")
"""