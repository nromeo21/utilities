import json
import sqlite3
import os
import argparse
import sys
from typing import Dict, Any, List, Union, Optional, Set
from collections import defaultdict, Counter

def merge_json(existing_json: Dict[str, Any], new_json: Dict[str, Any], numeric_merge_strategy: str = "default") -> Dict[str, Any]:
    """Merge two JSON objects with improved handling for different data types."""
    merged = existing_json.copy()

    for key, value in new_json.items():
        if key == "merge_key":
            continue

        if key not in merged:
            merged[key] = value
        elif isinstance(merged[key], list) and isinstance(value, list):
            # Merge lists and remove duplicates while preserving order
            merged_list = []
            seen_hashes = set()

            for item in merged[key] + value:
                # Handle hashable items (strings, numbers, tuples, etc.)
                if isinstance(item, (str, int, float, bool, type(None))):
                    if item not in seen_hashes:
                        seen_hashes.add(item)
                        merged_list.append(item)
                # Handle unhashable items like dicts and lists
                elif isinstance(item, (dict, list)):
                    # Convert to JSON string for comparison
                    item_hash = json.dumps(item, sort_keys=True, separators=(',', ':'))
                    if item_hash not in seen_hashes:
                        seen_hashes.add(item_hash)
                        merged_list.append(item)
                else:
                    # For other types, just append
                    merged_list.append(item)

            merged[key] = merged_list
        elif isinstance(merged[key], dict) and isinstance(value, dict):
            # Recursively merge dictionaries
            merged[key] = merge_json(merged[key], value, numeric_merge_strategy)
        elif isinstance(merged[key], str) and isinstance(value, str):
            # Handle string merging by converting to arrays
            if merged[key] == value:
                # If strings are identical, keep as single string
                pass
            else:
                # Different strings - convert to array
                # Split existing string if it contains newlines (from previous merges)
                existing_values = merged[key].split('\n') if '\n' in merged[key] else [merged[key]]
                new_values = value.split('\n') if '\n' in value else [value]

                # Combine and deduplicate while preserving order
                combined = []
                seen = set()
                for val in existing_values + new_values:
                    val = val.strip()
                    if val and val not in seen:
                        seen.add(val)
                        combined.append(val)

                # If only one unique value, keep as string; otherwise make array
                if len(combined) == 1:
                    merged[key] = combined[0]
                else:
                    merged[key] = combined
        elif isinstance(merged[key], str) and isinstance(value, list):
            # Convert string to array and merge with list
            existing_array = merged[key].split('\n') if '\n' in merged[key] else [merged[key]]
            combined = []
            seen = set()

            for val in existing_array + value:
                val_str = str(val).strip()
                if val_str and val_str not in seen:
                    seen.add(val_str)
                    combined.append(val)

            merged[key] = combined
        elif isinstance(merged[key], list) and isinstance(value, str):
            # Convert string to array and merge with existing list
            new_array = value.split('\n') if '\n' in value else [value]
            combined = []
            seen = set()

            for val in merged[key] + new_array:
                val_str = str(val).strip()
                if val_str and val_str not in seen:
                    seen.add(val_str)
                    combined.append(val)

            merged[key] = combined
        elif isinstance(merged[key], (int, float)) and isinstance(value, (int, float)):
            # Apply merge strategy for numbers
            if numeric_merge_strategy == "sum":
                merged[key] = merged[key] + value
            elif numeric_merge_strategy == "max":
                merged[key] = max(merged[key], value)
            elif numeric_merge_strategy == "min":
                merged[key] = min(merged[key], value)
            elif numeric_merge_strategy == "append":
                merged[key] = [merged[key], value]
            else:  # overwrite (default)
                merged[key] = value
        else:
            # Different types or unhandled cases - convert to array if different values
            if merged[key] != value:
                if not isinstance(merged[key], list):
                    merged[key] = [merged[key]]
                if value not in merged[key]:
                    merged[key].append(value)
            # If same value, keep as is

    return merged

def analyze_jsonl_structure(input_path: str, sample_size: int = 100) -> Dict[str, Any]:
    """Analyze the structure of a JSONL file to understand common field patterns."""
    field_counter = Counter()
    nested_fields = set()
    sample_records = []
    total_lines = 0

    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                total_lines = line_num
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        # Count top-level fields
                        for key in obj.keys():
                            field_counter[key] += 1

                        # Find nested fields
                        def find_nested_fields(data, prefix=""):
                            if isinstance(data, dict):
                                for k, v in data.items():
                                    current_path = f"{prefix}.{k}" if prefix else k
                                    nested_fields.add(current_path)
                                    if isinstance(v, dict):
                                        find_nested_fields(v, current_path)

                        find_nested_fields(obj)

                        # Keep sample records
                        if len(sample_records) < sample_size:
                            sample_records.append({
                                'line_num': line_num,
                                'record': obj
                            })

                except json.JSONDecodeError:
                    continue

    except FileNotFoundError:
        return {"error": f"File not found: {input_path}"}

    return {
        "total_lines": total_lines,
        "common_fields": field_counter.most_common(20),
        "all_nested_fields": sorted(nested_fields),
        "sample_records": sample_records[:5]  # Return only first 5 samples
    }

def process_jsonl_merge_records(
    input_path: str,
    output_path: str,
    db_path: str = "merged_records.db",
    merge_field_path: str = "id",
    output_field_path: str = "merge_id",
    numeric_merge_strategy: str = "max",
    keep_original_field: bool = False,
    verbose: bool = False,
    batch_size: int = 1000,
    skip_missing: bool = False,
    analyze_first: bool = False
) -> None:
    """
    Process JSONL file to merge records based on a specified field.
    Enhanced with better error reporting and analysis.
    """

    # Analyze structure if requested
    if analyze_first:
        print("Analyzing file structure...")
        analysis = analyze_jsonl_structure(input_path)
        if "error" in analysis:
            print(f"Analysis error: {analysis['error']}")
            return

        print(f"Total lines: {analysis['total_lines']}")
        print(f"Most common fields: {analysis['common_fields'][:10]}")
        print(f"Available nested fields: {analysis['all_nested_fields'][:20]}")

        if merge_field_path not in analysis['all_nested_fields']:
            print(f"WARNING: Merge field '{merge_field_path}' not found in structure analysis!")
            similar_fields = [f for f in analysis['all_nested_fields'] if merge_field_path.split('.')[-1] in f]
            if similar_fields:
                print(f"Similar fields found: {similar_fields}")

        print("\nSample records:")
        for sample in analysis['sample_records']:
            print(f"  Line {sample['line_num']}: {list(sample['record'].keys())}")
        print()

    # Clean up already existing database
    if os.path.exists(db_path):
        os.remove(db_path)
        if verbose:
            print(f"Removed existing database: {db_path}")

    # Initialize database
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS merged_data (
            merge_id TEXT PRIMARY KEY,
            json_data TEXT
        )
    ''')

    def get_nested_value(obj: Dict[str, Any], path: str) -> Any:
        """Get value from nested dictionary using dot notation."""
        keys = path.split('.')
        current = obj
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current

    def set_nested_value(obj: Dict[str, Any], path: str, value: Any) -> None:
        """Set value in nested dictionary using dot notation."""
        keys = path.split('.')
        current = obj
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

    def remove_nested_key(obj: Dict[str, Any], path: str) -> None:
        """Remove key from nested dictionary using dot notation."""
        keys = path.split('.')
        current = obj
        for key in keys[:-1]:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return
        if isinstance(current, dict) and keys[-1] in current:
            del current[keys[-1]]

    processed_count = 0
    error_count = 0
    missing_merge_field_count = 0
    empty_line_count = 0
    lines_with_issues = []

    try:
        with open(input_path, 'r', encoding='utf-8') as infile:
            for line_num, line in enumerate(infile, 1):
                try:
                    line = line.strip()
                    if not line:
                        empty_line_count += 1
                        continue

                    obj = json.loads(line)
                    merge_values = get_nested_value(obj, merge_field_path)

                    if merge_values is None:
                        missing_merge_field_count += 1
                        lines_with_issues.append(line_num)

                        if verbose and missing_merge_field_count <= 10:  # Limit verbose output
                            print(f"Warning: No merge values found at line {line_num}")
                            print(f"  Available keys: {list(obj.keys()) if isinstance(obj, dict) else 'Not a dict'}")

                        if skip_missing:
                            continue
                        else:
                            # Continue processing but with a warning
                            continue

                    # Normalize to list
                    if isinstance(merge_values, str):
                        merge_values = [merge_values]
                    elif not isinstance(merge_values, list):
                        if verbose:
                            print(f"Warning: Merge values at line {line_num} are not string or list: {type(merge_values)}")
                        continue

                    # Process each merge value
                    for merge_value in merge_values:
                        if not merge_value:  # Skip empty merge values
                            continue

                        # Fetch existing record
                        cur.execute('SELECT json_data FROM merged_data WHERE merge_id = ?', (merge_value,))
                        row = cur.fetchone()

                        # Prepare new object
                        new_obj = obj.copy()
                        set_nested_value(new_obj, output_field_path, merge_value)

                        # Remove the original merge field to avoid duplication
                        if not keep_original_field:
                            remove_nested_key(new_obj, merge_field_path)

                        # Merge or insert
                        if row:
                            existing_obj = json.loads(row[0])
                            merged = merge_json(existing_obj, new_obj, numeric_merge_strategy)
                        else:
                            merged = new_obj

                        # Update database
                        cur.execute('''
                            INSERT INTO merged_data (merge_id, json_data)
                            VALUES (?, ?)
                            ON CONFLICT(merge_id) DO UPDATE SET
                            json_data = excluded.json_data
                        ''', (merge_value, json.dumps(merged, separators=(',', ':'))))

                    processed_count += 1

                    if processed_count % batch_size == 0:
                        if verbose:
                            print(f"Processed {processed_count} records...")
                        conn.commit()  # Periodic commits for large files

                except json.JSONDecodeError as e:
                    print(f"JSON decode error at line {line_num}: {e}")
                    error_count += 1
                    lines_with_issues.append(line_num)
                except Exception as e:
                    print(f"Error processing line {line_num}: {e}")
                    error_count += 1
                    lines_with_issues.append(line_num)

        # Final commit
        conn.commit()

        # Write output file
        with open(output_path, 'w', encoding='utf-8') as outfile:
            record_count = 0
            for row in cur.execute('SELECT json_data FROM merged_data ORDER BY merge_id'):
                outfile.write(row[0] + '\n')
                record_count += 1

        print(f"Processing complete!")
        print(f"- Processed {processed_count} input records")
        print(f"- Generated {record_count} merged records")
        print(f"- Empty lines: {empty_line_count}")
        print(f"- Missing merge field: {missing_merge_field_count}")
        print(f"- JSON decode errors: {error_count}")

        if lines_with_issues and verbose:
            print(f"- Lines with issues: {lines_with_issues[:20]}{'...' if len(lines_with_issues) > 20 else ''}")

    except FileNotFoundError:
        print(f"Error: Input file '{input_path}' not found")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(
        description="Merge JSONL records based on a specified field with enhanced diagnostics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze file structure first
  python enhanced_merger.py input.jsonl --analyze-first --verbose

  # Basic usage with better error handling
  python enhanced_merger.py input.jsonl --skip-missing --verbose

  # Merge CVE records with diagnostics
  python enhanced_merger.py cve_data.jsonl -o cve_merged.jsonl --merge-field "cveId" --analyze-first --verbose
        """
    )

    # Required arguments
    parser.add_argument('input_path',
                       help='Path to input JSONL file')

    # Optional arguments
    parser.add_argument('-o', '--output',
                       dest='output_path',
                       help='Path to output JSONL file (default: input_path with _merged suffix)')

    parser.add_argument('--merge-field',
                       default='id',
                       help='Dot-notation path to merge field (default: id)')

    parser.add_argument('--output-field',
                       default='merge_id',
                       help='Dot-notation path where merge ID should be placed in output (default: merge_id)')

    parser.add_argument('--db-path',
                       default='merged_records.db',
                       help='Path to SQLite database for temporary storage (default: merged_records.db)')

    parser.add_argument('--numeric-merge-strategy',
                       choices=['sum', 'max', 'min', 'append', 'overwrite'],
                       default='max',
                       help='Strategy for merging numeric values (default: max)')

    parser.add_argument('--keep-original',
                       action='store_true',
                       help='Keep the original merge field in output records')

    parser.add_argument('--verbose', '-v',
                       action='store_true',
                       help='Enable verbose logging')

    parser.add_argument('--batch-size',
                       type=int,
                       default=1000,
                       help='Number of records to process before committing to database (default: 1000)')

    parser.add_argument('--cleanup-db',
                       action='store_true',
                       help='Remove temporary database file after processing')

    parser.add_argument('--skip-missing',
                       action='store_true',
                       help='Skip records without merge field instead of warning')

    parser.add_argument('--analyze-first',
                       action='store_true',
                       help='Analyze file structure before processing')

    args = parser.parse_args()

    # Set default output path if not provided
    if not args.output_path:
        input_name, input_ext = os.path.splitext(args.input_path)
        args.output_path = f"{input_name}_merged{input_ext}"
        if args.verbose:
            print(f"Using default output path: {args.output_path}")

    # Validate input file exists
    if not os.path.exists(args.input_path):
        print(f"Error: Input file '{args.input_path}' does not exist")
        sys.exit(1)

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        if args.verbose:
            print(f"Created output directory: {output_dir}")

    # Process the file
    process_jsonl_merge_records(
        input_path=args.input_path,
        output_path=args.output_path,
        db_path=args.db_path,
        merge_field_path=args.merge_field,
        output_field_path=args.output_field,
        numeric_merge_strategy=args.numeric_merge_strategy,
        keep_original_field=args.keep_original,
        verbose=args.verbose,
        batch_size=args.batch_size,
        skip_missing=args.skip_missing,
        analyze_first=args.analyze_first
    )

    # Cleanup database if requested
    if args.cleanup_db and os.path.exists(args.db_path):
        os.remove(args.db_path)
        if args.verbose:
            print(f"Removed temporary database: {args.db_path}")

if __name__ == "__main__":
    main()
