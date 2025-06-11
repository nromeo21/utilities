#!/usr/bin/env python3
"""
JSONL File Joiner - Efficiently merge JSONL files with minimal memory usage
"""

import json
import argparse
from pathlib import Path
from typing import Dict, Any, Union, List, Set
from collections import defaultdict


class JSONLJoiner:
    def __init__(self):
        self.merged_data = defaultdict(dict)

    def _merge_values(self, existing: Any, new: Any) -> Any:
        """
        Merge two values, keeping unique items and handling different types.
        """
        # If both are dicts, merge recursively
        if isinstance(existing, dict) and isinstance(new, dict):
            result = existing.copy()
            for key, value in new.items():
                if key in result:
                    result[key] = self._merge_values(result[key], value)
                else:
                    result[key] = value
            return result

        # If both are lists, combine and keep unique values
        elif isinstance(existing, list) and isinstance(new, list):
            # Convert to sets for uniqueness, handling unhashable types
            unique_items = []
            seen = set()

            for item in existing + new:
                # Handle unhashable types (like dicts/lists)
                if isinstance(item, (dict, list)):
                    item_str = json.dumps(item, sort_keys=True)
                    if item_str not in seen:
                        seen.add(item_str)
                        unique_items.append(item)
                else:
                    if item not in seen:
                        seen.add(item)
                        unique_items.append(item)

            return unique_items

        # If one is a list and the other isn't, convert to list and merge
        elif isinstance(existing, list) and not isinstance(new, list):
            return self._merge_values(existing, [new])
        elif not isinstance(existing, list) and isinstance(new, list):
            return self._merge_values([existing], new)

        # If values are different, create a list with both
        elif existing != new:
            return self._merge_values([existing], [new])

        # If values are the same, return one of them
        else:
            return existing

    def _process_file(self, filepath: str, merge_key: str) -> None:
        """
        Process a single JSONL file line by line.
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        data = json.loads(line)
                        if not isinstance(data, dict):
                            print(f"Warning: Line {line_num} in {filepath} is not a JSON object, skipping")
                            continue

                        # Get the merge key value
                        if merge_key not in data:
                            print(f"Warning: Line {line_num} in {filepath} missing merge key '{merge_key}', skipping")
                            continue

                        key_value = data[merge_key]

                        # Convert key to string for consistent hashing
                        key_str = str(key_value)

                        # Merge with existing data
                        if key_str in self.merged_data:
                            for field, value in data.items():
                                if field in self.merged_data[key_str]:
                                    self.merged_data[key_str][field] = self._merge_values(
                                        self.merged_data[key_str][field], value
                                    )
                                else:
                                    self.merged_data[key_str][field] = value
                        else:
                            self.merged_data[key_str] = data.copy()

                    except json.JSONDecodeError as e:
                        print(f"Warning: Invalid JSON on line {line_num} in {filepath}: {e}")
                        continue

        except FileNotFoundError:
            print(f"Error: File {filepath} not found")
        except Exception as e:
            print(f"Error processing file {filepath}: {e}")

    def join_files(self, file_configs: List[tuple], output_file: str) -> None:
        """
        Join multiple JSONL files and write to output file.

        Args:
            file_configs: List of (filepath, merge_key) tuples
            output_file: Path to output JSONL file
        """
        print(f"Starting to process {len(file_configs)} files...")

        # Process each file
        for i, (filepath, merge_key) in enumerate(file_configs, 1):
            print(f"Processing file {i}/{len(file_configs)}: {filepath} (merge key: {merge_key})")
            self._process_file(filepath, merge_key)

        # Write merged data to output file
        print(f"Writing merged data to {output_file}...")
        try:
            with open(output_file, 'w', encoding='utf-8') as outfile:
                for merged_item in self.merged_data.values():
                    outfile.write(json.dumps(merged_item, ensure_ascii=False) + '\n')

            print(f"Successfully merged {len(self.merged_data)} unique items to {output_file}")

        except Exception as e:
            print(f"Error writing to output file: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the merged data."""
        return {
            'total_unique_items': len(self.merged_data),
            'memory_usage_items': len(self.merged_data)
        }


def main():
    parser = argparse.ArgumentParser(
        description='Join JSONL files by merge keys with minimal memory usage',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Join two files with different merge keys
  python jsonl_joiner.py -o merged.jsonl users.jsonl id products.jsonl product_id

  # Join multiple files with same merge key
  python jsonl_joiner.py -o output.jsonl file1.jsonl id file2.jsonl id file3.jsonl id
        ''')

    parser.add_argument('-o', '--output', required=True,
                        help='Output JSONL file path')

    parser.add_argument('files', nargs='+',
                        help='Alternating file paths and merge keys: file1.jsonl key1 file2.jsonl key2 ...')

    args = parser.parse_args()

    # Parse file and key pairs
    if len(args.files) % 2 != 0:
        print("Error: Files and merge keys must be provided in pairs")
        print("Usage: file1.jsonl key1 file2.jsonl key2 ...")
        return

    file_configs = []
    for i in range(0, len(args.files), 2):
        filepath = args.files[i]
        merge_key = args.files[i + 1]
        file_configs.append((filepath, merge_key))

    # Validate input files exist
    for filepath, _ in file_configs:
        if not Path(filepath).exists():
            print(f"Error: File {filepath} does not exist")
            return

    # Join files
    joiner = JSONLJoiner()
    joiner.join_files(file_configs, args.output)

    # Print stats
    stats = joiner.get_stats()
    print(f"\nStats:")
    print(f"  Total unique items: {stats['total_unique_items']}")


if __name__ == '__main__':
    main()