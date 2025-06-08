#!/usr/bin/env python3
import json
import sys
from collections import Counter

def analyze_jsonl(filepath, stop_after_lines=1000, num_samples=10):
    """Quick analysis of JSONL file structure"""
    print(f"Analyzing: {filepath}")
    print("=" * 60)

    field_counts = Counter()
    samples = []
    line_count = 0

    def extract_all_paths(obj, prefix=""):
        """Extract all possible field paths from a JSON object"""
        paths = []
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{prefix}.{key}" if prefix else key
                paths.append(current_path)
                if isinstance(value, dict):
                    paths.extend(extract_all_paths(value, current_path))
                elif isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    paths.extend(extract_all_paths(value[0], f"{current_path}[0]"))
        return paths

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line_count = line_num
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)

                    paths = extract_all_paths(obj)
                    for path in paths:
                        field_counts[path] += 1

                    if len(samples) < num_samples:
                        samples.append({
                            'line': line_num,
                            'keys': list(obj.keys()) if isinstance(obj, dict) else str(type(obj)),
                            'sample': obj
                        })

                except json.JSONDecodeError as e:
                    print(f"JSON error at line {line_num}: {e}")
                    continue

                if line_num >= stop_after_lines:
                    break

    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
        return

    print(f"Total lines analyzed: {min(line_count, 1000)}")
    print(f"Total lines in file: {line_count}")
    print()

    print("ALL AVAILABLE FIELD PATHS:")
    print("-" * 40)
    for path, count in field_counts.most_common(50):
        percentage = (count / min(line_count, 1000)) * 100
        print(f"{path:<40} {count:>6} ({percentage:5.1f}%)")

    print("\nSAMPLE RECORDS:")
    print("-" * 40)
    for i, sample in enumerate(samples[:3], 1):
        print(f"Sample {i} (line {sample['line']}):")
        if isinstance(sample['sample'], dict):
            print(json.dumps(sample['sample'], indent=2)[:500] + "...")
        else:
            print(f"  Type: {type(sample['sample'])}")
            print(f"  Value: {str(sample['sample'])[:200]}...")
        print()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python quick_analyzer.py <jsonl_file> [stop_after_lines: int]")
        sys.exit(1)

    analyze_jsonl(sys.argv[1], int(sys.argv[2]))
