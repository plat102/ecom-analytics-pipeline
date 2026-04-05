"""
CLI wrapper for GCS format conversion

Usage:
    python scripts/convert_json_to_jsonl.py \
        --input gs://bucket/file.json \
        --output gs://bucket/file.jsonl.gz \
        [--filter-field status --filter-value success]
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.storage.gcs_format_utils import json_array_to_jsonl_gz, filter_by_field


def main():
    parser = argparse.ArgumentParser(description='Convert JSON array to JSONL.gz on GCS')
    parser.add_argument('--input', required=True, help='Input GCS URI')
    parser.add_argument('--output', required=True, help='Output GCS URI')
    parser.add_argument('--filter-field', help='Field to filter on')
    parser.add_argument('--filter-value', help='Value to keep')

    args = parser.parse_args()

    filter_func = None
    if args.filter_field and args.filter_value:
        filter_func = filter_by_field(args.filter_field, args.filter_value)

    count = json_array_to_jsonl_gz(args.input, args.output, filter_func)
    print(f"Written {count} items to {args.output}")


if __name__ == '__main__':
    main()
