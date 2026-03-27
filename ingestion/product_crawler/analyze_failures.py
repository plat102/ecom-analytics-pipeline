"""
Analyze DLQ failures - simple breakdown of failed products.
"""

import json
from pathlib import Path
from collections import Counter

project_root = Path(__file__).parent.parent.parent
RETRY_FILE = project_root / "data/exports/retry_failed_results.json"


def main():
    with open(RETRY_FILE) as f:
        results = json.load(f)

    failed = [r for r in results if r.get('status') == 'error']

    print(f"\nTotal failed: {len(failed)}")

    # HTTP Status breakdown
    http_status = Counter(r.get('http_status') for r in failed)
    print(f"\nHTTP Status:")
    for status, count in http_status.most_common():
        print(f"  {status}: {count} ({count/len(failed)*100:.1f}%)")

    # Domain breakdown
    domains = Counter(r['url'].split('/')[2] for r in failed)
    print(f"\nTop 10 domains:")
    for domain, count in domains.most_common(10):
        print(f"  {domain}: {count} ({count/len(failed)*100:.1f}%)")

    # Error messages
    errors = Counter(r.get('error_message', 'unknown')[:60] for r in failed)
    print(f"\nTop 5 error messages:")
    for msg, count in errors.most_common(5):
        print(f"  [{count}] {msg}")


if __name__ == "__main__":
    main()
