"""
Events Exporter - CLI Entry Point

Export MongoDB events to GCS in JSONL.gz format.

Usage:
    # Full export (all 27 event types)
    python -m ingestion.sources.mongodb.events export --mode full

    # Filter export (specific collections)
    python -m ingestion.sources.mongodb.events export --mode filter --collections view_product_detail,checkout_success

    # Resume from checkpoint
    python -m ingestion.sources.mongodb.events export --mode full --resume

    # Custom date
    python -m ingestion.sources.mongodb.events export --mode full --date 20260404
"""

import argparse
import sys

from common.utils.logger import get_logger
from ingestion.sources.mongodb.events.exporter import export_events

logger = get_logger(__name__)


def create_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.sources.mongodb.events",
        description="Export MongoDB events to GCS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full export (all collections)
  python -m ingestion.sources.mongodb.events export --mode full

  # Filter specific collections
  python -m ingestion.sources.mongodb.events export --mode filter --collections view_product_detail,checkout_success

  # Resume from checkpoint
  python -m ingestion.sources.mongodb.events export --mode full --resume

  # Custom date
  python -m ingestion.sources.mongodb.events export --mode full --date 20260404
        """
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Export command
    export_parser = subparsers.add_parser(
        "export",
        help="Export events to GCS"
    )
    export_parser.add_argument(
        "--mode",
        choices=["full", "filter"],
        required=True,
        help="Export mode: 'full' (all collections) or 'filter' (specific collections)"
    )
    export_parser.add_argument(
        "--collections",
        type=str,
        help="Comma-separated list of collection names (required if mode=filter)"
    )
    export_parser.add_argument(
        "--date",
        type=str,
        help="Date string YYYYMMDD (default: today)"
    )
    export_parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from checkpoint"
    )

    return parser


def main():
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "export":
        # Parse collections
        collections = None
        if args.collections:
            collections = [c.strip() for c in args.collections.split(',')]

        success = export_events(
            mode=args.mode,
            collections=collections,
            date_str=args.date,
            resume=args.resume
        )

        if not success:
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
