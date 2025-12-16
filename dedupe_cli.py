#!/usr/bin/env python3
"""
Gene Record Deduplication CLI Tool

Detects and optionally removes duplicate records from JSONL files.
Designed for cleaning LLM training data.

Usage:
    python dedupe_cli.py -i /path/to/jsonl/files
    python dedupe_cli.py -i /path/to/jsonl/files --delete
    python dedupe_cli.py -i /path/to/jsonl/files -d /path/to/db
"""

import argparse
import sys
from pathlib import Path

from dedupe_genes import run_detection
from dedupe_delete import run_deletion


def main():
    parser = argparse.ArgumentParser(
        description="Detect and remove duplicate gene records from JSONL files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s -i ./data                    # Detect duplicates, prompt to delete
  %(prog)s -i ./data --delete           # Detect and auto-delete duplicates
  %(prog)s -i ./data -d ./cache/db      # Use custom database path
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help="Path to directory containing JSONL files (required)"
    )
    
    parser.add_argument(
        '-d', '--db',
        default=None,
        help="Path for SQLite database (default: same as input directory)"
    )
    
    parser.add_argument(
        '--delete',
        action='store_true',
        help="Skip confirmation prompt and automatically delete duplicates"
    )
    
    parser.add_argument(
        '-s', '--sample',
        type=int,
        default=2000,
        help="Sample size for near-duplicate detection (default: 2000)"
    )
    
    args = parser.parse_args()
    
    # Validate input path
    input_path = Path(args.input).resolve()
    if not input_path.exists():
        print(f"Error: Input path does not exist: {input_path}")
        sys.exit(1)
    
    if not input_path.is_dir():
        print(f"Error: Input path is not a directory: {input_path}")
        sys.exit(1)
    
    # Set database path
    if args.db:
        db_path = str(Path(args.db).resolve())
        # Ensure parent directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    else:
        db_path = str(input_path / "dedup.db")
    
    print("=" * 60)
    print("GENE RECORD DEDUPLICATION TOOL")
    print("=" * 60)
    print(f"Input directory: {input_path}")
    print(f"Database path: {db_path}")
    print(f"Auto-delete: {args.delete}")
    print()
    
    # Step 1: Run detection
    print("STEP 1: Detecting duplicates...")
    print("-" * 60)
    
    results = run_detection(str(input_path), db_path, args.sample)
    
    # Results are always saved to input directory
    print(f"\nDuplicate list saved to: {results['results_file']}")
    
    # Check if there are duplicates to delete
    if results['total_dupe_records'] == 0:
        print("\nNo duplicates found. Nothing to delete.")
        sys.exit(0)
    
    # Calculate records that would be deleted (total - unique groups = deletions)
    records_to_delete = results['total_dupe_records'] - results['unique_dupe_groups']
    
    print(f"\n" + "=" * 60)
    print("DUPLICATE SUMMARY")
    print("=" * 60)
    print(f"Total records: {results['total_records']:,}")
    print(f"Duplicate groups: {results['unique_dupe_groups']:,}")
    print(f"Records in duplicates: {results['total_dupe_records']:,}")
    print(f"Records to delete (keeping 1 per group): {records_to_delete:,}")
    print()
    
    # Step 2: Delete or prompt
    if args.delete:
        # Auto-delete
        print("STEP 2: Deleting duplicates (--delete flag set)...")
        print("-" * 60)
        delete_results = run_deletion(str(input_path), db_path)
    else:
        # Prompt user
        print("Would you like to delete the duplicate records?")
        print("(This will keep one copy of each duplicate and remove the rest)")
        print()
        
        while True:
            response = input("Delete duplicates? (yes/no): ").strip().lower()
            
            if response == 'yes':
                print()
                print("STEP 2: Deleting duplicates...")
                print("-" * 60)
                delete_results = run_deletion(str(input_path), db_path)
                break
            elif response == 'no':
                print()
                print("Deletion cancelled.")
                print(f"Duplicate list has been saved to: {results['results_file']}")
                print("You can run deletion later with: python dedupe_delete.py -i <input> -d <db>")
                sys.exit(0)
            else:
                print("Please enter 'yes' or 'no'")
    
    # Final summary
    print()
    print("=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"Records deleted: {delete_results['deleted']:,}")
    print(f"Files modified: {delete_results['files_modified']:,}")
    print(f"Results file: {results['results_file']}")


if __name__ == "__main__":
    main()

