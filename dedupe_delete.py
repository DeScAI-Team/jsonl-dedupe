"""
Gene Record Deduplication - Deletion Script
Removes duplicate records from JSONL files, keeping one copy of each.
"""

import sqlite3
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm


def get_duplicates_to_delete(db_path: str):
    """
    Query the database to find all duplicate records.
    For each duplicate group, keep the first occurrence and mark the rest for deletion.
    
    Args:
        db_path: Path to the SQLite database created by dedupe_genes.py
        
    Returns:
        dict: {filename: set of line_numbers to delete}
    """
    conn = sqlite3.connect(db_path)
    
    # Get all hashes that have duplicates
    cursor = conn.execute('''
        SELECT hash FROM records 
        GROUP BY hash 
        HAVING COUNT(*) > 1
    ''')
    
    duplicate_hashes = [row[0] for row in cursor.fetchall()]
    print(f"Found {len(duplicate_hashes):,} duplicate groups to process")
    
    # For each duplicate hash, get all locations and mark all but the first for deletion
    files_to_delete = defaultdict(set)
    total_to_delete = 0
    
    for hash_val in tqdm(duplicate_hashes, desc="Processing duplicate groups"):
        # Get all locations for this hash, ordered by filename and line number
        locations = conn.execute('''
            SELECT filename, line_num FROM records 
            WHERE hash = ? 
            ORDER BY filename, line_num
        ''', (hash_val,)).fetchall()
        
        # Keep the first one, delete the rest
        for filename, line_num in locations[1:]:
            files_to_delete[filename].add(line_num)
            total_to_delete += 1
    
    conn.close()
    
    print(f"Total records to delete: {total_to_delete:,}")
    print(f"Files affected: {len(files_to_delete):,}")
    
    return dict(files_to_delete)


def rewrite_jsonl_files(input_path: str, files_to_delete: dict):
    """
    Rewrite JSONL files, removing duplicate lines.
    
    Args:
        input_path: Directory containing JSONL files
        files_to_delete: {filename: set of line_numbers to delete}
    """
    input_dir = Path(input_path)
    
    total_deleted = 0
    total_kept = 0
    
    for filename, lines_to_delete in tqdm(files_to_delete.items(), desc="Rewriting files"):
        filepath = input_dir / filename
        
        if not filepath.exists():
            print(f"Warning: File not found: {filepath}")
            continue
        
        # Read all lines
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Write back, skipping lines to delete
        with open(filepath, 'w', encoding='utf-8') as f:
            for line_num, line in enumerate(lines, 1):
                if line_num not in lines_to_delete:
                    f.write(line)
                    total_kept += 1
                else:
                    total_deleted += 1
    
    return total_deleted, total_kept


def run_deletion(input_path: str, db_path: str):
    """
    Run the duplicate deletion pipeline.
    
    Args:
        input_path: Directory containing JSONL files
        db_path: Path to the SQLite database
        
    Returns:
        dict: Deletion statistics
    """
    input_path = str(Path(input_path).resolve())
    db_path = str(Path(db_path).resolve())
    
    print("=" * 60)
    print("Gene Record Deduplication - Deletion")
    print("=" * 60)
    print(f"Input: {input_path}")
    print(f"Database: {db_path}")
    
    # Check database exists
    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}. Run detection first.")
    
    # Get duplicates to delete
    print("\n" + "-" * 40)
    print("Analyzing duplicates...")
    print("-" * 40)
    
    files_to_delete = get_duplicates_to_delete(db_path)
    
    if not files_to_delete:
        print("No duplicates found to delete.")
        return {'deleted': 0, 'kept': 0}
    
    # Rewrite files
    print("\n" + "-" * 40)
    print("Removing duplicates from files...")
    print("-" * 40)
    
    total_deleted, total_kept = rewrite_jsonl_files(input_path, files_to_delete)
    
    # Summary
    print("\n" + "=" * 60)
    print("DELETION COMPLETE")
    print("=" * 60)
    print(f"Records deleted: {total_deleted:,}")
    print(f"Records kept: {total_kept:,}")
    print(f"Files modified: {len(files_to_delete):,}")
    
    return {
        'deleted': total_deleted,
        'kept': total_kept,
        'files_modified': len(files_to_delete)
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Delete duplicate gene records from JSONL files")
    parser.add_argument('-i', '--input', required=True, help="Input directory containing JSONL files")
    parser.add_argument('-d', '--db', required=True, help="Database path from detection step")
    
    args = parser.parse_args()
    
    run_deletion(args.input, args.db)

