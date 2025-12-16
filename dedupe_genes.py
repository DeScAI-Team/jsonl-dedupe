"""
Gene Record Deduplication - Detection Script
Finds exact duplicates and near-duplicates across JSONL files.
Uses SQLite for memory-efficient processing of large datasets.
"""

import json
import glob
import hashlib
import sqlite3
from pathlib import Path
from tqdm import tqdm
import random
from difflib import SequenceMatcher


def create_db(db_path: str):
    """Create SQLite database for storing hashes."""
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA journal_mode = OFF')
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA cache_size = 1000000')
    conn.execute('PRAGMA temp_store = MEMORY')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS records (
            hash TEXT,
            filename TEXT,
            line_num INTEGER
        )
    ''')
    return conn


def find_exact_duplicates(input_path: str, db_path: str):
    """
    Find exact duplicates using optimized SQLite bulk insert.
    
    Args:
        input_path: Directory containing JSONL files
        db_path: Path for SQLite database
        
    Returns:
        tuple: (connection, unique_dupe_groups, total_dupe_records, total_records)
    """
    # Remove old db if exists
    db_file = Path(db_path)
    if db_file.exists():
        db_file.unlink()
    
    conn = create_db(db_path)
    
    jsonl_files = glob.glob(str(Path(input_path) / "*_full.jsonl"))
    print(f"Processing {len(jsonl_files)} JSONL files")
    
    total_records = 0
    batch = []
    batch_size = 100000
    
    for filepath in tqdm(jsonl_files, desc="Hashing records"):
        filename = Path(filepath).name
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        data = json.loads(line)
                        text = data.get('text', '')
                        if text:
                            text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
                            batch.append((text_hash, filename, line_num))
                            total_records += 1
                            
                            if len(batch) >= batch_size:
                                conn.executemany('INSERT INTO records VALUES (?, ?, ?)', batch)
                                batch = []
                    except json.JSONDecodeError:
                        pass
    
    if batch:
        conn.executemany('INSERT INTO records VALUES (?, ?, ?)', batch)
    
    conn.commit()
    
    print(f"\nTotal records: {total_records:,}")
    print("Creating index...")
    conn.execute('CREATE INDEX IF NOT EXISTS idx_hash ON records(hash)')
    conn.commit()
    
    print("Finding duplicates...")
    
    cursor = conn.execute('''
        SELECT hash, COUNT(*) as cnt 
        FROM records 
        GROUP BY hash 
        HAVING cnt > 1
    ''')
    
    duplicate_hashes = cursor.fetchall()
    total_dupe_records = sum(cnt for _, cnt in duplicate_hashes)
    
    return conn, len(duplicate_hashes), total_dupe_records, total_records


def sample_for_near_dupes(input_path: str, sample_size: int = 2000):
    """Reservoir sample records for near-duplicate detection."""
    reservoir = []
    count = 0
    
    jsonl_files = glob.glob(str(Path(input_path) / "*_full.jsonl"))
    
    for filepath in tqdm(jsonl_files, desc="Sampling for near-dupes"):
        filename = Path(filepath).name
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        data = json.loads(line)
                        text = data.get('text', '')
                        if text:
                            count += 1
                            if len(reservoir) < sample_size:
                                reservoir.append((filename, line_num, text))
                            else:
                                j = random.randint(0, count - 1)
                                if j < sample_size:
                                    reservoir[j] = (filename, line_num, text)
                    except json.JSONDecodeError:
                        pass
    
    return reservoir


def find_near_duplicates(records: list, threshold: float = 0.95):
    """Find near-duplicates using SequenceMatcher."""
    near_dupes = []
    n = len(records)
    total_comparisons = n * (n - 1) // 2
    
    print(f"Comparing {n} sampled records ({total_comparisons:,} comparisons)")
    
    with tqdm(total=total_comparisons, desc="Finding near-duplicates") as pbar:
        for i in range(n):
            for j in range(i + 1, n):
                text1 = records[i][2]
                text2 = records[j][2]
                
                len1, len2 = len(text1), len(text2)
                if min(len1, len2) / max(len1, len2) < threshold:
                    pbar.update(1)
                    continue
                
                matcher = SequenceMatcher(None, text1, text2)
                if matcher.quick_ratio() >= threshold:
                    ratio = matcher.ratio()
                    if ratio >= threshold:
                        near_dupes.append((
                            (records[i][0], records[i][1]),
                            (records[j][0], records[j][1]),
                            ratio,
                            text1[:100],
                            text2[:100]
                        ))
                
                pbar.update(1)
    
    return near_dupes


def save_results(conn, input_path: str, total_records: int, unique_dupe_groups: int, 
                 total_dupe_records: int, near_dupes: list):
    """Save duplicate detection results to file."""
    results_file = Path(input_path) / "dedup_results.txt"
    
    with open(results_file, 'w', encoding='utf-8') as f:
        f.write(f"Total records: {total_records:,}\n")
        f.write(f"Exact duplicate groups: {unique_dupe_groups:,}\n")
        f.write(f"Records in duplicates: {total_dupe_records:,}\n")
        f.write(f"Duplicate rate: {total_dupe_records/total_records*100:.2f}%\n\n")
        
        f.write("ALL DUPLICATE GROUPS\n")
        f.write("=" * 60 + "\n\n")
        
        cursor = conn.execute('''
            SELECT hash, COUNT(*) as cnt 
            FROM records 
            GROUP BY hash 
            HAVING cnt > 1
            ORDER BY cnt DESC
        ''')
        
        for hash_val, cnt in cursor.fetchall():
            f.write(f"Hash: {hash_val} ({cnt} occurrences)\n")
            locs = conn.execute(
                'SELECT filename, line_num FROM records WHERE hash = ?',
                (hash_val,)
            ).fetchall()
            for filename, line_num in locs:
                f.write(f"  {filename}:L{line_num}\n")
            f.write("\n")
        
        f.write("\nNEAR DUPLICATES (95%+)\n")
        f.write("=" * 60 + "\n\n")
        for (f1, l1), (f2, l2), ratio, p1, p2 in near_dupes:
            f.write(f"Similarity: {ratio:.2%}\n")
            f.write(f"  A: {f1}:L{l1}\n")
            f.write(f"  B: {f2}:L{l2}\n\n")
    
    return results_file


def run_detection(input_path: str, db_path: str = None, sample_size: int = 2000):
    """
    Run the full duplicate detection pipeline.
    
    Args:
        input_path: Directory containing JSONL files
        db_path: Path for SQLite database (defaults to input_path/dedup.db)
        sample_size: Number of records to sample for near-duplicate detection
        
    Returns:
        dict: Results including stats and paths
    """
    input_path = str(Path(input_path).resolve())
    
    if db_path is None:
        db_path = str(Path(input_path) / "dedup.db")
    else:
        db_path = str(Path(db_path).resolve())
    
    print("=" * 60)
    print("Gene Record Deduplication - Detection")
    print("=" * 60)
    print(f"Input: {input_path}")
    print(f"Database: {db_path}")
    
    # Find exact duplicates
    print("\n" + "-" * 40)
    print("EXACT DUPLICATES")
    print("-" * 40)
    
    conn, unique_dupe_groups, total_dupe_records, total_records = find_exact_duplicates(
        input_path, db_path
    )
    
    print(f"\nUnique texts with duplicates: {unique_dupe_groups:,}")
    print(f"Total records in duplicates: {total_dupe_records:,}")
    print(f"Duplicate rate: {total_dupe_records/total_records*100:.2f}%")
    
    # Show top duplicates
    print("\nTop 5 duplicate groups (by count):")
    cursor = conn.execute('''
        SELECT hash, COUNT(*) as cnt 
        FROM records 
        GROUP BY hash 
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 5
    ''')
    
    for i, (hash_val, cnt) in enumerate(cursor.fetchall()):
        print(f"\n  Duplicate set {i+1} ({cnt} occurrences):")
        locs = conn.execute(
            'SELECT filename, line_num FROM records WHERE hash = ? LIMIT 3',
            (hash_val,)
        ).fetchall()
        for filename, line_num in locs:
            print(f"    - {filename}:L{line_num}")
        if cnt > 3:
            print(f"    ... and {cnt - 3} more")
    
    # Near duplicates
    print("\n" + "-" * 40)
    print(f"NEAR DUPLICATES (95%+, {sample_size} sample)")
    print("-" * 40)
    
    random.seed(42)
    sampled = sample_for_near_dupes(input_path, sample_size)
    near_dupes = find_near_duplicates(sampled, threshold=0.95)
    
    print(f"\nNear-duplicate pairs found: {len(near_dupes):,}")
    
    if near_dupes:
        print("\nSample near-duplicates (first 5):")
        for i, ((f1, l1), (f2, l2), ratio, p1, p2) in enumerate(near_dupes[:5]):
            print(f"\n  Pair {i+1} ({ratio:.2%}):")
            print(f"    A: {f1}:L{l1}: {p1}...")
            print(f"    B: {f2}:L{l2}: {p2}...")
    
    # Save results
    results_file = save_results(conn, input_path, total_records, unique_dupe_groups, 
                                total_dupe_records, near_dupes)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total records: {total_records:,}")
    print(f"Exact duplicate groups: {unique_dupe_groups:,}")
    print(f"Records in exact duplicates: {total_dupe_records:,}")
    print(f"Duplicate rate: {total_dupe_records/total_records*100:.2f}%")
    print(f"Near-duplicate pairs (95%+): {len(near_dupes):,}")
    print(f"\nResults saved to: {results_file}")
    print(f"Database: {db_path}")
    
    conn.close()
    
    return {
        'total_records': total_records,
        'unique_dupe_groups': unique_dupe_groups,
        'total_dupe_records': total_dupe_records,
        'near_dupe_pairs': len(near_dupes),
        'results_file': str(results_file),
        'db_path': db_path,
        'input_path': input_path
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Detect duplicate gene records in JSONL files")
    parser.add_argument('-i', '--input', required=True, help="Input directory containing JSONL files")
    parser.add_argument('-d', '--db', default=None, help="Database path (default: input_dir/dedup.db)")
    parser.add_argument('-s', '--sample', type=int, default=2000, help="Sample size for near-duplicate detection")
    
    args = parser.parse_args()
    
    run_detection(args.input, args.db, args.sample)
