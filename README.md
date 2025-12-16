# JSONl Deduplication Tool

A CLI tool for detecting and removing duplicate records from JSONL files. Designed for cleaning ncbi gene records but should work for any JSONL files.

## Features

- **Exact duplicate detection** using MD5 hashing with SQLite storage (memory-efficient)
- **Near-duplicate detection** (95%+ similarity) using reservoir sampling
- **Interactive or automated deletion** of duplicate records
- Handles large datasets (100M+ records) without running out of memory

## Installation

```bash
# Clone or download the repository
cd genes

# Create virtual environment
python -m venv venv

# Activate (Windows)
.\venv\Scripts\activate
# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Basic Usage (Interactive)

```bash
python dedupe_cli.py -i /path/to/jsonl/files
```

This will:
1. Scan all `*_full.jsonl` files in the directory
2. Detect exact duplicates and near-duplicates
3. Save results to `dedup_results.txt`
4. Prompt you to delete duplicates (yes/no)

### Auto-Delete Mode

```bash
python dedupe_cli.py -i /path/to/jsonl/files --delete
```

Skips the confirmation prompt and automatically deletes duplicates.

### Custom Database Path

```bash
python dedupe_cli.py -i /path/to/jsonl/files -d /path/to/cache/dedup.db
```

By default, the SQLite database is stored in the input directory.

### All Options

```
usage: dedupe_cli.py [-h] -i INPUT [-d DB] [--delete] [-s SAMPLE]

options:
  -h, --help            show this help message and exit
  -i INPUT, --input INPUT
                        Path to directory containing JSONL files (required)
  -d DB, --db DB        Path for SQLite database (default: same as input directory)
  --delete              Skip confirmation prompt and automatically delete duplicates
  -s SAMPLE, --sample SAMPLE
                        Sample size for near-duplicate detection (default: 2000)
```

## Individual Scripts

You can also run the detection and deletion steps separately:

### Detection Only

```bash
python dedupe_genes.py -i /path/to/jsonl/files
```

### Deletion Only (after detection)

```bash
python dedupe_delete.py -i /path/to/jsonl/files -d /path/to/dedup.db
```

## Output Files

- `dedup_results.txt` - Full list of all duplicate groups with file locations
- `dedup.db` - SQLite database with hash index (can be deleted after processing)

## How It Works

1. **Hashing Phase**: Each record's text is MD5 hashed and stored in SQLite with its file location
2. **Index Creation**: A hash index is created for fast duplicate lookup
3. **Duplicate Detection**: SQL query finds all hashes with count > 1
4. **Near-Duplicate Sampling**: Reservoir sampling selects records for SequenceMatcher comparison
5. **Deletion**: For each duplicate group, keeps the first occurrence and removes the rest

## Expected JSONL Format

```json
{"text": "Gene description text here..."}
{"text": "Another gene record..."}
```

## Performance

Tested on 102 million records across 3,262 files:
- Detection: ~5-10 minutes
- Memory usage: <2GB (uses disk-based SQLite)
- Duplicate rate found: ~4.65%

## License

MIT

