---
name: avanza-investment-tracker
description: "A toolset to track investments and parse stock transactions using Avanza and other broker CSV exports. Use this skill when you need to calculate Time-Weighted Return (TWRR), Modified Dietz returns, or process transaction CSV data into a sqlite database."
metadata: {"openclaw": {"requires": {"bins": ["python3"]}}}
---

# Avanza Investment Tracker

Process investment CSV exports and calculate portfolio performance metrics (TWRR, Modified Dietz).

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Create Required Directories and Files
```bash
mkdir -p data
# Create special cases config (or copy from template)
cp assets/special_cases_template.json data/special_cases.json
```

### 3. Import Transaction Data
Place your CSV export from Avanza (or compatible broker) in the data directory, then run:
```bash
cd scripts
python cli.py import ../data/your_transactions.csv
```

### 4. Calculate Performance Statistics
```bash
python cli.py stats --update-prices auto
```

### 5. View Account Status
```bash
python cli.py accounts
python cli.py status
```

## CLI Commands Reference

All commands assume execution from the `scripts/` directory.

### Import Data
**Command:** `python cli.py import <csv_file> [options]`

**Example:**
```bash
python cli.py import ../data/avanza_export.csv --database ../data/portfolio.db
```

**Options:**
- `--database PATH` - SQLite database file (default: data/asset_data.db)
- `--special-cases PATH` - JSON file for handling corporate actions (default: data/special_cases.json)

### Calculate Statistics
**Command:** `python cli.py stats [options]`

**Examples:**
```bash
python cli.py stats                    # Show current stats
python cli.py stats --update-prices auto  # Update prices and calculate
python cli.py stats --from-date 2024-01-01  # Stats from specific date
```

**Options:**
- `--update-prices {yes,no,auto}` - Fetch current prices before calculation
- `--from-date YYYY-MM-DD` - Start date for calculations
- `--to-date YYYY-MM-DD` - End date for calculations

### Show Accounts
**Command:** `python cli.py accounts [options]`

Displays account summaries including asset values and cash positions.

**Examples:**
```bash
python cli.py accounts
python cli.py accounts --database ../data/portfolio.db
```

### Check Status
**Command:** `python cli.py status`

Shows system status including database info and cached data.

### Reset Database
**Command:** `python cli.py reset`

**WARNING:** This clears all data from the database. Use with caution.

**Example:**
```bash
python cli.py reset --confirm
```

## File Structure

```
.
├── SKILL.md                       # This file
├── LICENSE                        # MIT-0 License
├── requirements.txt               # Python dependencies
├── .gitignore                     # Git ignore rules
├── assets/
│   └── special_cases_template.json   # Template for corporate actions
└── scripts/                       # Main executables
    ├── cli.py                     # CLI entry point
    ├── database_handler.py        # SQLite database management
    ├── data_parser.py             # CSV parsing and transaction processing
    ├── calculate_stats.py         # TWRR and Modified Dietz calculations
    └── data/                      # Runtime data directory (not versioned)
        ├── asset_data.db          # SQLite database (created on first run)
        └── special_cases.json     # Corporate actions configuration
```

## Special Cases Configuration

Some transactions require manual handling (stock splits, spin-offs, ticker changes).

1. Copy the template: `cp assets/special_cases_template.json data/special_cases.json`
2. Edit `data/special_cases.json` to define special handling rules
3. The parser will reference this file when processing ambiguous transactions

## Dependencies

From `requirements.txt`:
- `requests` - For fetching current stock prices
- Standard library: `sqlite3`, `csv`, `json`, `datetime`, `argparse`

## Workflow for New Portfolio

1. **Setup:** Install deps, create directories, copy special cases template
2. **First Import:** `python cli.py import data/export.csv`
3. **Review:** `python cli.py status` - verify accounts and transactions
4. **Calculate:** `python cli.py stats --update-prices yes`
5. **Ongoing:** Import new CSV files periodically, then recalculate stats

## Troubleshooting

- **Database locked:** Ensure no other process is using the .db file
- **Import errors:** Check CSV format matches Avanza export format
- **Missing prices:** `--update-prices auto` uses external API; check network
