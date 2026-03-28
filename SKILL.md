---
name: avanza-investment-tracker
description: "A toolset to track investments and parse stock transactions using Avanza and other broker CSV exports. Use this skill when you need to calculate Time-Weighted Return (TWRR), Modified Dietz returns, or process transaction CSV data into a sqlite database."
metadata: {"openclaw": {"requires": {"bins": ["python3"]}}}
---

# Avanza Investment Tracker

Process investment CSV exports and calculate portfolio performance metrics (TWRR, Modified Dietz).

## Working Directory

**CRITICAL:** All commands must be run from the skill ROOT directory (where SKILL.md is located).

The skill expects this structure:
```
.
├── SKILL.md              # You are here
├── scripts/              # Python executables
│   ├── cli.py
│   ├── data_parser.py
│   ├── database_handler.py
│   └── calculate_stats.py
└── data/                 # Runtime data (CREATED BY YOU)
    ├── asset_data.db     # SQLite database (auto-created)
    └── special_cases.json # Config file (copy from template)
```

## Quick Start

### 1. Install Dependencies
From skill root:
```bash
pip install -r requirements.txt
```

### 2. Create Data Directory
Create `data/` at the skill root (same level as `scripts/`):
```bash
mkdir -p data
cp assets/special_cases_template.json data/special_cases.json
```

### 3. Import Transaction Data
Place your CSV export in `data/`, then from skill root run:
```bash
python scripts/cli.py import data/your_transactions.csv
```

### 4. Calculate Statistics
```bash
python scripts/cli.py stats --update-prices auto
```

### 5. View Status
```bash
python scripts/cli.py accounts
python scripts/cli.py status
```

## CLI Commands Reference

**Working Directory:** Skill root (where SKILL.md lives)

### Import Data
```bash
python scripts/cli.py import data/transactions.csv
```

**Default Paths (relative to skill root):**
- Database: `data/asset_data.db`
- Special cases: `data/special_cases.json`

### Calculate Statistics
```bash
python scripts/cli.py stats
python scripts/cli.py stats --update-prices auto
```

### Show Accounts
```bash
python scripts/cli.py accounts
```

### Check Status
```bash
python scripts/cli.py status
```

### Reset Database
```bash
python scripts/cli.py reset --confirm
```

## Data Storage Rules

**DO:**
- Store all data in `data/` at skill root
- Keep input CSVs in `data/`
- Store database at `data/asset_data.db`
- Store config at `data/special_cases.json`

**DON'T:**
- Store data in `scripts/` directory
- Store data outside the skill folder
- Modify files in `assets/` (templates only)

## File Structure

### Skill Files (do not modify)
```
.
├── SKILL.md              # This documentation
├── requirements.txt      # Python dependencies
├── .gitignore           # Git ignore rules
├── assets/              # Templates
│   └── special_cases_template.json
└── scripts/             # Python code
    ├── cli.py
    ├── database_handler.py
    ├── data_parser.py
    └── calculate_stats.py
```

### Runtime Data (you create this)
```
data/                    # Create this directory
├── asset_data.db        # SQLite database (auto-created on import)
├── special_cases.json   # Copy from assets/template
└── *.csv                # Your transaction exports
```

## Special Cases Configuration

Some transactions need manual rules (splits, spin-offs, ticker changes):

1. Copy template: `cp assets/special_cases_template.json data/special_cases.json`
2. Edit `data/special_cases.json` with your rules
3. The parser reads this automatically

## Dependencies

- `requests` - Fetch stock prices
- Standard library: `sqlite3`, `csv`, `json`, `datetime`, `argparse`, `logging`

## Complete Workflow

1. **Setup:** `pip install -r requirements.txt && mkdir -p data`
2. **Configure:** `cp assets/special_cases_template.json data/special_cases.json`
3. **Import:** Place CSV in `data/`, then `python scripts/cli.py import data/file.csv`
4. **Review:** `python scripts/cli.py status`
5. **Calculate:** `python scripts/cli.py stats --update-prices auto`
6. **Repeat:** Add new CSVs periodically, re-run import + stats

## Troubleshooting

- **"No such file or directory":** Are you in skill root? Check with `ls SKILL.md`
- **Database locked:** Close any database viewers
- **Import fails:** Check CSV format matches Avanza export
- **Missing prices:** Check internet connection for price fetching
