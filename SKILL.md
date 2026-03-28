---
name: avanza-investment-tracker
description: "Process Avanza CSV exports, calculate TWRR/Modified Dietz returns, and track portfolio performance. Use when importing stock transactions, calculating investment returns, or managing portfolio data."
---

# Avanza Investment Tracker

Parse transaction CSVs and compute portfolio performance metrics.

## Quick Start

Work from skill root (where SKILL.md is located):

```bash
# 1. Create data directory at skill root
mkdir -p data

# 2. Copy template for special cases handling
cp assets/special_cases_template.json data/special_cases.json

# 3. Import your CSV (place it in data/ first)
python scripts/cli.py import data/transactions.csv

# 4. Calculate stats
python scripts/cli.py stats --update-prices auto
```

## Data Storage

**CREATE THIS DIRECTORY:** `data/` at skill root (same level as `scripts/`)

Store runtime files here:
- `data/asset_data.db` - SQLite database (auto-created on first import)
- `data/special_cases.json` - Corporate actions config (copy from assets/)
- `data/*.csv` - Your transaction exports

**DO NOT** store data in `scripts/` - it gets packaged with the skill.

## CLI Reference

Run all commands from skill root:

| Command | Description |
|---------|-------------|
| `python scripts/cli.py import data/file.csv` | Import transactions from CSV |
| `python scripts/cli.py stats` | Show performance stats |
| `python scripts/cli.py stats --update-prices auto` | Update prices, then show stats |
| `python scripts/cli.py accounts` | Show account summaries |
| `python scripts/cli.py status` | Check system status |
| `python scripts/cli.py reset --confirm` | Clear database (DESTRUCTIVE) |

## File Structure

```
.
├── SKILL.md              # This file
├── requirements.txt      # pip dependencies
├── assets/               # Templates and boilerplate
│   └── special_cases_template.json
├── scripts/              # Python code
│   ├── cli.py           # Main CLI entry
│   ├── data_parser.py
│   ├── database_handler.py
│   └── calculate_stats.py
└── data/                # YOUR DATA (create this)
    ├── asset_data.db
    ├── special_cases.json
    └── *.csv
```

## Dependencies

- `requests` - For fetching stock prices
- Standard library: `sqlite3`, `csv`, `json`, `datetime`, `argparse`

Install: `pip install -r requirements.txt`

## Special Cases

Corporate actions (splits, spin-offs) may need manual rules:

1. Copy template: `cp assets/special_cases_template.json data/special_cases.json`
2. Edit `data/special_cases.json` with your rules
3. Re-import if needed - parser reads this automatically

## See Also

- **Detailed workflows**: See [references/workflows.md](references/workflows.md)
- **Troubleshooting**: See [references/troubleshooting.md](references/troubleshooting.md)
