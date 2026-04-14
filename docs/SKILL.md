---
name: avanza-investment-tracker
version: "2.0.2"
description: "Process Avanza CSV exports, calculate TWRR/Modified Dietz returns, and track portfolio performance. Use when importing stock transactions, calculating investment returns, or managing portfolio data."
---

# Avanza Investment Tracker

Parse transaction CSVs and compute portfolio performance metrics.

## Quick Start

Run from skill root with data paths pointing to your workspace:

```bash
# Import transactions (data lives outside skill)
python scripts/cli.py import ../data/avanza/transactions.csv

# Calculate stats with auto price update
python scripts/cli.py stats --update-prices auto --database ../data/avanza/asset_data.db

# Or use defaults (assumes you cd into a data directory first)
cd ../data/avanza
python ../../skills/avanza-investment-tracker/scripts/cli.py import transactions.csv
```

## Data Storage Pattern

**User data lives OUTSIDE the skill directory.** Recommended structure:

```
workspace-finance/
├── skills/avanza-investment-tracker/   # Portable skill (shareable)
│   ├── SKILL.md
│   ├── scripts/
│   └── assets/
└── data/avanza/                        # Your private data
    ├── transactions.csv
    ├── special_cases.json
    └── asset_data.db
```

The skill provides logic. Your data stays private and portable.

## CLI Reference

| Command | Description |
|---------|-------------|
| `python scripts/cli.py import FILE` | Import transactions from CSV |
| `python scripts/cli.py stats` | Show performance stats |
| `python scripts/cli.py stats --update-prices auto` | Update prices, then show stats |
| `python scripts/cli.py accounts` | Show account summaries |
| `python scripts/cli.py status` | Check system status |
| `python scripts/cli.py reset --confirm` | Clear database (DESTRUCTIVE) |

All commands accept:
- `--database PATH` (default: `data/asset_data.db`)
- `--special-cases PATH` (default: `data/special_cases.json`)

## Skill Contents

```
avanza-investment-tracker/
├── SKILL.md              # This file
├── requirements.txt      # pip dependencies
├── assets/               # Templates (copy to your data dir)
│   └── special_cases_template.json
├── scripts/              # Python code
│   ├── cli.py           # Main CLI entry
│   ├── data_parser.py
│   ├── database_handler.py
│   └── calculate_stats.py
└── references/           # Detailed guides (loaded as needed)
    ├── workflows.md
    └── troubleshooting.md
```

## Dependencies

- `requests` - For fetching stock prices
- Standard library: `sqlite3`, `csv`, `json`, `datetime`, `argparse`

Install: `pip install -r requirements.txt`

## Special Cases

Corporate actions (splits, spin-offs) may need manual rules:

1. Copy template: `cp assets/special_cases_template.json ../data/avanza/special_cases.json`
2. Edit with your rules
3. Import with `--special-cases ../data/avanza/special_cases.json`

## See Also

- **Detailed workflows**: See [references/workflows.md](references/workflows.md)
- **Troubleshooting**: See [references/troubleshooting.md](references/troubleshooting.md)

## Account Filtering

By default, stats show all accounts. Use `settings default-accounts` to set your preferred accounts:

```bash
# Set default accounts (your main portfolio)
python scripts/cli.py --database ../data/avanza/asset_data.db settings default-accounts "1234567,Savings Account,9876543"

# View stats for default accounts only
python scripts/cli.py --database ../data/avanza/asset_data.db stats --account default

# Or specify accounts directly
python scripts/cli.py stats --account "1234567,Savings Account"

# View all accounts
python scripts/cli.py stats --account all
```

## Account Nicknames

Set friendly names for accounts to make output more readable:

```bash
# Set a nickname
python scripts/cli.py settings account-nickname 1234567 "Main Account"

# List all nicknames
python scripts/cli.py settings account-nickname --list

# Remove a nickname
python scripts/cli.py settings account-nickname --remove 1234567
```

Nicknames appear in `accounts` and `stats` output instead of raw account numbers.

## Publishing & PII Guidelines

### PII Handling (ZERO TOLERANCE)

- **NEVER** include real account numbers, financial figures, or portfolio values in PR descriptions, commit messages, or any public-facing content.
- Even "fake" numbers clearly derived from real data constitute a leak.
- PR descriptions must be **technical summaries of the change**, not before/after reports with data.
- All examples in documentation must use placeholder values (e.g., `1234567`, `Savings Account`).

### Publish Verification Procedure

After every ClawHub publish:

1. Install to a temp directory: `clawhub install avanza-investment-tracker@<version>`
2. Diff-check key files against source: compare line counts and grep for expected code changes
3. Only consider the publish successful once the installed package contains the actual changes

### Common Pitfalls

- ClawHub may serve stale cached versions — always verify with a fresh install
- Rate limits on the registry can delay installs — wait and retry with explicit version
- Publishing without `--version` flag may fail silently — always specify version explicitly

## Changelog

### v2.0.2
- Fix: ensure transfer_net code is included in published package
- Added publishing verification and PII guidelines to SKILL.md

### v2.0.1
- Republish: ensure transfer_net code is included in package

### v2.0.0
- Fix phantom gains/losses from internal transfers between accounts
- Added `transfer_net` column to cohort data for accurate per-account gain/loss

### v1.1.0
- Fix phantom gains/losses from internal transfers between accounts
- Added `transfer_net` column to cohort data for accurate per-account gain/loss

