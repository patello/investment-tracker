---
name: avanza-investment-tracker
description: "Process Avanza CSV exports, calculate TWRR/Modified Dietz returns, and track portfolio performance. Use when importing stock transactions, calculating investment returns, or managing portfolio data. Reads/writes a local SQLite database, and (for live prices and risk metrics) makes outbound HTTPS requests to Avanza, Riksbanken, and Yahoo Finance. Includes irreversible deletion commands (reset --hard, delete-tx, account delete) — see Security and Data Access in SKILL.md/README."
metadata:
  openclaw:
    requires:
      bins:
        - python3
    permissions:
      filesystem:
        - "read/write: user-specified local SQLite database (--database path); no other files accessed"
      network:
        - "https://www.avanza.se (price/FX/chart data for held assets; optional, disable with --update-prices never)"
        - "https://api.riksbank.se (reference rates for risk metrics; optional)"
        - "https://query1.finance.yahoo.com (benchmark index prices for beta/correlation; optional)"
---

# Avanza Investment Tracker

Parse transaction CSVs and compute portfolio performance metrics.

## Security and Data Access

Be aware of what this skill does before running it:

- **Local database writes:** imports, price updates, and portfolio management read and write a local SQLite database.
- **Network access (optional but on by default):** live price/FX lookups contact Avanza's public API with the asset names in your portfolio; risk metrics (`--risk`, `--beta`) may also contact the Riksbanken API and Yahoo Finance (benchmark ticker + date range). Use `--update-prices never` to stay fully offline.
- **Irreversible deletions:** `reset --hard`, `delete-tx`, `account allocate --undo`, and `account delete` permanently remove transactions and rebuild derived tables. There is no built-in undo. Back up your database first (e.g. `cp` or git), and prefer `delete-tx --dry-run` to preview. Avoid broad selectors like `delete-tx --since` unless you are certain of the blast radius.

## Quick Start

Run commands from your workspace root, specifying the paths to your database and CSV:

```bash
# 1. Import new transactions
python path/to/cli.py --database data/asset_data.db import path/to/transactions.csv

# 2. Update price cache and show statistics
python path/to/cli.py --database data/asset_data.db stats --update-prices auto

# 3. View portfolio allocation and APY
python path/to/cli.py --database data/asset_data.db portfolio --account default
```

## Data Storage Pattern

**User data lives OUTSIDE the skill directory.** Recommended structure:

```
workspace-finance/
├── skills/avanza-investment-tracker/   # Portable skill logic
│   ├── SKILL.md
│   ├── scripts/
│   └── assets/
└── data/avanza/                        # Private portfolio data
    ├── transactions.csv
    ├── special_cases.json
    └── asset_data.db
```

<!-- INSERT:SECTION:## CLI Reference -->

<!-- INSERT:SECTION:## Special Cases -->

## See Also

- **Detailed workflows**: [references/workflows.md](references/workflows.md)
- **Troubleshooting guide**: [references/troubleshooting.md](references/troubleshooting.md)
