---
name: avanza-investment-tracker
description: "A toolset to track investments and parse stock transactions using Avanza and other broker CSV exports. Use this skill when you need to calculate Time-Weighted Return (TWRR), Modified Dietz returns, or process transaction CSV data into a sqlite database."
metadata: {"openclaw": {"requires": {"bins": ["python3"]}}}
---

# Investment Tracker

## Overview

This skill provides a Python-based pipeline for parsing investment transaction CSVs (like those from Avanza) into a local SQLite database, and calculating performance metrics like Time-Weighted Rate of Return (TWRR) and Modified Dietz.

## Core Capabilities

### 1. Data Parsing (`data_parser.py`)
Parses CSV exports of stock transactions. Handles buys, sells, dividends, deposits, and special corporate actions (splits, spin-offs).

### 2. Database Management (`database_handler.py`)
Initializes and maintains a SQLite database (`data/investments.db`) to store accounts, assets, transactions, and daily performance snapshots.

### 3. Performance Calculation (`calculate_stats.py`)
Computes portfolio performance metrics on a daily basis:
- **TWRR (Time-Weighted Rate of Return)**
- **Modified Dietz** (capital-weighted returns)
- **Total Profit/Loss**

### 4. CLI Interface (`cli.py`)
Provides the main entrypoint to run imports, trigger calculations, and view portfolio status. 

