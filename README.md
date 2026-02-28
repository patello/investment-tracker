# Investment Tracker

## Description

This project aims to create a tool for tracking stocks and investments on investment platforms. The original idea for this project was when I was about to buy a SteamDeck. Then I thought better of it and started to wonder how much the money would grow over time if I invested it instead. I realized that it would require me to keep track of the assets that I purchase a particular month, even if I sell them and buy new ones later on. Or if I get dividends and reinvest them.

With this project, I am able to parse data from my investment platform, keep latest asset values up to date and calculate relevant statistics.

This project is a work in progress and will be updated as I go along.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Features

- Parse and store data from an investment platform.
- Keep track of where money invested each month is moved and grown over time.
- Calculate statistics for months and years:
    - Deposit
    - Withdrawal
    - Current Value
    - Total Gain/Loss
    - Realized Gain/Loss
    - Unrealized Gain/Loss
    - APY (Annual Percentage Yield)

## Installation

1. Clone the repository.
2. Install the dependencies with `pip install -r requirements.txt`.

## Usage

### Command Line Interface (CLI)

A unified CLI is available via `cli.py`. It provides subcommands for all major operations:

```bash
# Parse a CSV file (automatically detects old/new Avanza format)
python cli.py parse data/newdata.csv

# Parse and immediately process transactions
python cli.py parse data/newdata.csv --process

# Process transactions already in the database
python cli.py process

# Reset processed flag for all transactions (useful for reprocessing)
python cli.py reset

# Fetch latest prices from Avanza API
python cli.py update-prices

# Calculate monthly and yearly statistics
python cli.py calculate-stats

# Show statistics (default: month, current deposits)
python cli.py show-stats
python cli.py show-stats --period year --deposits all --accumulated

# Run the full pipeline: parse, process, update prices, calculate stats, show stats
python cli.py run-all data/newdata.csv
```

All commands accept optional `--database` and `--special-cases` arguments to override default paths:

```bash
python cli.py --database path/to/db.db --special-cases path/to/special.json parse data.csv
```

### CSV Format Support

The parser automatically detects whether your CSV file uses Avanza's old or new export format (the new format includes a `Transaktionsvaluta` column). The following transaction types are recognized:

- Insättning (deposit)
- Uttag (withdrawal)
- Köp (purchase)
- Sälj (sale)
- Utdelning (dividend)
- Räntor / Ränta / Inlåningsränta (interest)
- Utländsk källskatt / Prelskatt / Preliminärskatt (taxes)
- Byte / Övrigt (listing changes)
- Tillgångsinsättning (asset deposit)

Empty numeric fields (like `Antal`, `Kurs`) are treated as zero.

**Price fetching note:** The `update-prices` command (and the original `calculate_stats.py`) fetches current asset prices from Avanza's public search API (`www.avanza.se/_api/search/filtered-search`). This API is intended for web frontend use and may have rate limits or terms of service restrictions. Use at your own risk and consider using official APIs if available. Always review the website's terms of service before using their data.

### Original Scripts (Alternative)

You can still use the original scripts directly:

1. Add data in the `data` folder.
    - You might need to create a "special_cases.json" file in order to match and replace certain values in the data. See file specification in the documentation for the SpecialCases class.
    - You will probably need to update the process_transactions function in the DataParser class to match your data.
2. Run the script "data_parser.py" with `python data_parser.py`.
3. Run the script calculate_stats.py with `python calculate_stats.py`.
    - Note: This script fetches current asset prices from Avanza's public search API (`www.avanza.se/_api/search/filtered-search`). This API is intended for web frontend use and may have rate limits or terms of service restrictions. Use at your own risk and consider using official APIs if available. Always review the website's terms of service before using their data.

## Contributing

Thank you for your interest in contributing to this project! As a single-person hobby project, contributions are not expected but always welcome. If you have any ideas, bug fixes, or improvements, feel free to submit a pull request.

To contribute to this project, please follow these guidelines:

1. Fork the repository and create a new branch for your contribution.
2. Make your changes and ensure that the code is clean and well-documented.
3. Test your changes thoroughly to ensure they do not introduce any regressions.
4. Submit a pull request, explaining the purpose and details of your contribution.

Please note that as a hobby project, there may be limited resources available for reviewing and merging pull requests. Your patience is appreciated.

Thank you for your support and happy coding!

## License

Please make sure that you are allowed to access information from the price source that you are using. The author of this project is not responsible for any legal issues that may arise from the use of this project. The url used in the example script is only for demonstration purposes and should not be used without permission.

The code in this project is licensed under the MIT License. See [LICENSE](LICENSE) file for details.

Please note that this project uses other libraries. The licenses for these libraries are as follows:

- Libraries in `requirements.txt`:
  - requests: Apache License 2.0


Please respect the licenses for these libraries when using this project.