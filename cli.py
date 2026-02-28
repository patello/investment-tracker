#!/usr/bin/env python3
"""
CLI interface for the Avanza investment tracker.

Provides command-line access to data parsing, transaction processing,
price updates, and statistics calculation.
"""

import argparse
import sys
import logging
from datetime import datetime

from database_handler import DatabaseHandler
from data_parser import DataParser, SpecialCases
from calculate_stats import StatCalculator


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def parse_data(args):
    """Parse CSV data from Avanza and add to database."""
    try:
        db = DatabaseHandler(args.database)
        special_cases = SpecialCases(args.special_cases) if args.special_cases else None
        data_parser = DataParser(db, special_cases)
        rows_added = data_parser.add_data(args.file)
        print(f"Added {rows_added} rows to the database")
        if getattr(args, 'process', False):
            data_parser.process_transactions()
            print("Transactions processed")
        return 0
    except Exception as e:
        logging.error(f"Failed to parse data: {e}")
        return 1


def process_transactions(args):
    """Process transactions in the database."""
    try:
        db = DatabaseHandler(args.database)
        special_cases = SpecialCases(args.special_cases) if args.special_cases else None
        data_parser = DataParser(db, special_cases)
        data_parser.process_transactions()
        print("Transactions processed successfully")
        return 0
    except Exception as e:
        logging.error(f"Failed to process transactions: {e}")
        return 1


def reset_processed(args):
    """Reset processed flag for all transactions."""
    try:
        db = DatabaseHandler(args.database)
        special_cases = SpecialCases(args.special_cases) if args.special_cases else None
        data_parser = DataParser(db, special_cases)
        data_parser.reset_processed_transactions()
        print("Processed transactions reset")
        return 0
    except Exception as e:
        logging.error(f"Failed to reset processed transactions: {e}")
        return 1


def update_prices(args):
    """Fetch latest prices from Avanza API."""
    try:
        db = DatabaseHandler(args.database)
        stat_calculator = StatCalculator(db)
        stat_calculator.update_prices()
        print("Prices updated")
        return 0
    except Exception as e:
        logging.error(f"Failed to update prices: {e}")
        return 1


def calculate_stats(args):
    """Calculate monthly and yearly statistics."""
    try:
        db = DatabaseHandler(args.database)
        stat_calculator = StatCalculator(db)
        stat_calculator.calculate_stats()
        print("Statistics calculated")
        return 0
    except Exception as e:
        logging.error(f"Failed to calculate statistics: {e}")
        return 1


def show_stats(args):
    """Display statistics."""
    try:
        db = DatabaseHandler(args.database)
        stat_calculator = StatCalculator(db)
        print(f"{args.period.capitalize()} stats:")
        stat_calculator.print_stats(period=args.period, deposits=args.deposits)
        if args.accumulated:
            print(f"Accumulated {args.period} stats:")
            stat_calculator.print_accumulated(period=args.period, deposits=args.deposits)
        return 0
    except Exception as e:
        logging.error(f"Failed to show statistics: {e}")
        return 1


def run_all(args):
    """Run the full pipeline: parse, process, update prices, calculate stats."""
    exit_codes = []
    
    # Parse data if file provided
    if args.file:
        exit_codes.append(parse_data(args))
        if any(ec != 0 for ec in exit_codes):
            return max(exit_codes)
    
    # Process transactions
    exit_codes.append(process_transactions(args))
    if any(ec != 0 for ec in exit_codes):
        return max(exit_codes)
    
    # Update prices
    exit_codes.append(update_prices(args))
    if any(ec != 0 for ec in exit_codes):
        return max(exit_codes)
    
    # Calculate stats
    exit_codes.append(calculate_stats(args))
    if any(ec != 0 for ec in exit_codes):
        return max(exit_codes)
    
    # Show stats
    show_stats(args)
    return max(exit_codes) if exit_codes else 0


def main():
    parser = argparse.ArgumentParser(
        description="Avanza investment tracker CLI",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--database",
        default="data/asset_data.db",
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--special-cases",
        default="data/special_cases.json",
        help="Path to special cases JSON file (optional, defaults to data/special_cases.json)",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    subparsers.required = True
    
    # Parse data command
    parse_parser = subparsers.add_parser("parse", help="Parse CSV data from Avanza")
    parse_parser.add_argument("file", help="Path to CSV file")
    parse_parser.add_argument(
        "--process",
        action="store_true",
        help="Process transactions after parsing",
    )
    parse_parser.set_defaults(func=parse_data)
    
    # Process transactions command
    process_parser = subparsers.add_parser("process", help="Process transactions in database")
    process_parser.set_defaults(func=process_transactions)
    
    # Reset processed transactions command
    reset_parser = subparsers.add_parser("reset", help="Reset processed flag for all transactions")
    reset_parser.set_defaults(func=reset_processed)
    
    # Update prices command
    update_parser = subparsers.add_parser("update-prices", help="Fetch latest prices from Avanza API")
    update_parser.set_defaults(func=update_prices)
    
    # Calculate stats command
    calc_parser = subparsers.add_parser("calculate-stats", help="Calculate monthly and yearly statistics")
    calc_parser.set_defaults(func=calculate_stats)
    
    # Show stats command
    show_parser = subparsers.add_parser("show-stats", help="Display statistics")
    show_parser.add_argument(
        "--period",
        choices=["month", "year"],
        default="month",
        help="Time period to show",
    )
    show_parser.add_argument(
        "--deposits",
        choices=["current", "all"],
        default="current",
        help="Which deposits to include",
    )
    show_parser.add_argument(
        "--accumulated",
        action="store_true",
        help="Show accumulated statistics as well",
    )
    show_parser.set_defaults(func=show_stats)
    
    # Run all command
    runall_parser = subparsers.add_parser("run-all", help="Run full pipeline")
    runall_parser.add_argument(
        "file",
        nargs="?",
        help="Optional CSV file to parse before processing",
    )
    runall_parser.set_defaults(func=run_all)
    
    args = parser.parse_args()
    
    # Special handling for run-all where file might be None
    if args.command == "run-all" and args.file is None:
        # If no file provided, skip parsing step
        pass
    
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()