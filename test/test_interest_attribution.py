"""
Test interest attribution with FIFO capital tracking.
Demonstrates the bug in handle_interest method.
"""
import pytest
import sqlite3
import tempfile
import os
from datetime import date
from data_parser import DataParser
from database_handler import DatabaseHandler

def test_interest_attribution_bug():
    """
    Test scenario from Patello's question:
    1. 2020: Deposit 100 SEK
    2. 2021: Earn 20 SEK interest on the 100 SEK
    3. 2022: Deposit 80 SEK, purchase 100 SEK asset (FIFO uses 2020 capital)
    4. 2023: Earn 20 SEK interest on remaining 100 SEK cash
    
    Expected: 2023 interest split 4/5 to 2022 deposit, 1/5 to 2020 deposit (via 2021 interest)
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Create test transactions
        test_transactions = [
            # 2020: Initial deposit
            (date(2020, 1, 1), '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            
            # 2021: Interest earned
            (date(2021, 1, 1), '1111111', 'Ränta', '', 0, 0, 20, 0, 'SEK', ''),
            
            # 2022: New deposit and purchase
            (date(2022, 1, 1), '1111111', 'Insättning', '', 0, 0, 80, 0, 'SEK', ''),
            (date(2022, 1, 2), '1111111', 'Köp', 'Asset A', 10, 10, -100, 0, 'SEK', 'TEST001'),
            
            # 2023: More interest
            (date(2023, 1, 1), '1111111', 'Ränta', '', 0, 0, 20, 0, 'SEK', ''),
        ]
        
        # Add transactions to database
        db.connect()
        cur = db.conn.cursor()
        for trans in test_transactions:
            cur.execute('''
                INSERT INTO transactions 
                (date, account, transaction_type, asset_name, amount, price, total, courtage, currency, isin, processed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''', trans)
        db.conn.commit()
        db.disconnect()
        
        # Process transactions
        parser.process_transactions()
        
        # Check results
        db.connect()
        cur = db.conn.cursor()
        
        # Get month_data
        cur.execute('''
            SELECT month, capital 
            FROM month_data 
            ORDER BY month
        ''')
        
        month_data = cur.fetchall()
        
        db.disconnect()
        
        print("\n=== Interest Attribution Test ===")
        print("Scenario:")
        print("1. 2020: Deposit 100 SEK")
        print("2. 2021: Earn 20 SEK interest on 100 SEK")
        print("3. 2022: Deposit 80 SEK, purchase 100 SEK asset (FIFO uses 2020 capital)")
        print("4. 2023: Earn 20 SEK interest on remaining 100 SEK cash")
        print()
        print("Month data after processing:")
        for month, capital in month_data:
            print(f"  {month}: {capital:.2f} SEK")
        
        # Expected with BUG FIXED:
        # 2020: 100 (deposit) + 20 (2021 interest) - 100 (purchase) + 4 (2023 interest) = 24
        # 2022: 80 (deposit) + 16 (2023 interest) = 96
        # Total: 24 + 96 = 120 (matches total deposits + interest: 100 + 80 + 20 + 20 = 220 - 100 purchase = 120)
        
        # Expected with CURRENT BUG:
        # 2020: Gets all 2023 interest until remaining_amount = 0
        # 2022: Gets nothing from 2023 interest
        
        # Find months
        month_2020 = None
        month_2022 = None
        for month, capital in month_data:
            if str(month).startswith('2020'):
                month_2020 = capital
            elif str(month).startswith('2022'):
                month_2022 = capital
        
        print(f"\n2020 month capital: {month_2020:.2f} SEK")
        print(f"2022 month capital: {month_2022:.2f} SEK")
        
        if month_2020 is not None and month_2022 is not None:
            print(f"\nWith bug fixed, expected:")
            print(f"  2020: 24.00 SEK (20 from 2021 interest + 4 from 2023 interest)")
            print(f"  2022: 96.00 SEK (80 deposit + 16 from 2023 interest)")
            
            print(f"\nWith current bug, likely:")
            print(f"  2020: ~40.00 SEK (gets all/most of 2023 interest)")
            print(f"  2022: ~80.00 SEK (gets little/none of 2023 interest)")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_interest_distribution_simple():
    """
    Simple test to demonstrate the bug more clearly.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Two deposits, then interest
        test_transactions = [
            (date(2020, 1, 1), '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            (date(2021, 1, 1), '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            (date(2022, 1, 1), '1111111', 'Ränta', '', 0, 0, 60, 0, 'SEK', ''),  # 60 SEK interest
        ]
        
        db.connect()
        cur = db.conn.cursor()
        for trans in test_transactions:
            cur.execute('''
                INSERT INTO transactions 
                (date, account, transaction_type, asset_name, amount, price, total, courtage, currency, isin, processed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''', trans)
        db.conn.commit()
        db.disconnect()
        
        parser.process_transactions()
        
        db.connect()
        cur = db.conn.cursor()
        cur.execute('SELECT month, capital FROM month_data ORDER BY month')
        month_data = cur.fetchall()
        db.disconnect()
        
        print("\n=== Simple Interest Distribution Test ===")
        print("Two deposits of 100 SEK each, then 60 SEK interest")
        print("Expected with bug fixed: Each gets 30 SEK (proportional)")
        print("Expected with current bug: First gets 60, second gets 0")
        print()
        print("Results:")
        for month, capital in month_data:
            print(f"  {month}: {capital:.2f} SEK")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

