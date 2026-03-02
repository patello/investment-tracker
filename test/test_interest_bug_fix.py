"""
Test for interest attribution bug fix.

The bug: In handle_interest(), remaining_amount -= capital should be remaining_amount -= capital*dividend_per_capital
This could cause rounding errors to not be distributed to dividend_month.
"""
import pytest
import sqlite3
import tempfile
import os
from datetime import date
from data_parser import DataParser
from database_handler import DatabaseHandler

def test_interest_distribution_with_rounding():
    """
    Test that interest is fully distributed even with rounding.
    
    Scenario: 101 SEK deposit, 33 SEK interest (33/101 = 0.326732673...)
    With floating point rounding, some tiny amount might remain after proportional distribution.
    This should go to dividend_month.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Test with amounts that cause repeating decimal
        test_transactions = [
            (date(2020, 6, 15), '1111111', 'Insättning', '', 0, 0, 101, 0, 'SEK', ''),
            (date(2021, 6, 15), '1111111', 'Ränta', '', 0, 0, 33, 0, 'SEK', ''),
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
        
        # Get total capital after interest distribution
        cur.execute('SELECT SUM(capital) FROM month_data')
        total_capital = cur.fetchone()[0]
        
        # Get capital by month
        cur.execute('SELECT month, capital FROM month_data ORDER BY month')
        month_data = cur.fetchall()
        
        db.disconnect()
        
        # Expected: 101 deposit + 33 interest = 134 total
        expected_total = 101 + 33
        
        print(f"\n=== Interest Distribution with Rounding Test ===")
        print(f"Deposit: 101 SEK, Interest: 33 SEK")
        print(f"Expected total capital: {expected_total} SEK")
        print(f"Actual total capital: {total_capital} SEK")
        print(f"Month data: {month_data}")
        
        # The bug would cause total_capital < expected_total if rounding errors weren't distributed
        # With the fix, all interest should be distributed (either proportionally or to dividend_month)
        assert abs(total_capital - expected_total) < 0.01, f"Interest not fully distributed. Expected {expected_total}, got {total_capital}"
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_interest_proportional_distribution():
    """
    Test that interest is distributed proportionally to all months with capital.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Two deposits, then interest
        test_transactions = [
            (date(2020, 6, 15), '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            (date(2021, 6, 15), '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            (date(2022, 6, 15), '1111111', 'Ränta', '', 0, 0, 60, 0, 'SEK', ''),
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
        
        # Get the two deposit months (skip dividend_month if it has 0)
        deposit_months = [(month, capital) for month, capital in month_data if capital > 0]
        
        db.disconnect()
        
        print(f"\n=== Proportional Interest Distribution Test ===")
        print(f"Two deposits of 100 SEK each, 60 SEK interest")
        print(f"Expected: Each gets 30 SEK interest (130 total each)")
        print(f"Results: {deposit_months}")
        
        # Both should have ~130 (100 + 30)
        for month, capital in deposit_months:
            assert abs(capital - 130) < 0.01, f"Month {month} should have ~130 SEK, got {capital}"
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_patello_scenario():
    """
    Test the exact scenario from Patello's question.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        test_transactions = [
            # 2020: Deposit 100 SEK
            (date(2020, 6, 15), '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            
            # 2021: Earn 20 SEK interest
            (date(2021, 6, 15), '1111111', 'Ränta', '', 0, 0, 20, 0, 'SEK', ''),
            
            # 2022: Deposit 80 SEK and purchase 100 SEK asset
            (date(2022, 6, 15), '1111111', 'Insättning', '', 0, 0, 80, 0, 'SEK', ''),
            (date(2022, 6, 16), '1111111', 'Köp', 'Asset A', 10, 10, -100, 0, 'SEK', 'TEST001'),
            
            # 2023: Earn 20 SEK interest on remaining cash
            (date(2023, 6, 15), '1111111', 'Ränta', '', 0, 0, 20, 0, 'SEK', ''),
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
        cur.execute('''
            SELECT month, capital 
            FROM month_data 
            WHERE capital > 0
            ORDER BY month
        ''')
        month_data = cur.fetchall()
        
        # Also check total to ensure all interest distributed
        cur.execute('SELECT SUM(capital) FROM month_data')
        total_capital = cur.fetchone()[0]
        
        db.disconnect()
        
        print(f"\n=== Patello's Scenario Test ===")
        print("2020: Deposit 100 SEK")
        print("2021: Earn 20 SEK interest")
        print("2022: Deposit 80 SEK, purchase 100 SEK asset (FIFO uses 2020 capital)")
        print("2023: Earn 20 SEK interest on remaining 100 SEK cash")
        print()
        print(f"Month data: {month_data}")
        print(f"Total capital: {total_capital}")
        
        # Expected: 2020 gets 24 (20 from 2021 interest + 4 from 2023 interest)
        #           2022 gets 96 (80 deposit + 16 from 2023 interest)
        # Total: 24 + 96 = 120
        
        # Find months (they might be allocated to different actual months due to cutoff_days)
        capitals = [capital for month, capital in month_data]
        
        # Should have two months with capital > 0
        assert len(capitals) == 2, f"Expected 2 months with capital, got {len(capitals)}"
        
        # One should have ~24, the other ~96
        capitals.sort()
        assert abs(capitals[0] - 24) < 0.01, f"Expected one month with ~24 SEK, got {capitals[0]}"
        assert abs(capitals[1] - 96) < 0.01, f"Expected one month with ~96 SEK, got {capitals[1]}"
        
        # Total should be 120
        expected_total = 100 + 80 + 20 + 20 - 100  # deposits + interest - purchase
        assert abs(total_capital - expected_total) < 0.01, f"Total capital mismatch. Expected {expected_total}, got {total_capital}"
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

