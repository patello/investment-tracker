"""
Test for cash tracking bug: capital is tracked globally, not per account.
This causes phantom cash to appear in investment accounts when savings account
cash is counted as available for purchases.
"""
import pytest
import sqlite3
import tempfile
import os
from datetime import date
from data_parser import DataParser, SpecialCases
from database_handler import DatabaseHandler

def test_capital_tracked_globally_not_per_account():
    """
    Demonstrates the bug where capital is tracked globally instead of per account.
    
    Scenario:
    1. Deposit 50,000 SEK to Sparkonto (savings account)
    2. Deposit 20,000 SEK to ISK (investment account)  
    3. Buy 20,000 SEK of assets in ISK
    
    Expected: ISK has 0 SEK cash, Sparkonto has 50,000 SEK cash
    Actual bug: System shows ISK has 20,000 SEK cash, Sparkonto has 50,000 SEK cash
    """
    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Create test transactions
        test_transactions = [
            # Format: date, account, type, asset, amount, price, total, courtage, currency, isin
            (date(2024, 1, 1), 'Sparkonto', 'Insättning', '', 0, 0, 50000, 0, 'SEK', ''),
            (date(2024, 1, 2), '7485280', 'Insättning', '', 0, 0, 20000, 0, 'SEK', ''),
            (date(2024, 1, 3), '7485280', 'Köp', 'Test Fund', 100, 200, -20000, 0, 'SEK', 'TEST123'),
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
        
        # Get capital from month_data
        cur.execute("SELECT month, capital FROM month_data ORDER BY month")
        capital_results = cur.fetchall()
        
        # Get asset amounts
        cur.execute("SELECT asset, amount FROM assets")
        asset_results = cur.fetchall()
        
        db.disconnect()
        
        print("\n=== Test Results ===")
        print(f"Capital by month: {capital_results}")
        print(f"Assets: {asset_results}")
        
        # The bug: capital shows 50,000 SEK (all from Sparkonto)
        # But in reality, this 50,000 should be associated with Sparkonto only
        # The ISK purchase of 20,000 should have consumed the ISK deposit, not left it as cash
        
        # With the bug: Total capital = 50,000 SEK
        # System thinks: ISK has 20,000 cash (wrong!), Sparkonto has 50,000 cash
        # Actually: ISK has 0 cash, Sparkonto has 50,000 cash
        
        assert len(capital_results) > 0, "Should have capital entries"
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_multi_account_capital_tracking():
    """
    More complex test with multiple accounts and transactions.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Create test transactions mixing accounts
        test_transactions = [
            # Sparkonto deposits
            (date(2024, 1, 1), 'Sparkonto', 'Insättning', '', 0, 0, 10000, 0, 'SEK', ''),
            (date(2024, 2, 1), 'Sparkonto', 'Insättning', '', 0, 0, 15000, 0, 'SEK', ''),
            
            # ISK deposits and purchases
            (date(2024, 1, 15), '7485280', 'Insättning', '', 0, 0, 5000, 0, 'SEK', ''),
            (date(2024, 1, 16), '7485280', 'Köp', 'Fund A', 50, 100, -5000, 0, 'SEK', 'FUND001'),
            
            # AF account
            (date(2024, 2, 15), '1234567', 'Insättning', '', 0, 0, 8000, 0, 'SEK', ''),
            (date(2024, 2, 16), '1234567', 'Köp', 'Fund B', 40, 200, -8000, 0, 'SEK', 'FUND002'),
            
            # Sparkonto withdrawal
            (date(2024, 3, 1), 'Sparkonto', 'Uttag', '', 0, 0, -7000, 0, 'SEK', ''),
        ]
        
        # Add transactions
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
        
        cur.execute("SELECT month, capital FROM month_data ORDER BY month")
        capital = cur.fetchall()
        
        cur.execute("SELECT SUM(capital) FROM month_data")
        total_capital = cur.fetchone()[0]
        
        db.disconnect()
        
        print(f"\n=== Multi-Account Test ===")
        print(f"Capital by month: {capital}")
        print(f"Total capital: {total_capital:.0f} SEK")
        
        # Expected: Sparkonto has 18,000 SEK (10k + 15k - 7k withdrawal)
        # ISK has 0 SEK (5k deposit used for purchase)
        # AF has 0 SEK (8k deposit used for purchase)
        # Total: 18,000 SEK
        
        # With bug: System shows 18,000 SEK capital but can't distinguish per account
        # This leads to incorrect cash attribution
        
        assert abs(total_capital - 18000) < 0.01, f"Expected ~18000 SEK, got {total_capital}"
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

