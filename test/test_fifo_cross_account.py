"""
Test to demonstrate FIFO cross-account capital usage bug.
Shows that savings account cash gets used for investment purchases.
"""
import pytest
import sqlite3
import tempfile
import os
from datetime import date
from data_parser import DataParser
from database_handler import DatabaseHandler

def test_fifo_uses_savings_for_investment_purchases():
    """
    Demonstrates that FIFO accounting uses savings cash for investment purchases.
    
    Scenario:
    1. Month 1: Deposit 100 SEK to Sparkonto (savings)
    2. Month 2: Deposit 50 SEK to ISK (investment)
    3. Month 2: Buy asset for 50 SEK in ISK
    
    Expected: Should use the 50 SEK ISK deposit for ISK purchase
    Actual bug: Uses 50 SEK from the 100 SEK savings (oldest capital first)
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Create test transactions
        test_transactions = [
            # Month 1: Savings deposit (older)
            (date(2024, 1, 10), 'Sparkonto', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
            
            # Month 2: ISK deposit and purchase (same month)
            (date(2024, 2, 1), '7485280', 'Insättning', '', 0, 0, 50, 0, 'SEK', ''),
            (date(2024, 2, 5), '7485280', 'Köp', 'Test Fund', 5, 10, -50, 0, 'SEK', 'TEST001'),
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
        
        # Get capital by month after processing
        cur.execute('''
            SELECT month, capital, deposit, withdrawal 
            FROM month_data 
            ORDER BY month
        ''')
        
        month_data = cur.fetchall()
        
        # Get asset allocation
        cur.execute("SELECT asset_id FROM assets WHERE asset = 'Test Fund'")
        asset_id = cur.fetchone()[0]
        
        cur.execute('''
            SELECT month, amount 
            FROM month_assets 
            WHERE asset_id = ?
            ORDER BY month
        ''', (asset_id,))
        
        asset_allocation = cur.fetchall()
        
        db.disconnect()
        
        print("\n=== FIFO Cross-Account Test ===")
        print("Scenario:")
        print("  1. Month 1: Deposit 100 SEK to Sparkonto (savings)")
        print("  2. Month 2: Deposit 50 SEK to ISK (investment)")
        print("  3. Month 2: Buy 50 SEK asset in ISK")
        print()
        
        print("Results:")
        for month, capital, deposit, withdrawal in month_data:
            print(f"  Month {month}: capital={capital:.0f} SEK, deposit={deposit:.0f} SEK, withdrawal={withdrawal:.0f} SEK")
        
        print()
        print("Asset allocation by month:")
        for month, amount in asset_allocation:
            print(f"  Month {month}: {amount:.2f} shares")
        
        print()
        print("ANALYSIS:")
        print("The 50 SEK ISK purchase should use the 50 SEK ISK deposit.")
        print("But with FIFO, it uses the oldest capital first (100 SEK from Sparkonto).")
        print()
        print("Result: Sparkonto cash decreases by 50 SEK (to 50 SEK)")
        print("        ISK deposit of 50 SEK remains as 'capital' (phantom cash)")
        print("        Asset is allocated to Month 1 (Sparkonto month)")
        
        # Expected: Month 1: 100 SEK, Month 2: 0 SEK (50 deposit - 50 purchase)
        # Actual bug: Month 1: 50 SEK (100 - 50), Month 2: 50 SEK (deposit untouched)
        
        assert len(month_data) == 2, "Should have two months of data"
        
        # Month 1 should have 50 SEK capital (100 - 50 used for purchase)
        # Month 2 should have 50 SEK capital (ISK deposit untouched - WRONG!)
        
        month1_capital = month_data[0][1]
        month2_capital = month_data[1][1]
        
        print(f"\nMonth 1 capital: {month1_capital:.0f} SEK (expected: 50)")
        print(f"Month 2 capital: {month2_capital:.0f} SEK (expected: 0)")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_real_world_example():
    """
    Real-world example showing the phantom cash issue.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Simulate actual portfolio scenario
        test_transactions = [
            # 2023: Build up savings
            (date(2023, 1, 15), 'Sparkonto', 'Insättning', '', 0, 0, 50000, 0, 'SEK', ''),
            
            # 2024: ISK investments
            (date(2024, 1, 10), '7485280', 'Insättning', '', 0, 0, 20000, 0, 'SEK', ''),
            (date(2024, 1, 15), '7485280', 'Köp', 'Fund A', 100, 200, -20000, 0, 'SEK', 'FUND001'),
            
            (date(2024, 2, 10), '7485280', 'Insättning', '', 0, 0, 15000, 0, 'SEK', ''),
            (date(2024, 2, 15), '7485280', 'Köp', 'Fund B', 75, 200, -15000, 0, 'SEK', 'FUND002'),
            
            # Savings continues
            (date(2024, 3, 1), 'Sparkonto', 'Insättning', '', 0, 0, 10000, 0, 'SEK', ''),
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
        
        # Process
        parser.process_transactions()
        
        # Check
        db.connect()
        cur = db.conn.cursor()
        
        cur.execute('SELECT SUM(capital) FROM month_data')
        total_capital = cur.fetchone()[0]
        
        cur.execute('SELECT month, capital FROM month_data ORDER BY month')
        capital_by_month = cur.fetchall()
        
        db.disconnect()
        
        print(f"\n=== Real-World Example ===")
        print(f"Total capital in system: {total_capital:.0f} SEK")
        print("Capital by month:")
        for month, capital in capital_by_month:
            print(f"  {month}: {capital:.0f} SEK")
        
        print()
        print("ACTUAL cash:")
        print("  Sparkonto: 60,000 SEK (50,000 + 10,000)")
        print("  ISK: 0 SEK (all invested)")
        
        print()
        print("SYSTEM shows:")
        print(f"  Total: {total_capital:.0f} SEK (should be 60,000)")
        print("  But can't distinguish Sparkonto vs ISK")
        print("  FIFO used savings cash for ISK purchases")
        print("  Creates phantom ISK cash")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

