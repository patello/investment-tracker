"""
Test case demonstrating the realized gain calculation bug.
The bug: When assets are sold and proceeds are reinvested, 
the realized gain disappears from statistics.
"""

import pytest
import sqlite3
import tempfile
import os
from datetime import date
from database_handler import DatabaseHandler
from data_parser import DataParser
from calculate_stats import StatCalculator

def test_realized_gain_disappears_when_reinvested():
    """
    Scenario:
    1. Deposit 100 SEK
    2. Buy Asset A for 100 SEK
    3. Asset A appreciates (simulated by setting price higher)
    4. Sell Asset A for 150 SEK (50 SEK gain)
    5. Buy Asset B for 150 SEK (reinvest)
    
    Expected: Realized gain should remain 50 SEK
    Actual: Realized gain becomes 0 or negative
    """
    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        
        # Manually create transactions to simulate the scenario
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        
        # Create tables if they don't exist (simplified)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                date DATE,
                account TEXT,
                transaction_type TEXT,
                asset_name TEXT,
                amount REAL,
                price REAL,
                total REAL,
                courtage REAL,
                currency TEXT,
                isin TEXT,
                processed INTEGER DEFAULT 0
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS month_data (
                month DATE PRIMARY KEY,
                deposit REAL DEFAULT 0,
                withdrawal REAL DEFAULT 0,
                capital REAL DEFAULT 0
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS assets (
                asset_id INTEGER PRIMARY KEY,
                asset TEXT UNIQUE,
                amount REAL DEFAULT 0,
                average_price REAL DEFAULT 0,
                average_purchase_price REAL DEFAULT 0,
                average_sale_price REAL DEFAULT 0,
                purchased_amount REAL DEFAULT 0,
                sold_amount REAL DEFAULT 0,
                latest_price REAL,
                latest_price_date DATE
            )
        ''')
        
        cur.execute('''
            CREATE TABLE IF NOT EXISTS month_assets (
                month DATE,
                asset_id INTEGER,
                amount REAL DEFAULT 0,
                average_price REAL DEFAULT 0,
                average_purchase_price REAL DEFAULT 0,
                average_sale_price REAL DEFAULT 0,
                purchased_amount REAL DEFAULT 0,
                sold_amount REAL DEFAULT 0,
                PRIMARY KEY (month, asset_id)
            )
        ''')
        
        # 1. Deposit 100 SEK
        cur.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   ('2026-01-01', 'test', 'Insättning', 'Deposit', 0, 0, 100, 0, 'SEK', '', 0))
        
        # 2. Buy Asset A for 100 SEK
        cur.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   ('2026-01-02', 'test', 'Köp', 'Asset A', 10, 10, -100, 0, 'SEK', 'TEST001', 0))
        
        # 3. Sell Asset A for 150 SEK (50 SEK gain)
        cur.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   ('2026-01-03', 'test', 'Sälj', 'Asset A', -10, 15, 150, 0, 'SEK', 'TEST001', 0))
        
        # 4. Buy Asset B for 150 SEK (reinvest)
        cur.execute("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   ('2026-01-04', 'test', 'Köp', 'Asset B', 15, 10, -150, 0, 'SEK', 'TEST002', 0))
        
        conn.commit()
        
        # Process transactions
        data_parser = DataParser(db)
        
        # Manually process in correct order
        # Process deposit
        cur.execute("UPDATE month_data SET deposit = 100, capital = 100 WHERE month = '2026-01-31'")
        cur.execute("INSERT OR IGNORE INTO month_data (month) VALUES ('2026-01-31')")
        cur.execute("UPDATE month_data SET deposit = 100, capital = 100 WHERE month = '2026-01-31'")
        cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = 1")
        
        # Process purchase of Asset A
        cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES ('Asset A')")
        asset_a_id = cur.execute("SELECT asset_id FROM assets WHERE asset = 'Asset A'").fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO month_assets (month, asset_id) VALUES ('2026-01-31', ?)", (asset_a_id,))
        cur.execute('''UPDATE month_assets SET 
                      amount = amount + 10,
                      average_price = (10*10 + amount*average_price)/(10 + amount),
                      average_purchase_price = (10*10 + purchased_amount*average_purchase_price)/(10 + purchased_amount),
                      purchased_amount = purchased_amount + 10
                      WHERE month = '2026-01-31' AND asset_id = ?''', (asset_a_id,))
        cur.execute("UPDATE month_data SET capital = capital - 100 WHERE month = '2026-01-31'")
        cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = 2")
        
        # Process sale of Asset A
        cur.execute('''UPDATE month_assets SET 
                      amount = amount - 10,
                      sold_amount = sold_amount + 10,
                      average_sale_price = (10*15 + sold_amount*average_sale_price)/(10 + sold_amount)
                      WHERE month = '2026-01-31' AND asset_id = ?''', (asset_a_id,))
        cur.execute("UPDATE month_data SET capital = capital + 150 WHERE month = '2026-01-31'")
        cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = 3")
        
        # Process purchase of Asset B
        cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES ('Asset B')")
        asset_b_id = cur.execute("SELECT asset_id FROM assets WHERE asset = 'Asset B'").fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO month_assets (month, asset_id) VALUES ('2026-01-31', ?)", (asset_b_id,))
        cur.execute('''UPDATE month_assets SET 
                      amount = amount + 15,
                      average_price = (15*10 + amount*average_price)/(15 + amount),
                      average_purchase_price = (15*10 + purchased_amount*average_purchase_price)/(15 + purchased_amount),
                      purchased_amount = purchased_amount + 15
                      WHERE month = '2026-01-31' AND asset_id = ?''', (asset_b_id,))
        cur.execute("UPDATE month_data SET capital = capital - 150 WHERE month = '2026-01-31'")
        cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = 4")
        
        conn.commit()
        
        # Set prices for assets
        cur.execute("UPDATE assets SET latest_price = 10 WHERE asset = 'Asset A'")
        cur.execute("UPDATE assets SET latest_price = 10 WHERE asset = 'Asset B'")
        conn.commit()
        
        # Calculate stats
        calculator = StatCalculator(db)
        calculator.calculate_stats()
        
        # Check month stats
        cur.execute('''
            SELECT deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss
            FROM month_stats WHERE month = '2026-01-31'
        ''')
        result = cur.fetchone()
        
        print(f"\nTest Results:")
        print(f"Deposit: {result[0]}")
        print(f"Withdrawal: {result[1]}")
        print(f"Value: {result[2]}")
        print(f"Total Gain/Loss: {result[3]}")
        print(f"Realized Gain/Loss: {result[4]}")
        print(f"Unrealized Gain/Loss: {result[5]}")
        
        # The bug: Realized gain should be 50 SEK (sale at 150 - cost 100)
        # But with current formula: realized = withdrawal + capital - deposit
        # withdrawal=0, capital=-100 (after reinvestment), deposit=100
        # realized = 0 + (-100) - 100 = -200 ❌
        
        assert result[4] == 50, f"Expected realized gain of 50, got {result[4]}"
        
    finally:
        os.unlink(db_path)

if __name__ == "__main__":
    test_realized_gain_disappears_when_reinvested()
    print("\nTest passed! (If this runs, the bug is fixed)")
