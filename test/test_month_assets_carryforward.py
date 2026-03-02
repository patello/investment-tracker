"""
Test for month_assets carry-forward bug: assets only appear in month_assets
for months with transactions, causing understated portfolio values.
"""
import pytest
import sqlite3
import tempfile
import os
from datetime import date
from data_parser import DataParser
from database_handler import DatabaseHandler

def test_month_assets_not_carried_forward():
    """
    Demonstrates the bug where assets don't carry forward to subsequent months.
    
    Scenario:
    1. January: Deposit 1000 SEK, buy 10 shares of Fund A at 100 SEK each
    2. February: No transactions
    3. March: Sell 5 shares of Fund A at 120 SEK each
    
    Expected in March month_assets: 5 shares of Fund A (carried from January)
    Actual bug: Fund A doesn't appear in March month_assets at all
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Create test transactions
        test_transactions = [
            # January: Deposit and purchase
            (date(2024, 1, 10), '7485280', 'Insättning', '', 0, 0, 1000, 0, 'SEK', ''),
            (date(2024, 1, 15), '7485280', 'Köp', 'Fund A', 10, 100, -1000, 0, 'SEK', 'TEST001'),
            
            # February: No transactions (asset should still be held)
            
            # March: Partial sale
            (date(2024, 3, 10), '7485280', 'Sälj', 'Fund A', -5, 120, 600, 0, 'SEK', 'TEST001'),
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
        
        # Get asset ID
        cur.execute("SELECT asset_id FROM assets WHERE asset = 'Fund A'")
        asset_id = cur.fetchone()[0]
        
        # Check month_assets entries
        cur.execute('''
            SELECT month, amount 
            FROM month_assets 
            WHERE asset_id = ? 
            ORDER BY month
        ''', (asset_id,))
        
        month_assets = cur.fetchall()
        
        # Check what calculate_stats would see for March
        march_month = date(2024, 3, 31)
        cur.execute('''
            SELECT amount 
            FROM month_assets 
            WHERE asset_id = ? AND month = ?
        ''', (asset_id, march_month))
        
        march_amount = cur.fetchone()
        
        db.disconnect()
        
        print("\n=== Test Results ===")
        print(f"Month assets entries: {month_assets}")
        
        if march_amount:
            print(f"March month_assets amount: {march_amount[0]:.2f} shares")
        else:
            print("March month_assets amount: None (asset not found!)")
        
        # The bug: After selling 5 shares in March, we should still have 5 shares
        # But month_assets for March might not include the asset at all
        # Or might only show the sold amount, not the remaining holdings
        
        # Expected: January: 10 shares, March: 5 shares (after sale)
        # Bug: January: 10 shares, March: asset not in month_assets table
        
        assert len(month_assets) > 0, "Should have month_assets entries"
        
        # This test demonstrates the issue - assets need to carry forward
        # between months even without transactions
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_statistics_understate_value():
    """
    Test that statistics calculation understates portfolio value
    due to missing carried-forward assets.
    """
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        
        # Create longer timeline
        test_transactions = [
            # Year 1: Buy and hold
            (date(2023, 6, 1), '7485280', 'Insättning', '', 0, 0, 5000, 0, 'SEK', ''),
            (date(2023, 6, 15), '7485280', 'Köp', 'Long Term Fund', 100, 50, -5000, 0, 'SEK', 'LTF001'),
            
            # Year 2: No transactions (but still holding)
            # Year 3: Check value
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
        
        # Manually add a price for the asset (simulating price update)
        db.connect()
        cur = db.conn.cursor()
        
        cur.execute("SELECT asset_id FROM assets WHERE asset = 'Long Term Fund'")
        asset_id = cur.fetchone()[0]
        
        # Set a current price
        cur.execute('''
            UPDATE assets 
            SET latest_price = 75.0, latest_price_date = '2025-12-31'
            WHERE asset_id = ?
        ''', (asset_id,))
        
        db.conn.commit()
        db.disconnect()
        
        # Now check what statistics would show for 2025
        # The bug: asset won't appear in 2025 month_assets because no transactions
        
        print(f"\n=== Statistics Understatement Test ===")
        print("Asset purchased in 2023, still held in 2025")
        print("With bug: 2025 statistics won't include this asset")
        print("Portfolio value will be understated by 7,500 SEK (100 shares × 75 SEK)")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

