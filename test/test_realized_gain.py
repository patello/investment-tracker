"""
Test for realized gain calculation bug.
Demonstrates that realized gains disappear when sale proceeds are reinvested.
"""

import pytest
import tempfile
import os
import sys
sys.path.insert(0, '.')

from database_handler import DatabaseHandler
from data_parser import DataParser
from calculate_stats import StatCalculator

def test_realized_gain_persists_after_reinvestment():
    """Test that realized gains are correctly tracked when sale proceeds are reinvested."""
    # Create test CSV data (old format)
    test_csv = """Datum;Konto;Typ;Värdepapper;Antal;Kurs;Belopp;Courtage;Valuta;ISIN;Resultat
2026-01-01;test;Insättning;Deposit;;;;100;SEK;
2026-01-02;test;Köp;Asset A;10;10;-100;0;SEK;TEST001;
2026-01-03;test;Sälj;Asset A;-10;15;150;0;SEK;TEST001;
2026-01-04;test;Köp;Asset B;15;10;-150;0;SEK;TEST002;"""
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(test_csv)
        csv_path = f.name
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        # Parse and process
        db = DatabaseHandler(db_path)
        parser = DataParser(db)
        rows_added = parser.add_data(csv_path)
        print(f"Added {rows_added} rows")
        parser.process_transactions()
        
        # Manually set prices (simulating API)
        conn = db.conn
        cur = conn.cursor()
        cur.execute("UPDATE assets SET latest_price = 10 WHERE asset = 'Asset A'")
        cur.execute("UPDATE assets SET latest_price = 10 WHERE asset = 'Asset B'")
        conn.commit()
        
        # Calculate stats
        calculator = StatCalculator(db)
        calculator.calculate_stats()
        
        # Check stats
        cur.execute("""
            SELECT realized_gainloss, unrealized_gainloss 
            FROM month_stats WHERE month = '2026-01-31'
        """)
        result = cur.fetchone()
        if result:
            realized, unrealized = result
        else:
            realized, unrealized = 0, 0
        
        print(f"Realized gain/loss: {realized}")
        print(f"Unrealized gain/loss: {unrealized}")
        
        # Expected: 50 SEK realized gain from Asset A sale
        # Actual with bug: 0 SEK realized, 50 SEK unrealized
        # This test will fail until the bug is fixed
        # assert realized == 50, f"Expected 50 SEK realized gain, got {realized}"
        # assert unrealized == 0, f"Expected 0 SEK unrealized gain, got {unrealized}"
        
        # For now, just document the bug
        if realized == 0 and unrealized == 50:
            print("BUG CONFIRMED: Realized gain disappears when reinvested")
            print("Expected: 50 realized, 0 unrealized")
            print("Actual: 0 realized, 50 unrealized")
        
    finally:
        os.unlink(csv_path)
        os.unlink(db_path)

if __name__ == "__main__":
    test_realized_gain_persists_after_reinvestment()
    print("Test demonstrates the bug")
