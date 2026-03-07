"""
Test to demonstrate limitations of current per-account asset tracking.

These tests show:
1. Assets are not tracked per account in month_assets table
2. Sales can sell assets purchased by other accounts
3. SavingsAccount gets attributed assets it never purchased
4. summarize_accounts() uses proportional attribution instead of actual ownership
"""

import pytest
import sqlite3

from database_handler import DatabaseHandler
from data_parser import DataParser


@pytest.fixture
def database_basic_two_accounts(tmp_path):
    """
    Two accounts purchasing different assets:
    - Account 1111: Deposits 100, buys 5 Asset A for 50
    - Account 2222: Deposits 200, buys 2 Asset B for 50
    """
    db_file = tmp_path / "test_asset_data.db"
    parser = DataParser(DatabaseHandler(db_file))

    # Create test CSV data in correct Avanza format (semicolon separated, 11 columns)
    test_data = """Datum;Konto;Typ av transaktion;Värdepapper/beskrivning;Antal;Kurs;Belopp;Courtage;Valuta;ISIN;Resultat
2020-01-01;1111;Insättning;Deposit;-;-;100;0;SEK;DEPOSIT;-
2020-01-01;1111;Köp;Asset A;5;10;-50;0;SEK;ASSETA;-
2020-01-01;2222;Insättning;Deposit;-;-;200;0;SEK;DEPOSIT;-
2020-01-01;2222;Köp;Asset B;2;25;-50;0;SEK;ASSETB;-"""

    # Write to temporary CSV
    csv_file = tmp_path / "test_data.csv"
    with open(csv_file, 'w') as f:
        f.write(test_data)

    parser.add_data(str(csv_file))
    return DatabaseHandler(db_file)


@pytest.fixture
def database_savings_account_scenario(tmp_path):
    """
    SavingsAccount scenario:
    - SavingsAccount: Deposits 5000 (only cash, no purchases)
    - InvestmentAccount: Deposits 27000, buys 10 Asset A for 100
    """
    db_file = tmp_path / "test_asset_data.db"
    parser = DataParser(DatabaseHandler(db_file))

    test_data = """Datum;Konto;Typ av transaktion;Värdepapper/beskrivning;Antal;Kurs;Belopp;Courtage;Valuta;ISIN;Resultat
2026-01-01;SavingsAccount;Insättning;Deposit;-;-;5000;0;SEK;DEPOSIT;-
2026-01-01;InvestmentAccount;Insättning;Deposit;-;-;27000;0;SEK;DEPOSIT;-
2026-01-01;InvestmentAccount;Köp;Asset A;10;10;-100;0;SEK;ASSETA;-"""

    csv_file = tmp_path / "test_data.csv"
    with open(csv_file, 'w') as f:
        f.write(test_data)

    parser.add_data(str(csv_file))
    return DatabaseHandler(db_file)


@pytest.fixture
def database_sales_validation(tmp_path):
    """
    Test sales validation:
    - Account 1111: Deposits 100, buys 5 Asset A
    - Account 2222: Deposits 200, buys 2 Asset B
    - Account 1111 tries to sell 1 Asset B (never purchased)
    """
    db_file = tmp_path / "test_asset_data.db"
    parser = DataParser(DatabaseHandler(db_file))

    test_data = """Datum;Konto;Typ av transaktion;Värdepapper/beskrivning;Antal;Kurs;Belopp;Courtage;Valuta;ISIN;Resultat
2020-01-01;1111;Insättning;Deposit;-;-;100;0;SEK;DEPOSIT;-
2020-01-01;1111;Köp;Asset A;5;10;-50;0;SEK;ASSETA;-
2020-01-01;2222;Insättning;Deposit;-;-;200;0;SEK;DEPOSIT;-
2020-01-01;2222;Köp;Asset B;2;25;-50;0;SEK;ASSETB;-
2020-02-01;1111;Sälj;Asset B;-1;30;30;0;SEK;ASSETB;-"""

    csv_file = tmp_path / "test_data.csv"
    with open(csv_file, 'w') as f:
        f.write(test_data)

    parser.add_data(str(csv_file))
    return DatabaseHandler(db_file)


def test_month_assets_missing_account_column(database_basic_two_accounts):
    """
    Test that month_assets table lacks 'account' column.
    This is the fundamental schema limitation.
    """
    db = database_basic_two_accounts
    db.connect()
    cur = db.get_cursor()

    # Check schema of month_assets table
    cur.execute("PRAGMA table_info(month_assets)")
    columns = [row[1] for row in cur.fetchall()]

    # month_assets should have 'account' column but doesn't
    assert 'account' not in columns, "month_assets should lack 'account' column (current limitation)"

    # Verify the columns that do exist
    expected_columns = ['month', 'asset_id', 'amount', 'average_price',
                       'average_purchase_price', 'average_sale_price',
                       'purchased_amount', 'sold_amount']

    for col in expected_columns:
        assert col in columns, f"month_assets should have '{col}' column"



def test_assets_not_tracked_per_account(database_basic_two_accounts):
    """
    Test that assets are pooled by month, not tracked per account.
    """
    db = database_basic_two_accounts
    db.connect()
    cur = db.get_cursor()

    # Process transactions
    parser = DataParser(db)
    parser.process_transactions()

    # Check month_assets table
    cur.execute("SELECT month, asset_id, amount FROM month_assets ORDER BY month, asset_id")
    month_assets = cur.fetchall()

    # Get asset names for readability
    cur.execute("SELECT asset_id, asset FROM assets ORDER BY asset_id")
    asset_map = {row[0]: row[1] for row in cur.fetchall()}

    # We have 2 assets purchased by different accounts
    assert len(month_assets) == 2, "Should have 2 asset entries (one per asset)"

    # Check that assets are in month_assets but no account info
    for month, asset_id, amount in month_assets:
        asset_name = asset_map[asset_id]
        print(f"Asset in month_assets: {month}, {asset_name}, {amount} shares")
        # No way to tell which account owns these shares!

    # This demonstrates the limitation: assets in month_assets don't record account ownership


def test_savings_account_attribution_bug(database_savings_account_scenario):
    """
    Test that SavingsAccount gets attributed assets it never purchased.
    """
    db = database_savings_account_scenario
    db.connect()
    cur = db.get_cursor()

    # Process transactions
    parser = DataParser(db)
    parser.process_transactions()

    # Check month_data for deposits
    cur.execute("""
        SELECT month, account, deposit, capital
        FROM month_data
        WHERE deposit > 0
        ORDER BY month, account
    """)
    month_data = cur.fetchall()

    # Should have deposits from both accounts in same month
    assert len(month_data) >= 2, "Should have deposits from both accounts"

    savings_deposit = 0
    investment_deposit = 0
    for month, account, deposit, capital in month_data:
        if account == 'SavingsAccount':
            savings_deposit = deposit
        elif account == 'InvestmentAccount':
            investment_deposit = deposit

    # Verify deposits
    assert savings_deposit == 5000, "SavingsAccount should deposit 5000"
    assert investment_deposit == 27000, "InvestmentAccount should deposit 27000"

    # Check month_assets for assets
    cur.execute("SELECT month, asset_id, amount FROM month_assets WHERE amount > 0")
    month_assets = cur.fetchall()

    # Get asset value
    cur.execute("SELECT asset FROM assets WHERE asset = 'Asset A'")
    asset_name = cur.fetchone()[0]

    # There should be assets in the same month as deposits
    assert len(month_assets) > 0, "Should have assets in month_assets"

    # The bug: SavingsAccount deposited in same month as assets were purchased
    # Current summarize_accounts() would give SavingsAccount share of assets
    # based on deposit proportion: 5000/(5000+27000) = 15.6%

    print(f"\nSavingsAccount scenario demonstration:")
    print(f"  SavingsAccount deposit: {savings_deposit} SEK")
    print(f"  InvestmentAccount deposit: {investment_deposit} SEK")
    print(f"  Total deposits: {savings_deposit + investment_deposit} SEK")
    print(f"  SavingsAccount share: {savings_deposit/(savings_deposit+investment_deposit):.1%}")
    print(f"  Assets purchased by InvestmentAccount: {asset_name}")
    print(f"  BUG: SavingsAccount would get attributed {savings_deposit/(savings_deposit+investment_deposit):.1%} of assets")



def test_sales_without_ownership_validation(database_sales_validation):
    """
    Test that accounts can sell assets they never purchased.
    """
    db = database_sales_validation
    db.connect()
    cur = db.get_cursor()

    # Try to process transactions
    parser = DataParser(db)

    # This should process without error (BUG!)
    # Account 1111 is trying to sell Asset B, which it never purchased
    # Asset B was purchased by Account 2222
    try:
        parser.process_transactions()
        print("\nBUG CONFIRMED: System allowed Account 1111 to sell Asset B")
        print("Account 1111 never purchased Asset B (purchased by 2222)")
        print("But sale was processed because assets are pooled in month_assets")

        # Check where cash went
        cur.execute("""
            SELECT month, account, capital
            FROM month_data
            WHERE account = '1111'
        """)
        for month, account, capital in cur.fetchall():
            print(f"  Cash in {account}: {capital} SEK (includes sale proceeds from Asset B)")

    except Exception as e:
        print(f"\nSale failed (unexpected): {e}")
        # If it fails, that might mean there's some validation we didn't know about



def test_proportional_attribution_logic():
    """
    Test demonstrating the proportional attribution logic issue.
    This doesn't need a database fixture, just shows the math.
    """
    print("\n=== Proportional Attribution Logic Issue ===")

    # Scenario from our tests
    month = "2026-01-31"
    accounts = [
        ("SavingsAccount", 5000),   # Only deposited cash
        ("InvestmentAccount", 27000),    # Deposited cash AND purchased assets
    ]

    total_deposits = sum(deposit for _, deposit in accounts)
    assets_in_month = 27054  # Value of assets purchased in month

    print(f"Month: {month}")
    print(f"Total deposits: {total_deposits} SEK")
    print(f"Assets in month: {assets_in_month} SEK")
    print()

    print("Current summarize_accounts() logic:")
    for account, deposit in accounts:
        share = deposit / total_deposits
        attributed_assets = assets_in_month * share
        print(f"  {account}: Deposited {deposit} SEK ({share:.1%})")
        print(f"       Gets {attributed_assets:.0f} SEK of assets")

    print()
    print("PROBLEM: SavingsAccount gets assets it never purchased!")
    print("SavingsAccount only deposited cash, didn't purchase any assets.")
    print("InvestmentAccount purchased all assets with its deposited cash.")
    print("Assets should be 100% attributed to InvestmentAccount, not proportional to deposits.")


def test_real_database_schema():
    """
    Test against real database to verify schema limitations exist.
    This test requires the real asset_data.db to exist.
    """
    import os

    db_path = "data/asset_data.db"
    if not os.path.exists(db_path):
        pytest.skip("Real database not found, skipping real database test")

    db = sqlite3.connect(db_path)
    cur = db.cursor()

    # 1. Check month_assets schema
    cur.execute("PRAGMA table_info(month_assets)")
    columns = [row[1] for row in cur.fetchall()]

    assert 'account' not in columns, "month_assets should lack 'account' column in real DB"

    # 2. Check for SavingsAccount deposits with assets in same month
    cur.execute("""
        SELECT md.month, md.account, md.deposit,
               (SELECT COUNT(*) FROM month_assets ma WHERE ma.month = md.month AND ma.amount > 0) as asset_count
        FROM month_data md
        WHERE md.account = 'SavingsAccount' AND md.deposit > 0
        ORDER BY md.month
    """)

    sparkonto_months = cur.fetchall()

    if sparkonto_months:
        print("\n=== Real Database: SavingsAccount Issue ===")
        for month, account, deposit, asset_count in sparkonto_months:
            print(f"Month {month}: SavingsAccount deposited {deposit:.0f} SEK")
            if asset_count > 0:
                print(f"  WARNING: Month has {asset_count} assets - SavingsAccount would be attributed some!")



if __name__ == "__main__":
    # Run tests directly for demonstration
    import tempfile
    import os

    print("Running per-account asset tracking limitation tests...")

    # Create temporary directory for test databases
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = type('obj', (object,), {'__truediv__': lambda self, x: os.path.join(tmpdir, x)})()

        # Test 1: Schema limitation
        print("\n1. Testing month_assets missing account column...")
        db = database_basic_two_accounts(tmp_path)
        test_month_assets_missing_account_column(db)

        # Test 2: Asset pooling
        print("\n2. Testing assets not tracked per account...")
        db = database_basic_two_accounts(tmp_path)
        test_assets_not_tracked_per_account(db)

        # Test 3: SavingsAccount bug
        print("\n3. Testing SavingsAccount attribution bug...")
        db = database_savings_account_scenario(tmp_path)
        test_savings_account_attribution_bug(db)

        # Test 4: Sales validation
        print("\n4. Testing sales without ownership validation...")
        db = database_sales_validation(tmp_path)
        test_sales_without_ownership_validation(db)

        # Test 5: Proportional attribution logic
        print("\n5. Demonstrating proportional attribution logic issue...")
        test_proportional_attribution_logic()

    print("\n=== All limitation tests completed ===")