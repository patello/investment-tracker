import pytest

from database_handler import DatabaseHandler
from data_parser import DataParser, AssetDeficit, SpecialCases

# Define all datasets to be used that should pass
passing_datasets = [
    "./test/data/small_data.csv", 
    "./test/data/listing_change.csv", 
    "./test/data/reordered_data.csv",
    "./test/data/asset_deposit.csv",
    "./test/data/interest_fees.csv",
    "./test/data/fraction_writeoff.csv"]

# Parametrize the fixture
# Indirect parametrization allows us to use different datasets for the same fixture
@pytest.fixture
def databases(tmp_path, request):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataParser(DatabaseHandler(db_file), SpecialCases("./test/data/special_cases_test.json"))
    # Add data to database
    # The dataset used is specified in the test function that uses this fixture
    data_adder.add_data(request.param)
    return DatabaseHandler(db_file)

# Create a temporary SQLite database in the tmp_path directory using the small dataset
# Special cases are not handled, creating a small deficit in the database
@pytest.fixture
def database_small_deficit(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataParser(DatabaseHandler(db_file))
    # Add data to database
    data_adder.add_data("./test/data/small_data.csv")
    return DatabaseHandler(db_file)

# Create a temporary SQLite database in the tmp_path directory using the small dataset with different accounts
@pytest.fixture
def database_small_diff_accounts(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataParser(DatabaseHandler(db_file),SpecialCases("./test/data/special_cases_test.json"))
    # Add data to database
    data_adder.add_data("./test/data/small_data_diff_accounts.csv")
    return DatabaseHandler(db_file)

# Create a temporary SQLite database in the tmp_path directory using the small dataset with different accounts
# The dataset has an error which causes one account to have a deficit
@pytest.fixture
def database_small_wrong_accounts(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataParser(DatabaseHandler(db_file),SpecialCases("./test/data/special_cases_test.json"))
    # Add data to database
    data_adder.add_data("./test/data/small_data_wrong_accounts.csv")
    return DatabaseHandler(db_file)

@pytest.mark.parametrize('databases', passing_datasets, indirect=True)
def test_data_parser__passing_datasets(databases):
    # Create DataParser object
    data_parser = DataParser(databases)
    # Connect to database and get number of unprocessed transactions
    databases.connect()
    unprocessed = databases.get_db_stat("Unprocessed" )
    # Process transactions
    data_parser.process_transactions()
    # Check that all previously unprocessed transactions are now processed
    assert databases.get_db_stat("Processed" ) == unprocessed
    assert databases.get_db_stat("Unprocessed" ) == 0

def test_data_parser__deficit(database_small_deficit):
    # Create DataParser object
    data_parser = DataParser(database_small_deficit)
    # Connect to database and get number of unprocessed transactions
    database_small_deficit.connect()
    unprocessed = database_small_deficit.get_db_stat("Unprocessed" )
    # Process transactions, since there is a deficit, an exception should be raised
    with pytest.raises(AssetDeficit):
        data_parser.process_transactions()
    # Check that all previously unprocessed transactions are still unprocessed
    assert database_small_deficit.get_db_stat("Processed" ) == 0
    assert database_small_deficit.get_db_stat("Unprocessed" ) == unprocessed

def test_data_parser__diff_accounts(database_small_diff_accounts):
    # Create DataParser object
    data_parser = DataParser(database_small_diff_accounts)
    # Connect to database and get number of unprocessed transactions
    database_small_diff_accounts.connect()
    unprocessed = database_small_diff_accounts.get_db_stat("Unprocessed" )
    # Process transactions
    data_parser.process_transactions()
    # Check that all previously unprocessed transactions are now processed
    assert database_small_diff_accounts.get_db_stat("Processed" ) == unprocessed
    assert database_small_diff_accounts.get_db_stat("Unprocessed" ) == 0 

@pytest.mark.parametrize('databases', [passing_datasets[0]], indirect=True)
def test_data_parser__reset(databases):
    # Create DataParser object
    data_parser = DataParser(databases)
    # Connect to database and get number of unprocessed transactions
    databases.connect()
    unprocessed = databases.get_db_stat("Unprocessed" )
    # Process transactions
    data_parser.process_transactions()
    # Check that all previously unprocessed transactions are now processed
    assert databases.get_db_stat("Processed" ) == unprocessed
    assert databases.get_db_stat("Unprocessed" ) == 0
    # Reset the database
    data_parser.reset_processed_transactions()
    # Check that all previously processed transactions are now unprocessed
    assert databases.get_db_stat("Processed" ) == 0
    assert databases.get_db_stat("Unprocessed" ) == unprocessed
    # Test that all transactions can be processed again
    data_parser.process_transactions()
    assert databases.get_db_stat("Processed" ) == unprocessed
    assert databases.get_db_stat("Unprocessed" ) == 0


# Functionality not implemented yet
@pytest.mark.xfail(reason="Functionality not implemented yet")
def test_data_parser__wrong_accounts(database_small_wrong_accounts):
    # Create DataParser object
    data_parser = DataParser(database_small_wrong_accounts)
    # Connect to database and get number of unprocessed transactions
    database_small_wrong_accounts.connect()
    unprocessed = database_small_wrong_accounts.get_db_stat("Unprocessed" )
    # Process transactions, since there is a deficit, an exception should be raised
    with pytest.raises(AssetDeficit):
        data_parser.process_transactions()
    # Check that all previously unprocessed transactions are still unprocessed
    assert database_small_wrong_accounts.get_db_stat("Processed" ) == 0
    assert database_small_wrong_accounts.get_db_stat("Unprocessed" ) == unprocessed

def test_interest_distribution_proportional(tmp_path):
    """Test that interest is distributed proportionally to capital in all months."""
    # Create a temporary database
    db_file = tmp_path / "test_interest.db"
    db = DatabaseHandler(db_file)
    
    # Add test transactions: two deposits, then interest
    db.connect()
    cur = db.conn.cursor()
    
    test_transactions = [
        ('2020-06-15', '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
        ('2021-06-15', '1111111', 'Insättning', '', 0, 0, 100, 0, 'SEK', ''),
        ('2022-06-15', '1111111', 'Ränta', '', 0, 0, 60, 0, 'SEK', ''),
    ]
    
    for trans in test_transactions:
        cur.execute('''
            INSERT INTO transactions 
            (date, account, transaction_type, asset_name, amount, price, total, courtage, currency, isin, processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        ''', trans)
    
    db.conn.commit()
    db.disconnect()
    
    # Process transactions
    parser = DataParser(db)
    parser.process_transactions()
    
    # Check results
    db.connect()
    cur = db.conn.cursor()
    
    # Get capital by month
    cur.execute('SELECT month, capital FROM month_data WHERE capital > 0 ORDER BY month')
    month_data = cur.fetchall()
    
    db.disconnect()
    
    # Both months should have capital (100 deposit + 30 interest each)
    assert len(month_data) == 2
    for month, capital in month_data:
        assert abs(capital - 130) < 0.01  # 100 + 30
    
def test_interest_distribution_rounding(tmp_path):
    """Test that interest rounding errors are properly distributed."""
    # Create a temporary database
    db_file = tmp_path / "test_interest_rounding.db"
    db = DatabaseHandler(db_file)
    
    # Add test transactions: deposit with repeating decimal interest
    db.connect()
    cur = db.conn.cursor()
    
    test_transactions = [
        ('2020-06-15', '1111111', 'Insättning', '', 0, 0, 101, 0, 'SEK', ''),
        ('2021-06-15', '1111111', 'Ränta', '', 0, 0, 33, 0, 'SEK', ''),
    ]
    
    for trans in test_transactions:
        cur.execute('''
            INSERT INTO transactions 
            (date, account, transaction_type, asset_name, amount, price, total, courtage, currency, isin, processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
        ''', trans)
    
    db.conn.commit()
    db.disconnect()
    
    # Process transactions
    parser = DataParser(db)
    parser.process_transactions()
    
    # Check results
    db.connect()
    cur = db.conn.cursor()
    
    # Get total capital
    cur.execute('SELECT SUM(capital) FROM month_data')
    total_capital = cur.fetchone()[0]
    
    db.disconnect()
    
    # All interest should be distributed (101 + 33 = 134)
    assert abs(total_capital - 134) < 0.01

