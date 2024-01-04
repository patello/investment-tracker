import pytest

from database_handler import DatabaseHandler
from add_data import DataAdder, SpecialCases
from data_parser import DataParser, AssetDeficit

# Create a temporary SQLite database in the tmp_path directory using the small dataset
@pytest.fixture
def database_small(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataAdder(DatabaseHandler(db_file),SpecialCases("./test/special_cases_test.json"))
    # Add data to database
    data_adder.add_data("./test/small_data.csv")
    return DatabaseHandler(db_file)

# Create a temporary SQLite database in the tmp_path directory using the small dataset
# Special cases are not handled, creating a small deficit in the database
@pytest.fixture
def database_small_deficit(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataAdder(DatabaseHandler(db_file))
    # Add data to database
    data_adder.add_data("./test/small_data.csv")
    return DatabaseHandler(db_file)

def test_data_parser__small(database_small):
    # Create DataParser object
    data_parser = DataParser(database_small)
    # Connect to database and get number of unprocessed transactions
    database_small.connect()
    unprocessed = database_small.get_db_stat("Unprocessed" )
    # Process transactions
    data_parser.process_transactions()
    # Check that all previously unprocessed transactions are now processed
    assert database_small.get_db_stat("Processed" ) == unprocessed
    assert database_small.get_db_stat("Unprocessed" ) == 0

def test_data_parser__defecit(database_small_deficit):
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

    