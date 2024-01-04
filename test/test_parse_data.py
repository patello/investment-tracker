import pytest

from database_handler import DatabaseHandler
from add_data import DataAdder, SpecialCases
from data_parser import DataParser

@pytest.fixture
def database_small(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    # Create DataAdder object
    data_adder = DataAdder(DatabaseHandler(db_file),SpecialCases("./test/special_cases_test.json"))
    # Add data to database
    data_adder.add_data("./test/small_data.csv")
    return DatabaseHandler(db_file)

def test_data_parser__small(database_small):
    # Create DataParser object
    data_parser = DataParser(database_small)
    # Connect to database and get number of unprocessed transactions
    database_small.connect()
    unprocessed = database_small.get_db_stats(["Unprocessed"])
    # Process transactions
    data_parser.process_transactions()
    # Get number of processed transactions after processing. 
    processed = database_small.get_db_stats(["Processed"])
    # Check that all previously unprocessed transactions are now processed
    assert processed["Processed"] == unprocessed["Unprocessed"]
    # Get new number of unprocessed transactions
    unprocessed = database_small.get_db_stats(["Unprocessed"])
    # Check that there are no unprocessed transactions
    assert unprocessed["Unprocessed"] == 0
