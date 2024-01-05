import pytest

from database_handler import DatabaseHandler
from add_data import SpecialCases, DataAdder

@pytest.fixture(scope='function')
def db(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    return DatabaseHandler(str(db_file))

@pytest.fixture(scope='function')
def special_cases():
    return SpecialCases("./test/data/special_cases_test.json")

def test_data_adder_init(db,special_cases):
    # Create DataAdder object
    data_adder = DataAdder(db,special_cases)
    assert data_adder is not None
    # Create DataAdder object without special cases
    data_adder = DataAdder(db)
    assert data_adder is not None

def test_data_adder_add_data(db,special_cases):
    # Create DataAdder object
    data_adder = DataAdder(db,special_cases)
    # Add data to database
    rows_added = data_adder.add_data("./test/data/small_data.csv")
    # Check that the correct number of rows were added
    assert rows_added == 7
    # Check that the correct number of rows are in the database
    db.connect()
    assert db.get_db_stats(["Transactions"])["Transactions"] == rows_added
    db.disconnect()

    # Add the same data again
    new_rows_added = data_adder.add_data("./test/data/small_data.csv")
    # Check that no rows were added
    assert new_rows_added == 0

    # Add some overlapping data
    new_rows_added = data_adder.add_data("./test/data/small_data_plus.csv")
    # Check that the correct number of rows were added
    assert new_rows_added == 6

    # Check that the correct number of rows are in the database
    db.connect()
    assert db.get_db_stats(["Transactions"])["Transactions"] == rows_added+new_rows_added
    db.disconnect()
