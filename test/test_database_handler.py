import pytest

from database_handler import DatabaseHandler

@pytest.fixture(scope='function')
def db_handler(tmp_path):
    # Create a temporary SQLite database in the tmp_path directory
    db_file = tmp_path / "test_asset_data.db"
    return DatabaseHandler(str(db_file))

def test_database_handler__init(db_handler):
    assert db_handler is not None

def test_database_handler__connect(db_handler):
    db_handler.connect()
    assert db_handler.conn is not None
    db_handler.disconnect()
    assert db_handler.conn is None

def test_database_hanler__get_cursor(db_handler):
    # Test that get_cursor() returns a cursor object even when the database is not connected
    cursor1 = db_handler.get_cursor()
    assert cursor1 is not None

    # Test that get_cursor() returns a cursor object when the database is already connected
    cursor2 = db_handler.get_cursor()
    assert cursor2 is not None

# Test that the database handler can return stats correctly
def test_database_handler__get_db_stats(db_handler):
    db_handler.connect()
    stats = db_handler.get_db_stats(["Transactions", "Unprocessed", "Processed", "Assets", "Capital", "Tables"])
    assert stats["Transactions"] == 0
    assert stats["Unprocessed"] == 0
    assert stats["Processed"] == 0
    assert stats["Assets"] == 0
    assert stats["Capital"] == 0
    assert stats["Tables"] == 4
    db_handler.disconnect()

# Test that the database handler can return stats correctly
def test_database_handler__get_db_stat(db_handler):
    db_handler.connect()
    assert db_handler.get_db_stat("Transactions") == 0
    assert db_handler.get_db_stat("Unprocessed") == 0
    assert db_handler.get_db_stat("Processed") == 0
    assert db_handler.get_db_stat("Assets") == 0
    assert db_handler.get_db_stat("Capital") == 0
    assert db_handler.get_db_stat("Tables") == 4
    db_handler.disconnect()