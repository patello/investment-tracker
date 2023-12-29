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

def test_database_handler__get_db_stats(db_handler):
    db_handler.connect()
    stats = db_handler.get_db_stats(["Transactions", "Unprocessed", "Assets", "Capital", "Tables"])
    assert stats["Transactions"] == 0
    assert stats["Unprocessed"] == 0
    assert stats["Assets"] == 0
    assert stats["Capital"] == 0
    assert stats["Tables"] == 4
    db_handler.disconnect()