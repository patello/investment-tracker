import sqlite3

class DatabaseHandler:

    def __init__(self, db_file):
        self.db_file = db_file
        self.conn = None
        self.connect()
        self.create_tables()
        self.disconnect()

    def connect(self):
        # Connect to database
        # PARSE_DECLTYPES is used to convert sqlite3 date objects to python datetime objects
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.PARSE_DECLTYPES
        # PARSE_COLNAMES is used to access columns by name
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.PARSE_COLNAMES
        self.conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    def disconnect(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    # Create tables if they do not exist
    def create_tables(self):
        if not self.conn:
            raise Exception("Database connection not established.")

        cursor = self.conn.cursor()

        # Enable foreign key support, used by month_assets table to reference assets and month_data
        cursor.execute("PRAGMA foreign_keys = ON;")

        # transactions contains all raw transactions
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions(
                date DATE NOT NULL, 
                account TEXT NOT NULL,
                transaction_type TEXT NOT NULL,
                asset_name TEXT NOT NULL,
                amount REAL NOT NULL,
                price REAL NOT NULL,
                total REAL NOT NULL,
                courtage REAL NOT NULL,
                currency TEXT NOT NULL,
                isin TEXT NOT NULL,
                processed INT DEFAULT 0
                )""")

        # month_data contains the total capital, deposits and withdrawals for each month
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS month_data(
                month DATE NOT NULL, 
                deposit REAL DEFAULT 0,
                withdrawal REAL DEFAULT 0,
                capital REAL DEFAULT 0,
                PRIMARY KEY(month)
                );""")

        # assets contains the total amount of each asset and the latest price
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS assets(
                asset_id INTEGER,
                asset TEXT UNIQUE NOT NULL,
                amount REAL DEFAULT 0,
                average_price REAL DEFAULT 0,
                average_purchase_price REAL DEFAULT 0,
                average_sale_price REAL DEFAULT 0,
                purchased_amount REAL DEFAULT 0,
                sold_amount REAL DEFAULT 0,
                latest_price REAL,
                latest_price_date DATE,
                PRIMARY KEY(asset_id)
                );""")

        # month_assets contains the amount of each asset held, purchased and sold each month
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS month_assets(
                month DATE NOT NULL,
                asset_id INTEGER NOT NULL,
                amount REAL DEFAULT 0,
                average_price REAL DEFAULT 0,
                average_purchase_price REAL DEFAULT 0,
                average_sale_price REAL DEFAULT 0,
                purchased_amount REAL DEFAULT 0,
                sold_amount REAL DEFAULT 0,
                FOREIGN KEY (month) REFERENCES month_data (month), 
                FOREIGN KEY (asset_id) REFERENCES assets (asset_id)
                PRIMARY KEY(month, asset_id)
                );""")

        self.conn.commit()

    # Function that takes a list of different stats that should be retreived from the database
    # and returns a list of the corresponding values
    # "Transactions" - Number of transactions
    # "Unprocessed" - Number of unprocessed transactions
    # "Assets" - Number of unique assets
    # "Capital" - Total capital
    # "Tables" - Number of tables in the database
    def get_db_stats(self, stats):
        if not self.conn:
            raise Exception("Database connection not established.")

        cursor = self.conn.cursor()

        # Create a dictionary to store the stats
        stat_value = {}

        # Get number of transactions
        if "Transactions" in stats:
            cursor.execute("SELECT COUNT(*) FROM transactions")
            stat_value["Transactions"] = cursor.fetchone()[0]

        # Get number of unprocessed transactions
        if "Unprocessed" in stats:
            cursor.execute("SELECT COUNT(*) FROM transactions WHERE processed = 0")
            stat_value["Unprocessed"] = cursor.fetchone()[0]

        # Get number of unique assets
        if "Assets" in stats:
            cursor.execute("SELECT COUNT(*) FROM assets")
            stat_value["Assets"] = cursor.fetchone()[0]

        # Get total capital
        if "Capital" in stats:
            # Take SUM of capital if there are any rows in month_data, otherwise set capital to 0
            cursor.execute("SELECT CASE WHEN COUNT(*) > 0 THEN SUM(capital) ELSE 0 END FROM month_data")
            stat_value["Capital"] = cursor.fetchone()[0]

        # Get number of tables
        if "Tables" in stats:
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            stat_value["Tables"] = cursor.fetchone()[0]

        return stat_value

# Example Usage
if __name__ == "__main__":
    # Connect to asset_data.db sqllite3 database
    db_handler = DatabaseHandler('./data/asset_data.db')
    db_handler.connect()

    # Create table for storing transactions if it does not exist
    db_handler.create_tables()

    # Get stats from database
    stats = ["Transactions", "Unprocessed", "Assets", "Capital", "Tables"]
    stats = db_handler.get_db_stats(stats)

    # Print each stat and its value
    for stat in stats:
        print(stat, stats[stat])

    db_handler.disconnect()

