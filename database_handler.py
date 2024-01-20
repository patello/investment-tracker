import sqlite3

class DatabaseHandler:
    """
    A class that handles connection to a sqllite3 database.
    """

    def __init__(self, db_file: str):
        """
        Parameters:
        db_file (str): Path to the database file.
        """
        self.db_file = db_file
        self.conn = None
        self.connect()
        self.tables = self.create_tables()
        self.disconnect()

    def connect(self) -> None:
        """
        Connects to the database with PARSE_DECLTYPES and PARSE_COLNAMES enabled.
        """
        # PARSE_DECLTYPES is used to convert sqlite3 date objects to python datetime objects
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.PARSE_DECLTYPES
        # PARSE_COLNAMES is used to access columns by name
        # https://docs.python.org/3/library/sqlite3.html#sqlite3.PARSE_COLNAMES
        self.conn = sqlite3.connect(self.db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)

    def disconnect(self) -> None:
        """
        Disconnects from the database. Sets self.conn to None.
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    def commit(self) -> None:
        """
        Commits changes to the database. Raises exception if no connection is established.
        """
        if self.conn:
            self.conn.commit()
        else:
            raise Exception("Cannot commit changes, database connection not established.")
        
    def get_cursor(self) -> sqlite3.Cursor:
        """
        Returns a cursor to the database.

        Returns:
        sqlite3.Cursor: Cursor to the database.
        """
        if self.conn == None:
            self.connect()
        return self.conn.cursor()

    def create_tables(self) -> list:
        """
        Creates transaction tables in the database if they do not exist. Raises exception if no connection is established.

        Returns:
        list: List of tables in the database (wether existing or created).
        """
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
        
        # month_stats contains statistics about capital transfers and gain/loss for each month
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS month_stats(
                month DATE NOT NULL,
                deposit REAL DEFAULT 0,
                withdrawal REAL DEFAULT 0,
                capital REAL DEFAULT 0,
                value REAL DEFAULT 0,
                total_gainloss REAL DEFAULT 0,
                realized_gainloss REAL DEFAULT 0,
                unrealized_gainloss REAL DEFAULT 0,
                total_gainloss_per REAL DEFAULT 0,
                realized_gainloss_per REAL DEFAULT 0,
                unrealized_gainloss_per REAL DEFAULT 0,
                annual_per_yield REAL DEFAULT NULL,
                acc_deposit REAL DEFAULT 0,
                acc_value REAL DEFAULT 0,
                acc_withdrawal REAL DEFAULT 0,
                acc_net_deposit REAL DEFAULT 0,
                acc_total_gainloss REAL DEFAULT 0,
                acc_realized_gainloss REAL DEFAULT 0,
                acc_unrealized_gainloss REAL DEFAULT 0,
                PRIMARY KEY(month)
                );""")
        
        # year_stats contains statistics about capital transfers and gain/loss for each year
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS year_stats(
                year DATE NOT NULL,
                deposit REAL DEFAULT 0,
                withdrawal REAL DEFAULT 0,
                capital REAL DEFAULT 0,
                value REAL DEFAULT 0,
                total_gainloss REAL DEFAULT 0,
                realized_gainloss REAL DEFAULT 0,
                unrealized_gainloss REAL DEFAULT 0,
                total_gainloss_per REAL DEFAULT 0,
                realized_gainloss_per REAL DEFAULT 0,
                unrealized_gainloss_per REAL DEFAULT 0,
                annual_per_yield REAL DEFAULT NULL,
                acc_deposit REAL DEFAULT 0,
                acc_value REAL DEFAULT 0,
                acc_withdrawal REAL DEFAULT 0,
                acc_net_deposit REAL DEFAULT 0,
                acc_total_gainloss REAL DEFAULT 0,
                acc_realized_gainloss REAL DEFAULT 0,
                acc_unrealized_gainloss REAL DEFAULT 0,
                PRIMARY KEY(year)
                );""")

        self.conn.commit()

        # Return a list of tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [table[0] for table in cursor.fetchall()]

    def reset_table(self, table: str) -> int:
        """
        Deletes all rows from the specified table. Raises exception if no connection is established.

        Parameters:
        table (str): Name of the table to be reset.

        Returns:
        int: Number of rows deleted from the table.
        """
        if not self.conn:
            raise Exception("Database connection not established.")

        cursor = self.conn.cursor()

        cursor.execute("DELETE FROM {}".format(table))

        return cursor.rowcount

    def get_db_stats(self, stats: list) -> dict:
        """
        Returns a dictionary with the requested stats from the database. Available stats are:
        * "Transactions" - Number of transactions
        * "Unprocessed" - Number of unprocessed transactions
        * "Processed" - Number of processed transactions
        * "Assets" - Number of unique assets
        * "Capital" - Total capital
        * "Tables" - Number of tables in the database

        Parameters:
        stats (list): List of stats to be retreived from the database.

        Returns:
        dict: Dictionary with the requested stats from the database.
        """
        if not self.conn:
            raise Exception("Database connection not established.")

        cursor = self.conn.cursor()

        # Create a dictionary to store the stats
        stat_value = {}

        # Get number of transactions
        if "Transactions" in stats:
            cursor.execute("SELECT COUNT(*) FROM transactions")
            stat_value["Transactions"] = cursor.fetchone()[0]

        # Get number of processed transactions
        if "Processed" in stats:
            cursor.execute("SELECT COUNT(*) FROM transactions WHERE processed = 1")
            stat_value["Processed"] = cursor.fetchone()[0]

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

    def get_db_stat(self, stat: str) -> float:
        """
        Function that takes a single string corresponding to a stat in get_db_stats

        Parameters:
        stat (str): String corresponding to a stat in get_db_stats

        Returns:
        int or float: Value of the requested stat
        """
        return self.get_db_stats([stat])[stat]

# Example Usage
if __name__ == "__main__":
    # Connect to asset_data.db sqllite3 database
    db_handler = DatabaseHandler('./data/asset_data.db')
    db_handler.connect()

    # Create table for storing transactions if it does not exist
    db_handler.create_tables()

    # Get stats from database
    stats = ["Transactions", "Processed", "Unprocessed", "Assets", "Capital", "Tables"]
    stats = db_handler.get_db_stats(stats)

    # Print each stat and its value
    for stat in stats:
        print(stat, stats[stat])

    db_handler.disconnect()

