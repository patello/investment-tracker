import sqlite3
import calendar
import logging
import csv
import operator
import json

from datetime import date, datetime
from functools import reduce
from database_handler import DatabaseHandler

logging.basicConfig(level=logging.DEBUG)

class AssetDeficit(Exception):
    """
    Exception that is raised when there is a mismatch between the amount of assets in the database and the amount of assets in the transactions
    """
    def __init__(self, message, data_parser: 'DataParser') -> None:
        """
        Parameters:
        message (str): The error message to be displayed.
        data_parser (DataParser): The DataParser instance where the error occurred.
        """
        super().__init__(message)
        self.data_parser = data_parser
        logging.error(message)
        data_parser.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
        logging.error("Unprocessed transactions:")
        for row in data_parser.transaction_cur.fetchall():
            logging.error(row)
        data_parser.db.conn.rollback()

class SpecialCases:
    """
    SpecialCases class handles special cases when adding data to the database. 
    The special cases are defined in a json file as a list of cases with the following structure:
    [{
        "condition": [
            { 
                "index": int, // Integer value of the column
                "value": str // String that the value of the column should be compared to
                ("operator": str) // If index represents a date, then operator needs to be provided
            },
            // ... more conditions ... ]
        },
        "replacement": [
            {
                "index": int, // Integer value of the column
                "value": str // String that the value of the column should be replaced with
            },
            // ... more replacements ... ] 
    } ... more cases ... ]
    """
    def __init__(self, file_path: str):
        """
        Parameters:
        file_path (str): Path to json file containing special cases.
        """
        # Read special cases from json file
        special_cases_file = open(file_path, "r")
        special_cases = json.load(special_cases_file)
        special_cases_file.close()

        # Define a mapping from operator strings to functions
        ops = {
            "==": operator.eq,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
            "!=": operator.ne
        }
        # Create a list of functions that check if a row matches a special case
        # and a list of functions that replace a row with a new row
        special_conditions = []
        special_replacements = []
        for case in special_cases:
            conditions = []
            for condition in case["condition"]:
                index = condition["index"]
                value = condition["value"]
                # If index is 0, then the value is a date encoded as YYYY-MM-DD and needs to be converted to a date object
                if index == 0:
                    value = datetime.strptime(value, "%Y-%m-%d").date()
                op_func = ops.get(condition.get("operator", "=="))  # default to "=="
                # Create a function that checks if a row matches a condition
                # Default values are used to avoid late binding
                conditions.append(lambda x, index=index, op_func=op_func, value=value: op_func(x[index], value))
            special_conditions.append(lambda x, conditions=conditions: all(condition(x) for condition in conditions))
            replacements = []
            for replacement in case["replacement"]:
                index = replacement["index"]
                value = replacement["value"]
                # Create a function that replaces a row with a new row
                # Default values are used to avoid late binding
                replacements.append(lambda x, index=index, value=value: x[:index] + (value,) + x[index + 1:])
            special_replacements.append(reduce(lambda f, g: lambda x: g(f(x)), replacements))

        # Check that special_conditions and special_replacements have the same length
        # If not, raise an error saying there was an error parsing the special cases file
        if len(special_conditions) != len(special_replacements):
            raise ValueError("Error parsing special cases file. Number of conditions and replacements do not match.")

        # Combine special_conditions and special_replacements into a single list
        self.special_cases = list(zip(special_conditions, special_replacements))

    #Check if a row matches a special case
    def handle_special_cases(self, row: list):
        """
        Takes a row from the csv file as a list and does replacements if the row matches a special case.

        Parameters:
        row (list): A row from the csv file.

        Returns:
        list: The same row after replacements have been made.
        """
        for i in range(len(self.special_cases)):
            #Special conditions are functions that check if a row matches a special case
            #They are stored in the first element of the special_cases list
            if self.special_cases[i][0](row):
                #Special replacements are functions that replace a row with a new row
                #They are stored in the second element of the special_cases list
                row = self.special_cases[i][1](row)
        return row

# TODO: The DataParser does not check which account the transaction is made from, consider implementing this

class DataParser:
    """
    DataParser class handles the processing of transactions in the database.
    """
    def __init__(self, db: DatabaseHandler, special_cases: SpecialCases = None):
        """
        Parameters:
        database (DatabaseHandler): The database to add data to.
        special_cases (SpecialCases): SpecialCases object that handles special rules when adding data to the database.
        """
        self.listing_change = {"to_asset":None,"to_asset_amount":None,"to_rowid":None}
        self.db = db
        self.special_cases = special_cases
        # Two cursors are used, one for handling writing processed lines and one responsible for keeping track of unprocessed lines
        self._data_cur = None
        self._transaction_cur = None

    # Getter function for self.data_cur
    @property
    def data_cur(self) -> sqlite3.Cursor:
        """
        Returns:
        sqlite3.Cursor: Cursor for creating new transactions in the database.
        """
        # If self._data_cursor is None, ask the database for a cursor
        if self._data_cur is None:
            self._data_cur = self.db.get_cursor()
        return self._data_cur
    
    # Getter function for self.transaction_cur
    @property
    def transaction_cur(self) -> sqlite3.Cursor:
        """
        Returns:
        sqlite3.Cursor: Cursor for iterating over transactions in the database.
        """
        # If self._transaction_cur is None, ask the database for a cursor
        if self._transaction_cur is None:
            self._transaction_cur = self.db.get_cursor()
        return self._transaction_cur

    
    def convert_number(self,number_string : str) -> float:
        """
        Takes a number as a string and converts it to a float, also converts comma to dot as decimal separator and '-' to 0.
        
        Parameters:
        number_string (str): A number as a string.

        Returns:
        float: The number as a float.
        """
        return 0.0 if number_string == "-" else float(number_string.replace(",","."))
    
    def add_data(self, file_path: str) -> int:
        """
        Takes a path to a csv file downloaded from Avanza and adds the data to the database.

        Parameters:
        file_path (str): Path to csv file downloaded from Avanza.

        Returns:
        int: Number of rows added to the database.
        """
        # file_path = ./data/newdata.csv
        avanza_data_file = open(file_path,"r")
        avanza_data = csv.reader(avanza_data_file, delimiter=';')
        # Currently unused, but could be used to check that the file is in the correct format
        # next() used to skip header row
        avanza_header_row = next(avanza_data)
        avanza_data = list(avanza_data)

        # Find overlapping transactions to avoid adding douplicates
        max_date = max([datetime.strptime(row[0],"%Y-%m-%d") for row in avanza_data]).date()
        min_date = min([datetime.strptime(row[0],"%Y-%m-%d") for row in avanza_data]).date()
        logging.debug("Date range of transactions to be added: {} - {}".format(min_date,max_date))

        # Variable to keep track of number of rows added to the database
        rows_added = 0

        # Connect to database
        self.db.connect()
        cur = self.db.conn.cursor()
        existing_rows = cur.execute("SELECT date, account, transaction_type, asset_name, amount, price FROM transactions WHERE date >= ? and date <= ?",(min_date,max_date)).fetchall()

        #Add transactions to database
        for transaction in avanza_data:
            row = (\
                datetime.strptime(transaction[0], "%Y-%m-%d").date(),\
                transaction[1],transaction[2],transaction[3],\
                self.convert_number(transaction[4]),self.convert_number(transaction[5]),\
                self.convert_number(transaction[6]),self.convert_number(transaction[7]),\
                transaction[8],transaction[9]
                )
            # If special_cases is not None, handle special cases
            if self.special_cases != None:
                row = self.special_cases.handle_special_cases(row)
            # If row is not in existing_rows, add it to the database
            if row[0:6] not in existing_rows:
                logging.debug("Adding row to database: {}".format(row))
                cur.execute('INSERT INTO transactions(date, account, transaction_type,asset_name,amount,price,total,courtage,currency,isin) VALUES(?,?,?,?,?,?,?,?,?,?);',row)
                rows_added += 1
            else:
                logging.debug("Row already in database: {}".format(row))

        # Commit changes to database and disconnect
        self.db.conn.commit()
        self.db.disconnect()

        # Return number of rows added to the database
        logging.info("Added {} rows to the database".format(rows_added))
        return rows_added
    
    def reset_processed_transactions(self) -> None:
        """
        Resets the processed flag for all transactions in the database. Also resets month_data, month_assets and assets tables.
        """
        self.data_cur.execute("UPDATE transactions SET processed = 0")
        self.db.reset_table("month_data")
        self.db.reset_table("month_assets")
        self.db.reset_table("assets")
        self.db.commit()

    def allocate_to_month(self, transaction_date: date) -> date:
        """
        Takes a date and returns which month the transaction should be allocated to.
        If the transaction is made within the first cutoff_days of the month, allocate it to the previous month.

        Parameters:
        transaction_date (date): The date of the transaction.

        Returns:
        date: The date of the month the transaction should be allocated to.
        """
        cutoff_days = 10
        day = transaction_date.day
        month = transaction_date.month
        year = transaction_date.year

        if day <= cutoff_days:
            if month > 1:
                month = month - 1
            else:
                month = 12
                year = year - 1
        
        day = calendar.monthrange(year,month)[1]
        return date(year,month,day)

    def available_capital(self) -> list:
        """
        Get available capital for each recorded month. If there is no recorded month, return (None,0).

        Returns:
        list: List of tuples with the first element being the month and the second element being the available capital for that month.
        """
        res = self.data_cur.execute("SELECT month, capital FROM month_data WHERE capital > 0 ORDER BY month ASC").fetchall()
        if len(res) > 0:
            return res
        else:
            return [(None,0)]

    def available_asset(self, asset_id: int) -> list:
        """
        Get available assets for each recorded month. If there is no recorded month, return (None,0).

        Parameters:
        asset_id (int): The id of the asset to get available assets for.

        Returns:
        list: List of tuples with the first element being the month and the second element being the amount of available assets for that month.
        """
        res = self.data_cur.execute("SELECT month, amount FROM month_assets WHERE amount > 0 AND asset_id = ? ORDER BY month ASC",(asset_id,)).fetchall()
        if len(res) > 0:
            return res
        else:
            return [(None,0)]

    def handle_deposit(self, row: tuple) -> None:
        """
        Takes a transaction row and adds the amount to the capital of the month the transaction is allocated to.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        month = self.allocate_to_month(row[0])
        amount = row[6]
        self.data_cur.execute("UPDATE month_data SET capital = capital + ?, deposit = deposit + ? WHERE month = ?",(amount,amount,month))
        # Reset transaction_cur since new funds are available
        self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_withdrawal(self, row: tuple) -> None:
        """
        Takes a transaction row and subtracts the amount from the capital of the oldest month(s) with available capital.
        If there is not enough total capital available, the transaction is not processed.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        total_amount = -row[6]
        remaining_amount = total_amount
        month_capital = self.available_capital()
        total_capital = sum(e[1] for e in month_capital)
        if total_capital + 1e-4 >= total_amount:
            i = 0
            while remaining_amount > 1e-4:
                (oldest_available,capital) = month_capital[i]
                month_amount = min(remaining_amount,capital)
                self.data_cur.execute("UPDATE month_data SET capital = capital - ?, withdrawal = withdrawal + ? WHERE month = ?",(month_amount,month_amount,oldest_available))
                remaining_amount -= month_amount
                i += 1
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_purchase(self, row: tuple) -> None: 
        """
        Takes a transaction row and subtracts the purchase amount from the capital of the oldest month(s) with available capital.
        Then allocates a proportional amount of the asset to those months.
        If there is not enough total capital available, the transaction is not processed.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        asset = row[3]
        self.data_cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES (?) ",(asset,))
        asset_id = self.data_cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
        asset_amount = row[4]
        price = row[5]
        total_amount = -row[6]
        remaining_amount = total_amount
        month_capital = self.available_capital()
        total_capital = sum(e[1] for e in month_capital)
        if total_capital + 1e-3 >= total_amount:
            i = 0
            while remaining_amount > 1e-3:
                (oldest_available,capital) = month_capital[i]
                month_amount = min(remaining_amount,capital)
                month_asset_amount = month_amount / total_amount * asset_amount
                self.data_cur.execute("UPDATE month_data SET capital = capital - ? WHERE month = ?",(month_amount,oldest_available))
                self.data_cur.execute("INSERT OR IGNORE INTO month_assets(month,asset_id) VALUES (?,?)",(oldest_available,asset_id))
                self.data_cur.execute("UPDATE month_assets SET average_price = ?/(amount+?)*?+amount/(amount+?)*average_price WHERE month = ? AND asset_id = ?",(month_amount,month_amount,price,month_amount,oldest_available,asset_id))
                self.data_cur.execute("UPDATE month_assets SET average_purchase_price = ?/(purchased_amount+?)*?+purchased_amount/(purchased_amount+?)*average_purchase_price WHERE month = ? AND asset_id = ?",(month_amount,month_amount,price,month_amount,oldest_available,asset_id))
                self.data_cur.execute("UPDATE month_assets SET amount = amount + ?, purchased_amount = purchased_amount + ? WHERE month = ? AND asset_id = ?",(month_asset_amount, month_asset_amount, oldest_available,asset_id))
                remaining_amount -= month_amount
                i += 1
            # Reset transaction_cur since new assets are available
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
 
    def handle_sale(self, row: tuple) -> None:
        """
        Takes a transaction row and subtracts the sale asset amount from the oldest month(s) with that available asset.
        Then adds the sale amount to the capital of those months.
        If there is not enough total assets available, the transaction is not processed.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        asset = row[3]
        self.data_cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES (?) ",(asset,))
        asset_id = self.data_cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
        asset_amount = -row[4]
        price = row[5]
        total_amount = row[6]
        remaining_amount = asset_amount
        month_asset_amounts = self.available_asset(asset_id)
        total_asset_amount = sum(e[1] for e in month_asset_amounts)
        if total_asset_amount + 1e-3 >= asset_amount:
            i = 0
            while remaining_amount > 1e-3:
                (oldest_available,amount) = month_asset_amounts[i]
                month_amount = min(remaining_amount,amount)
                month_capital_amount = month_amount / asset_amount * total_amount
                self.data_cur.execute("UPDATE month_assets SET average_sale_price = ?/(sold_amount+?)*?+sold_amount/(sold_amount+?)*average_sale_price WHERE month = ? AND asset_id = ?",(month_amount,month_amount,price,month_amount,oldest_available,asset_id))
                self.data_cur.execute("UPDATE month_assets SET amount = amount - ?, sold_amount = sold_amount + ? WHERE month = ? AND asset_id = ?",(month_amount, month_amount, oldest_available,asset_id))
                self.data_cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(month_capital_amount,oldest_available))
                remaining_amount -= month_amount
                i += 1
            # Reset transaction_cur since new funds are available
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_dividend(self, row: tuple) -> None:
        """
        Takes a transaction row and adds the dividend amount proporionally to the capital of all the month(s) with that available asset.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        dividend_month = self.allocate_to_month(row[0])
        asset = row[3]
        asset_id = self.data_cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
        remaining_amount = row[4]
        dividend_per_asset = row[5]
        month_asset_amounts = self.available_asset(asset_id)
        for (month,asset_amount) in month_asset_amounts:
                self.data_cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(asset_amount*dividend_per_asset,month))
                remaining_amount -= asset_amount
        if remaining_amount > 0:
            self.data_cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(remaining_amount*dividend_per_asset,dividend_month))
        # Reset transaction_cur since new funds are available
        self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_interest(self, row: tuple) -> None:
        """
        Takes a transaction row and adds the dividend amount proporionally to the capital of all the month(s) with that available capital.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        dividend_month = self.allocate_to_month(row[0])
        remaining_amount = row[6]
        month_capital = self.available_capital()
        total_capital = sum([month[1] for month in month_capital])
        dividend_per_capital = remaining_amount/total_capital
        for (month,capital) in month_capital:
                self.data_cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(capital*dividend_per_capital,month))
                remaining_amount -= capital
        if remaining_amount > 0:
            self.data_cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(remaining_amount,dividend_month))
        # Reset transaction_cur since new funds are available
        self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_fees(self, row: tuple) -> None:
        """
        Takes a transaction row and subtracts the fee amount from the capital of the oldest month(s) with available capital.
        If there is not enough total capital available, the transaction is not processed.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        total_amount = -row[6]
        remaining_amount = total_amount
        month_capital = self.available_capital()
        total_capital = sum(e[1] for e in month_capital)
        if total_capital + 1e-4 >= total_amount:
            i = 0
            while remaining_amount > 1e-4:
                (oldest_available,capital) = month_capital[i]
                month_amount = min(remaining_amount,capital)
                self.data_cur.execute("UPDATE month_data SET capital = capital - ? WHERE month = ?",(month_amount,oldest_available))
                remaining_amount -= month_amount
                i += 1
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_listing_change(self, row: tuple) -> None:
        """
        Takes a transaction row and changes the asset name and amount.
        The first time the function is called, the old asset name and amount is saved and the transaction is not processed.
        The second time the function is called, the old asset name and amount is changed to the new asset name and amount and both transactions are marked as processed.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """  
        if self.listing_change["to_asset"] is None:
            self.listing_change["to_asset"] = row[3]
            self.listing_change["to_asset_amount"] = row[4]
            self.listing_change["to_rowid"] = row[-1]
        else:
            asset = row[3]
            amount = -row[4]
            (asset_id,) = self.data_cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()
            self.data_cur.execute("UPDATE assets SET asset = ?, amount = ? WHERE asset_id = ?",(self.listing_change["to_asset"],self.listing_change["to_asset_amount"],asset_id))
            change_factor = self.listing_change["to_asset_amount"]/amount
            self.data_cur.execute("UPDATE month_assets SET amount = amount * ? WHERE asset_id = ?",(change_factor,asset_id))
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ? OR rowid = ?",(row[-1],self.listing_change["to_rowid"],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
            self.listing_change = {"to_asset":None,"to_asset_amount":None,"to_rowid":None}
    
    def handle_asset_deposit(self, row: tuple) -> None:
        """
        Takes a transaction row and adds the amount to the assets of the month the transaction is allocated to.
        Counts as a deposit for the month the transaction is allocated to so the price is also added to the deposit of that month.

        Parameters:
        row (tuple): A row from the transactions table in the database.
        """
        month = self.allocate_to_month(row[0])
        asset = row[3]
        amount = row[4]
        price = row[5]
        self.data_cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES (?) ",(asset,))
        asset_id = self.data_cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
        self.data_cur.execute("INSERT OR IGNORE INTO month_assets(month,asset_id) VALUES (?,?)",(month,asset_id))
        # Update average price and average purchase price
        self.data_cur.execute("UPDATE month_assets SET average_price = (? * ? + amount * average_price) / (amount + ?) WHERE month = ? AND asset_id = ?", (amount, price, amount, month, asset_id))
        self.data_cur.execute("UPDATE month_assets SET average_purchase_price = (? * ? + purchased_amount * average_purchase_price) / (purchased_amount + ?) WHERE month = ? AND asset_id = ?", (amount, price, amount, month, asset_id))# Update amount and purchased amount
        # Update amount and purchased amount
        self.data_cur.execute("UPDATE month_assets SET amount = amount + ? WHERE month = ? AND asset_id = ?",(amount,month,asset_id))
        self.data_cur.execute("UPDATE month_data SET deposit = deposit + ? WHERE month = ?",(amount*price,month))
        # Reset transaction_cur since new assets are available
        self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def process_transactions(self) -> None:
        """
        Process transactions all transactions in the database that have not been processed yet.
        After attempting to processing all transactions, the function checks if there are any unprocessed transactions left.
        If there are, an AssetDeficit exception is raised and the database is rolled back. Otherwise, the changes are committed.
        """
        unprocessed_lines = self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
        row = unprocessed_lines.fetchone()
        #Consider upgrading to python3.8 to make this more elegant with := statment
        while row is not None:
            month = self.allocate_to_month(row[0])
            self.data_cur.execute("INSERT OR IGNORE INTO month_data(month) VALUES(?)",(month,))
            if row[2] == "Insättning":
                self.handle_deposit(row)
            elif row[2] == "Uttag":
                self.handle_withdrawal(row)
            elif row[2] == "Köp":
                self.handle_purchase(row)
            elif row[2] == "Sälj":
                self.handle_sale(row)
            elif row[2] == "Utdelning":
                self.handle_dividend(row)
            elif row[2] == "Räntor" or row[2] == "Ränta":
                # Interest can either be negative and handled as a fee, or positive and handled like a dividend on capital
                if row[6] > 0:
                    self.handle_interest(row)
                else:
                    self.handle_fees(row)
            elif "Utländsk källskatt" in row[2] in row[2] or "Prelskatt" in row[2] or "Preliminärskatt" in row[2]:
                self.handle_fees(row)
            elif "Byte" in row[2] or row[2] == "Övrigt":
                self.handle_listing_change(row)
            elif row[2] == "Tillgångsinsättning":
                self.handle_asset_deposit(row)
            else:
                raise(ValueError)
            row = unprocessed_lines.fetchone()

        unprocessed_count = self.transaction_cur.execute("SELECT COUNT(*) FROM transactions WHERE processed == 0").fetchone()[0]
        if unprocessed_count > 0:
            raise AssetDeficit("There are {} transaction(s) that could not be processed due to a missmatch of assets in the database".format(unprocessed_count),self)
        else:
            #Calculate summary data and put it in asset table
            asset_ids = self.data_cur.execute("SELECT asset_id FROM assets").fetchall()
            for (id,) in asset_ids:
                month_asset_data = self.data_cur.execute("SELECT amount, average_price, average_purchase_price, average_sale_price, purchased_amount, sold_amount FROM month_assets WHERE asset_id = ?",(id,)).fetchall()
                amount = 0
                average_price = 0
                average_purchase_price = 0
                average_sale_price = 0
                purchased_amount = 0
                sold_amount = 0
                for month_asset in month_asset_data:
                    month_amount, month_average_price, month_average_purchase_price, month_average_sale_price, month_purchased_amount, month_sold_amount = month_asset
                    if month_amount > 1e-3:
                        average_price = month_amount/(month_amount+amount)*month_average_price+amount/(month_amount+amount)*average_price
                        amount += month_amount
                    if month_purchased_amount > 1e-3:
                        average_purchase_price = month_purchased_amount/(month_purchased_amount+purchased_amount)*month_average_purchase_price+purchased_amount/(month_purchased_amount+purchased_amount)*average_purchase_price
                        purchased_amount += month_purchased_amount
                    if month_sold_amount > 1e-3:
                        average_sale_price = month_sold_amount/(month_sold_amount+sold_amount)*month_average_sale_price+sold_amount/(month_sold_amount+sold_amount)*average_sale_price
                        sold_amount += month_sold_amount
                self.data_cur.execute("UPDATE assets SET amount = ?, average_price = ?, average_purchase_price = ?, average_sale_price = ?, purchased_amount = ?, sold_amount = ? WHERE asset_id = ?",(amount,average_price,average_purchase_price,average_sale_price,purchased_amount,sold_amount,id,))
            #Commit changes
            self.db.commit()

if __name__ == "__main__":
    # Create DatabaseHandler object
    db = DatabaseHandler("data/asset_data.db")
    # Create SpecialCases object
    special_cases = SpecialCases("data/special_cases.json")
    # Create DataAdder object
    data_adder = DataParser(db,special_cases)
    # Add data from newdata.csv to the database
    rows_added = data_adder.add_data("data/newdata.csv")

    # Print number of rows added to the database
    print("Added {} rows to the database".format(rows_added))
    
    # Process transactions
    data_adder.process_transactions()
