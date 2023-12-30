import sqlite3
import calendar
import logging

from datetime import date
from database_handler import DatabaseHandler
from add_data import SpecialCases, DataAdder

logging.basicConfig(level=logging.DEBUG)

# Execption that is rased when there is a missmatch between the amount of assets in the database and the amount of assets in the transactions
class AssetDeficit(Exception):
    pass

class DataParser:
    def __init__(self, db):
        self.listing_change = {"to_asset":None,"to_asset_amount":None,"to_rowid":None}
        self.db = db
        self.conn = None
        #Two cursors are used, one for handling writing processed lines and one responsible for keeping track of unprocessed lines
        self._data_cur = None
        self._transaction_cur = None

    # Getter function for self.data_cur
    @property
    def data_cur(self):
        # If self._data_cursor is None, ask the database for a cursor
        if self._data_cur is None:
            self._data_cur = self.db.get_cursor()
        return self._data_cur
    
    # Getter function for self.transaction_cur
    @property
    def transaction_cur(self):
        # If self._transaction_cur is None, ask the database for a cursor
        if self._transaction_cur is None:
            self._transaction_cur = self.db.get_cursor()
        return self._transaction_cur
    
    # Takes a date and returns which month the transaction should be allocated to.
    # If the transaction is made within the first cutoff_days of the month, allocate it to the previous month
    def allocate_to_month(self, transaction_date):
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

    def available_capital(self):
        res = self.data_cur.execute("SELECT month, capital FROM month_data WHERE capital > 0 ORDER BY month ASC").fetchall()
        if len(res) > 0:
            return res
        else:
            return [(None,0)]

    def available_asset(self, asset_id):
        res = self.data_cur.execute("SELECT month, amount FROM month_assets WHERE amount > 0 AND asset_id = ? ORDER BY month ASC",(asset_id,)).fetchall()
        if len(res) > 0:
            return res
        else:
            return [(None,0)]

    def handle_deposit(self, row):
        month = self.allocate_to_month(row[0])
        amount = row[6]
        self.data_cur.execute("UPDATE month_data SET capital = capital + ?, deposit = deposit + ? WHERE month = ?",(amount,amount,month))
        self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_withdrawal(self, row):
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

    def handle_purchase(self, row):    
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
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
 
    def handle_sale(self, row):    
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
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_dividend(self, row):
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
        self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

    def handle_fees(self, row):
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

    def handle_listing_change(self, row):
        global listing_change
        if listing_change["to_asset"] is None:
            listing_change["to_asset"] = row[3]
            listing_change["to_asset_amount"] = row[4]
            listing_change["to_rowid"] = row[-1]
        else:
            asset = row[3]
            amount = -row[4]
            (asset_id,) = self.data_cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()
            self.data_cur.execute("UPDATE assets SET asset = ?, amount = ? WHERE asset_id = ?",(listing_change["to_asset"],listing_change["to_asset_amount"],asset_id))
            change_factor = listing_change["to_asset_amount"]/amount
            self.data_cur.execute("UPDATE month_assets SET amount = amount * ? WHERE asset_id = ?",(change_factor,asset_id))
            self.transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ? OR rowid = ?",(row[-1],listing_change["to_rowid"],))
            self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
            listing_change = {"to_asset":None,"to_asset_amount":None,"to_rowid":None}
    
    def process_transactions(self):
        self._transaction_cur = self.data_cur
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
            elif "Utländsk källskatt" in row[2] or "Ränt" in row[2] or "Prelskatt" in row[2] or "Preliminärskatt" in row[2]:
                self.handle_fees(row)
            elif "Byte" in row[2] or row[2] == "Övrigt":
                self.handle_listing_change(row)
            else:
                raise(ValueError)
            row = unprocessed_lines.fetchone()

        unprocessed_lines = self.transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
        if unprocessed_lines.fetchone() is not None:
            raise AssetDeficit
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
            self.data_cur.commit()

if __name__ == "__main__":
    # Create DatabaseHandler object
    db = DatabaseHandler("data/asset_data.db")
    # Create SpecialCases object
    special_cases = SpecialCases("data/special_cases.json")
    # Create DataAdder object
    data_adder = DataAdder(db,special_cases)
    # Add data from newdata.csv to the database
    rows_added = data_adder.add_data("data/newdata.csv")

    # Print number of rows added to the database
    print("Added {} rows to the database".format(rows_added))

    # Create DataParser object
    data_parser = DataParser(db)
    # Process transactions
    data_parser.process_transactions()