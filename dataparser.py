import sqlite3

class AssetDeficit(Exception):
    pass

con = sqlite3.connect("data/asset_data.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

cur.execute("PRAGMA foreign_keys = ON;")

cur.execute("""
    CREATE TABLE IF NOT EXISTS month_data(
        month DATE NOT NULL, 
        deposit REAL DEFAULT 0,
        withdrawal REAL DEFAULT 0,
        capital REAL DEFAULT 0,
        PRIMARY KEY(month)
        );""")

cur.execute("""
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

cur.execute("""
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


listing_change = {"to_asset":None,"to_asset_amount":None,"to_rowid":None}

def available_capital():
    res = cur.execute("SELECT month, capital FROM month_data WHERE capital > 0 ORDER BY month ASC").fetchall()
    if len(res) > 0:
        return res
    else:
        return [(None,0)]

def available_asset(asset_id):
    res = cur.execute("SELECT month, amount FROM month_assets WHERE amount > 0 AND asset_id = ? ORDER BY month ASC",(asset_id,)).fetchall()
    if len(res) > 0:
        return res
    else:
        return [(None,0)]


def handle_deposit(row):
    month = row[0].replace(day=1)
    amount = row[6]
    cur.execute("UPDATE month_data SET capital = capital + ?, deposit = deposit + ? WHERE month = ?",(amount,amount,month))
    transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
    transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

def handle_withdrawal(row):
    total_amount = -row[6]
    remaining_amount = total_amount
    month_capital = available_capital()
    total_capital = sum(e[1] for e in month_capital)
    if total_capital + 1e-4 >= total_amount:
        i = 0
        while remaining_amount > 1e-4:
            (oldest_available,capital) = month_capital[i]
            month_amount = min(remaining_amount,capital)
            cur.execute("UPDATE month_data SET capital = capital - ?, withdrawal = withdrawal + ? WHERE month = ?",(month_amount,month_amount,oldest_available))
            remaining_amount -= month_amount
            i += 1
        transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

def handle_purchase(row):    
    asset = row[3]
    cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES (?) ",(asset,))
    asset_id = cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
    asset_amount = row[4]
    price = row[5]
    total_amount = -row[6]
    remaining_amount = total_amount
    month_capital = available_capital()
    total_capital = sum(e[1] for e in month_capital)
    if total_capital + 1e-3 >= total_amount:
        i = 0
        while remaining_amount > 1e-3:
            (oldest_available,capital) = month_capital[i]
            month_amount = min(remaining_amount,capital)
            month_asset_amount = month_amount / total_amount * asset_amount
            cur.execute("UPDATE month_data SET capital = capital - ? WHERE month = ?",(month_amount,oldest_available))
            cur.execute("INSERT OR IGNORE INTO month_assets(month,asset_id) VALUES (?,?)",(oldest_available,asset_id))
            cur.execute("UPDATE month_assets SET average_price = ?/(amount+?)*?+amount/(amount+?)*average_price WHERE month = ? AND asset_id = ?",(month_amount,month_amount,price,month_amount,oldest_available,asset_id))
            cur.execute("UPDATE month_assets SET average_purchase_price = ?/(purchased_amount+?)*?+purchased_amount/(purchased_amount+?)*average_purchase_price WHERE month = ? AND asset_id = ?",(month_amount,month_amount,price,month_amount,oldest_available,asset_id))
            cur.execute("UPDATE month_assets SET amount = amount + ?, purchased_amount = purchased_amount + ? WHERE month = ? AND asset_id = ?",(month_asset_amount, month_asset_amount, oldest_available,asset_id))
            remaining_amount -= month_amount
            i += 1
        transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
 
def handle_sale(row):    
    asset = row[3]
    cur.execute("INSERT OR IGNORE INTO assets (asset) VALUES (?) ",(asset,))
    asset_id = cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
    asset_amount = -row[4]
    price = row[5]
    total_amount = row[6]
    remaining_amount = asset_amount
    month_asset_amounts = available_asset(asset_id)
    total_asset_amount = sum(e[1] for e in month_asset_amounts)
    if total_asset_amount + 1e-3 >= asset_amount:
        i = 0
        while remaining_amount > 1e-3:
            (oldest_available,amount) = month_asset_amounts[i]
            month_amount = min(remaining_amount,amount)
            month_capital_amount = month_amount / asset_amount * total_amount
            cur.execute("UPDATE month_assets SET average_sale_price = ?/(sold_amount+?)*?+sold_amount/(sold_amount+?)*average_sale_price WHERE month = ? AND asset_id = ?",(month_amount,month_amount,price,month_amount,oldest_available,asset_id))
            cur.execute("UPDATE month_assets SET amount = amount - ?, sold_amount = sold_amount + ? WHERE month = ? AND asset_id = ?",(month_amount, month_amount, oldest_available,asset_id))
            cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(month_capital_amount,oldest_available))
            remaining_amount -= month_amount
            i += 1
        transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

def handle_dividend(row):
    dividend_month = row[0].replace(day=1)
    asset = row[3]
    asset_id = cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()[0]
    remaining_amount = row[4]
    dividend_per_asset = row[5]
    month_asset_amounts = available_asset(asset_id)
    for (month,asset_amount) in month_asset_amounts:
            cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(asset_amount*dividend_per_asset,month))
            remaining_amount -= asset_amount
    if remaining_amount > 0:
        cur.execute("UPDATE month_data SET capital = capital + ? WHERE month = ?",(remaining_amount*dividend_per_asset,dividend_month))
    transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
    transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

def handle_fees(row):
    total_amount = -row[6]
    remaining_amount = total_amount
    month_capital = available_capital()
    total_capital = sum(e[1] for e in month_capital)
    if total_capital + 1e-4 >= total_amount:
        i = 0
        while remaining_amount > 1e-4:
            (oldest_available,capital) = month_capital[i]
            month_amount = min(remaining_amount,capital)
            cur.execute("UPDATE month_data SET capital = capital - ? WHERE month = ?",(month_amount,oldest_available))
            remaining_amount -= month_amount
            i += 1
        transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ?",(row[-1],))
        transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")

def handle_listing_change(row):
    global listing_change
    if listing_change["to_asset"] is None:
        listing_change["to_asset"] = row[3]
        listing_change["to_asset_amount"] = row[4]
        listing_change["to_rowid"] = row[-1]
    else:
        asset = row[3]
        amount = -row[4]
        (asset_id,) = cur.execute("SELECT asset_id FROM assets WHERE asset = ?",(asset,)).fetchone()
        cur.execute("UPDATE assets SET asset = ?, amount = ? WHERE asset_id = ?",(listing_change["to_asset"],listing_change["to_asset_amount"],asset_id))
        change_factor = listing_change["to_asset_amount"]/amount
        cur.execute("UPDATE month_assets SET amount = amount * ? WHERE asset_id = ?",(change_factor,asset_id))
        transaction_cur.execute("UPDATE transactions SET processed = 1 WHERE rowid = ? OR rowid = ?",(row[-1],listing_change["to_rowid"],))
        transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
        listing_change = {"to_asset":None,"to_asset_amount":None,"to_rowid":None}

transaction_cur = con.cursor()
debug_cur = con.cursor()
unprocessed_lines = transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
row = unprocessed_lines.fetchone()
#Consider upgrading to python3.8 to make this more elegant with := statment
while row is not None:
    date = row[0]
    date = date.replace(date.year,date.month,1)
    cur.execute("INSERT OR IGNORE INTO month_data(month) VALUES(?)",(date,))
    if row[2] == "Insättning":
        handle_deposit(row)
    elif row[2] == "Uttag":
        handle_withdrawal(row)
    elif row[2] == "Köp":
        handle_purchase(row)
    elif row[2] == "Sälj":
        handle_sale(row)
    elif row[2] == "Utdelning":
        handle_dividend(row)
    elif "Utländsk källskatt" in row[2] or "Ränt" in row[2] or "Prelskatt" in row[2]:
        handle_fees(row)
    elif "Byte" in row[2]:
        handle_listing_change(row)
    else:
        raise(ValueError)
    row = unprocessed_lines.fetchone()

unprocessed_lines = transaction_cur.execute("SELECT *,rowid FROM transactions WHERE processed == 0 ORDER BY date ASC")
if unprocessed_lines.fetchone() is not None:
    raise AssetDeficit
else:
    #Calculate summary data and put it in asset table
    asset_ids = cur.execute("SELECT asset_id FROM assets").fetchall()
    for (id,) in asset_ids:
        month_asset_data = cur.execute("SELECT amount, average_price, average_purchase_price, average_sale_price, purchased_amount, sold_amount FROM month_assets WHERE asset_id = ?",(id,)).fetchall()
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
        cur.execute("UPDATE assets SET amount = ?, average_price = ?, average_purchase_price = ?, average_sale_price = ?, purchased_amount = ?, sold_amount = ? WHERE asset_id = ?",(amount,average_price,average_purchase_price,average_sale_price,purchased_amount,sold_amount,id,))
    #Commit changes
    con.commit()