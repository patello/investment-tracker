import sqlite3

con = sqlite3.connect("data/asset_data.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()


#Remove all entries from tables where the parsed data ends up
cur.execute("DELETE FROM month_data")
cur.execute("DELETE FROM assets")
cur.execute("DELETE FROM month_assets")

#Set all transactions to unhandeled
cur.execute("UPDATE transactions SET processed = 0")

con.commit()