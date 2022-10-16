from datetime import datetime, timedelta
from multiprocessing.sharedctypes import Value
import sqlite3

con = sqlite3.connect("data/asset_data.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

cur.execute("PRAGMA foreign_keys = ON;")

cur.execute("DROP TABLE month_stats")
cur.execute("""
    CREATE TABLE month_stats(
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
        acc_net_deposit REAL DEFAULT 0,
        acc_value REAL DEFAULT 0,
        acc_gainloss REAL DEFAULT 0,
        FOREIGN KEY (month) REFERENCES month_data (month), 
        PRIMARY KEY(month)
        );""")

cur.execute("DROP TABLE year_stats")
cur.execute("""
    CREATE TABLE year_stats(
        year INT NOT NULL,
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
        acc_net_deposit REAL DEFAULT 0,
        acc_value REAL DEFAULT 0,
        acc_gainloss REAL DEFAULT 0,
        PRIMARY KEY(year)
        );""")

month_data = cur.execute("SELECT month, deposit, withdrawal, capital FROM month_data ORDER BY month ASC").fetchall()
acc_net_deposit = 0
acc_gainloss = 0
acc_value = 0

for (month, deposit, withdrawal, capital) in month_data:
    value = capital
    month_assets = cur.execute("SELECT asset_id, amount FROM month_assets WHERE month = ? AND amount > 0.001", (month,)).fetchall()
    for (asset_id, amount) in month_assets:
        (price,) = cur.execute("SELECT latest_price FROM assets WHERE asset_id = ?", (asset_id,)).fetchone()
        value += amount*price

    total_gainloss = withdrawal + value - deposit
    if(withdrawal + capital >= deposit or (withdrawal + capital < deposit and value <= 0)):
        realized_gainloss = withdrawal + capital - deposit
    else:
        realized_gainloss = 0.0
    unrealized_gainloss = total_gainloss - realized_gainloss
    
    if deposit > 0:
        total_gainloss_per = 100*total_gainloss/deposit
        unrealized_gainloss_per = 100*unrealized_gainloss/deposit
        realized_gainloss_per = 100*realized_gainloss/deposit
    else:
        total_gainloss_per = 0
        unrealized_gainloss_per = 0
        realized_gainloss_per = 0

    middle_date = month.replace(day=15)
    if datetime.today().date() >= middle_date + timedelta(365.25) and total_gainloss_per !=0:
        annual_per_yield = 100*((total_gainloss_per/100+1)**(1/((datetime.today().date()-middle_date).days/365.25))-1)
    else:
        annual_per_yield = None

    net_deposit = deposit - withdrawal
    if net_deposit > 0:
        acc_net_deposit += net_deposit
    acc_value += value
    acc_gainloss += total_gainloss
    

    cur.execute("""
        INSERT INTO month_stats(
            month,deposit,withdrawal,capital,value,
            total_gainloss,realized_gainloss,unrealized_gainloss,
            total_gainloss_per,realized_gainloss_per,unrealized_gainloss_per,
            annual_per_yield,acc_net_deposit,acc_value,acc_gainloss) 
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",\
            (month,deposit,withdrawal,capital,value,\
            total_gainloss,realized_gainloss,unrealized_gainloss,\
            total_gainloss_per,realized_gainloss_per,unrealized_gainloss_per,\
            annual_per_yield,acc_net_deposit,acc_value,acc_gainloss))

month_stats = cur.execute("SELECT month, deposit, withdrawal, capital, value FROM month_stats").fetchall()
for (month, deposit, withdrawal, capital, value) in month_stats:
    year = month.year
    cur.execute("INSERT OR IGNORE INTO year_stats(year) VALUES(?)",(year,))
    cur.execute("""
        UPDATE year_stats SET deposit = deposit + ?, withdrawal = withdrawal + ?, capital = capital + ?,
        value = value + ? WHERE year = ?""",\
        (deposit,withdrawal,capital,value,year))

year_stats = cur.execute("SELECT year, deposit, withdrawal, capital, value FROM year_stats").fetchall()
for (year, deposit, withdrawal, capital, value) in year_stats:
    total_gainloss = withdrawal + value - deposit
    if(withdrawal + capital >= deposit or (withdrawal + capital < deposit and value <= 0)):
        realized_gainloss = withdrawal + capital - deposit
    else:
        realized_gainloss = 0.0
    unrealized_gainloss = total_gainloss - realized_gainloss
    
    if deposit > 0:
        total_gainloss_per = 100*total_gainloss/deposit
        unrealized_gainloss_per = 100*unrealized_gainloss/deposit
        realized_gainloss_per = 100*realized_gainloss/deposit
    else:
        total_gainloss_per = 0
        unrealized_gainloss_per = 0
        realized_gainloss_per = 0

    middle_date = datetime(year=year,month=7,day=1).date()
    if datetime.today().date() >= middle_date + timedelta(365.25) and total_gainloss_per !=0:
        annual_per_yield = 100*((total_gainloss_per/100+1)**(1/((datetime.today().date()-middle_date).days/365.25))-1)
    else:
        annual_per_yield = None

    (acc_net_deposit, acc_value, acc_gainloss) = cur.execute("""
        SELECT acc_net_deposit, acc_value, acc_gainloss FROM month_stats 
        WHERE month > ? AND month < ? ORDER BY month DESC LIMIT 1""",\
        (datetime(year=year,month=1,day=1).date(),datetime(year=year,month=12,day=31).date())).fetchone()
    cur.execute("""
        UPDATE year_stats SET total_gainloss = ?, realized_gainloss = ?, unrealized_gainloss = ?, 
        total_gainloss_per = ?, realized_gainloss_per = ?, unrealized_gainloss_per = ?,
        annual_per_yield = ?,acc_net_deposit = ?, acc_value = ?, acc_gainloss = ? WHERE year = ?""",\
        (total_gainloss,realized_gainloss,unrealized_gainloss,\
        total_gainloss_per,realized_gainloss_per,unrealized_gainloss_per,\
        annual_per_yield,acc_net_deposit,acc_value,acc_gainloss,year))

def print_stats(period = "month"):
    if period == "month":
        stats = cur.execute("""
            SELECT month, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield
            FROM month_stats ORDER BY month ASC""").fetchall()
    elif period == "year":
        stats = cur.execute("""
            SELECT year, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield
            FROM year_stats ORDER BY year ASC""").fetchall()
    else:
        raise ValueError(period)
    for (date, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield) in stats:
        if deposit > 0:
            print(date)
            print("Deposited: {deposited:.0f}".format(deposited=deposit))
            print("Value: {value:.0f}".format(value=value))
            print("Withdrawal: {withdrawal:.0f}".format(withdrawal=withdrawal))
            print("Gain/Loss: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=total_gainloss,gainloss_per=total_gainloss_per))
            print("- Unrealized: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=unrealized_gainloss,gainloss_per=unrealized_gainloss_per))
            print("- Realized: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=realized_gainloss,gainloss_per=realized_gainloss_per))
            if annual_per_yield is not None:
                print("APY: {apy:.1f}%".format(apy=annual_per_yield))
            print("")

def print_accumulated(period = "month"):
    if period == "month":
        acc_stats = cur.execute("""
            SELECT month, acc_net_deposit, acc_value, acc_gainloss
            FROM month_stats ORDER BY month ASC""").fetchall()
    elif period == "year":
        acc_stats = cur.execute("""
            SELECT year, acc_net_deposit, acc_value, acc_gainloss
            FROM year_stats ORDER BY year ASC""").fetchall()
    else:
        raise ValueError(period)
    print("Date, Net Deposit, Value, Gain/Loss")
    for (date, acc_net_deposit, acc_value, acc_gainloss) in acc_stats:
        print("{date}: {deposit:.0f}, {value:.0f}, {gain_loss:.0f}".format(date= date,deposit=acc_net_deposit,value=acc_value,gain_loss=acc_gainloss))

print("--Monthly Info--")
print_stats(period="month")
print("> Accumulated")
print_accumulated(period = "month")
print()
print("--Yearly Info--")
print_stats(period="year")
print("> Accumulated")
print_accumulated(period = "year")