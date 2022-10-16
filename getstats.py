import sqlite3

con = sqlite3.connect("data/asset_data.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

cur.execute("PRAGMA foreign_keys = ON;")

def get_stats(period = "month", deposits = "current"):
    if period == "month" and deposits == "current":
        stats = cur.execute("""
            SELECT month, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield
            FROM month_stats WHERE value > 0 ORDER BY month ASC""").fetchall()
    elif period == "month" and deposits == "all":
        stats = cur.execute("""
            SELECT month, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield
            FROM month_stats ORDER BY month ASC""").fetchall()
    elif period == "year" and deposits == "current":
        stats = cur.execute("""
            SELECT year, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield
            FROM year_stats WHERE value > 0 ORDER BY year ASC""").fetchall()
    elif period == "year" and deposits == "all":
        stats = cur.execute("""
            SELECT year, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield
            FROM year_stats ORDER BY year ASC""").fetchall()
    else:
        raise ValueError(period)
    return stats
        
def get_accumulated(period = "month",deposits = "current"):
    if period == "month" and deposits == "current":
        acc_stats = cur.execute("""
            SELECT month, acc_net_deposit, acc_value, acc_unrealized_gainloss
            FROM month_stats WHERE value > 0 ORDER BY month ASC""").fetchall()
    elif period == "month" and deposits == "all":
        acc_stats = cur.execute("""
            SELECT month, acc_deposit, acc_value, acc_total_gainloss
            FROM month_stats ORDER BY month ASC""").fetchall()
    elif period == "year" and deposits == "current":
        acc_stats = cur.execute("""
            SELECT year, acc_net_deposit, acc_value, acc_unrealized_gainloss
            FROM year_stats WHERE value > 0 ORDER BY year ASC""").fetchall()
    elif period == "year" and deposits == "all":
        acc_stats = cur.execute("""
            SELECT year, acc_deposit, acc_value, acc_total_gainloss
            FROM year_stats ORDER BY year ASC""").fetchall()
    else:
        raise ValueError((period,deposits))
    return acc_stats

def get_accumulated_stacked(**kwargs):
    acc_stats = get_accumulated(**kwargs)
    dates = [x[0].strftime("%Y-%m-%d") for x in acc_stats]
    y_deposit = [x[1] for x in acc_stats]
    y_gainloss = [x[2] for x in acc_stats]
    return (dates,y_deposit,y_gainloss)

def get_accumulated_gainloss(**kwargs):
    acc_stats = get_accumulated(**kwargs)
    dates = [x[0].strftime("%Y-%m-%d") for x in acc_stats]
    y_deposit = [x[1] for x in acc_stats]
    y_gainloss = [x[3] for x in acc_stats]
    return (dates,y_deposit,y_gainloss)

def print_accumulated(**kwargs):
    acc_stats = get_accumulated(**kwargs)
    print("Date, Deposit, Value, Gain/Loss")
    for (date, acc_net_deposit, acc_value, acc_gainloss) in acc_stats:
        print("{date}: {deposit:.0f}, {value:.0f}, {gain_loss:.0f}".format(date= date,deposit=acc_net_deposit,value=acc_value,gain_loss=acc_gainloss))

def print_stats(**kwargs):
    stats = get_stats(**kwargs)
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

if __name__ == "__main__":
    get_accumulated_gainloss(period = "year",deposits = "all")
    print("--Monthly Info--")
    print_stats(period="month")
    print("> Accumulated")
    print_accumulated(period = "month")
    print()
    print("--Yearly Info--")
    print_stats(period="year")
    print("> Accumulated")
    print_accumulated(period = "year")