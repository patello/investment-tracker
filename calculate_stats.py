from datetime import datetime, timedelta
from database_handler import DatabaseHandler
import requests
import json
import logging
import time

logging.basicConfig(level=logging.INFO)

class StatCalculator:
    """
    Class for getting monthly and yearly stats such as capital transfers and gain/loss.
    """
    def __init__(self, db: DatabaseHandler):
        """
        Parameters:
        db (DatabaseHandler): Database handler.
        """
        self.db = db
    
    def calculate_month_stats(self):
        """
        Calculate monthly stats such as capital transfers and gain/loss. Store results in month_stats table.
        """
        self.db.connect()
        # Reset month_stats table
        self.db.reset_table("month_stats")
        # Get month data
        cur = self.db.get_cursor()
        today = datetime.today().date()
        month_data = cur.execute("SELECT month, deposit, withdrawal, capital FROM month_data ORDER BY month ASC").fetchall()
        acc_deposit = 0
        acc_value = 0
        acc_withdrawal = 0
        acc_net_deposit = 0
        acc_total_gainloss = 0
        acc_realized_gainloss = 0
        acc_unrealized_gainloss = 0
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
            if today >= middle_date + timedelta(365.25) and total_gainloss_per !=0:
                annual_per_yield = 100*((total_gainloss_per/100+1)**(1/((datetime.today().date()-middle_date).days/365.25))-1)
            else:
                annual_per_yield = None

            acc_deposit += deposit
            acc_value += value
            acc_withdrawal += withdrawal
            net_deposit = deposit - withdrawal
            if net_deposit > 0:
                acc_net_deposit += net_deposit
            acc_total_gainloss += total_gainloss
            acc_realized_gainloss += realized_gainloss
            acc_unrealized_gainloss += unrealized_gainloss

            if month >= today.replace(day=1):
                month = today

            cur.execute("""
                INSERT INTO month_stats(
                    month,deposit,withdrawal,capital,value,
                    total_gainloss,realized_gainloss,unrealized_gainloss,
                    total_gainloss_per,realized_gainloss_per,unrealized_gainloss_per,
                    annual_per_yield,acc_deposit,acc_value,acc_withdrawal,acc_net_deposit,
                    acc_total_gainloss,acc_realized_gainloss,acc_unrealized_gainloss) 
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);""",\
                    (month,deposit,withdrawal,capital,value,\
                    total_gainloss,realized_gainloss,unrealized_gainloss,\
                    total_gainloss_per,realized_gainloss_per,unrealized_gainloss_per,\
                    annual_per_yield,acc_deposit,acc_value,acc_withdrawal,acc_net_deposit,
                    acc_total_gainloss,acc_realized_gainloss,acc_unrealized_gainloss))

        self.db.commit()
            
    def calculate_year_stats(self):
        """
        Calculate yearly stats such as capital transfers and gain/loss. Store results in year_stats table.
        """
        self.db.connect()
        # Reset year_stats table
        self.db.reset_table("year_stats")
        # Get month data and calculate year stats
        cur = self.db.get_cursor()
        today = datetime.today().date()
        month_stats = cur.execute("SELECT month, deposit, withdrawal, capital, value FROM month_stats").fetchall()
        for (month, deposit, withdrawal, capital, value) in month_stats:
            if month.year < today.year:
                year = month.replace(month=12,day=31)
            else:
                year = today
            cur.execute("INSERT OR IGNORE INTO year_stats(year) VALUES(?)",(year,))
            cur.execute("""
                UPDATE year_stats SET deposit = deposit + ?, withdrawal = withdrawal + ?, capital = capital + ?,
                value = value + ? WHERE year = ?""",\
                (deposit,withdrawal,capital,value,year))

        year_stats = cur.execute("SELECT year, deposit, withdrawal, capital, value FROM year_stats").fetchall()
        acc_net_deposit = 0
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

            middle_date = datetime(year=year.year,month=7,day=1).date()
            if datetime.today().date() >= middle_date + timedelta(365.25) and total_gainloss_per !=0:
                annual_per_yield = 100*((total_gainloss_per/100+1)**(1/((datetime.today().date()-middle_date).days/365.25))-1)
            else:
                annual_per_yield = None

            (acc_deposit, acc_value, acc_withdrawal, acc_total_gainloss, acc_realized_gainloss, acc_unrealized_gainloss) = cur.execute("""
                SELECT acc_deposit, acc_value, acc_withdrawal, acc_total_gainloss, acc_realized_gainloss, acc_unrealized_gainloss FROM month_stats 
                WHERE month >= ? AND month <= ? ORDER BY month DESC LIMIT 1""",\
                (datetime(year=year.year,month=1,day=1).date(),datetime(year=year.year,month=12,day=31).date())).fetchone()
            net_deposit = deposit - withdrawal
            if net_deposit > 0:
                acc_net_deposit += net_deposit
            cur.execute("""
                UPDATE year_stats SET total_gainloss = ?, realized_gainloss = ?, unrealized_gainloss = ?, 
                total_gainloss_per = ?, realized_gainloss_per = ?, unrealized_gainloss_per = ?,
                annual_per_yield = ?, acc_deposit = ?, acc_value = ?, acc_withdrawal = ?, acc_net_deposit = ?,
                acc_total_gainloss = ?, acc_realized_gainloss = ?, acc_unrealized_gainloss = ? WHERE year = ?""",\
                (total_gainloss,realized_gainloss,unrealized_gainloss,\
                total_gainloss_per,realized_gainloss_per,unrealized_gainloss_per,\
                annual_per_yield,acc_deposit,acc_value,acc_withdrawal,acc_net_deposit,
                acc_total_gainloss,acc_realized_gainloss,acc_unrealized_gainloss,year))
            
            self.db.commit()
            
    def calculate_stats(self):
        """
        Calculate monthly and yearly stats such as capital transfers and gain/loss.
        """
        self.calculate_month_stats()
        self.calculate_year_stats()

    def get_stats(self, period: str = "month", deposits: str = "current") -> list:
        """
        Get stats such as capital transfers and gain/loss for either months or years.
        "deposits" determine if only months/years with non-withdrawn capital are returned or all months/years.
        Stats are returned as a list in the following order:
        month, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
        total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield

        Parameters:
        period (str): "month" or "year".
        deposits (str): "current" or "all".

        Returns:
        list: List of stats:     
        """
        self.db.connect()
        cur = self.db.get_cursor()
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
            
    def get_accumulated(self, period: str = "month", deposits: str = "current") -> list:
        """
        Get accumulated stats such as capital transfers and gain/loss for either months or years.
        "deposits" determine if only months/years with non-withdrawn capital are returned or all months/years.
        Accumulated stats are returned as a list in the following order:
        month, acc_net_deposit, acc_value, acc_unrealized_gainloss
        
        Parameters:
        period (str): "month" or "year".
        deposits (str): "current" or "all".

        Returns:
        list: List of accumulated stats.
        """
        self.db.connect()
        cur = self.db.get_cursor()
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

    def get_accumulated_stacked(self, **kwargs) -> tuple:
        """
        Gets deposits and gain/loss accumulated over time for use when stacked in a graph. 
        Returns a tuple of lists in the following order:
        dates, y_deposit, y_gainloss

        Parameters:
        See get_accumulated

        Returns:
        tuple: Tuple of lists.
        """
        acc_stats = self.get_accumulated(**kwargs)
        dates = [x[0].strftime("%Y-%m-%d") for x in acc_stats]
        y_deposit = [x[1] for x in acc_stats]
        y_gainloss = [x[2] for x in acc_stats]
        return (dates,y_deposit,y_gainloss)

    def get_accumulated_gainloss(self, **kwargs) -> tuple:
        """
        Get accumulated gain/loss accumulated over time.
        Returns a tuple of lists in the following order:
        dates, y_deposit, y_gainloss

        Parameters:
        See get_accumulated

        Returns:
        tuple: Tuple of lists.
        """
        acc_stats = self.get_accumulated(**kwargs)
        dates = [x[0].strftime("%Y-%m-%d") for x in acc_stats]
        y_deposit = [x[1] for x in acc_stats]
        y_gainloss = [x[3] for x in acc_stats]
        return (dates,y_deposit,y_gainloss)

    def get_bar_data(self, **kwargs) -> tuple:
        """
        Get data for bar graph. The data can be used for stacked bars for each month or year.
        Returns a tuple of lists in the following order:
        dates, deposited, withdrawn, realized_gain, realized_loss, unrealized_gain, unrealized_loss

        Parameters:
        See get_stats

        Returns:
        tuple: Tuple of lists.
        """
        acc_stats = self.get_stats(**kwargs)
        dates = [x[0].strftime("%Y-%m-%d") for x in acc_stats]
        deposited = [x[1] for x in acc_stats]
        withdrawn = [x[2] for x in acc_stats]
        realized_gain = [x[5] if x[5] > 0 else 0 for x in acc_stats]
        realized_loss = [-x[5] if x[5] < 0 else 0 for x in acc_stats]
        unrealized_gain = [x[6] if x[6] > 0 else 0 for x in acc_stats]
        unrealized_loss = [-x[6] if x[6] < 0 else 0 for x in acc_stats]
        deposited = [x[0]+x[1]-x[2]-x[3]-x[4] for x in zip(deposited,realized_gain,unrealized_loss,realized_loss,withdrawn)]
        return (dates,deposited,withdrawn,realized_gain,realized_loss,unrealized_gain,unrealized_loss)

    def print_accumulated(self, **kwargs) -> None:
        """
        Print accumulated stats such as capital transfers and gain/loss for either months or years.

        Parameters:
        See get_accumulated
        """
        acc_stats = self.get_accumulated(**kwargs)
        print("Date, Deposit, Value, Gain/Loss")
        for (date, acc_net_deposit, acc_value, acc_gainloss) in acc_stats:
            print("{date}: {deposit:.0f}, {value:.0f}, {gain_loss:.0f}".format(date= date,deposit=acc_net_deposit,value=acc_value,gain_loss=acc_gainloss))

    def print_stats(self, **kwargs) -> None:
        """
        Print stats such as capital transfers and gain/loss for either months or years.

        Parameters:
        See get_stats
        """
        stats = self.get_stats(**kwargs)
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

    def update_prices(self, force: bool = False):
        """
        Update prices in database. Prices are fetched from external site. 
        Prices are only updated if they are older than 1 day, unless force is True.

        Parameters:
        force (bool): If True, update prices even if they are already up to date.
        """
        self.db.connect()
        cur = self.db.get_cursor()

        today = datetime.today().date()

        if not force:
            # Check if prices are already up to date
            (latest_price_date_str,) = cur.execute("SELECT MIN(latest_price_date) FROM assets").fetchone()
            # Even with detect_types=sqlite3.PARSE_DECLTYPES, the MAX function returns a string instead of a date object
            # Therefore, we need to convert the string to a date object, if it exists
            if latest_price_date_str is not None:
                latest_price_date = datetime.strptime(latest_price_date_str, '%Y-%m-%d').date()
            else:
                latest_price_date = None
            # If latest price_date is today or later, prices are already up to date
            # If it is None, then no prices have been fetched yet
            if latest_price_date is not None and latest_price_date >= today - timedelta(days=1):
                logging.debug("Prices are already up to date. Latest price date: {latest_price_date}".format(latest_price_date=latest_price_date))
                return
        

        assets = cur.execute("SELECT asset,asset_id FROM assets WHERE amount > 0").fetchall()
        
        # Current working endpoint (discovered 2026-02-27)
        url = "https://www.avanza.se/_api/search/filtered-search"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            ),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        for (asset,asset_id) in assets:
            r = requests.post(url, headers=headers, json={"query": asset, "limit": 5}, timeout=10)

            if r.status_code == 200:
                resp = r.json()
                # Check if we have hits and price data
                if "hits" in resp and len(resp["hits"]) > 0:
                    hit = resp["hits"][0]
                    if "price" in hit and hit["price"] and "last" in hit["price"]:
                        price_str = hit["price"]["last"]
                        # Strip non-breaking spaces used as thousands separator, spaces, replace comma with dot
                        raw_price = price_str.replace("\u00a0", "").replace(" ", "").replace(",", ".")
                        try:
                            price = float(raw_price)
                            cur.execute("UPDATE assets SET latest_price = ?, latest_price_date = ? WHERE asset_id = ?",
                                        (price, today, asset_id))
                        except ValueError:
                            logging.warning(f"Could not parse price '{price_str}' for asset {asset}")
                    else:
                        logging.warning(f"No price field in response for asset {asset}")
                else:
                    logging.warning(f"No hits in response for asset {asset}")
            else:
                logging.warning(f"HTTP {r.status_code} for asset {asset}")
            
            # Be polite to the API
            time.sleep(0.05)

        self.db.commit()

if __name__ == "__main__":
    db = DatabaseHandler("data/asset_data.db")
    stat_calculator = StatCalculator(db)
    stat_calculator.update_prices()
    stat_calculator.calculate_stats()
    print("Month stats:")
    stat_calculator.print_stats(period="month",deposits="current")
    stat_calculator.print_accumulated(period="month",deposits="current")
    print("Year stats:")
    stat_calculator.print_stats(period="year",deposits="current")
    stat_calculator.print_accumulated(period="year",deposits="current")
