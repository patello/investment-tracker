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
        month_data = cur.execute("SELECT month, SUM(deposit), SUM(withdrawal), SUM(capital) FROM month_data GROUP BY month ORDER BY month ASC").fetchall()
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
                if price is not None:
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

    def get_stats(self, accounts=None, period: str = "month", deposits: str = "current") -> list:
        """
        Get stats such as capital transfers and gain/loss for either months or years.
        "deposits" determine if only months/years with non-withdrawn capital are returned or all months/years.
        Stats are returned as a list in the following order:
        month, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,
        total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield

        Parameters:
        accounts (list or None): List of account strings to include, or None for all accounts.
            When None and period/deposits match cached table queries, uses pre-calculated stats for performance.
        period (str): "month" or "year".
        deposits (str): "current" or "all".

        Returns:
        list: List of stats:     
        """
        # If no account filtering requested and we can use cached tables, do so for performance
        if accounts is None:
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
        
        # Account filtering requested - compute stats fresh from raw data
        self.db.connect()
        cur = self.db.get_cursor()
        today = datetime.today().date()
        
        # Build account filter clause
        placeholders = ",".join("?" * len(accounts))
        account_filter = f" AND account IN ({placeholders})"
        account_params = tuple(accounts)
        asset_account_filter = f" AND account IN ({placeholders})"
        asset_account_params = tuple(accounts)
        
        # Get month data filtered by accounts
        month_data_query = f"""
            SELECT month, SUM(deposit), SUM(withdrawal), SUM(capital)
            FROM month_data
            WHERE 1=1{account_filter}
            GROUP BY month ORDER BY month ASC
        """
        month_data = cur.execute(month_data_query, account_params).fetchall()
        
        stats = []
        acc_deposit = 0
        acc_value = 0
        acc_withdrawal = 0
        acc_net_deposit = 0
        acc_total_gainloss = 0
        acc_realized_gainloss = 0
        acc_unrealized_gainloss = 0
        
        for (month, deposit, withdrawal, capital) in month_data:
            value = capital
            
            # Get assets for this month filtered by accounts
            month_assets_query = f"""
                SELECT asset_id, amount FROM month_assets
                WHERE month = ? AND amount > 0.001{asset_account_filter}
            """
            params = (month,) + asset_account_params if asset_account_params else (month,)
            month_assets = cur.execute(month_assets_query, params).fetchall()
            
            for (asset_id, amount) in month_assets:
                (price,) = cur.execute("SELECT latest_price FROM assets WHERE asset_id = ?", (asset_id,)).fetchone()
                if price is not None:
                    value += amount * price
            
            total_gainloss = withdrawal + value - deposit
            if (withdrawal + capital >= deposit) or (withdrawal + capital < deposit and value <= 0):
                realized_gainloss = withdrawal + capital - deposit
            else:
                realized_gainloss = 0.0
            unrealized_gainloss = total_gainloss - realized_gainloss
            
            if deposit > 0:
                total_gainloss_per = 100 * total_gainloss / deposit
                unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                realized_gainloss_per = 100 * realized_gainloss / deposit
            else:
                total_gainloss_per = 0
                unrealized_gainloss_per = 0
                realized_gainloss_per = 0
            
            middle_date = month.replace(day=15)
            if today >= middle_date + timedelta(365.25) and total_gainloss_per != 0:
                annual_per_yield = 100 * ((total_gainloss_per / 100 + 1) ** (1 / ((datetime.today().date() - middle_date).days / 365.25)) - 1)
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
            
            stats.append((
                month, deposit, withdrawal, value,
                total_gainloss, realized_gainloss, unrealized_gainloss,
                total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                annual_per_yield
            ))
        
        # If period is "year", aggregate monthly stats by year
        if period == "year":
            yearly_stats = {}
            for row in stats:
                month = row[0]
                year = month.year if hasattr(month, 'year') else datetime.strptime(month, "%Y-%m-%d").year
                if year not in yearly_stats:
                    yearly_stats[year] = {
                        'deposit': 0,
                        'withdrawal': 0,
                        'value': 0,
                        'total_gainloss': 0,
                        'realized_gainloss': 0,
                        'unrealized_gainloss': 0,
                        'months': 0
                    }
                yearly_stats[year]['deposit'] += row[1]
                yearly_stats[year]['withdrawal'] += row[2]
                yearly_stats[year]['value'] += row[3]
                yearly_stats[year]['total_gainloss'] += row[4]
                yearly_stats[year]['realized_gainloss'] += row[5]
                yearly_stats[year]['unrealized_gainloss'] += row[6]
                yearly_stats[year]['months'] += 1
            
            # Convert to list and calculate percentages
            stats = []
            for year in sorted(yearly_stats.keys()):
                data = yearly_stats[year]
                deposit = data['deposit']
                withdrawal = data['withdrawal']
                value = data['value']
                total_gainloss = data['total_gainloss']
                realized_gainloss = data['realized_gainloss']
                unrealized_gainloss = data['unrealized_gainloss']
                
                if deposit > 0:
                    total_gainloss_per = 100 * total_gainloss / deposit
                    unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                    realized_gainloss_per = 100 * realized_gainloss / deposit
                else:
                    total_gainloss_per = 0
                    unrealized_gainloss_per = 0
                    realized_gainloss_per = 0
                
                # For year stats, annual_per_yield is the same as total_gainloss_per
                annual_per_yield = total_gainloss_per
                
                stats.append((
                    year, deposit, withdrawal, value,
                    total_gainloss, realized_gainloss, unrealized_gainloss,
                    total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                    annual_per_yield
                ))
        
        # Filter by deposits parameter
        if deposits == "current":
            stats = [row for row in stats if row[3] > 0]  # value > 0
        
        return stats
        """
        Get stats filtered by accounts. If accounts is None, returns stats for all accounts.
        If accounts is a list, only include those accounts.
        
        Parameters:
        accounts (list or None): List of account strings to include, or None for all.
        period (str): "month" or "year".
        deposits (str): "current" or "all".
        
        Returns:
        list: List of stats in same format as get_stats().
        """
        self.db.connect()
        cur = self.db.get_cursor()
        today = datetime.today().date()
        
        # Build account filter clause
        if accounts is None:
            account_filter = ""
            account_params = ()
            asset_account_filter = ""
            asset_account_params = ()
        else:
            placeholders = ",".join("?" * len(accounts))
            account_filter = f" AND account IN ({placeholders})"
            account_params = tuple(accounts)
            asset_account_filter = f" AND account IN ({placeholders})"
            asset_account_params = tuple(accounts)
        
        # Get month data filtered by accounts
        month_data_query = f"""
            SELECT month, SUM(deposit), SUM(withdrawal), SUM(capital)
            FROM month_data
            WHERE 1=1{account_filter}
            GROUP BY month ORDER BY month ASC
        """
        month_data = cur.execute(month_data_query, account_params).fetchall()
        
        stats = []
        acc_deposit = 0
        acc_value = 0
        acc_withdrawal = 0
        acc_net_deposit = 0
        acc_total_gainloss = 0
        acc_realized_gainloss = 0
        acc_unrealized_gainloss = 0
        
        for (month, deposit, withdrawal, capital) in month_data:
            value = capital
            
            # Get assets for this month filtered by accounts
            month_assets_query = f"""
                SELECT asset_id, amount FROM month_assets
                WHERE month = ? AND amount > 0.001{asset_account_filter}
            """
            params = (month,) + asset_account_params if asset_account_params else (month,)
            month_assets = cur.execute(month_assets_query, params).fetchall()
            
            for (asset_id, amount) in month_assets:
                (price,) = cur.execute("SELECT latest_price FROM assets WHERE asset_id = ?", (asset_id,)).fetchone()
                if price is not None:
                    value += amount * price
            
            total_gainloss = withdrawal + value - deposit
            if (withdrawal + capital >= deposit) or (withdrawal + capital < deposit and value <= 0):
                realized_gainloss = withdrawal + capital - deposit
            else:
                realized_gainloss = 0.0
            unrealized_gainloss = total_gainloss - realized_gainloss
            
            if deposit > 0:
                total_gainloss_per = 100 * total_gainloss / deposit
                unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                realized_gainloss_per = 100 * realized_gainloss / deposit
            else:
                total_gainloss_per = 0
                unrealized_gainloss_per = 0
                realized_gainloss_per = 0
            
            middle_date = month.replace(day=15)
            if today >= middle_date + timedelta(365.25) and total_gainloss_per != 0:
                annual_per_yield = 100 * ((total_gainloss_per / 100 + 1) ** (1 / ((datetime.today().date() - middle_date).days / 365.25)) - 1)
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
            
            stats.append((
                month, deposit, withdrawal, value,
                total_gainloss, realized_gainloss, unrealized_gainloss,
                total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                annual_per_yield
            ))
        
        # If period is "year", aggregate monthly stats by year
        if period == "year":
            yearly_stats = {}
            for row in stats:
                month = row[0]
                year = month.year if hasattr(month, 'year') else datetime.strptime(month, "%Y-%m-%d").year
                if year not in yearly_stats:
                    yearly_stats[year] = {
                        'deposit': 0,
                        'withdrawal': 0,
                        'value': 0,
                        'total_gainloss': 0,
                        'realized_gainloss': 0,
                        'unrealized_gainloss': 0,
                        'months': 0
                    }
                yearly_stats[year]['deposit'] += row[1]
                yearly_stats[year]['withdrawal'] += row[2]
                yearly_stats[year]['value'] += row[3]
                yearly_stats[year]['total_gainloss'] += row[4]
                yearly_stats[year]['realized_gainloss'] += row[5]
                yearly_stats[year]['unrealized_gainloss'] += row[6]
                yearly_stats[year]['months'] += 1
            
            # Convert to list and calculate percentages
            stats = []
            for year in sorted(yearly_stats.keys()):
                data = yearly_stats[year]
                deposit = data['deposit']
                withdrawal = data['withdrawal']
                value = data['value']
                total_gainloss = data['total_gainloss']
                realized_gainloss = data['realized_gainloss']
                unrealized_gainloss = data['unrealized_gainloss']
                
                if deposit > 0:
                    total_gainloss_per = 100 * total_gainloss / deposit
                    unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                    realized_gainloss_per = 100 * realized_gainloss / deposit
                else:
                    total_gainloss_per = 0
                    unrealized_gainloss_per = 0
                    realized_gainloss_per = 0
                
                # For year stats, annual_per_yield is the same as total_gainloss_per
                annual_per_yield = total_gainloss_per
                
                stats.append((
                    year, deposit, withdrawal, value,
                    total_gainloss, realized_gainloss, unrealized_gainloss,
                    total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                    annual_per_yield
                ))
        
        # Filter by deposits parameter
        if deposits == "current":
            stats = [row for row in stats if row[4] > 0]  # value > 0
        
        return stats
    
    def get_accumulated(self, accounts=None, period: str = "month", deposits: str = "current") -> list:
        """
        Get accumulated stats such as capital transfers and gain/loss for either months or years.
        "deposits" determine if only months/years with non-withdrawn capital are returned or all months/years.
        Accumulated stats are returned as a list in the following order:
        month, acc_net_deposit, acc_value, acc_unrealized_gainloss
        
        Parameters:
        accounts (list or None): List of account strings to include, or None for all accounts.
            When None and period/deposits match cached table queries, uses pre-calculated stats for performance.
            NOTE: Account filtering is not currently supported for accumulated stats due to algorithm complexity.
        period (str): "month" or "year".
        deposits (str): "current" or "all".

        Returns:
        list: List of accumulated stats.
        """
        # Account filtering is not currently supported for accumulated stats
        # The accumulation algorithm in cached tables is complex and not yet replicated
        # for filtered account subsets. Use --account all for accumulated stats.
        if accounts is not None:
            raise ValueError(
                "Account filtering is not currently supported for accumulated stats. "
                "The complex gain/loss accumulation algorithm has only been implemented "
                "for global statistics (--account all). Use --account all for accumulated stats."
            )
        
        # Use cached tables (global aggregates only)
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

    def get_account_summaries(self, accounts=None):
        """
        Get current account summaries including cash and asset values.
        
        Parameters:
        accounts (list or None): List of account strings to include, or None for all.
        
        Returns:
        list: List of tuples (account, cash, asset_value, total_value)
        """
        self.db.connect()
        cur = self.db.get_cursor()
        
        # Build account filter clause
        if accounts is None:
            account_filter = ""
            account_params = ()
        else:
            placeholders = ",".join("?" * len(accounts))
            account_filter = f" WHERE account IN ({placeholders})"
            account_params = tuple(accounts)
        
        # Get all accounts (filtered if specified)
        accounts_query = f"""
            SELECT DISTINCT account FROM month_data
            {account_filter}
            ORDER BY account
        """
        account_rows = cur.execute(accounts_query, account_params).fetchall()
        if not account_rows:
            return []
        
        summaries = []
        
        for (account,) in account_rows:
            # Get cash balance for this account
            cash_query = """
                SELECT SUM(capital) FROM month_data 
                WHERE account = ?
            """
            (cash,) = cur.execute(cash_query, (account,)).fetchone()
            cash = cash or 0
            
            # Get asset holdings for this account
            asset_query = """
                SELECT ma.asset_id, ma.amount, a.latest_price
                FROM month_assets ma
                JOIN assets a ON ma.asset_id = a.asset_id
                WHERE ma.account = ? AND ma.amount > 0.001
                AND a.latest_price IS NOT NULL
            """
            asset_rows = cur.execute(asset_query, (account,)).fetchall()
            
            asset_value = 0
            for asset_id, amount, price in asset_rows:
                if price is not None:
                    asset_value += amount * price
            
            total_value = cash + asset_value
            summaries.append((account, cash, asset_value, total_value))
        
        return summaries
    
    def print_account_summary(self, accounts=None):
        """
        Print account summaries in table format.
        
        Parameters:
        accounts (list or None): List of account strings to include, or None for all.
        """
        summaries = self.get_account_summaries(accounts)
        
        if not summaries:
            print("No account data found")
            return
        
        # Calculate totals
        total_cash = sum(s[1] for s in summaries)
        total_assets = sum(s[2] for s in summaries)
        total_total = sum(s[3] for s in summaries)
        
        # Print table header
        print(f"{'Account':<20} {'Cash (SEK)':>12} {'Assets (SEK)':>12} {'Total (SEK)':>12}")
        print("-" * 56)
        
        # Print each account
        for account, cash, asset_value, total_value in summaries:
            print(f"{account:<20} {cash:>12.0f} {asset_value:>12.0f} {total_value:>12.0f}")
        
        # Print totals
        print("-" * 56)
        print(f"{'TOTAL':<20} {total_cash:>12.0f} {total_assets:>12.0f} {total_total:>12.0f}")
        
        # Print percentage breakdown if there are multiple accounts
        if len(summaries) > 1:
            print("\nPercentage of total portfolio:")
            for account, cash, asset_value, total_value in summaries:
                if total_total > 0:
                    percentage = 100 * total_value / total_total
                    print(f"  {account}: {percentage:.1f}%")

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
            # Check if prices are already up to date for all currently held assets
            # First check: any held assets missing prices?
            result = cur.execute(
                "SELECT COUNT(*) FROM assets WHERE amount > 0 AND latest_price_date IS NULL"
            ).fetchone()
            
            if result and result[0] > 0:
                logging.debug(f"Price update needed: {result[0]} held assets missing prices")
            else:
                # Second check: get oldest price date for assets with amount > 0
                (latest_price_date_str,) = cur.execute("SELECT MIN(latest_price_date) FROM assets WHERE amount > 0").fetchone()
                # Even with detect_types=sqlite3.PARSE_DECLTYPES, the MIN function returns a string instead of a date object
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
