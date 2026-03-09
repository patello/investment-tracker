from datetime import datetime, timedelta, date
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
    
    def _ensure_per_account_tables(self):
        """Create per-account statistics tables if they don't exist."""
        self.db.connect()
        cur = self.db.get_cursor()
        
        # Drop old global cached tables first
        tables_to_drop = ['month_stats', 'year_stats']
        for table in tables_to_drop:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        
        # Check if per-account tables exist
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='account_month_stats'")
        if cur.fetchone():
            # Tables exist
            return
        
        # Create account_month_stats
        cur.execute("""
            CREATE TABLE account_month_stats (
                account TEXT NOT NULL,
                month DATE NOT NULL,
                deposit REAL,
                withdrawal REAL,
                value REAL,
                total_gainloss REAL,
                realized_gainloss REAL,
                unrealized_gainloss REAL,
                total_gainloss_per REAL,
                realized_gainloss_per REAL,
                unrealized_gainloss_per REAL,
                annual_per_yield REAL,
                acc_net_deposit REAL,
                acc_deposit REAL,
                acc_value REAL,
                acc_unrealized_gainloss REAL,
                acc_total_gainloss REAL,
                PRIMARY KEY (account, month)
            )
        """)
        
        # Create account_year_stats
        cur.execute("""
            CREATE TABLE account_year_stats (
                account TEXT NOT NULL,
                year DATE NOT NULL,
                deposit REAL,
                withdrawal REAL,
                value REAL,
                total_gainloss REAL,
                realized_gainloss REAL,
                unrealized_gainloss REAL,
                total_gainloss_per REAL,
                realized_gainloss_per REAL,
                unrealized_gainloss_per REAL,
                annual_per_yield REAL,
                acc_net_deposit REAL,
                acc_deposit REAL,
                acc_value REAL,
                acc_unrealized_gainloss REAL,
                acc_total_gainloss REAL,
                PRIMARY KEY (account, year)
            )
        """)
        
        self.db.commit()
        logging.info("Created per-account statistics tables (dropped old global tables)")
    
    def _drop_old_tables(self):
        """Drop old global cached tables if they exist."""
        self.db.connect()
        cur = self.db.get_cursor()
        
        tables_to_drop = ['month_stats', 'year_stats']
        for table in tables_to_drop:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        
        self.db.commit()
        logging.info("Dropped old global cached tables")
    
    def calculate_month_stats(self):
        """
        Calculate monthly stats such as capital transfers and gain/loss.
        Stores results in account_month_stats table (per account).
        """
        self._ensure_per_account_tables()
        self.db.connect()
        
        # Reset account_month_stats table
        self.db.reset_table("account_month_stats")
        
        cur = self.db.get_cursor()
        today = datetime.today().date()
        
        # Get all accounts
        accounts = [row[0] for row in cur.execute(
            "SELECT DISTINCT account FROM month_data ORDER BY account"
        ).fetchall()]
        
        logging.info(f"Calculating monthly stats for {len(accounts)} accounts")
        
        for account in accounts:
            # Get all months for this account in chronological order
            cur.execute("""
                SELECT month, deposit, withdrawal, capital
                FROM month_data
                WHERE account = ?
                ORDER BY month ASC
            """, (account,))
            month_rows = cur.fetchall()
            
            if not month_rows:
                continue
            
            # Initialize accumulators (PER ACCOUNT)
            acc_deposit = 0.0
            acc_value = 0.0
            acc_withdrawal = 0.0
            acc_net_deposit = 0.0
            acc_total_gainloss = 0.0
            acc_realized_gainloss = 0.0
            acc_unrealized_gainloss = 0.0
            
            for month_str, deposit, withdrawal, capital in month_rows:
                # Handle NULL values
                deposit = deposit or 0.0
                withdrawal = withdrawal or 0.0
                capital = capital or 0.0
                
                # Get asset holdings for this account in this month
                cur.execute("""
                    SELECT ma.asset_id, ma.amount, a.latest_price
                    FROM month_assets ma
                    JOIN assets a ON ma.asset_id = a.asset_id
                    WHERE ma.account = ? AND ma.month = ? AND ma.amount > 0.001
                    AND a.latest_price IS NOT NULL
                """, (account, month_str))
                asset_rows = cur.fetchall()
                
                # Calculate asset value
                asset_value = 0.0
                for asset_id, amount, price in asset_rows:
                    if price is not None:
                        asset_value += amount * price
                
                # Total value = cash + assets
                value = capital + asset_value
                
                # Calculate gain/loss using cash-based formulas (same as original)
                total_gainloss = withdrawal + value - deposit
                
                # Calculate realized gain/loss (same logic as original)
                if (withdrawal + capital >= deposit) or (withdrawal + capital < deposit and value <= 0):
                    realized_gainloss = withdrawal + capital - deposit
                else:
                    realized_gainloss = 0.0
                
                unrealized_gainloss = total_gainloss - realized_gainloss
                
                # Calculate percentages
                if deposit > 0:
                    total_gainloss_per = 100 * total_gainloss / deposit
                    realized_gainloss_per = 100 * realized_gainloss / deposit
                    unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                else:
                    total_gainloss_per = 0.0
                    realized_gainloss_per = 0.0
                    unrealized_gainloss_per = 0.0
                
                # Calculate APY (same logic as original)
                if isinstance(month_str, str):
                    month_date = datetime.strptime(month_str, "%Y-%m-%d").date()
                else:
                    month_date = month_str  # already a date object
                middle_date = month_date.replace(day=15)
                if today >= middle_date + timedelta(days=365.25) and total_gainloss_per != 0:
                    annual_per_yield = 100 * ((total_gainloss_per/100 + 1) ** (1 / ((today - middle_date).days / 365.25)) - 1)
                else:
                    annual_per_yield = None
                
                # Update accumulators (same logic as original)
                acc_deposit += deposit
                acc_value += value
                acc_withdrawal += withdrawal
                net_deposit = deposit - withdrawal
                if net_deposit > 0:
                    acc_net_deposit += net_deposit
                acc_total_gainloss += total_gainloss
                acc_realized_gainloss += realized_gainloss
                acc_unrealized_gainloss += unrealized_gainloss
                
                # Adjust month if it's in the future (same as original)
                if month_date >= today.replace(day=1):
                    month_date = today
                
                # Insert into per-account table
                cur.execute("""
                    INSERT INTO account_month_stats (
                        account, month, deposit, withdrawal, value,
                        total_gainloss, realized_gainloss, unrealized_gainloss,
                        total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                        annual_per_yield, acc_net_deposit, acc_deposit, acc_value,
                        acc_unrealized_gainloss, acc_total_gainloss
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    account, month_date, deposit, withdrawal, value,
                    total_gainloss, realized_gainloss, unrealized_gainloss,
                    total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                    annual_per_yield, acc_net_deposit, acc_deposit, acc_value,
                    acc_unrealized_gainloss, acc_total_gainloss
                ))
        
        self.db.commit()
        logging.info(f"Monthly stats calculated for {len(accounts)} accounts")
            
    def calculate_year_stats(self):
        """
        Calculate yearly stats from monthly stats.
        Stores results in account_year_stats table (per account).
        """
        self._ensure_per_account_tables()
        self.db.connect()
        
        # Reset account_year_stats table
        self.db.reset_table("account_year_stats")
        
        cur = self.db.get_cursor()
        today = datetime.today().date()
        
        # Get all accounts
        accounts = [row[0] for row in cur.execute(
            "SELECT DISTINCT account FROM account_month_stats ORDER BY account"
        ).fetchall()]
        
        logging.info(f"Calculating yearly stats for {len(accounts)} accounts")
        
        for account in accounts:
            # Get all years for this account
            cur.execute("""
                SELECT DISTINCT strftime('%Y', month) as year
                FROM account_month_stats
                WHERE account = ?
                ORDER BY year
            """, (account,))
            years = [row[0] for row in cur.fetchall()]
            
            for year_str in years:
                # Get last month of each year
                cur.execute("""
                    SELECT * FROM account_month_stats
                    WHERE account = ? AND strftime('%Y', month) = ?
                    ORDER BY month DESC
                    LIMIT 1
                """, (account, year_str))
                last_month = cur.fetchone()
                
                if last_month:
                    year_date = f"{year_str}-01-01"
                    
                    # Calculate APY for yearly stats (same logic as original)
                    middle_date = datetime(year=int(year_str), month=7, day=1).date()
                    total_gainloss_per = last_month[8]  # total_gainloss_per column
                    
                    if today >= middle_date + timedelta(days=365.25) and total_gainloss_per != 0:
                        annual_per_yield = 100 * ((total_gainloss_per/100 + 1) ** (1 / ((today - middle_date).days / 365.25)) - 1)
                    else:
                        annual_per_yield = None
                    
                    # Insert into per-account yearly table
                    cur.execute("""
                        INSERT INTO account_year_stats (
                            account, year, deposit, withdrawal, value,
                            total_gainloss, realized_gainloss, unrealized_gainloss,
                            total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                            annual_per_yield, acc_net_deposit, acc_deposit, acc_value,
                            acc_unrealized_gainloss, acc_total_gainloss
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        account, year_date, last_month[2], last_month[3],  # deposit, withdrawal
                        last_month[4], last_month[5], last_month[6],  # value, total_gainloss, realized_gainloss
                        last_month[7], last_month[8], last_month[9],  # unrealized_gainloss, total_gainloss_per, realized_gainloss_per
                        last_month[10], annual_per_yield, last_month[12],  # unrealized_gainloss_per, annual_per_yield, acc_net_deposit
                        last_month[13], last_month[14], last_month[15], last_month[16]  # acc_deposit, acc_value, acc_unrealized_gainloss, acc_total_gainloss
                    ))
        
        self.db.commit()
        logging.info(f"Yearly stats calculated for {len(accounts)} accounts")
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
            Uses per-account cached tables when available.
        period (str): "month" or "year".
        deposits (str): "current" or "all".

        Returns:
        list: List of stats:     
        """
        self._ensure_per_account_tables()
        self.db.connect()
        cur = self.db.get_cursor()
        
        # If no accounts specified, use all accounts (global view)
        if accounts is None:
            accounts = [row[0] for row in cur.execute(
                "SELECT DISTINCT account FROM account_month_stats ORDER BY account"
            ).fetchall()]
        
        # Single account - direct query from cached tables
        if len(accounts) == 1:
            account = accounts[0]
            if period == "month":
                query = """
                    SELECT month, deposit, withdrawal, value,
                           total_gainloss, realized_gainloss, unrealized_gainloss,
                           total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                           annual_per_yield
                    FROM account_month_stats
                    WHERE account = ?
                    ORDER BY month
                """
            else:  # year
                query = """
                    SELECT year, deposit, withdrawal, value,
                           total_gainloss, realized_gainloss, unrealized_gainloss,
                           total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                           annual_per_yield
                    FROM account_year_stats
                    WHERE account = ?
                    ORDER BY year
                """
            
            cur.execute(query, (account,))
            rows = cur.fetchall()
            
            # Convert to list of tuples
            stats = []
            for row in rows:
                stats.append(tuple(row))
            
            # Filter by deposits parameter
            if deposits == "current":
                stats = [row for row in stats if row[3] > 0]  # value > 0
            
            return stats
        
        # Multiple accounts - merge from cached tables
        placeholders = ",".join("?" * len(accounts))
        
        if period == "month":
            # Merge monthly stats
            query = f"""
                SELECT month,
                       SUM(deposit) as deposit,
                       SUM(withdrawal) as withdrawal,
                       SUM(value) as value,
                       SUM(total_gainloss) as total_gainloss,
                       SUM(realized_gainloss) as realized_gainloss,
                       SUM(unrealized_gainloss) as unrealized_gainloss
                FROM account_month_stats
                WHERE account IN ({placeholders})
                GROUP BY month
                ORDER BY month
            """
            cur.execute(query, accounts)
            rows = cur.fetchall()
            
            # Recalculate percentages and APY
            stats = []
            today = datetime.today().date()
            
            for row in rows:
                month = row[0]
                deposit = row[1] or 0.0
                withdrawal = row[2] or 0.0
                value = row[3] or 0.0
                total_gainloss = row[4] or 0.0
                realized_gainloss = row[5] or 0.0
                unrealized_gainloss = row[6] or 0.0
                
                # Calculate percentages
                if deposit > 0:
                    total_gainloss_per = 100 * total_gainloss / deposit
                    realized_gainloss_per = 100 * realized_gainloss / deposit
                    unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                else:
                    total_gainloss_per = 0.0
                    realized_gainloss_per = 0.0
                    unrealized_gainloss_per = 0.0
                
                # Calculate APY
                if isinstance(month, str):
                    month_date = datetime.strptime(month, "%Y-%m-%d").date()
                else:
                    month_date = month
                
                middle_date = month_date.replace(day=15)
                if today >= middle_date + timedelta(days=365.25) and total_gainloss_per != 0:
                    annual_per_yield = 100 * ((total_gainloss_per/100 + 1) ** (1 / ((today - middle_date).days / 365.25)) - 1)
                else:
                    annual_per_yield = None
                
                stats.append((
                    month_date, deposit, withdrawal, value,
                    total_gainloss, realized_gainloss, unrealized_gainloss,
                    total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                    annual_per_yield
                ))
        
        else:  # period == "year"
            # Merge yearly stats
            query = f"""
                SELECT year,
                       SUM(deposit) as deposit,
                       SUM(withdrawal) as withdrawal,
                       SUM(value) as value,
                       SUM(total_gainloss) as total_gainloss,
                       SUM(realized_gainloss) as realized_gainloss,
                       SUM(unrealized_gainloss) as unrealized_gainloss
                FROM account_year_stats
                WHERE account IN ({placeholders})
                GROUP BY year
                ORDER BY year
            """
            cur.execute(query, accounts)
            rows = cur.fetchall()
            
            # Recalculate percentages and APY
            stats = []
            today = datetime.today().date()
            
            for row in rows:
                year_str = row[0]
                if isinstance(year_str, str):
                    year_date = datetime.strptime(year_str, "%Y-%m-%d").date()
                    year = year_date.year
                else:
                    year_date = year_str
                    year = year_date.year
                
                deposit = row[1] or 0.0
                withdrawal = row[2] or 0.0
                value = row[3] or 0.0
                total_gainloss = row[4] or 0.0
                realized_gainloss = row[5] or 0.0
                unrealized_gainloss = row[6] or 0.0
                
                # Calculate percentages
                if deposit > 0:
                    total_gainloss_per = 100 * total_gainloss / deposit
                    realized_gainloss_per = 100 * realized_gainloss / deposit
                    unrealized_gainloss_per = 100 * unrealized_gainloss / deposit
                else:
                    total_gainloss_per = 0.0
                    realized_gainloss_per = 0.0
                    unrealized_gainloss_per = 0.0
                
                # Calculate APY for year stats
                middle_date = datetime(year=year, month=7, day=1).date()
                if today >= middle_date + timedelta(days=365.25) and total_gainloss_per != 0:
                    annual_per_yield = 100 * ((total_gainloss_per/100 + 1) ** (1 / ((today - middle_date).days / 365.25)) - 1)
                else:
                    annual_per_yield = None
                
                stats.append((
                    date(year, 1, 1), deposit, withdrawal, value,
                    total_gainloss, realized_gainloss, unrealized_gainloss,
                    total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per,
                    annual_per_yield
                ))
        
        # Filter by deposits parameter
        if deposits == "current":
            stats = [row for row in stats if row[3] > 0]  # value > 0
        
        return stats
    def get_accumulated(self, accounts=None, period: str = "month", deposits: str = "current") -> list:
        """
        Get accumulated stats such as capital transfers and gain/loss for either months or years.
        "deposits" determine if only months/years with non-withdrawn capital are returned or all months/years.
        
        Accumulated stats are returned as a list in the following order:
        month, acc_net_deposit, acc_value, acc_unrealized_gainloss
        """
        self._ensure_per_account_tables()
        self.db.connect()
        cur = self.db.get_cursor()
        
        # If no accounts specified, use all accounts
        if accounts is None:
            accounts = [row[0] for row in cur.execute(
                "SELECT DISTINCT account FROM account_month_stats ORDER BY account"
            ).fetchall()]
        
        # Single account - direct query from cached tables
        if len(accounts) == 1:
            account = accounts[0]
            if period == "month":
                query = """
                    SELECT month, acc_net_deposit, acc_value, acc_unrealized_gainloss
                    FROM account_month_stats
                    WHERE account = ?
                    ORDER BY month
                """
            else:  # year
                query = """
                    SELECT year, acc_net_deposit, acc_value, acc_unrealized_gainloss
                    FROM account_year_stats
                    WHERE account = ?
                    ORDER BY year
                """
            
            cur.execute(query, (account,))
            rows = cur.fetchall()
            
            # Filter by deposits parameter
            stats = []
            for row in rows:
                if deposits == "current" and row[2] <= 0:  # acc_value <= 0
                    continue
                stats.append(tuple(row))
            
            return stats
        
        # Multiple accounts - merge from cached tables
        placeholders = ",".join("?" * len(accounts))
        
        if period == "month":
            query = f"""
                SELECT month,
                       SUM(acc_net_deposit) as acc_net_deposit,
                       SUM(acc_value) as acc_value,
                       SUM(acc_unrealized_gainloss) as acc_unrealized_gainloss
                FROM account_month_stats
                WHERE account IN ({placeholders})
                GROUP BY month
                ORDER BY month
            """
        else:  # year
            query = f"""
                SELECT year,
                       SUM(acc_net_deposit) as acc_net_deposit,
                       SUM(acc_value) as acc_value,
                       SUM(acc_unrealized_gainloss) as acc_unrealized_gainloss
                FROM account_year_stats
                WHERE account IN ({placeholders})
                GROUP BY year
                ORDER BY year
            """
        
        cur.execute(query, accounts)
        rows = cur.fetchall()
        
        # Filter by deposits parameter
        stats = []
        for row in rows:
            if deposits == "current" and row[2] <= 0:  # acc_value <= 0
                continue
            stats.append(tuple(row))
        
        return stats
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
        # Extract period from kwargs for date formatting
        period = kwargs.get('period', 'month')
        acc_stats = self.get_accumulated(**kwargs)
        print("Date, Deposit, Value, Gain/Loss")
        for (date_val, acc_net_deposit, acc_value, acc_gainloss) in acc_stats:
            # Format date based on period
            if hasattr(date_val, 'year'):
                if period == "year":
                    # For year stats, show just the year
                    display_date = date_val.year
                else:
                    # For month stats, show month name and year (e.g., "Jan 2026")
                    display_date = date_val.strftime("%b %Y")
            else:
                # Handle string dates if they occur
                try:
                    if period == "year":
                        display_date = date_val[:4]  # Just the year
                    else:
                        # Parse and format as month year
                        from datetime import datetime
                        dt = datetime.strptime(date_val, "%Y-%m-%d")
                        display_date = dt.strftime("%b %Y")
                except:
                    display_date = date_val
            print("{date}: {deposit:.0f}, {value:.0f}, {gain_loss:.0f}".format(date= display_date,deposit=acc_net_deposit,value=acc_value,gain_loss=acc_gainloss))

    def print_stats(self, **kwargs) -> None:
        """
        Print stats such as capital transfers and gain/loss for either months or years.

        Parameters:
        See get_stats
        """
        # Extract period from kwargs for date formatting
        period = kwargs.get('period', 'month')
        stats = self.get_stats(**kwargs)
        
        for (date_val, deposit, withdrawal, value, total_gainloss, realized_gainloss, unrealized_gainloss,total_gainloss_per, realized_gainloss_per, unrealized_gainloss_per, annual_per_yield) in stats:
            if deposit > 0:
                # Format date based on period
                if hasattr(date_val, 'year'):
                    if period == "year":
                        # For year stats, show just the year
                        display_date = date_val.year
                    else:
                        # For month stats, show month name and year (e.g., "Jan 2026")
                        display_date = date_val.strftime("%b %Y")
                else:
                    # Handle string dates if they occur
                    try:
                        if period == "year":
                            display_date = date_val[:4]  # Just the year
                        else:
                            # Parse and format as month year
                            from datetime import datetime
                            dt = datetime.strptime(date_val, "%Y-%m-%d")
                            display_date = dt.strftime("%b %Y")
                    except:
                        display_date = date_val
                print(display_date)
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
        Get account summaries (assets, cash, percentages).
        
        Parameters:
        accounts (list or None): List of account strings to include, or None for all.
        
        Returns:
        list: List of tuples: (account, cash, asset_value, total_value)
        """
        self.db.connect()
        cur = self.db.get_cursor()
        
        # If no accounts specified, use all accounts
        if accounts is None:
            accounts = [row[0] for row in cur.execute(
                "SELECT DISTINCT account FROM month_data ORDER BY account"
            ).fetchall()]
        
        placeholders = ",".join("?" * len(accounts))
        
        # Get latest month for each account
        query = f"""
            SELECT ma.account, ma.month, 
                   SUM(ma.amount * a.latest_price) as asset_value,
                   md.capital as cash
            FROM month_assets ma
            JOIN assets a ON ma.asset_id = a.asset_id
            JOIN month_data md ON ma.account = md.account AND ma.month = md.month
            WHERE ma.account IN ({placeholders}) AND ma.amount > 0.001
            AND a.latest_price IS NOT NULL
            GROUP BY ma.account, ma.month
            HAVING ma.month = (
                SELECT MAX(month) FROM month_assets WHERE account = ma.account
            )
            ORDER BY ma.account
        """
        
        cur.execute(query, accounts)
        rows = cur.fetchall()
        
        summaries = []
        for row in rows:
            account = row[0]
            asset_value = row[2] or 0.0
            cash = row[3] or 0.0
            total = asset_value + cash
            
            summaries.append((account, cash, asset_value, total))
        
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
