import csv
import sqlite3
import json
import operator
import logging

from datetime import datetime,date
from functools import reduce
from database_handler import DatabaseHandler

#Sometimes Avanza changes the name of an asset, but the ISIN stays the same.
#This class handles these and other special cases
class SpecialCases:
    def __init__(self, file_path):
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
                    value = datetime.strptime(value, "%Y-%m-%d")
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
    def handle_special_cases(self,row):
        for i in range(len(self.special_cases)):
            #Special conditions are functions that check if a row matches a special case
            #They are stored in the first element of the special_cases list
            if self.special_cases[i][0](row):
                #Special replacements are functions that replace a row with a new row
                #They are stored in the second element of the special_cases list
                row = self.special_cases[i][1](row)
        return row
    
class DataAdder:
    def __init__(self,database, special_cases = None):
        self.database = database
        self.special_cases = special_cases

    #Convert numeric data stored as text in the .csv file to float
    #Numbers in csv file use comma as decimal separator, convert to dot. '-' is used for zero values
    def convert_number(self,number_string):
        return 0 if number_string == "-" else float(number_string.replace(",","."))
    
    #Read data from csv file downloaded from Avanza and add it to the database
    #Returns number of rows added to the database
    def add_data(self,file_path):
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
        self.database.connect()
        cur = self.database.conn.cursor()
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
        self.database.conn.commit()
        self.database.disconnect()

        # Return number of rows added to the database
        logging.info("Added {} rows to the database".format(rows_added))
        return rows_added
    
if __name__ == "__main__":
    # Connect to asset_data.db sqllite3 database
    con = sqlite3.connect("data/asset_data.db")
    cur = con.cursor()
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
