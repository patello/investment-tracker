import csv
import sqlite3
import json
import operator

from datetime import datetime,date
from functools import reduce


#Read data from csv file downloaded from Avanza
avanza_data_file = open("./data/newdata.csv","r")
avanza_data = csv.reader(avanza_data_file, delimiter=';')
avanza_header_row = next(avanza_data)
avanza_data = list(avanza_data)

#Sometimes Avanza changes the name of an asset, but the ISIN stays the same.
#This function handles these and other special cases
class SpecialCases:
    def __init__(self, special_cases):
        self.special_cases = special_cases

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

# Parse special cases from json file
def parse_special_cases(file_path):
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
    special_cases = list(zip(special_conditions, special_replacements))

    return SpecialCases(special_cases)
   
if __name__ == "__main__":
    #Connect to asset_data.db sqllite3 database
    con = sqlite3.connect("data/asset_data.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    cur = con.cursor()

    #Set up special cases
    special_cases = parse_special_cases("./data/special_cases.json")

    #Create table for storing transactions if it does not exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions(
            date DATE NOT NULL, 
            account TEXT NOT NULL,
            transaction_type TEXT NOT NULL,
            asset_name TEXT NOT NULL,
            amount REAL NOT NULL,
            price REAL NOT NULL,
            total REAL NOT NULL,
            courtage REAL NOT NULL,
            currency TEXT NOT NULL,
            isin TEXT NOT NULL,
            processed INT DEFAULT 0
            )""")

    #Find overlapping transactions to avoid adding douplicates
    max_date = max([datetime.strptime(row[0],"%Y-%m-%d") for row in avanza_data]).date()
    min_date = min([datetime.strptime(row[0],"%Y-%m-%d") for row in avanza_data]).date()
    existing_rows = cur.execute("SELECT date, account, transaction_type, asset_name, amount, price FROM transactions WHERE date >= ? and date <= ?",(min_date,max_date)).fetchall()

    #Convert numeric data stored as text in the .csv file to float
    #Numbers in csv file use comma as decimal separator, convert to dot. '-' is used for zero values
    def convert_number(number_string):
        return 0 if number_string == "-" else float(number_string.replace(",","."))

    #Add transactions to database
    for transaction in avanza_data:
        row = (\
            datetime.strptime(transaction[0], "%Y-%m-%d").date(),\
            transaction[1],transaction[2],transaction[3],\
            convert_number(transaction[4]),convert_number(transaction[5]),\
            convert_number(transaction[6]),convert_number(transaction[7]),\
            transaction[8],transaction[9]
            )
        if special_cases != None:
            row = special_cases.handle_special_cases(row)
        if row[0:6] not in existing_rows:
            cur.execute('INSERT INTO transactions(date, account, transaction_type,asset_name,amount,price,total,courtage,currency,isin) VALUES(?,?,?,?,?,?,?,?,?,?);',row)

    #Commit changes to database
    con.commit()