import csv
import json

from json import JSONDecodeError
from datetime import datetime

class AssetDeficit(Exception):
    pass

data_file = open("./data/avanza_data.csv","r")
data = csv.reader(data_file, delimiter=';')
header_row = next(data)
data = list(data)

buffer_sources = {}
asset_sources = {}
deposits = {}
withdrawals = {}
deferred_lines = {"deferred":False,"stop":0,"lines":[]}

listing_change = {"change":False,"from_asset":"","from_amount":0,"to_asset":"","to_amount":0}

def oldest_available_buffer(buffer_sources):
    oldest_available = max(buffer_sources)
    deficit = False 
    for date in buffer_sources:
        if buffer_sources[date] > 0 and date < oldest_available:
            oldest_available = date
    if buffer_sources[oldest_available] <= 0:
        #If we have a deficit, put it on the latest one and return deficit True
        deficit = True
        return (max(buffer_sources),deficit)
    return (oldest_available,deficit)

def handle_deposit(line,buffer_sources,deposits):
    amount = float(line[6].replace(",","."))
    if date in buffer_sources:
        buffer_sources[date] += amount
    else:
        buffer_sources[date] = amount
    if date in deposits:
        deposits[date] += amount
    else:
        deposits[date] = amount

def handle_withdrawal(line,buffer_sources,withdrawals,deferred_lines):
    total_amount = -float(line[6].replace(",","."))
    remaining_amount = total_amount
    withdrawal_sources = {}
    while remaining_amount > 0:
        (oldest_available,deficit) = oldest_available_buffer(buffer_sources)
        if not deferred_lines["deferred"] and deficit:
            remainder_line = line
            remainder_line[6] = str(-remaining_amount)
            deferred_lines["lines"].append(remainder_line)
            remaining_amount = 0
        elif deferred_lines["deferred"] and deficit:
            #Still deficit, put it here anyway
            buffer_sources[oldest_available] -= remaining_amount
            if oldest_available in withdrawal_sources:
                withdrawal_sources[oldest_available] += remaining_amount
            else:
                withdrawal_sources[oldest_available] = remaining_amount
            remaining_amount = 0
        elif buffer_sources[oldest_available] >= remaining_amount:
            buffer_sources[oldest_available] -= remaining_amount
            withdrawal_sources[oldest_available] = remaining_amount
            remaining_amount = 0
        else:
            withdrawal_sources[oldest_available] = buffer_sources[oldest_available]
            remaining_amount -= buffer_sources[oldest_available]
            buffer_sources[oldest_available] = 0
    for date in withdrawal_sources:
        if date in withdrawals:
            withdrawals[date] += withdrawal_sources[date]
        else:
            withdrawals[date] = withdrawal_sources[date]

def handle_purchase(line,buffer_sources,asset_sources,deferred_lines):
    asset = line[3]
    asset_amount = float(line[4].replace(",","."))
    remaining_asset_amount = asset_amount
    total_amount = -float(line[6].replace(",","."))
    remaining_amount = total_amount
    amount_sources = {}
    while remaining_amount > 0:
        (oldest_available,deficit) = oldest_available_buffer(buffer_sources)
        if not deferred_lines["deferred"] and deficit:
            remainder_line = line
            remainder_line[6] = str(-remaining_amount)
            remainder_line[4] = str(remaining_asset_amount)
            deferred_lines["lines"].append(remainder_line)
            remaining_amount = 0
        elif deferred_lines["deferred"] and deficit:
            #Deficit, put it here anyway
            buffer_sources[oldest_available] -= remaining_amount
            if oldest_available in amount_sources:
                amount_sources[oldest_available] += remaining_amount/total_amount
            else:
                amount_sources[oldest_available] = remaining_amount/total_amount
            remaining_amount = 0
            remaining_asset_amount = 0
        elif buffer_sources[oldest_available] >= remaining_amount:
            buffer_sources[oldest_available] -= remaining_amount
            amount_sources[oldest_available] = remaining_amount/total_amount
            remaining_amount = 0  
            remaining_asset_amount = 0          
        else:
            amount_sources[oldest_available] = buffer_sources[oldest_available]/total_amount
            remaining_amount -= buffer_sources[oldest_available]
            buffer_sources[oldest_available] = 0
            remaining_asset_amount -= amount_sources[oldest_available]*asset_amount
    if asset not in asset_sources:
        asset_sources[asset] = {}
    for date in amount_sources:
        if date in asset_sources[asset]:
            asset_sources[asset][date] += amount_sources[date]*asset_amount
        else:
            asset_sources[asset][date] = amount_sources[date]*asset_amount

def handle_sale(line,buffer_sources,asset_sources,deferred_lines):
    asset = line[3]
    total_amount = -float(line[4].replace(",","."))
    remaining_amount = total_amount
    amount_sources = {}
    while remaining_amount > 1e-3:
        (oldest_available,deficit) = oldest_available_buffer(asset_sources[asset])
        if not deferred_lines["deferred"] and deficit:
            remainder_line = line
            remainder_line[6] = str(-remaining_amount)
            deferred_lines["lines"].append(remainder_line)
            remaining_amount = 0
        elif deferred_lines["deferred"] and deficit:
            raise(AssetDeficit)
        elif asset_sources[asset][oldest_available] >= remaining_amount:
            asset_sources[asset][oldest_available] -= remaining_amount
            amount_sources[oldest_available] = remaining_amount
            remaining_amount = 0
        else:
            amount_sources[oldest_available] = asset_sources[asset][oldest_available]
            remaining_amount -= asset_sources[asset][oldest_available]
            asset_sources[asset][oldest_available] = 0
    for date in amount_sources:
        sale_amount = float(line[6].replace(",","."))
        buffer_sources[date] += sale_amount*amount_sources[date]/total_amount

def handle_dividend(line,buffer_sources):
    asset = line[3]
    dividend_per_share = float(line[5].replace(",","."))
    for date in asset_sources[asset]:
        buffer_sources[date] += asset_sources[asset][date] * dividend_per_share

def handle_fees(line,buffer_sources,deferred_lines):
    total_amount = -float(line[6].replace(",","."))
    remaining_amount = total_amount
    while remaining_amount > 0:
        (oldest_available,deficit) = oldest_available_buffer(buffer_sources)
        if not deferred_lines["deferred"] and deficit:
            remainder_line = line
            remainder_line[6] = str(-remaining_amount)
            deferred_lines["lines"].append(remainder_line)
            remaining_amount = 0
        elif deferred_lines["deferred"] and deficit:
            #Deficit, put it here anyway
            buffer_sources[oldest_available] -= remaining_amount
            remaining_amount = 0
        elif buffer_sources[oldest_available] >= remaining_amount:
            buffer_sources[oldest_available] -= remaining_amount
            remaining_amount = 0            
        else:
            remaining_amount -= buffer_sources[oldest_available]
            buffer_sources[oldest_available] = 0

def handle_listing_change_from(line,listing_change):
    listing_change["change"] = True
    listing_change["from_asset"] = line[3]
    listing_change["from_amount"] = -float(line[4].replace(",",".")) 

def handle_listing_change_to(line,listing_change):
    listing_change["change"] = False
    listing_change["to_asset"] = line[3]
    listing_change["to_amount"] = float(line[4].replace(",","."))
    asset_sources[listing_change["to_asset"]] = asset_sources[listing_change["from_asset"]]
    del asset_sources[listing_change["from_asset"]]
    #Multiply each entry by the amount diference quotient 
    for date in asset_sources[listing_change["to_asset"]]:
        asset_sources[listing_change["to_asset"]][date] = asset_sources[listing_change["to_asset"]][date] * listing_change["to_amount"]/listing_change["from_amount"]

line_i=len(data)-1
while line_i >= 0:
    line = data[line_i]
    #Check if date is different from deferred lines
    if len(deferred_lines["lines"])>0 and line[0] != deferred_lines["lines"][0][0]:
        #Do two passes, first add sale events so that they are executed first
        for deferred_line in deferred_lines["lines"]:
            if deferred_line[2] == "Sälj":
                data.insert(line_i+1,deferred_line)
        for deferred_line in deferred_lines["lines"]:
            if deferred_line[2] != "Sälj":
                data.insert(line_i+1,deferred_line)
        line_i += len(deferred_lines["lines"])
        deferred_lines = {"deferred":True,"stop":line_i-len(deferred_lines["lines"]),"lines":[]}
        line = data[line_i]
    if deferred_lines["deferred"] and line_i <= deferred_lines["stop"]:
        deferred_lines["deferred"] = False

    date = datetime.strptime(line[0],"%Y-%m-%d")
    date = date.replace(date.year,date.month,1)
    if listing_change["change"]:
        handle_listing_change_to(line,listing_change)
    elif line[2] == "Insättning":
        handle_deposit(line,buffer_sources,deposits)
    elif line[2] == "Uttag":
        handle_withdrawal(line,buffer_sources,withdrawals,deferred_lines)
    elif line[2] == "Köp":
        handle_purchase(line,buffer_sources,asset_sources,deferred_lines)
    elif line[2] == "Sälj":
        handle_sale(line,buffer_sources,asset_sources,deferred_lines)
    elif line[2] == "Utdelning":
        handle_dividend(line,buffer_sources)
    elif "Utländsk källskatt" in line[2] or "Ränt" in line[2] or "Prelskatt" in line[2]:
        handle_fees(line,buffer_sources,deferred_lines)
    elif "Byte" in line[2]:
        handle_listing_change_from(line,listing_change)
    else:
        raise(ValueError)

    line_i -= 1

#Create active asset summary
asset_file = open("./data/asset_file.json","r+",encoding="utf-8") 
try:
    active_asset_info = json.load(asset_file)
except JSONDecodeError:
    active_asset_info = {}

for asset in asset_sources:
    active = False
    amount = 0
    for date in asset_sources[asset]:
        if asset_sources[asset][date] > 1e-3:
            active = True
            amount += asset_sources[asset][date]
    if active:
        active_asset_info[asset] = {}
        active_asset_info[asset]["amount"] = amount
    elif asset in active_asset_info:
        del active_asset_info[asset]

asset_file.seek(0)
asset_file.write(json.dumps(active_asset_info))
asset_file.truncate()
asset_file.close()

#Create monthly information summary
month_info = {}

start_date = datetime.strptime(data[-1][0],"%Y-%m-%d")
start_date = start_date.replace(start_date.year,start_date.month,1)

end_date = datetime.today()
end_date = end_date.replace(end_date.year,end_date.month,1)

month = start_date
while month <= end_date:
    if month in deposits:
        month_deposit = deposits[month]
    else:
        month_deposit = 0
    if month in withdrawals:
        month_withdrawal = withdrawals[month]
    else:
        month_withdrawal = 0
    if month in buffer_sources:
        month_buffer = buffer_sources[month]
        if month_buffer < 0:
            month_buffer = 0
    else:
        month_buffer = 0

    month_assets = {}
    for asset in active_asset_info:
        if month in asset_sources[asset]:
            month_assets[asset] = asset_sources[asset][month]

    month_info[month] = {"deposit":month_deposit,"withdrawal":month_withdrawal,"buffer":month_buffer,"assets":month_assets}

    if month.month != 12:
        month = month.replace(month.year,month.month+1,month.day)
    else:
        month = month.replace(month.year+1,1,month.day)

data_file.close()