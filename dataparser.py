import csv
import os

from datetime import datetime

data_file = open("./data/isk.csv","r")
data = csv.reader(data_file, delimiter=';')
header_row = next(data)

buffer_sources = {}
asset_sources = {}
deposits = {}
withdrawals = {}

listing_change = {"change":False,"from_asset":"","from_amount":0,"to_asset":"","to_amount":0}

def oldest_available_buffer(buffer_sources):
    oldest_available = max(buffer_sources)
    for date in buffer_sources:
        if buffer_sources[date] > 0 and date < oldest_available:
            oldest_available = date
    if buffer_sources[oldest_available] <= 0:
        #If we have a deficit, put it on the latest one
        return max(buffer_sources)
    return oldest_available

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

def handle_withdrawal(line,buffer_sources,withdrawals):
    total_amount = -float(line[6].replace(",","."))
    remaining_amount = total_amount
    withdrawal_sources = {}
    while remaining_amount > 0:
        oldest_available = oldest_available_buffer(buffer_sources)
        if buffer_sources[oldest_available] >= remaining_amount:
            buffer_sources[oldest_available] -= remaining_amount
            withdrawal_sources[oldest_available] = remaining_amount
            remaining_amount = 0
        elif buffer_sources[oldest_available] <= 0:
            #Deficit, put it here anyway
            buffer_sources[oldest_available] -= remaining_amount
            if oldest_available in withdrawal_sources:
                withdrawal_sources[oldest_available] += remaining_amount
            else:
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

def handle_purchase(line,buffer_sources,asset_sources):
    asset = line[3]
    amount = float(line[4].replace(",","."))
    total_amount = -float(line[6].replace(",","."))
    remaining_amount = total_amount
    amount_sources = {}
    while remaining_amount > 0:
        oldest_available = oldest_available_buffer(buffer_sources)
        if buffer_sources[oldest_available] >= remaining_amount:
            buffer_sources[oldest_available] -= remaining_amount
            amount_sources[oldest_available] = remaining_amount/total_amount
            remaining_amount = 0
        elif buffer_sources[oldest_available] <= 0:
            #Deficit, put it here anyway
            buffer_sources[oldest_available] -= remaining_amount
            if oldest_available in amount_sources:
                amount_sources[oldest_available] += remaining_amount/total_amount
            else:
                amount_sources[oldest_available] = remaining_amount/total_amount
            remaining_amount = 0
        else:
            amount_sources[oldest_available] = buffer_sources[oldest_available]/total_amount
            remaining_amount -= buffer_sources[oldest_available]
            buffer_sources[oldest_available] = 0
    if asset not in asset_sources:
        asset_sources[asset] = {}
    for date in amount_sources:
        if date in asset_sources[asset]:
            asset_sources[asset][date] += amount_sources[date]*amount
        else:
            asset_sources[asset][date] = amount_sources[date]*amount

def handle_sale(line,buffer_sources,asset_sources):
    asset = line[3]
    total_amount = -float(line[4].replace(",","."))
    remaining_amount = total_amount
    amount_sources = {}
    while remaining_amount > 1e-3:
        oldest_available = oldest_available_buffer(asset_sources[asset])
        if asset_sources[asset][oldest_available] >= remaining_amount:
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

def handle_fees(line,buffer_sources):
    total_amount = -float(line[6].replace(",","."))
    remaining_amount = total_amount
    while remaining_amount > 0:
        oldest_available = oldest_available_buffer(buffer_sources)
        if buffer_sources[oldest_available] >= remaining_amount:
            buffer_sources[oldest_available] -= remaining_amount
            remaining_amount = 0
        elif buffer_sources[oldest_available] <= 0:
            #Deficit, put it here anyway
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

for line in reversed(list(data)):
    date = datetime.strptime(line[0],"%Y-%m-%d")
    date = date.replace(date.year,date.month,1)
    if listing_change["change"]:
        handle_listing_change_to(line,listing_change)
    elif line[2] == "Insättning":
        handle_deposit(line,buffer_sources,deposits)
    elif line[2] == "Uttag":
        handle_withdrawal(line,buffer_sources,withdrawals)
    elif line[2] == "Köp":
        handle_purchase(line,buffer_sources,asset_sources)
    elif line[2] == "Sälj":
        handle_sale(line,buffer_sources,asset_sources)
    elif line[2] == "Utdelning":
        handle_dividend(line,buffer_sources)
    elif "Utländsk källskatt" in line[2] or "Ränt" in line[2]:
        handle_fees(line,buffer_sources)
    elif "Byte" in line[2]:
        handle_listing_change_from(line,listing_change)
data_file.close()