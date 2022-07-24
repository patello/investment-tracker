import csv
import os

from datetime import datetime

data_file = open("./data/isk.csv","r")
data = csv.reader(data_file, delimiter=';')
header_row = next(data)

buffer_sources = {}
asset_sources = {}

def oldest_available_buffer(buffer_sources):
    oldest_available = max(buffer_sources)
    for date in buffer_sources:
        if buffer_sources[date] > 0 and date < oldest_available:
            oldest_available = date
    if buffer_sources[oldest_available] <= 0:
        #If we have a deficit, put it on the latest one
        return max(buffer_sources)
    return oldest_available

for line in reversed(list(data)):
    date = datetime.strptime(line[0],"%Y-%m-%d")
    date = date.replace(date.year,date.month,1)
    if line[2] == "Insättning":
        amount = float(line[6].replace(",","."))
        if date in buffer_sources:
            buffer_sources[date] += amount
        else:
            buffer_sources[date] = amount
    elif line[2] == "Köp":
        asset = line[3]
        amount = float(line[4].replace(",","."))
        total_amount = float(line[7].replace(",",".").replace("-","0"))-float(line[6].replace(",","."))
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
    elif line[2] == "Sälj":
        asset = line[3]
        total_amount = -float(line[4].replace(",","."))
        remaining_amount = total_amount
        amount_sources = {}
        while remaining_amount > 1e-10:
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
            sale_amount = float(line[6].replace(",","."))-float(line[7].replace(",",".").replace("-","0"))
            buffer_sources[date] += sale_amount*amount_sources[date]/total_amount
    elif line[2] == "Utdelning":
        asset = line[3]
        dividend_per_share = float(line[5].replace(",","."))
        for date in asset_sources[asset]:
            buffer_sources[date] += asset_sources[asset][date] * dividend_per_share
data_file.close()