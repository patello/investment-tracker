import json

from datetime import datetime, timedelta

asset_file = open("./data/asset_file.json","r")
asset_info = json.load(asset_file)
asset_file.close()

month_file = open("./data/month_file.json","r")
month_info = json.load(month_file)
month_file.close()

for month in month_info:
    deposited = month_info[month]["deposit"]
    withdrawal = month_info[month]["withdrawal"]
    buffer = month_info[month]["buffer"]
    value = buffer
    for asset in month_info[month]["assets"]:
        value += month_info[month]["assets"][asset]*asset_info[asset]["price"]

    month_info[month]["value"] = value
    month_info[month]["total_gainloss"] = withdrawal + value - deposited
    if(withdrawal + buffer >= deposited or (withdrawal + buffer < deposited and value <= 0)):
        month_info[month]["realized_gainloss"] = withdrawal + buffer - deposited
    else:
        month_info[month]["realized_gainloss"] = 0.0
    month_info[month]["unrealized_gainloss"] = month_info[month]["total_gainloss"] - month_info[month]["realized_gainloss"]
    
    if deposited > 0:
        month_info[month]["total_gainloss_per"] = 100*month_info[month]["total_gainloss"]/month_info[month]["deposit"]
        month_info[month]["unrealized_gainloss_per"] = 100*month_info[month]["unrealized_gainloss"]/month_info[month]["deposit"]
        month_info[month]["realized_gainloss_per"] = 100*month_info[month]["realized_gainloss"]/month_info[month]["deposit"]
    else:
        month_info[month]["total_gainloss_per"] = 0
        month_info[month]["unrealized_gainloss_per"] = 0
        month_info[month]["realized_gainloss_per"] = 0

    middle_date = datetime.strptime(month,"%Y-%m-%d").replace(day=15)
    if datetime.today() >= middle_date + timedelta(365.25) and month_info[month]["total_gainloss_per"]!=0:
        month_info[month]["annual_per_yield"] = 100*((month_info[month]["total_gainloss_per"]/100+1)**(1/((datetime.today()-middle_date).days/365.25))-1)
    else:
        month_info[month]["annual_per_yield"] = None

year_info = {}

for month in month_info:
    year = month.split("-")[0]
    if year not in year_info:
        year_info[year] = {
            "deposit":0,
            "withdrawal":0,
            "buffer":0,
            "value":0,
            "total_gainloss":0,
            "realized_gainloss":0,
            "unrealized_gainloss":0
        }

    year_info[year]["deposit"] += month_info[month]["deposit"]
    year_info[year]["withdrawal"] += month_info[month]["withdrawal"]
    year_info[year]["buffer"] += month_info[month]["buffer"]
    year_info[year]["value"] += month_info[month]["value"]
    year_info[year]["total_gainloss"] += month_info[month]["total_gainloss"]
    year_info[year]["realized_gainloss"] += month_info[month]["realized_gainloss"]
    year_info[year]["unrealized_gainloss"] += month_info[month]["unrealized_gainloss"]

for year in year_info:
    if year_info[year]["deposit"] > 0:
        year_info[year]["total_gainloss_per"] = 100*year_info[year]["total_gainloss"]/year_info[year]["deposit"]
        year_info[year]["unrealized_gainloss_per"] = 100*year_info[year]["unrealized_gainloss"]/year_info[year]["deposit"]
        year_info[year]["realized_gainloss_per"] = 100*year_info[year]["realized_gainloss"]/year_info[year]["deposit"]
    else:
        year_info[year]["total_gainloss_per"] = 0
        year_info[year]["unrealized_gainloss_per"] = 0
        year_info[year]["realized_gainloss_per"] = 0
    
    middle_date = datetime.strptime(year,"%Y").replace(month=7,day=1)
    if datetime.today() >= middle_date + timedelta(365.25) and year_info[year]["total_gainloss_per"]!=0:
        year_info[year]["annual_per_yield"] = 100*((year_info[year]["total_gainloss_per"]/100+1)**(1/((datetime.today()-middle_date).days/365.25))-1)
    else:
        year_info[year]["annual_per_yield"] = None

accumulated = []
for month in month_info:
    net_deposit = month_info[month]["deposit"] - month_info[month]["withdrawal"]
    if net_deposit < 0:
        net_deposit = 0
    if len(accumulated) == 0:
        if month_info[month]["value"]+month_info[month]["buffer"] > 0:
            accumulated.append({"month":month,"deposit":net_deposit,"gainloss":month_info[month]["total_gainloss"]})
    else:
        accumulated.append({"month":month,"deposit":accumulated[-1]["deposit"]+net_deposit,"gainloss":accumulated[-1]["gainloss"]+month_info[month]["total_gainloss"]})



def print_month():
    for month in month_info:
        if month_info[month]["deposit"] > 0:
            print(month)
            print("Deposited: {deposited:.0f}".format(deposited=month_info[month]["deposit"]))
            print("Value: {value:.0f}".format(value=month_info[month]["value"]))
            print("Withdrawal: {withdrawal:.0f}".format(withdrawal=month_info[month]["withdrawal"]))
            print("Gain/Loss: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=month_info[month]["total_gainloss"],gainloss_per=month_info[month]["total_gainloss_per"]))
            print("- Unrealized: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=month_info[month]["unrealized_gainloss"],gainloss_per=month_info[month]["unrealized_gainloss_per"]))
            print("- Realized: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=month_info[month]["realized_gainloss"],gainloss_per=month_info[month]["realized_gainloss_per"]))
            if month_info[month]["annual_per_yield"] is not None:
                print("APY: {apy:.1f}%".format(apy=month_info[month]["annual_per_yield"]))
            print("")

def print_year():
    for year in year_info:
        if year_info[year]["deposit"] > 0:
            print(year)
            print("Deposited: {deposited:.0f}".format(deposited=year_info[year]["deposit"]))
            print("Value: {value:.0f}".format(value=year_info[year]["value"]))
            print("Withdrawal: {withdrawal:.0f}".format(withdrawal=year_info[year]["withdrawal"]))
            print("Gain/Loss: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=year_info[year]["total_gainloss"],gainloss_per=year_info[year]["total_gainloss_per"]))
            print("- Unrealized: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=year_info[year]["unrealized_gainloss"],gainloss_per=year_info[year]["unrealized_gainloss_per"]))
            print("- Realized: {gainloss:.0f} ({gainloss_per:.1f}%)".format(gainloss=year_info[year]["realized_gainloss"],gainloss_per=year_info[year]["realized_gainloss_per"]))
            if year_info[year]["annual_per_yield"] is not None:
                print("APY: {apy:.1f}%".format(apy=year_info[year]["annual_per_yield"]))
            print("")

def print_accumulated():
    for month in accumulated:
        print("{month}: {deposit:.0f}, {gain_loss:.0f}".format(month=month["month"],deposit=month["deposit"],gain_loss=month["gainloss"]))

print("--Monthly Info--")
print_month()
print()
print("--Yearly Info--")
print_year()
#print("--Accumulated--")
#print_accumulated()