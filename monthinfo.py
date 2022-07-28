import json

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
    value = 0
    for asset in month_info[month]["assets"]:
        value += month_info[month]["assets"][asset]*asset_info[asset]["price"]

    month_info[month]["value"] = value
    month_info[month]["total_gainloss"] = withdrawal + buffer + value - deposited
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
            print("")

print_month()