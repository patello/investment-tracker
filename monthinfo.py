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

    total_gainloss = withdrawal + buffer + value - deposited
    if(withdrawal + buffer >= deposited or (withdrawal + buffer < deposited and value <= 0)):
        realized_gainloss = withdrawal + buffer - deposited
    else:
        realized_gainloss = 0.0
    unrealized_gainloss = total_gainloss - realized_gainloss

    total_gainloss_str = str(int(round(total_gainloss,0)))
    realized_gainloss_str = str(int(round(realized_gainloss,0)))
    unrealized_gainloss_str = str(int(round(unrealized_gainloss,0)))

    if deposited > 0:
        total_gainloss_per = total_gainloss/deposited
        realized_gainloss_per = realized_gainloss/deposited
        unrealized_gainloss_per = unrealized_gainloss/deposited
    else:
        total_gainloss_per = 0
        realized_gainloss_per = 0
        unrealized_gainloss_per = 0
    
    total_gainloss_per_str = str(int(round(100*total_gainloss_per,0)))+"%"
    realized_gainloss_per_str = str(int(round(100*realized_gainloss_per,0)))+"%"
    unrealized_gainloss_per_str = str(int(round(100*unrealized_gainloss_per,0)))+"%"

    print(month)
    print("Gain/Loss (kr): {total} ({unrealized}/{realized})".format(
        total=total_gainloss_str, unrealized = unrealized_gainloss_str, realized = realized_gainloss_str
    ))
    print("Gain/Loss (%): {total} ({unrealized}/{realized})".format(
        total=total_gainloss_per_str, unrealized = unrealized_gainloss_per_str, realized = realized_gainloss_per_str
    ))  
    print("")
