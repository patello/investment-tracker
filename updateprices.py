import requests
import json

asset_file = open("./data/asset_file.json","r+",encoding="utf-8")
active_asset_info = json.load(asset_file)

url = "https://www.avanza.se/_cqbe/search/global-search/global-search-template?query={asset}"

for asset in active_asset_info:

    url_name = asset
    #If asset has a slash, everything after the slash should be dropped
    if url_name.find("/") > 0:
        url_name = url_name.split("/",1)[0]

    r = requests.get(url.format(asset=url_name))

    if r.status_code == 200:
        resp = json.loads(r.content)
        price = float(resp["resultGroups"][0]["hits"][0]["lastPrice"].replace(",","."))

        active_asset_info[asset]["price"] = price

asset_file.seek(0)
asset_file.write(json.dumps(active_asset_info))
asset_file.truncate()
asset_file.close()