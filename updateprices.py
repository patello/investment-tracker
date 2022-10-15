import requests
import json
import sqlite3

from datetime import datetime

con = sqlite3.connect("data/asset_data.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
cur = con.cursor()

assets = cur.execute("SELECT asset,asset_id FROM assets WHERE amount > 0").fetchall()

url = "https://www.avanza.se/_cqbe/search/global-search/global-search-template?query={asset}"

today = datetime.today().date()

for (asset,asset_id) in assets:
    url_name = asset
    #If asset has a slash, everything after the slash should be dropped
    if url_name.find("/") > 0:
        url_name = url_name.split("/",1)[0]

    r = requests.get(url.format(asset=url_name))

    if r.status_code == 200:
        resp = json.loads(r.content)
        price = float(resp["resultGroups"][0]["hits"][0]["lastPrice"].replace(",","."))

        cur.execute("UPDATE assets SET latest_price = ?, latest_price_date = ? WHERE asset_id = ?",(price,today,asset_id))

con.commit()