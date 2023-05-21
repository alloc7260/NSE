# -*- coding: utf-8 -*-
"""# Import Dependencies"""

import requests
import datetime
import json
import pandas as pd
from concurrent.futures import ALL_COMPLETED
from six.moves.urllib.parse import urlparse
import re

"""# Define Helper Functions

## F&O
"""

def get_data(querystring, rename=0):
    url = "https://www.nseindia.com/api/historical/foCPV"
    headers = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36"}
    cookies = fetch_cookies()
    response = requests.request("GET", url, headers=headers, params=querystring, cookies=cookies)
    data = json.loads(response.text)["data"]
    df = pd.DataFrame(data)
    if rename:
        header = ["id","instrument","symbol","expiry_date","strike_price","option_type","market_type","opening_price","trade_high_price","trade_low_price","closing_price","last_traded_price","prev_cls","settle_price","tot_traded_qty","tot_traded_val","open_int","change_in_oi","market_lot","short_timestamp","long_timestamp","underlying_value"]
        df.columns = header
    return df

def get_adjusted_headers():
    return {
        'Host': 'www.nseindia.com',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:85.0) Gecko/20100101 Firefox/85.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'X-Requested-With': 'XMLHttpRequest',
        'DNT': '1',
        'Connection': 'keep-alive',
    }

def fetch_cookies():
    BASE_URL = 'https://www.nseindia.com/'
    response = requests.get(BASE_URL, timeout=30, headers=get_adjusted_headers())
    if response.status_code != requests.codes.ok:
        # logging.error("Fetched url: %s with status code: %s and response from server: %s" % (
        #     BASE_URL, response.status_code, response.content))
        raise ValueError("Please try again in a minute.")
    return response.cookies.get_dict()

"""## Expiry"""

idx_exp = {}
vix_exp = {}
stk_exp = {}

def add_dt(instru, dt):
    if not dt.year in instru:
        instru[dt.year] = {}

    if not dt.month in instru[dt.year]:
        instru[dt.year][dt.month] = set()

    instru[dt.year][dt.month].add(dt)

def get_file():
    import http.client
    # fetch_cookies()
    conn = http.client.HTTPSConnection("www1.nseindia.com")
    payload = ""
    headers = {
        'Accept': "*/*",
        'Connection': "keep-alive",
        'Content-Type': "application/x-javascript"
        }
    conn.request("GET", "/products/resources/js/foExp.js", payload, headers)
    res = conn.getresponse()
    data = res.read()
    # print(data.decode("utf-8"))
    return data.decode("utf-8")

def build_dt_dict():
    re_date = re.compile("([0-9]{2}\-[0-9]{2}\-[0-9]{4})")
    lines = get_file()
    for line in lines.split('\n'):
        s = re_date.search(line)
        if s:
            dt = datetime.datetime.strptime(s.group(1), "%d-%m-%Y").date()
            # Start Kludge
            # The list on NSE portal for expiry date has a wrong entry for 20 Sep 2019
            # Handle this oulier use case by ignoring this date and skpping it for processing
            if dt == datetime.datetime(2019, 9, 20).date():
                continue
            # End Kludge
            if line.find('indxExpryDt') > -1:
                try:
                    existing_date = try_to_get_expiry_date(
                        dt.year, dt.month, index=True)
                    if existing_date < dt:
                        add_dt(idx_exp, dt)
                except:
                    add_dt(idx_exp, dt)
            if line.find('stk') > -1:
                try:
                    existing_date = try_to_get_expiry_date(
                        dt.year, dt.month, index=False, stock=False, vix=False)
                    if existing_date < dt:
                        add_dt(stk_exp, dt)
                except:
                    add_dt(stk_exp, dt)
            if line.find('vix') > -1:
                try:
                    existing_date = try_to_get_expiry_date(
                        dt.year, dt.month, index=False, stock=False, vix=True)
                    if existing_date < dt:
                        add_dt(vix_exp, dt)
                except:
                    add_dt(vix_exp, dt)


def try_to_get_expiry_date(year, month, index=True, stock=False, vix=False):
    try:
        if vix and vix_exp:
            return vix_exp[year][month]
        if stock and stk_exp:
            return stk_exp[year][month]
        if index and idx_exp:
            return idx_exp[year][month]
        raise Exception
    except:
        if index:
            name = 'index derivatives'
        elif stock:
            name = 'stock derivatives'
        else:
            name = 'vix derivatives'
        raise ExpiryDateError(
            'No expiry date found in the month of {}-{} for {}'.format(year, month, name))


def get_expiry_date(year, month, index=True, stock=False, vix=False, recursion=0):
    try:
        return try_to_get_expiry_date(year, month, index, stock, vix)
    except:
        if recursion > 1:
            raise
        else:
            pass
    #print("building dictionary")
    build_dt_dict()
    return get_expiry_date(year, month, index, stock, vix, recursion=recursion+1)

"""# Stock Futures"""

# # Stock futures (Similarly for index futures, set index = True)
# stock_fut = get_history(symbol="SBIN",
#                         start=date(2015,1,1),
#                         end=date(2015,1,10),
#                         futures=True,
#                         expiry_date=date(2015,1,29))

print(get_expiry_date(year=2023, month=2, stock=1))

querystring = {
    "from":"20-02-2023",
    "to":"20-04-2023",
    "instrumentType":"FUTSTK",
    "symbol":"ACC",
    "year":"2000",
    "expiryDate":"27-Apr-2023"}

print(get_data(querystring, rename=1))

"""# Stock Options"""

# # Stock options (Similarly for index options, set index = True)
# stock_opt = get_history(symbol="SBIN",
#                         start=date(2015,1,1),
#                         end=date(2015,1,10),
#                         option_type="CE",
#                         strike_price=300,
#                         expiry_date=date(2015,1,29))

print(get_expiry_date(year=2023, month=2, stock=1))

querystring = {
    "from":"20-04-2023",
    "to":"20-05-2023",
    "instrumentType":"OPTSTK",
    "symbol":"ACC",
    "year":"2023",
    "expiryDate":"25-May-2023",
    "optionType":"CE"}

print(get_data(querystring, rename=0))

"""# Index Futures"""

# nifty_fut = get_history(symbol="NIFTY",
#                         start=date(2015,1,1),
#                         end=date(2015,1,10),
#                         index=True,
#                         futures=True,
#                         expiry_date=date(2015,1,29))

print(get_expiry_date(year=2023, month=4))

querystring = {
    "from":"13-05-2023",
    "to":"20-05-2023",
    "instrumentType":"FUTIDX",
    "symbol":"BANKNIFTY",
    "year":"2023"}

print(get_data(querystring, rename=1))

"""# Index Options"""

# nifty_opt = get_history(symbol="NIFTY",
#                         start=date(2015,1,1),
#                         end=date(2015,1,10),
#                         index=True,
#                         option_type='CE',
#                         strike_price=8200,
#                         expiry_date=date(2015,1,29))

print(get_expiry_date(year=2023, month=4))

querystring = {
    "from":"13-05-2023",
    "to":"20-05-2023",
    "instrumentType":"OPTIDX",
    "symbol":"BANKNIFTY",
    "year":"2023",
    "optionType":"CE"}

print(get_data(querystring, rename=0))