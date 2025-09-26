import requests
import datetime
import pandas as pd

headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,en-GB;q=0.7",
            "cache-control": "max-age=0",
            "priority": "u=0, i",
            "sec-ch-ua": '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0"
        }

def nsefetch(payload):
    try:
        s = requests.Session()
        s.get("https://www.nseindia.com", headers=headers, timeout=10)
        s.get("https://www.nseindia.com/option-chain", headers=headers, timeout=10)
        output = s.get(payload, headers=headers, timeout=10).json()
    except ValueError:
        output = {}
    return output

def fnolist():
    positions = nsefetch('https://www.nseindia.com/api/equity-stockIndices?index=SECURITIES%20IN%20F%26O')
    nselist=['NIFTY','NIFTYIT','BANKNIFTY']
    i=0
    for x in range(i, len(positions['data'])):
        nselist=nselist+[positions['data'][x]['symbol']]
    return nselist

def nsesymbolpurify(symbol):
    symbol = symbol.replace('&','%26')
    return symbol

def nse_quote(symbol,section=""):
    symbol = nsesymbolpurify(symbol)
    if(section==""):
        if any(x in symbol for x in fnolist()):
            payload = nsefetch('https://www.nseindia.com/api/quote-derivative?symbol='+symbol)
        else:
            payload = nsefetch('https://www.nseindia.com/api/quote-equity?symbol='+symbol)
        return payload

def expiry_list(symbol):
    payload = nse_quote(symbol)
    dates=list(set((payload["expiryDates"])))
    dates.sort(key = lambda date: datetime.datetime.strptime(date, '%d-%b-%Y'))
    return dates

print(expiry_list("NIFTY")[0]) # nearest expiry

# ---------------------OR----------------------

print(expiry_list("NIFTY"))

# ---------------------OR----------------------

# indexes = ['NIFTY','FINNIFTY','BANKNIFTY',"MIDCPNIFTY"]
# expiry_data = {}

# for idx in indexes:
#     try:
#         expiry_dates = expiry_list(idx)
#         expiry_data[idx] = expiry_dates
#     except Exception as e:
#         expiry_data[idx] = []
#         print(f"Error fetching expiries for {idx}: {e}")

# df_expiries = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in expiry_data.items()]))
# print(df_expiries)