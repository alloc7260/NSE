# -*- coding: utf-8 -*-

"""### Import Dependencies"""

import math
import requests
import datetime
import json
import urllib
import pandas as pd
import concurrent
from concurrent.futures import ALL_COMPLETED
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

"""### Set Environment Variables """

HISTORICAL_DATA_URL = 'https://www.nseindia.com/api/historical/cm/equity?series=[%22EQ%22]&'
BASE_URL = 'https://www.nseindia.com/'

"""### Define Helper Functions """

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
    response = requests.get(BASE_URL, timeout=30, headers=get_adjusted_headers())
    if response.status_code != requests.codes.ok:
        # logging.error("Fetched url: %s with status code: %s and response from server: %s" % (
        #     BASE_URL, response.status_code, response.content))
        raise ValueError("Please try again in a minute.")
    return response.cookies.get_dict()

def fetch_url(session, url, cookies):
    """
        This is the function call made by each thread. A get request is made for given start and end date, response is
        parsed and dataframe is returned
    """
    response = session.get(url, timeout=30, headers=get_adjusted_headers(), cookies=cookies)
    if response.status_code == requests.codes.ok:
        json_response = json.loads(response.content)
        return pd.DataFrame.from_dict(json_response['data'])
    else:
        raise ValueError("Please try again in a minute.")

def scrape_data(start_date, end_date, name=None, input_type='stock'):
    """
    Called by stocks and indices to scrape data.
    Create threads for different requests, parses data, combines them and returns dataframe
    Args:
        start_date (datetime.datetime): start date
        end_date (datetime.datetime): end date
        input_type (str): Either 'stock' or 'index'
        name (str, optional): stock symbol or index name. Defaults to None.
    Returns:
        Pandas DataFrame: df containing data for stocksymbol for provided date range
    """
    cookies = fetch_cookies()
    
    # Requests session
    retries = Retry(total=10, backoff_factor=2)
    adapter = HTTPAdapter(max_retries=retries)
    session = requests.Session()
    session.mount('https://', adapter)

    start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
    end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")

    threads, url_list = [], []

    # set the window size to one year
    window_size = datetime.timedelta(days=50)

    current_window_start = start_date
    while current_window_start < end_date:
        current_window_end = current_window_start + window_size
        
        # check if the current window extends beyond the end_date
        if current_window_end > end_date:
            current_window_end = end_date
        
        st = current_window_start.strftime('%d-%m-%Y')
        et = current_window_end.strftime('%d-%m-%Y')
        # print(st,et)
        if input_type == 'stock':
            params = {'symbol': name,
                        'from': st,
                        'to': et}
            url = HISTORICAL_DATA_URL + urllib.parse.urlencode(params)
            url_list.append(url)

        # move the window start to the next day after the current window end
        current_window_start = current_window_end + datetime.timedelta(days=1)

    result = pd.DataFrame()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_url, url, cookies): url for url in url_list}
        concurrent.futures.wait(future_to_url, return_when=ALL_COMPLETED)
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                df = future.result()
                result = pd.concat([result, df])
            except Exception as exc:
                # logging.error('%r generated an exception: %s. Please try again later.' % (url, exc))
                raise exc
    session.close()
    return format_dataframe_result(result)


def format_dataframe_result(result):
    if not len(result):
        print("No price data found for the selected conditions")
        return None
    
    columns_required = ["CH_TIMESTAMP", "CH_SYMBOL", "CH_SERIES", "CH_TRADE_HIGH_PRICE",
                        "CH_TRADE_LOW_PRICE", "CH_OPENING_PRICE", "CH_CLOSING_PRICE", "CH_LAST_TRADED_PRICE",
                        "CH_PREVIOUS_CLS_PRICE", "CH_TOT_TRADED_QTY", "CH_TOT_TRADED_VAL", "CH_52WEEK_HIGH_PRICE",
                        "CH_52WEEK_LOW_PRICE"]
    result = result[columns_required]
    result = result.set_axis(
        ['Date', 'Symbol', 'Series', 'High Price', 'Low Price', 'Open Price', 'Close Price', 'Last Price',
         'Prev Close Price', 'Total Traded Quantity', 'Total Traded Value', '52 Week High Price',
         '52 Week Low Price'], axis=1)
    result['Date'] = pd.to_datetime(result['Date'])
    result = result.sort_values('Date', ascending=True)
    result.reset_index(drop=True, inplace=True)
    return result

"""### Scrape Directly to DataFrame """

df = scrape_data('01-01-2000','02-02-2023','MRF')
print(df)
