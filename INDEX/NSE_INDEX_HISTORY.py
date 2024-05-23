# -*- coding: utf-8 -*-

"""### Import Dependencies"""

import requests
import datetime
import json
import pandas as pd

"""### Set Environment Variables """

BASE_URL = 'https://www.niftyindices.com/'
HISTORICAL_DATA_URL = "https://www.niftyindices.com/Backpage.aspx/getHistoricaldatatabletoString"

"""### Define Helper Functions """

def get_adjusted_headers():
    return {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Content-Type": "application/json; charset=UTF-8",
    "Origin": "https://www.niftyindices.com",
    "Referer": "https://www.niftyindices.com/reports/historical-data",
    "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Mobile Safari/537.36"
}

def fetch_cookies():
    response = requests.get(BASE_URL, timeout=30, headers=get_adjusted_headers())
    if response.status_code != requests.codes.ok:
        # logging.error("Fetched url: %s with status code: %s and response from server: %s" % (
        #     BASE_URL, response.status_code, response.content))
        raise ValueError("Please try again in a minute.")
    return response.cookies.get_dict()

def scrape_data(start_date, end_date, name, input_type='index'):
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
    # cookies = fetch_cookies()

    start_date = datetime.datetime.strptime(start_date, "%d-%m-%Y")
    end_date = datetime.datetime.strptime(end_date, "%d-%m-%Y")

    pld = {
        'name' : name,
        'startDate' : start_date.strftime('%d-%b-%Y'),
        'endDate' : end_date.strftime('%d-%b-%Y'),
        'indexName' : name
    }

    payload = {"cinfo": str(pld)}
    response = requests.request("POST", HISTORICAL_DATA_URL, json=payload, timeout=30, headers=get_adjusted_headers())
    if response.status_code == requests.codes.ok:
        return pd.DataFrame(eval(json.loads(response.text)['d']))

"""### Scrape Directly to DataFrame """

scrape_data('01-01-2000','20-05-2023','NIFTY 50')