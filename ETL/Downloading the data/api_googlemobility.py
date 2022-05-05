# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# Install requirements
# %%
# pip install tqdm
# pip install requests
# pip install pandas
! pip install pytz 



# links
# https://www.google.com/covid19/mobility/
# https://covid19.apple.com/mobility


# Get CSV Data
import os
from re import I 
from tqdm import tqdm
import sys
import requests
import pandas as pd
import traceback
from datetime import datetime, timedelta
from pytz import timezone


dir_name = 'data'
# create directory if not exists
if not os.path.exists(dir_name):
    os.makedirs(dir_name)

# Current Date according to 
date_usa = (datetime.now()+ timedelta(days=-1)).astimezone(timezone('US/Pacific')).strftime('%Y-%m-%d')
urls = [
    'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv', 
    f'https://covid19-static.cdn-apple.com/covid19-mobility-data/2117HotfixDev17/v3/en-us/applemobilitytrends-{date_usa}.csv'
]

for url in urls:

    dest_name = url.split('/')[-1]
    dest_dir = os.path.join(dir_name, dest_name)

    try:
        # Streaming, so we can iterate over the response.
        response = requests.get(url, stream=True)
        total_size_in_bytes= int(response.headers.get('content-length', 0))
        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm( unit='B', unit_scale=True, unit_divisor=1024, miniters=0, desc=dest_name, total=total_size_in_bytes)
        with open(dest_dir, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        print('Downloading Complete')

        # Data Processing For Google Moblity
        if 'Global_Mobility_Report' in url:
            df = pd.read_csv(dest_dir, dtype='object')

            # Filter the data for Brasil Country
            print('Filtering where the country_region is Brazil')
            df_brazil = df.loc[
                (df['country_region'] == 'Brazil')
            ]
            df_brazil.to_csv(dest_dir, index=False)

        # Data Processing For Apple Mobility
        elif 'applemobilitytrends' in url:
            df_ = pd.read_csv(dest_dir, dtype='object')

            # Filter the data for Brasil Country
            print('Filtering where the country/region is Brazil')
            df_brazil_ = df_.loc[
                (df_['country'] == 'Brazil')
            ]
            df_brazil_.to_csv(dest_dir, index=False)

    except:
        traceback.print_exception(*sys.exc_info())
