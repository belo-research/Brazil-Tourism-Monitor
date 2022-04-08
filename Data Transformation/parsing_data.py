"""
Script Description: This script ...................

Step 1 - importing data

Step 2 - Merging with Geo-coded


"""


#%%
# 0. Import Libraries

import os
import pandas as pd

os.chdir(r'C:\Users\ThinkPad\Documents\rescarh\mapping\revised_process_m\revised_process')

#%%
# 1. Import data

geo_matching=pd.read_excel(r'input_data\geo_matching_filt.xlsx')

variations = os.listdir(r'raw_data\google_data')

d = []
for x in variations:
    d.append(pd.read_csv(r'raw_data/google_data/' + x))
    
full_data=pd.concat(d)  

#%%
# 2. Merging

full_data=full_data.merge(geo_matching,on='sub_region_2')
full_data=full_data.sort_values(['sub_region_2','date'])
full_data=full_data.drop_duplicates(['sub_region_2','date'])

full_data['retail_rec']=full_data['retail_and_recreation_percent_change_from_baseline']
full_data['retail_rec']=full_data.groupby('sub_region_2')['retail_rec'].bfill()
full_data['retail_rec']=((100+full_data['retail_rec'])/100)*1000
full_data['retail_rec']=full_data['retail_rec'].replace({0:1000})


full_data['parks']=full_data['parks_percent_change_from_baseline']
full_data['parks']=full_data.groupby('sub_region_2')['parks'].bfill()
full_data['parks']=((100+full_data['parks'])/100)*1000
full_data['parks']=full_data['parks'].replace({0:1000})



full_data['metric']=(full_data['parks'] + full_data['retail_rec'])/2

full_data=full_data.rename(columns={'sub_region_1_x':'sub_region_1'})
#%%
# 2. creating metrics

full_data=full_data[['sub_region_1','sub_region_2', 'Latitude', 'Longitude','date',  'NM_MUN',
                     'metric',
                     ]]


import itertools
for x in [7,28,84]: 
    full_data['metric_' + str(x)]=full_data.groupby('sub_region_2')['metric'].transform(lambda s: s.rolling(x).mean())
    
for x in [7,28,84]: 
    full_data['metric_' + str(x)]=full_data.groupby('sub_region_2')['metric_' + str(x)].transform(lambda s: s.pct_change(366))
    
#%%
# 3. exporting for use in visualisations


full_data=full_data.dropna(subset=['metric_28'])


full_data['metric_28']=full_data['metric_28']*100

###

max_date=full_data['date'].max()

map_data=full_data[full_data.date==max_date]


map_data['scaled_variable'] = (map_data['metric_28'] -
                               map_data['metric_28'].min()) / (map_data['metric_28']
                                                                .max() - map_data['metric_28'].min())
                                                                

map_data.to_csv('input_data/map_input_city.csv')

### joining

table_data=full_data.merge(map_data[['sub_region_2','metric_28']],on='sub_region_2',
                           suffixes=['','_mrchg'])

table_data.to_csv('input_data/table_input_city.csv')




# %%
