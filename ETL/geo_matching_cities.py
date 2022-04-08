"""
Script Description: This script ...................

Step 1 - importing data

Step 2 - Merging with Geo-coded

.......
"""


#%%
# 0. Import Libraries
import os
import pandas as pd
import googlemaps

#%%
# 1. Import data


os.chdir(r'C:\Users\ThinkPad\Documents\rescarh\mapping\process')

variations = os.listdir(r'raw_data')
d = []
for x in variations:
    d.append(pd.read_csv('raw_data/' + x))
    
full_data=pd.concat(d)  

#%%
# 2. Identify good locations

x=full_data[ 'sub_region_2'].unique()

dataf=full_data.dropna(subset=['sub_region_2'])
dfg = dataf.set_index(['sub_region_1','sub_region_2'])[['retail_and_recreation_percent_change_from_baseline',
          'grocery_and_pharmacy_percent_change_from_baseline',
          'parks_percent_change_from_baseline',
          'transit_stations_percent_change_from_baseline',
          'workplaces_percent_change_from_baseline',
          'residential_percent_change_from_baseline']].isnull().reset_index()
dfg['obs']=1
dfg = dfg.groupby(['sub_region_1','sub_region_2']).sum().astype(int)

for x in dfg.columns:
    dfg[x]=dfg[x]/dfg['obs']


dfgx=dfg[(dfg['retail_and_recreation_percent_change_from_baseline']<0.05) &
         (dfg['parks_percent_change_from_baseline']<0.05)]

gd=dfgx.reset_index().iloc[:,[0,1]]

#%%
# 2. Create address for Geo-matching

gd['address'] = gd['sub_region_2'] + ', ' + gd['sub_region_1'] + ', ' + 'Brazil'

#%%
# 3. Perform geomatching



client = googlemaps.Client(key='AIzaSyDX6i66KRpUJvUl3-9N8iCjhM914oRdN7s')
googlemaps_data = []
non_working=[]
for i, row in gd.iterrows():
    try:
        response = client.geocode(f'{row["address"]}')
        if len(response) == 0:
            googlemaps_data.append({})
        entry = response[0]
        googlemaps_data.append({
        'Full Address': entry['formatted_address'],
        'Latitude': entry['geometry']['location']['lat'],
        'Longitude': entry['geometry']['location']['lng'],
        'GeoAccuracy': entry['geometry']['location_type'], 
        })
    except:
        print(row['address'])
        non_working.append({
        'address': row['address'],
                })

#%%
# 4. Export to excel 

gm_d=pd.DataFrame(googlemaps_data)

gm_d=pd.concat([gm_d,gd],axis=1)

gm_d.to_excel('input_data/geo_matching.xlsx')