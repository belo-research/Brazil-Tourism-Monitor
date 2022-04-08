# %%

import pandas as pd
import altair as alt
import os
import geopandas as gpd
os.chdir(r"C:\Users\ThinkPad\Documents\rescarh\mapping\process")
# %%

### taking geo_matching file from before
cities=pd.read_excel(r'input_data/geo_matching.xlsx')
### converting into geopandas file
pois=gpd.GeoDataFrame(cities['sub_region_2'],geometry=gpd.points_from_xy(cities.Longitude,cities.Latitude))


# %%
### getting the muncipalities
munip=gpd.read_file(r'BR_Municipios_2020/BR_Municipios_2020.shp')
### getting the muncipalities
areas_turistico=pd.read_excel(r'input_data/relatorio_mapa_2019_layout_mkt.xlsx')


areas_turistico=areas_turistico[areas_turistico['CLUSTER']=='A']

munip_filter=munip.merge(areas_turistico,left_on='NM_MUN',right_on='MUNICIPIO')
# %%

dt=gpd.sjoin(munip_filter,pois,op='intersects')

dt=dt.drop('geometry',axis=1)

dt=dt.merge(cities,on='sub_region_2',how='left')


# %%
dt.to_excel('input_data/geo_matching_filt.xlsx')
# %%
