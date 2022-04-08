#%%
# 0. Import Libraries

import os
import pandas as pd

os.chdir(r'C:\Users\ThinkPad\Documents\rescarh\mapping\revised_process_m\revised_process')

#%%
# 1. Import data

geo_matching=pd.read_excel(r'input_data\geo_matching_airports_m.xlsx')

variations = os.listdir(r'C:\Users\ThinkPad\Documents\rescarh\mapping\revised_process_m\revised_process\raw_data\anac_data')
d = []
for x in variations:
    d.append(pd.read_csv(r'raw_data/anac_data/' + x , encoding='latin-1', sep=';'))
data=pd.concat(d) 


# %%
#2. Get only biggest airports

data2=data[(data[ 'AEROPORTO DE DESTINO (PAÍS)']=='BRASIL') & (data['ANO']==2019)]


data2=data2.groupby('AEROPORTO DE DESTINO (SIGLA)')['PASSAGEIROS PAGOS'].sum()
data2=data2.sort_values(ascending=False).reset_index()

data2=data2[data2['PASSAGEIROS PAGOS']>200000]

m = data['AEROPORTO DE DESTINO (SIGLA)'].isin(data2['AEROPORTO DE DESTINO (SIGLA)'])
dt = data[m]


# %%
# 3. Parsing

dt = dt[dt['AEROPORTO DE DESTINO (PAÍS)'] == 'BRASIL' ]
dt = dt[(dt['ANO']>2018)]


dt['Date'] = dt['ANO'].astype(str) + '-' + dt['MÊS'].astype(str)
dt['Date'] = pd.to_datetime(dt['Date'] )

dt = dt.groupby(['AEROPORTO DE DESTINO (SIGLA)', 'Date'])['PASSAGEIROS PAGOS'].sum()

dt = dt.reset_index()


#%%
# 4. Merge

dt2 = pd.merge(dt, geo_matching ,left_on = 'AEROPORTO DE DESTINO (SIGLA)', right_on='signal', how='inner')

dt2['destination_airports'] = dt2['name_2']


dt2= dt2.drop(columns=['AEROPORTO DE DESTINO (SIGLA)',  'Coluna1',
       'signal', 'Full Address', 'GeoAccuracy',
       'AEROPORTO DE DESTINO (NOME)', 'AEROPORTO DE DESTINO (UF)', 'address',
       'g_signal', 'name_2'])


dt2=dt2.groupby(['Date',  'Latitude', 'Longitude',
       'destination_airports'])['PASSAGEIROS PAGOS'].sum().reset_index()

# %%
#5. Metrics 

dt2['metric_yoy']=dt2.groupby('destination_airports')['PASSAGEIROS PAGOS'].pct_change(12)



# %%
# 6. Export data


full_data=dt2.dropna(subset=['metric_yoy'])


full_data['metric_yoy']=full_data['metric_yoy']*100
###

max_date=full_data['Date'].max()

map_data=full_data[full_data.Date==max_date]


map_data['scaled_variable'] = (map_data['metric_yoy'] -
                               map_data['metric_yoy'].min()) / (map_data['metric_yoy']
                                                                .max() - map_data['metric_yoy'].min())

map_data.to_csv('input_data/map_input_airport.csv')




### joining

table_data=full_data.merge(map_data[['destination_airports','metric_yoy']],on='destination_airports',
                           suffixes=['','_mrchg'])


table_data=table_data[table_data['Date']>'2021-01-01']

table_data.to_csv('input_data/table_input_airport.csv')





