
import os
import sys
import zipfile
import traceback
import matplotlib.pyplot as plt
import pandas as pd
import googlemaps
from tqdm import tqdm
import requests

import os


# Function to download the zip file

#################################

def download_zip_file():

    dir_name = r'C:\Users\ThinkPad\Documents\rescarh\mapping\process\anac_data'
    # create directory if not exists
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    url = "https://www.gov.br/anac/pt-br/assuntos/dados-e-estatisticas/dados-estatisticos/arquivos/Dados_Estatisticos.zip"
    dest_name = url.rsplit('/', maxsplit=1)[-1]
    dest_dir = os.path.join(dir_name, dest_name)

    try:
        # Streaming, so we can iterate over the response.
        response = requests.get(url, stream=True)
        total_size_in_bytes= int(response.headers.get('content-length', 0))
        block_size = 1024 #1 Kibibyte
        progress_bar = tqdm( unit='B', unit_scale=True, unit_divisor=1024, miniters=0, desc=dest_name, total=total_size_in_bytes)
        
        # Downloading the data
        with open(dest_dir, 'wb') as file:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                file.write(data)
        progress_bar.close()
        print('Downloading Complete')

        # Unzip
        zip = zipfile.ZipFile(dest_dir)
        zip.extractall(dir_name)
        print('Done !')

        # Get The filename of the extracted data
        filename = [info.filename for info in zip.infolist() if info.filename.endswith('csv')][0]
        filepath = r'C:\Users\ThinkPad\Documents\aviation\anac_data\{filename}'

        # Apply Transformations
        df = pd.read_csv(filepath, encoding='latin-1', sep=';')

        return df

    except:
        traceback.print_exception(*sys.exc_info())
        return False
    
df = pd.read_csv(r'C:\Users\ThinkPad\Documents\aviation\anac_data\Dados Estatísticos.csv', encoding='latin-1', sep=';')

df=df[(df[ 'AEROPORTO DE DESTINO (PAÍS)']=='BRASIL') & (df['ANO']==2019)]


x=df.groupby('AEROPORTO DE DESTINO (SIGLA)')['PASSAGEIROS PAGOS'].sum()
x=x.sort_values(ascending=False).reset_index()

xf=x[x['PASSAGEIROS PAGOS']>200000]

df_filt=df[df['AEROPORTO DE DESTINO (SIGLA)'].isin(xf['AEROPORTO DE DESTINO (SIGLA)'].unique())]
l_geoc=df_filt[[ 'AEROPORTO DE DESTINO (SIGLA)', 'AEROPORTO DE DESTINO (NOME)',
       'AEROPORTO DE DESTINO (UF)']].drop_duplicates(subset=['AEROPORTO DE DESTINO (SIGLA)'])


l_geoc['address'] = l_geoc['AEROPORTO DE DESTINO (SIGLA)'] + ', ' + l_geoc['AEROPORTO DE DESTINO (NOME)'] + ', ' + l_geoc['AEROPORTO DE DESTINO (UF)'] + ', ' + 'Brazil'

################################################################
#%%
# 3. Perform geomatching

client = googlemaps.Client(key='AIzaSyDX6i66KRpUJvUl3-9N8iCjhM914oRdN7s')
googlemaps_data = []
non_working=[]

for i, row in l_geoc.iterrows():
    try:
        response = client.geocode(f'{row["address"]}')
        if len(response) == 0:
            googlemaps_data.append({})
        entry = response[0]
        googlemaps_data.append({'signal':row['AEROPORTO DE DESTINO (SIGLA)'],
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

gm_d=pd.DataFrame(googlemaps_data)
l_geoc=l_geoc.rename(columns={'AEROPORTO DE DESTINO (SIGLA)':'signal'}) 
gm_d=gm_d.merge(l_geoc,on='signal')

gm_d.to_excel(r'C:\Users\ThinkPad\Documents\rescarh\mapping\process\input_data\geo_matching_airports.xlsx')


#%%
import numpy as np

def spherical_dist(pos1, pos2, r=3958.75):
    pos1 = pos1 * np.pi / 180
    pos2 = pos2 * np.pi / 180
    cos_lat1 = np.cos(pos1[..., 0])
    cos_lat2 = np.cos(pos2[..., 0])
    cos_lat_d = np.cos(pos1[..., 0] - pos2[..., 0])
    cos_lon_d = np.cos(pos1[..., 1] - pos2[..., 1])
    return r * np.arccos(cos_lat_d - cos_lat1 * cos_lat2 * (1 - cos_lon_d))

ary=gm_d[[ 'Latitude', 'Longitude']].to_numpy()

distance=pd.DataFrame(spherical_dist(ary[:, None], ary))
distance.index=gm_d['signal']
distance.columns=gm_d['signal'].values

distance=distance.stack()

distance=distance.reset_index()

distance.columns=['signal','matching_signal','value']

distance=distance[distance['value']!=0].sort_values('value')

distance=distance[distance['value']<=50].sort_values('value')
distance.groupby('signal').count()

groupings={}

for s in distance.signal.unique():
    temp=distance[distance['signal']==s]['matching_signal']
    temp=temp.append(pd.Series(s)).sort_values()
    groupings[s]=temp.str.cat( sep ="-")

groupings=pd.DataFrame([groupings]).T.reset_index()
groupings.columns=['signal','g_signal']
##########################################3

gm_d=gm_d.merge(groupings,on='signal',how='left').drop_duplicates(subset=['signal'])

gm_d['g_signal']=gm_d['g_signal'].fillna(gm_d['signal'])



gm_d.to_excel(r'C:\Users\ThinkPad\Documents\rescarh\mapping\process\input_data\geo_matching_airports.xlsx')
