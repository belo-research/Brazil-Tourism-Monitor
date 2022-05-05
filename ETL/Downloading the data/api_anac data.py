# %%
# Install requirements

! pip install tqdm
! pip install requests
! pip install pandas


# Running instructions
# python api.py

# Get CSV Data
import os
import sys
import zipfile
import traceback

import pandas as pd

from tqdm import tqdm
import requests


# Function to download the zip file
def download_zip_file():

    dir_name = 'anac_data'
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
        filepath = f'{dir_name}/{filename}'

        # Apply Transformations
        transform_data(filename=filepath)

        return True

    except:
        traceback.print_exception(*sys.exc_info())
        return False


# Function to Preprocess the data
def transform_data(filename: str):

    # Read the data and save it again
    df = pd.read_csv(filename, encoding='latin-1', sep=';')

    # Filter the data for Brasil Country
    print('Filtering where the AEROPORTO is BRASIL')
    df_brazil = df.loc[
        (df['AEROPORTO DE ORIGEM (PAÍS)'] == 'BRASIL') &
        (df['AEROPORTO DE DESTINO (PAÍS)'] == 'BRASIL')
    ]
    
    df_brazil.to_csv('anac_data/DATA BRASIL.csv', index=False)
    
    # Group data by Company
    print('Grouping Data with EMPRESA (NOME), ANO and MÊS')
    df_company = df.groupby(['EMPRESA (NOME)', 'ANO', 'MÊS']).size().reset_index(name='NB PASSAGEIROS')
    df_company['PERÍODO'] = df_company['ANO'].astype(str) + '-' + df_company['MÊS'].astype(str)
    df_company = df_company[['EMPRESA (NOME)', 'PERÍODO', 'NB PASSAGEIROS']]
    df_company.to_csv('anac_data/DATA EMPRESA ANO.csv', index=False)


# Reading the output
download_zip_file()