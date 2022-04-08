#%%
# 1. Import data ANAC

geo_matching=pd.read_excel(r'input_data\geo_matching_airports_m.xlsx')

variations = os.listdir(r'C:\Users\ThinkPad\Documents\rescarh\mapping\revised_process_m\revised_process\raw_data\anac_data')
d = []
for x in variations:
    d.append(pd.read_csv(r'raw_data/anac_data/' + x , encoding='latin-1', sep=';'))
data=pd.concat(d) 



#%%
# 2. Import data Google Mobility

geo_matching=pd.read_excel(r'input_data\geo_matching_filt.xlsx')

variations = os.listdir(r'raw_data\google_data')

d = []
for x in variations:
    d.append(pd.read_csv(r'raw_data/google_data/' + x))
    
full_data=pd.concat(d)  