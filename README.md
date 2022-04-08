# Brazil-Tourism-Monitor
Dashboard to give an advanced read on volume and flights at tourist destinations in Brazil

# Concept: 
Use Anac data on inbound flight volume to tourist destinations (international and domestic) and google mobility data to give an advanced read on volume at key tourist markets in Brazil. 

# Design:
Examine trends across two levels of detail (airports/cities) to first flag which markets are trending strongest given a time-period and secondly allowing for drilling down into individual markets of interest (map views)

# Analysis:
1. Ranking tables based on performance in most recent data points. 
Tables that show top performing markets based on trends across different periods with tables consisting of growth metric (year over year percent change) and also simple line graph

2. Map Views: 
Maps at different levels of detail showing performance at the spatial level, with growth metric (year over year percent change) and interactive tool-tips.

# Description of the Data: 
There are two data sources, google mobility and ANAC data. The google mobility contains daily mobility data, states and city levels, separated by places. The ANAC  data has monthly data on airports, containing the number of passengers and airport information.

# Data Sources: 
[https://www.anac.gov.br/acesso-a-informacao/dados-abertos](https://www.anac.gov.br/acesso-a-informacao/dados-abertos)

[https://www.google.com/covid19/mobility/](https://www.google.com/covid19/mobility/)

[https://covid19.apple.com/mobility](https://covid19.apple.com/mobility)

# Data Transformations: 
- Google Data: First step data is smoothed using a 7, 28, and 84 day moving average and then the percentage change is taken from the equivalent period from the previous year. The next step is to get the maximum values from all the values in a column (max date), then a min-max scaller to deal with plotting on charts the negatives and positive values, by transforming features by scaling each feature to a given range (between 0 and 1).
- Anac Data:  Same steps except for step one, the anac data doesn’t need the rolling mean, since it is monthly and less noisy.
(more details on data transformation folder)

## Section 2 - How to run
- Folder 'Data' contains the raw data except for google mobility which was too large, and the meta data (description).
- Folder 'Transformed data' contains the data ready to create the vizualiations and the geo matching files.
- Folder 'ETL' contains the metadata as well, the code for reading and concatening the data and the code to run the geo matching airports and cities. 
- Folder 'Data transformation' contains the code with the parsers and the metrics for cities and airports, and the detailed description step by step. 

# Link to dashboard
https://observablehq.com/d/e3e69813254721e0 




