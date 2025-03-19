#
# author: daba@nve.no
# 
# This script assumes you have a folder of streamflow data
# downloaded from Hydra II, where each .txt file belongs to 
# a single catchment (standard lescon_var output).
#
# For the lescon_var commands used to pull the streamflow data,
# see ~/datapipeline/raw-data/
#
# The script loads in the streamflow data, replaces '-9999' with 
# 'NA' (as required by neural hydrology) and concatenates the 
# streamflow data across stations into a single DataFrame. Then
# streamflow is converted from m3/s to mm/day using catchment area.
#
# The DataFrame is saved as a pickle file and used as input to 
# the script 'create-nc-files.py'
# ----------------------------------------------------------------------

import os
import pandas as pd
import numpy as np

# load in catchment area to convert m3/s to mm/day:
egenskaper = pd.read_csv("C:/MliNVE/datapipeline/raw-data/gamfelt_catchment_covariates.csv")

# List the files in the directory you downloaded the Hydag data files into
alltxtfiles = [f for f in os.listdir("C:/MliNVE/datapipeline/raw-data/hydag/") if f.endswith('.txt')]

# Remove command file, if it exists:
fileloop = [f for f in alltxtfiles if f != "lesconvar_commands_gfft_hydag.txt"]

# Initialize data frame to store data
streamflow = pd.DataFrame(columns=['date', 'cumecs', 'ID'])

for fl in fileloop:
    
    station_data = pd.read_csv(f"C:/MliNVE/datapipeline/raw-data/hydag/{fl}", sep=' ', engine='python', names=["date", "cumecs"])
    
    # Split the 'date' column into 'date' and 'hm' columns
    station_data[['date', 'hm']] = station_data['date'].str.split('/', expand=True)

    # Convert the 'date' column to datetime format
    station_data['date'] = pd.to_datetime(station_data['date'], format='%Y%m%d')
    
    # Only want data from 1990 onwards
    station_data = station_data[station_data['date'] > '1989-12-31']
        
    # Deal with missing streamflow values.
    # LSTM wants NA, not -9999
    station_data['cumecs'] = station_data['cumecs'].apply(lambda x: np.nan if x < 0 else x)
    
    # make station ID (RN-HN)
    station_data['ID'] = fl[:-4]
    streamflow = pd.concat([streamflow, station_data], ignore_index=True)

# convert m3/s to mm/day:

# merge streamflow and egenskaper(area) on key column ID:
q = pd.merge(streamflow, egenskaper[['ID','A']], on = 'ID', how='inner')

# define column mm_day:
sec_in_day = 86400 # number of seconds in a day
q['mm_day'] = (q['cumecs'] * sec_in_day) / (q['A'] * 1000000) * 1000

# we want only these columns:
q = q[['ID','date','mm_day']]

q = q.set_index(['date'])

q.to_pickle('C:/MliNVE/datapipeline/processed-data/q.pkl')