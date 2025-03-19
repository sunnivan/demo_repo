#
# author: daba@nve.no
#
# creates the netCDF files from timeseries
# of meteorological forcings (ex: precipitation, temperature)
# and observed streamflow
#
# input: 
#   - daily mean temperature for every catchment
#   - daily mean precipitation for every catchment
#   - daily mean streamflow for every catchment
#
# output: 
#   - one netCDF file per catchment
#   The file contains daily streamflow data (q in mm/day), 
#   precip (prec in mm/day), mean temperature (temp, Celsius), and a 
#   timestamp (num. of days after 1900-01-01).
#
#   note that there can be missing *values* (NA) for any of 
#   the streamflow or forcing data but there cannot be missing *dates*
# 
#   The file name is equal to the name of the catchment.
#   1-dimensional netCDF (time, days since 1900-01-01) 
# ----------------------------------------------------------------------

import pickle
import pandas as pd
import numpy as np
import xarray as xr
from netCDF4 import Dataset
import datetime 

## load in the data

# precip and temp timeseries:
with open("C:/MliNVE/datapipeline/processed-data/tm.pkl", 'rb') as file:
    tm = pickle.load(file)
tm = tm.rename(columns = {'value':'temp','Station':'ID'})

with open("C:/MliNVE/datapipeline/processed-data/rr.pkl", 'rb') as file:
    rr = pickle.load(file)
rr = rr.rename(columns = {'value':'precip'})

# streamflow timeseries:
with open("C:/MliNVE/datapipeline/processed-data/Q.pkl", 'rb') as file:
    qq = pickle.load(file)

# from rr and tm, select only the stations that are defined in qq.
# (normally we would just do this in the merge step, but we're
# implicitly relying on the continuous sequence of dates provided by
# gridAPI. The streamflow data from hydra II does not have a continuous
# sequence of dates, i.e. there's a lot of holes. Neural hydrology needs
# a continuous sequence in the netCDF files)
nstations = qq.groupby('ID').nunique()

rr = rr[rr['ID'].isin(nstations.index)]
tm = tm[tm['ID'].isin(nstations.index)]

## now we merge the dataframes together on key columns 'date' and 'ID':
dat = pd.merge(qq, tm, on = ['date','ID'], how='right')
dat = pd.merge(dat, rr, on = ['date','ID'])
## we lose one station...234-18. This one is not in the data we pull
## from girdAPI. Need to look into why. Maybe we timed out the API

## now, iterate by station:
nstations = dat.groupby('ID').nunique()

for i in nstations.index:

    sdat = dat[dat['ID'] == i]

    RN,HN = sdat['ID'].unique()[0].split('-')

    # Change into date format NH needs (no. of days since 1990)
    sdat['ncdate'] = (sdat.index - pd.to_datetime('1900-01-01')).days
    sdat = sdat.sort_values(by=['ncdate'])

    # First, create the netCDF file -------------------------------------------
  
    # path and file name, set dname
    sname = f"{RN}X{HN}X0"
    ncfname = f"C:/MliNVE/datapipeline/processed-data/time_series/{sname}.nc"

    # Then, define time index of the file ---------------------------------------

    nt3 = len(sdat['ncdate'])
    time3 = sdat['ncdate'].values
    tunits3 = "days since 1900-01-01"

    # Create and write the netCDF file
    with Dataset(ncfname, 'w', format='NETCDF4') as ncout:
        # Define dimensions
        ncout.createDimension('date', nt3)
        
        # Define variables
        dates = ncout.createVariable('date', 'i4', ('date',))
        dates.units = tunits3
        dates.calendar = 'standard'
        
        prec = ncout.createVariable('prec', 'f4', ('date',))
        prec.long_name = "Precipitation"
        prec.units = "mm_day"

        temp = ncout.createVariable('temp', 'f4', ('date',))
        temp.long_name = "Temperature"
        temp.units = "deg_C"
        
        streamflow = ncout.createVariable('q', 'f4', ('date',))
        streamflow.long_name = "Streamflow"
        streamflow.units = "mm_day"
        
        # Put variables
        dates[:] = time3
        prec[:] = sdat['precip'].values
        temp[:] = sdat['temp'].values
        streamflow[:] = sdat['mm_day'].values
        
        # Print summary of the created file
        print(ncout)










