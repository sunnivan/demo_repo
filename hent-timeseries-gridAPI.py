
# author: suno@nve.no

import xarray as xr
import pandas as pd
import geopandas as gp
import numpy as np
from datetime import datetime, timedelta
import requests
import json
from shapely.geometry import shape, mapping
from pyproj import Transformer

## dokumentasjon her: https://api.nve.no/doc/gridtimeseries-data-gts/
## du må endre 'payload' i skripten
## f. eks. temalag 'tm' = Temperatur

# Define the API key for accessing NVE data
nve_api_key = 'jTYL7CsgR067eNS/10i1Xw=='

# Set up the headers for the API request, specifying that we accept JSON responses and including the API key
head = {"Accept": "application/json", "X-API-Key": nve_api_key}

# Define the file path to the shapefiles for the catchments
ifilepath_shp = "C:/MliNVE/create_timeseries/Hydrologi_TotalNedborfeltMalestasjon.shp"

# Read the shapefiles using geopandas to get the spatial data
nf = gp.read_file(ifilepath_shp)

# Initialize an empty DataFrame to store all data pulled from gridAPI
df_all_st = pd.DataFrame()

# Define the file path to the CSV file containing IDs for the subset of catchments we are interested in
# The ID column in this CSV file is numeric and corresponds to 'regine nummer.hoved nummer'
ft = pd.read_csv('C:/MliNVE/stID_flomtabell.csv')


# Loop through each station number in the stations in ft
for st_nr in ft.stID:
    print(st_nr, datetime.now())
    # Filter the shapefile for the current station number
    nf_selected = nf[nf.stID == st_nr]
    nf_selected = nf_selected.reset_index()
    z = nf_selected['geometry']
    z_type = z.geom_type

    if z_type[0] == 'MultiPolygon':
        print('Skipping st nr', st_nr, 'since MultiPolygon')
        continue
    else:
        print('This is a good polygon :)')

    
    geojson_polygon = json.loads(z.to_json())['features'][0]['geometry']

    # Remove the z-axis from all coordinate rings
    def remove_z_axis(coordinates):
        if isinstance(coordinates[0][0], list):
            return [[coord[:2] for coord in ring] for ring in coordinates]
        else:
            return [coord[:2] for coord in coordinates]

    geojson_polygon["coordinates"] = remove_z_axis(geojson_polygon["coordinates"])

    # Transform the coordinates to correct system
    transformer = Transformer.from_crs("epsg:4326", "epsg:25833")

    # Transform coordinates and convert tuple to list
    transformed_coordinates = [
        [list(transformer.transform(lat, lon)) for lon, lat in ring]
        for ring in geojson_polygon["coordinates"]
    ]

    # Lag et nytt GeoJSON-objekt med de transformerede koordinatene
    transformed_geojson_polygon = {
        "type": "Polygon",
        "coordinates": transformed_coordinates,
        "spatialReference": {
            "wkid": 25833
        }
    }
    
    # Lag ei endeleg liste av tranformerte koorindater med riktig projeksjon (UTM33)
    these_tranf_coords = transformed_geojson_polygon["coordinates"]
    
    # Definer forespørselens payload - kva vil du hente ut fra API-en
    payload = {
        "Theme": "tm",
        "startDate": "1990-01-01",
        "endDate": "2023-12-31",
        "Format": "json",
        "Method": "avg",
        "Rings": f"{{'rings': {these_tranf_coords}, 'spatialReference': {{'wkid': 25833}}}}"
    }
    
    # Definer API-endepunkt og tilgangstoken
    url = "https://gts.nve.no/api/AggregationTimeSeries/ByGeoJson"
    headers = {
        "Authorization": "Bearer {nve_api_key}",
        "Content-Type": "application/json"
    }
    
    # Send POST-forespørsel til API
    response = requests.post(url, headers=headers, json=payload)
    
    # Sjekk responsen
    if response.status_code == 200:
        data = response.json()
    else:
        print(f"Feil: {response.status_code}, {response.text}")
        continue
    

    start_date = datetime.strptime(data['StartDate'], '%d.%m.%Y %H:%M')
    end_date = datetime.strptime(data['EndDate'], '%d.%m.%Y %H:%M')
    
    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    dfDT = pd.DataFrame({'date':date_range, 'value':data['Data']})
    dfDT = dfDT.set_index(['date'])
    
    # Add station number as a column
    dfDT['ID'] = st_nr.replace('.','-',1).split('.',2)[0]
    
    # Append the results to the main dataframe
    df_all_st = pd.concat([df_all_st, dfDT])

# Save the combined results to a single pickle file
df_all_st.to_pickle("C:/MliNVE/datapipeline/processed-data/tm.pkl")

