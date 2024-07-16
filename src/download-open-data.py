import mlrun
import pandas as pd
import requests
import json as js
import geopandas as gpd
import os

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/geojson"

def download_geo(context, name, artifact_name):
    json = requests.get(base_url.format(name = name)).json()  
    if not os.path.exists('./data'):
        os.makedirs('./data')

    with open('./data/'+name+'.geojson', 'w') as out_file:
            out_file.write(js.dumps(json, indent=4))
    gdf = gpd.read_file('./data/'+name+'.geojson')
    
    gdf.to_parquet('./data/'+name+'.parquet')
    with open('./data/'+name+'.parquet', 'rb') as in_file:
        content = in_file.read()
        context.log_artifact(artifact_name, body=content, format="parquet", db_key=artifact_name)
    return json

@mlrun.handler()
def download_buildings(context):
    data = download_geo(context, "rifter_edif_pl", "rifter_buildings_censiment")
    # return data

@mlrun.handler()
def download_addresses(context):
    data = download_geo(context, "rifter_civici_pt", "rifter_addresses")
    # return data

@mlrun.handler()
def download_walls(context):
    data = download_geo(context, "carta-tecnica-comunale-recinzioni", "ctm_walls")
    # return data

@mlrun.handler()
def download_buidlings_volumes(context):
    data = download_geo(context, "c_a944ctc_edifici_pl", "ctm_buildings_volumes")
    # return data

@mlrun.handler()
def download_buildings_symbols(context):
    data = download_geo(context, "carta-tecnica-comunale-simboli-edifici-volumetrici", "ctm_buildings_symbols")
    # return data

@mlrun.handler()
def download_terrain(context):
    data = download_geo(context, "carta-tecnica-comunale-divisioni-del-terreno", "ctm_terrain")
    # return data
    
@mlrun.handler()
def download_areas(context):
    data = download_geo(context, "zone_urbanistiche", "proximity_areas")
    # return data

