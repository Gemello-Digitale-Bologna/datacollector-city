
import pandas as pd
import requests
import json as js
import geopandas as gpd
import os
import boto3
import datetime

base_url = "https://opendata.comune.bologna.it/api/explore/v2.1/catalog/datasets/{name}/exports/geojson"

def download_share_geo(project, bucket, name, artifact_name):
    json = requests.get(base_url.format(name = name)).json()  
    if not os.path.exists('./data'):
        os.makedirs('./data')

    path_geojson = './data/'+name+'.geojson' 
    with open(path_geojson, 'w') as out_file:
            out_file.write(js.dumps(json, indent=4))
    gdf = gpd.read_file(path_geojson)
    # share_files(project, bucket, path_geojson, artifact_name)
    
    path_parquet = './data/'+name+'.parquet'
    gdf.to_parquet(path_parquet)
    share_files(project, bucket, path_parquet, artifact_name)  

def download_buildings(project, bucket):
    data = download_share_geo(project, bucket, "rifter_edif_pl", "rifter_buildings_censiment")

def download_addresses(project, bucket):
    data = download_share_geo(project, bucket, "rifter_civici_pt", "rifter_addresses")

def download_walls(project, bucket):
    data = download_share_geo(project, bucket, "carta-tecnica-comunale-recinzioni", "ctm_walls")

def download_buidlings_volumes(project, bucket):
    data = download_share_geo(project, bucket, "c_a944ctc_edifici_pl", "ctm_buildings_volumes")

def download_buildings_symbols(project, bucket):
    data = download_share_geo(project, bucket, "carta-tecnica-comunale-simboli-edifici-volumetrici", "ctm_buildings_symbols")

def download_terrain(project, bucket):
    data = download_share_geo(project, bucket, "carta-tecnica-comunale-divisioni-del-terreno", "ctm_terrain")
    
def download_areas(project, bucket):
    data = download_share_geo(project, bucket, "zone_urbanistiche", "proximity_areas")

def share_files(project, bucket: str = "dataspace", path: str = "city", artifactName: str = 'artifact'):
    """
    Uploads specified data items to a shared S3 bucket and folder.
    Requires the environment variables for S3 endpoint and credentials (S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
    args:
        bucket: The name of the bucket
        path: The path within the bucket
    """

    s3 = boto3.client('s3',
                    endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    
    path_date = datetime.datetime.now().strftime("%Y-%m-%d")
    path_latest = 'latest'
    
    fname = path.split("/")[-1]
    name = fname.split(".")[0]

    print(bucket)
    s3.upload_file("./data/" +fname, bucket, '/' + artifactName + '/' + path_latest + '/' + fname, ExtraArgs={'ContentType': 'application/octet-stream'})
    s3.upload_file("./data/" +fname, bucket, '/' + artifactName + '/' + path_date + '/' + fname, ExtraArgs={'ContentType': 'application/octet-stream'})
