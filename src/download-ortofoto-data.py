import os

import mlrun
import json
import boto3
import mercantile
import osmnx as ox
import urllib.request
import os
import pandas as pd

def doc_files(s3, bucket: str, prefix: str, name: str):
    array = []
    res = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
    array = array + res['Contents']
    while 'NextMarker' in res:
        res = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=1000, Marker=res['NextMarker'])
        array = array + res["Contents"]
    df = pd.DataFrame.from_records(array, index='Key')
    df.drop(columns=['ETag', 'StorageClass', 'Owner'], inplace=True)
    df.to_parquet(name +'.parquet')
    s3.upload_file(name +'.parquet', bucket, prefix + '/' + name +'.parquet', ExtraArgs={'ContentType': 'application/octet-stream'})
        
@mlrun.handler()
def get_ortofoto(context, query: str, server_base_url: str):
    gdf = ox.geocode_to_gdf(query)
    # # Extract the bounding box
    bbox = gdf['geometry'].bounds.iloc[0]

    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

    for i in range(22):
        tiles = mercantile.tiles(bbox.minx, bbox.miny, bbox.maxx, bbox.maxy, i)
        if not os.path.exists(f"data/tiles/{i}"):
            os.makedirs(f"data/tiles/{i}")
        for t in tiles:            
            if not os.path.exists(f"data/tiles/{t.z}/{t.x}"):
                os.makedirs(f"data/tiles/{t.z}/{t.x}")
            local_path = f"data/tiles/{t.z}/{t.x}/{t.y}.png"
            url = f"{server_base_url}/{t.z}/{t.x}/{t.y}.png"
            
            try:
                urllib.request.urlretrieve(url, local_path)        
            except:
                try:
                    urllib.request.urlretrieve(url, local_path)        
                except:
                    print(url)
                    continue
            # name = path + '/' + item_name
            s3.upload_file(local_path, "dataspace", f"city-data/ortofoto_tiles/latest/{t.z}/{t.x}/{t.y}.png", ExtraArgs={'ContentType': 'image/png'})
            os.remove(local_path)
    
    doc_files(s3, "dataspace", "city-data/ortofoto_tiles/latest", "ortofoto_tiles")
    