import mlrun
import boto3
import os
import datetime

@mlrun.handler()
def share_files(context, bucket: str = "dataspace", path: str = "city"):
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

    items = {
        "s3://datalake/projects/city-data/artifacts/city-data-pipeline/download-buildings/0/rifter_buildings_censiment.parquet": None,
        "s3://datalake/projects/city-data/artifacts/city-data-pipeline/download-addresses/0/rifter_addresses.parquet": None,
        "s3://datalake/projects/city-data/artifacts/city-data-pipeline/download-walls/0/ctm_walls.parquet": None,
        "s3://datalake/projects/city-data/artifacts/city-data-pipeline/download-buidlings-volumes/0/ctm_buildings_volumes.parquet": None,
        "s3://datalake/projects/city-data/artifacts/city-data-pipeline/download-buildings-symbols/0/ctm_buildings_symbols.parquet": None,
        "s3://datalake/projects/city-data/artifacts/city-data-pipeline/download-terrain/0/ctm_terrain.parquet": None,
        "s3://datalake/projects/city-data/artifacts/download-proximity-areas-download-areas/0/proximity_areas.parquet": None
    }
    
    base_folder = './data'
        
    if not os.path.exists(base_folder):
        os.makedirs(base_folder)

    for p, ct in items.items():
        fname = p.split("/")[-1]
        di = mlrun.get_dataitem(p)
        di.download("./data/" +fname)
        name = fname.split(".")[0]
        s3.upload_file("./data/" +fname, bucket, path + '/' + name + '/' + path_latest + '/' + fname, ExtraArgs={'ContentType': ct if ct else 'application/octet-stream'})
        s3.upload_file("./data/" +fname, bucket, path + '/' + name + '/' + path_date + '/' + fname, ExtraArgs={'ContentType': ct if ct else 'application/octet-stream'})