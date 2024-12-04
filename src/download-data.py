import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import mlrun
import json
import boto3

import pandas as pd

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def getGService(context):
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    
    token_info_var = context.get_secret('GOOGLE_TOKEN')

    if token_info_var is not None:
        token_info = json.loads(token_info_var)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
    else:
        raise Exception("No valid credentials")

    try:
        service = build("drive", "v3", credentials=creds)

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f"An error occurred: {error}")

    return service

def upload_file(s3, bucket: str, path: str, local_path: str, item_name: str):
    """
    Uploads specified data items to a shared S3 bucket and folder.
    Requires the environment variables for S3 endpoint and credentials (S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY).
    args:
        bucket: The name of the bucket
        path: The path within the bucket
    """

    name = path + '/' + item_name
    s3.upload_file(local_path, bucket, name, ExtraArgs={'ContentType': 'application/octet-stream'})

def copy_rec(service, folder_id, s3, bucket: str, destination_path: str):
    """Downloads recursively all content from a specific folder on Google Drive."""
    files = service.files()
    request = files.list(q=f"'{folder_id}' in parents", 
                         supportsAllDrives=True, includeItemsFromAllDrives=True, 
                         fields="nextPageToken, files(id, name, mimeType)")
    while request is not None:
        results = request.execute()
        
        items = results.get("files", [])
        for item in items:
            item_name = item["name"]
            item_id = item["id"]
            item_type = item["mimeType"]

            # If it's a folder, recursively download its content
            if item_type == "application/vnd.google-apps.folder":
                print(f"Downloading folder {item_name}...")
                if item_name != 'back':
                    copy_rec(service, item_id, s3, bucket, destination_path + '/' + item_name)
            else:
                try:
                    head = s3.head_object(Bucket=bucket, Key=destination_path + '/' + item_name)
                except:
                    print(f"Downloading file {item_name}...")
                    # Download the file into the specified local folder
                    r = service.files().get_media(fileId=item_id)
                    local_path = 'myfile'
                    with open(local_path, "wb") as fh:
                        downloader = MediaIoBaseDownload(fh, r)
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                    upload_file(s3, bucket, destination_path, local_path, item_name)
                    # return
        request = files.list_next(request, results)

def copy_files(context, query: str, s3, bucket: str, destination_path: str):
    service = getGService(context)
    results = (service.files()
               .list(q=query, pageSize=1, fields="files(id, name, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True)
               .execute())
    
    root_items = results.get("files", [])
    for item in root_items:
        copy_rec(service, item["id"], s3, bucket, destination_path)

def doc_files(s3, bucket: str, prefix: str, name: str):
    array = []
    res = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
    array = array + res['Contents']
    while 'NextMarker' in res:
        res = s3.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=1000, Marker=res['NextMarker'])
        array = array + res.Contents
    df = pd.DataFrame.from_records(array)
    df.drop(columns=['ETag', 'StorageClass', 'Owner'], inplace=True)
    df.to_parquet(name +'.parquet')
    s3.upload_file(name +'.parquet', bucket, prefix + '/' + name +'.parquet', ExtraArgs={'ContentType': 'application/octet-stream'})
        
@mlrun.handler()
def get_lidar(context, folder, target):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

    copy_files(context, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, "dataspace", f"city-data/lidar/{target}")
    doc_files(s3, "dataspace", f"city-data/lidar/{target}", "lidar")
    
    
@mlrun.handler()
def get_dtm(context, folder, target):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

    copy_files(context, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, "dataspace", f"city-data/dtm/{target}")
    doc_files(s3, "dataspace", f"city-data/dtm/{target}", "dtm")
    
@mlrun.handler()
def get_dsm(context, folder, target):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

    copy_files(context, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, "dataspace", f"city-data/dsm/{target}")
    doc_files(s3, "dataspace", f"city-data/dsm/{target}", "dsm")
    
@mlrun.handler()
def get_ortofoto_tiff(context):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))

    service = getGService(context)
    
    results = (service.files()
           .list(q=f"mimeType='application/vnd.google-apps.folder' and name='RETTIFICHE_TIFF_RGBN_ETRF2000'", 
                 pageSize=1, fields="files(id, name, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True)
           .execute())

    root_items = results.get("files", [])
    for item in root_items:
        print(item["name"])
        if item["name"] == 'RETTIFICHE_TIFF_RGBN_ETRF2000':
            id = item["id"]
            results = (service.files().list(q=f"'{id}' in parents", pageSize=1, fields="files(id, name, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute())
            items = results.get("files", [])
            for child_item in items:
                print(child_item["name"])
                if child_item["name"] == "RDN2008_GEOTIFF":
                    copy_rec(service, child_item["id"], s3, "dataspace", "city-data/ortofoto/latest")
