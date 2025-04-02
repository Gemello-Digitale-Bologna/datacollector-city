
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import json
import boto3

import pandas as pd

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def getGService(project, token_uri):
    creds = None
    token = token_uri.read_secret_value()
    try:
        token_info = json.loads(token)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)
        service = build("drive", "v3", credentials=creds)
    except HttpError as error:
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

def copy_rec(service, folder_id, s3, bucket: str, destination_path: str, skip_folders=None):
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
                if skip_folders and item_name in skip_folders:
                    print(f"path excluded: {item_name}")
                else:
                    print(f"Downloading folder {item_name}...")
                    copy_rec(service, item_id, s3, bucket, destination_path + '/' + item_name, skip_folders)
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

def copy_files(project, query: str, s3, bucket: str, destination_path: str, token_uri, skip_folders=None):
    service = getGService(project, token_uri)
    results = (service.files()
               .list(q=query, pageSize=1, fields="files(id, name, mimeType)", supportsAllDrives=True, includeItemsFromAllDrives=True)
               .execute())
    print(results.get("files"))
    root_items = results.get("files", [])
    for item in root_items:
        copy_rec(service, item["id"], s3, bucket, destination_path, skip_folders)

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
        
def get_lidar(project, folder, bucket, target: str = 'data'):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    token_uri = project.get_secret('token')
    copy_files(project, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, bucket, f"city-data/lidar/{target}", token_uri)
    doc_files(s3, bucket, f"city-data/lidar/{target}", "lidar")
        
def get_dtm(project, folder, bucket, target: str = 'data'):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    token_uri = project.get_secret('token')
    copy_files(project, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, bucket, f"city-data/dtm/{target}", token_uri)
    doc_files(s3, bucket, f"city-data/dtm/{target}", "dtm")
    
def get_dsm(project, folder, bucket, target: str = 'data'):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    token_uri = project.get_secret('token')
    copy_files(project, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, bucket, f"city-data/dsm/{target}", token_uri)
    doc_files(s3, bucket, f"city-data/dsm/{target}", "dsm")

def get_orto(project, folder, bucket, target: str = 'data', type: str = 'ortofoto'):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    token_uri = project.get_secret('token')
    copy_files(project, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, bucket, f"city-data/ortofoto/{target}", token_uri)
    doc_files(s3, bucket, f"city-data/ortofoto/{target}", type)

def get_foto(project, folder, bucket, target: str = 'data'):
    s3 = boto3.client('s3',
                endpoint_url=os.environ.get('S3_ENDPOINT_URL'),
                aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
    token_uri = project.get_secret('token')
    copy_files(project, f"mimeType='application/vnd.google-apps.folder' and name='{folder}'", s3, bucket, f"city-data/foto_oblique/{target}", token_uri, skip_folders=['back', 'for', 'left', 'right'])
    doc_files(s3, bucket, f"city-data/foto_oblique/{target}", type)
