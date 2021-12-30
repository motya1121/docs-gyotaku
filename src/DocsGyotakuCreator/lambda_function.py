import requests
import tempfile
import os
import json
import boto3
from boto3.dynamodb.conditions import Key

S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
DDB_TABLE_NAME = os.environ['DDBTablename']


def get_site_url(WebSiteId):
    db_session = boto3.Session(region_name='ap-northeast-1')
    dynamodb = db_session.resource('dynamodb')
    table = dynamodb.Table(DDB_TABLE_NAME)
    responses = table.query(KeyConditionExpression=Key('WebSiteId').eq(WebSiteId) & Key('SortId').eq(WebSiteId))

    return responses['Items'][0]['url']


def lambda_handler(event, context):
    for Record in event['Records']:
        WebSiteId = Record['dynamodb']['NewImage']['WebSiteId']['S']
        timestamp = Record['dynamodb']['NewImage']['timestamp']['N']
        # get web code
        url = get_site_url(WebSiteId)
        result = requests.get(url)
        print(result.text)

        # create web data
        web_data = {
            "WebSiteId": WebSiteId,
            "hash": Record['dynamodb']['NewImage']['SortId']['S'],
            "timestamp": timestamp,
            "url": url
        }

        # create gzip file
        temp_dir = tempfile.TemporaryDirectory()
        temp_web_file_path = os.path.join(temp_dir.name, f"{timestamp}.html")
        temp_data_file_path = os.path.join(temp_dir.name, 'data.json')
        with open(temp_web_file_path, 'w') as f:
            f.write(result.text)

        with open(temp_data_file_path, 'w') as f:
            json.dump(web_data, f)

        # upload to S3
        s3 = boto3.resource('s3', region_name='ap-northeast-1')
        bucket = s3.Bucket(S3_BUCKET_NAME)
        bucket.upload_file(temp_web_file_path, f'ArchiveData/{WebSiteId}/{timestamp}.html')
        bucket.upload_file(temp_data_file_path, f'ArchiveData/{WebSiteId}/data.json')

        temp_dir.cleanup()

        # result.text
