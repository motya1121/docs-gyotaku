import requests
import tempfile
import os
import json
import boto3
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()

S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
DDB_TABLE_NAME = os.environ['DDBTablename']


def get_site_url(WebSiteId):
    db_session = boto3.Session(region_name='ap-northeast-1')
    dynamodb = db_session.resource('dynamodb')
    table = dynamodb.Table(DDB_TABLE_NAME)
    responses = table.query(KeyConditionExpression=Key('WebSiteId').eq(WebSiteId) & Key('SortId').eq(WebSiteId))

    return responses['Items'][0]['url']


def lambda_handler(event, context):
    print(json.dumps(event))
    for Record in event['Records']:
        # Exclusion add and update website
        if Record['dynamodb']['Keys']['WebSiteId']['S'] == Record['dynamodb']['Keys']['SortId']['S']:
            continue
        if 'NewImage' not in Record['dynamodb'].keys():
            continue

        WebSiteId = Record['dynamodb']['NewImage']['WebSiteId']['S']
        timestamp = Record['dynamodb']['NewImage']['timestamp']['N']
        site_hash = Record['dynamodb']['NewImage']['SortId']['S']

        # get web site data
        url = get_site_url(WebSiteId)
        result = requests.get(url)

        # create web site infomation
        web_data = {"WebSiteId": WebSiteId, "hash": site_hash, "timestamp": timestamp, "url": url}

        # export site data to file
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
        bucket.upload_file(temp_web_file_path, f'ArchiveData/{WebSiteId}/{timestamp}/{timestamp}.html')
        bucket.upload_file(temp_data_file_path, f'ArchiveData/{WebSiteId}/{timestamp}/data.json')

        temp_dir.cleanup()

        logger.info(f'site: {WebSiteId},  new hash: {site_hash}, timestamp: {timestamp}')
