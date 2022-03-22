import requests
import tempfile
import os
import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime as dt
from decimal import Decimal
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
DDB_TABLE_NAME = os.environ['DDBTablename']


def json_serial(obj):
    if isinstance(obj, (dt)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return int(obj)

    raise TypeError("Type %s not serializable" % type(obj))


def lambda_handler(event, context):
    for Record in event['Records']:
        log_info = {
            "PartitionKey": Record['dynamodb']['Keys']['PartitionKey']['S'],
            "SortKey": Record['dynamodb']['Keys']['SortKey']['S'],
            "eventName": Record['eventName']
        }
        logger.info(json.dumps(log_info, default=json_serial))

        # Exclusion add and update website
        if Record['dynamodb']['Keys']['PartitionKey']['S'] == Record['dynamodb']['Keys']['SortKey']['S']:
            continue
        if 'NewImage' not in Record['dynamodb'].keys():
            continue

        WebSiteId = Record['dynamodb']['NewImage']['PartitionKey']['S']
        timestamp = Record['dynamodb']['NewImage']['timestamp']['N']
        site_hash = Record['dynamodb']['NewImage']['SortKey']['S']
        url = Record['dynamodb']['NewImage']['url']['S']

        # get web site data
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

        log_info['siteId'] = WebSiteId
        log_info['siteHash'] = site_hash
        log_info['new_timestamp_dt'] = dt.fromtimestamp(timestamp)
        logger.info(json.dumps(log_info, default=json_serial))
