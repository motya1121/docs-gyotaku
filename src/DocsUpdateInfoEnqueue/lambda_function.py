import os
import json
import boto3
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()

DocsUpdateNotifyQueueURL = os.environ['DocsUpdateNotifyQueueURL']
DDB_TABLE_NAME = os.environ['DDBTablename']


def get_site_url(WebSiteId):
    db_session = boto3.Session(region_name='ap-northeast-1')
    dynamodb = db_session.resource('dynamodb')
    table = dynamodb.Table(DDB_TABLE_NAME)
    responses = table.query(KeyConditionExpression=Key('WebSiteId').eq(WebSiteId) & Key('SortId').eq(WebSiteId))

    return responses['Items'][0]['url']


def lambda_handler(event, context):
    for Record in event['Records']:
        # Exclusion add and update website
        if Record['dynamodb']['Keys']['WebSiteId']['S'] == Record['dynamodb']['Keys']['SortId']['S']:
            continue
        if 'NewImage' not in Record['dynamodb'].keys():
            continue

        WebSiteId = Record['dynamodb']['NewImage']['WebSiteId']['S']
        timestamp = Record['dynamodb']['NewImage']['timestamp']['N']
        site_hash = Record['dynamodb']['NewImage']['SortId']['S']
        url = get_site_url(WebSiteId)

        # create web site infomation
        web_data = {"WebSiteId": WebSiteId, "hash": site_hash, "timestamp": timestamp, "url": url}

        sqs_client = boto3.client("sqs")
        _ = sqs_client.send_message(QueueUrl=DocsUpdateNotifyQueueURL, MessageBody=json.dumps(web_data))
