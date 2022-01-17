import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime as dt, timedelta
import requests

logger = logging.getLogger()

DDB_TABLE_NAME = os.environ['DDBTablename']
SLACK_TOKEN = os.environ['SLACK_TOKEN']
AWS_REAGION = os.getenv("AWS_DEFAULT_REGION")


def get_target_site(siteId):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    query_kwargs = {'KeyConditionExpression': Key('PartitionKey').eq(siteId) & Key('SortKey').eq(siteId)}
    return table.query(**query_kwargs)['Items'][0]


def lambda_handler(event, context):
    for record in event['Records']:
        if record['eventName'] != 'INSERT':
            continue
        site_data = record['dynamodb']['NewImage']
        if site_data['PartitionKey']['S'].find('user') != -1:
            # add user
            continue
        elif site_data['PartitionKey']['S'] == site_data['SortKey']['S']:
            # add site
            continue

        siteId = site_data['PartitionKey']['S']
        target_site = get_target_site(siteId=siteId)
        target_siot_url = site_data['url']['S']
        if 'title' in site_data.keys():
            title = site_data['title']['S']
        else:
            title = target_site['title']
        updated_dt_jst = dt.fromtimestamp(int(site_data['timestamp']['N'])) + timedelta(hours=9)
        updated_dt_jst = updated_dt_jst.strftime('%Y-%m-%d %H:%M:%S')

        # slack notification
        # https://api.slack.com/docs/messages/builder
        url = f"https://hooks.slack.com/services/{SLACK_TOKEN}"
        data = {
            "blocks": [{
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"- title: {title}\n- 更新日時: {updated_dt_jst}\n- {target_siot_url}"
                }
            }]
        }
        headers = {"Content-type": "application/json"}
        requests.post(url=url, headers=headers, data=json.dumps(data))
