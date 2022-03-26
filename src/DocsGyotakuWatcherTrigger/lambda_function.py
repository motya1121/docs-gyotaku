import boto3
import os
from boto3.dynamodb.conditions import Key
import logging
import json
from decimal import Decimal
from datetime import date, datetime as dt, timedelta

logger = logging.getLogger()
DDB_TABLE_NAME = os.environ['DDBTablename']
SQSURL = os.environ['SQSUrl']
db_session = boto3.Session(region_name='ap-northeast-1')
dynamodb = db_session.resource('dynamodb')
table = dynamodb.Table(DDB_TABLE_NAME)
sqs_client = boto3.client("sqs")


def json_serial(obj):
    if isinstance(obj, (dt, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return int(obj)

    raise TypeError("Type %s not serializable" % type(obj))


def get_latest_data(target_site):
    # get latest data
    query_kwargs = {
        'IndexName': 'SiteData',
        'Limit': 2,
        'KeyConditionExpression': Key('PartitionKey').eq(target_site["PartitionKey"]),
        'ScanIndexForward': False
    }
    responses = table.query(**query_kwargs)['Items']
    latest_sortKey = ""
    latest_timestamp = dt.timestamp(dt.utcnow() - timedelta(days=1))
    for response in responses:
        if response["PartitionKey"] != response["SortKey"]:
            latest_sortKey = response["SortKey"]
            latest_timestamp = response["timestamp"]

    return latest_sortKey, latest_timestamp


def get_target_site_list():
    scan_kwargs = {
        'FilterExpression': Key('is_watch').eq(True),
    }
    target_sites = table.scan(**scan_kwargs)['Items']
    ret_target_sites = []
    for target_site in target_sites:
        # If the set duration_sec has not elapsed, skip
        if 'property' in target_site.keys() and 'duration_sec' in target_site['property'].keys():
            befor_watch_timestamp = target_site['timestamp']
            next_watch_timestamp = int(befor_watch_timestamp) + int(target_site['property']['duration_sec'])
            now_timestamp = dt.timestamp(dt.utcnow())
            if now_timestamp < next_watch_timestamp:
                continue

        latest_sortKey, latest_timestamp = get_latest_data(target_site)
        target_site["latest_data"] = {"SortKey": latest_sortKey}
        target_site["timestamp"] = latest_timestamp
        ret_target_sites.append(target_site)

    return ret_target_sites


def lambda_handler(event, context):

    for target_site in get_target_site_list():
        _ = sqs_client.send_message(QueueUrl=SQSURL, MessageBody=json.dumps(target_site, default=json_serial))
        logger.info(f'site: {target_site["PartitionKey"]}')
