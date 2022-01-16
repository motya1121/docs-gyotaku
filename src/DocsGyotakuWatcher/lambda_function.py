import boto3
import os
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
import requests
import hashlib
import time
import logging

logger = logging.getLogger()
DDB_TABLE_NAME = os.environ['DDBTablename']
db_session = boto3.Session(region_name='ap-northeast-1')
dynamodb = db_session.resource('dynamodb')
table = dynamodb.Table(DDB_TABLE_NAME)


def update_dynammodb(target_site, timestamp, hash_result):
    option = {
        'Key': {
            'PartitionKey': target_site['PartitionKey'],
            'SortKey': hash_result
        },
        'UpdateExpression': 'set #timestamp = :timestamp, #url = :url',
        'ExpressionAttributeNames': {
            '#timestamp': 'timestamp',
            '#url': 'url'
        },
        'ExpressionAttributeValues': {
            ':timestamp': timestamp,
            ':url': target_site['url']
        }
    }
    table.update_item(**option)
    logger.info(f'site: {target_site["PartitionKey"]} updated(SortKey: {hash_result})')


def verify_web_site(target_site):
    result = requests.get(target_site['url'])
    hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

    # TODO: If format is specified
    # soup = BeautifulSoup(esult.text, "html.parser")
    # print(soup.h1)

    if hash_result == target_site['SortKey']:
        return False
    else:
        update_dynammodb(target_site, int(time.time()), hash_result)
        return


def verify_msdocs_site(target_site):
    pass


def get_target_site_list():
    scan_kwargs = {
        'FilterExpression': Key('is_watch').eq(True),
    }
    target_sites = table.scan(**scan_kwargs)

    return target_sites['Items']


def lambda_handler(event, context):

    for target_site in get_target_site_list():
        logger.info(f'site: {target_site["PartitionKey"]}')

        if target_site['type'] == "web":
            _ = verify_web_site(target_site=target_site)
        elif target_site['type'] == "msdocs":
            _ = verify_msdocs_site(target_site=target_site)
