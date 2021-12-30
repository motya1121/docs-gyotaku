import boto3
import os
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
import requests
import hashlib
import time
import logging

logger = logging.getLogger()
DDBTablename = os.environ['DDBTablename']
db_session = boto3.Session(region_name='ap-northeast-1')
dynamodb = db_session.resource('dynamodb')
table = dynamodb.Table(DDBTablename)


def update_dynammodb(site_data):
    option = {
        'Key': {
            'WebSiteId': site_data['WebSiteId'],
            'SortId': site_data['SortId']
        },
        'UpdateExpression': 'set #timestamp = :timestamp',
        'ExpressionAttributeNames': {
            '#timestamp': 'timestamp'
        },
        'ExpressionAttributeValues': {
            ':timestamp': site_data['timestamp']
        }
    }
    table.update_item(**option)
    logger.info(f'site: {site_data["WebSiteId"]} updated(SortId: {site_data["SortId"]})')


def verify_web_site(site_data):
    result = requests.get(site_data['url'])
    hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

    # TODO: If format is specified
    # soup = BeautifulSoup(esult.text, "html.parser")
    # print(soup.h1)

    if hash_result == site_data['SortId']:
        return False
    else:
        site_data['timestamp'] = int(time.time())
        site_data['SortId'] = hash_result
        update_dynammodb(site_data)
        return site_data


def verify_msdocs_site(site_data):
    pass


def lambda_handler(event, context):

    site_datas = table.scan()
    for site_data in site_datas['Items']:
        logger.info(f'site: {site_data["WebSiteId"]}')

        print(site_data)
        if site_data['type'] == "web":
            _ = verify_web_site(site_data=site_data)
        elif site_data['type'] == "msdocs":
            _ = verify_msdocs_site(site_data=site_data)
