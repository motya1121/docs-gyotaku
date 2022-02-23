import boto3
import os
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
import requests
import hashlib
import time
import logging
import feedparser
from datetime import datetime as dt
import json

logger = logging.getLogger()
DDB_TABLE_NAME = os.environ['DDBTablename']
db_session = boto3.Session(region_name='ap-northeast-1')
dynamodb = db_session.resource('dynamodb')
table = dynamodb.Table(DDB_TABLE_NAME)


def update_dynammodb(target_site, url, timestamp, hash_result):
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
            ':url': url
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
        update_dynammodb(target_site, target_site['url'], int(time.time()), hash_result)
        return


def verify_msdocs_site(target_site):
    pass


def verify_github_site(target_site):
    # https://qiita.com/nannany_hey/items/23f847e0a331da52ed77
    # https://api.github.com/repos/motya1121/web-update-test/commits

    # get latest timestamp
    query_kwargs = {
        'IndexName': 'SiteData',
        'Limit': 1,
        'KeyConditionExpression': Key('PartitionKey').eq(target_site["PartitionKey"]),
        'ScanIndexForward': False
    }
    last_modifed_timestamp = int(table.query(**query_kwargs)['Items'][0]['timestamp'])
    last_modifed_dt = dt.utcfromtimestamp(last_modifed_timestamp)

    url = "https://api.github.com/repos/{0[owner]}/{0[repo]}/commits?path={0[path]}".format(target_site['property'])
    result = requests.get(url=url)
    commits_data = json.loads(result.text)

    for commit_d in commits_data:
        commit_dt = dt.strptime(commit_d['commit']['committer']['date'], '%Y-%m-%dT%H:%M:%SZ')
        if commit_dt <= last_modifed_dt:
            continue

        # get hash
        result = requests.get(commit_d['url'])
        hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

        update_dynammodb(target_site, commit_d['html_url'], int(time.time()), hash_result)

    pass


def verify_rss_site(target_site):
    # get latest timestamp
    query_kwargs = {
        'IndexName': 'SiteData',
        'Limit': 1,
        'KeyConditionExpression': Key('PartitionKey').eq(target_site["PartitionKey"]),
        'ScanIndexForward': False
    }
    last_modifed_timestamp = int(table.query(**query_kwargs)['Items'][0]['timestamp'])
    last_modifed_dt = dt.utcfromtimestamp(last_modifed_timestamp)

    # get hash
    result = requests.get(target_site['url'])
    hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

    # parse rss
    d = feedparser.parse(target_site['url'])
    for entry in d.entries:
        published_dt = dt(*entry['published_parsed'][:6])
        if last_modifed_dt < published_dt:
            update_dynammodb(target_site, entry['link'], int(time.time()), hash_result)


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
        elif target_site['type'] == "rss":
            _ = verify_rss_site(target_site=target_site)
