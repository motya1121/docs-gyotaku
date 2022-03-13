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


def update_dynammodb(SiteId, hash_result, url, timestamp):
    option = {
        'Key': {
            'PartitionKey': SiteId,
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
    logger.info(f'site: {SiteId} updated(SortKey: {hash_result})')


def update_latest_timestamp(SiteId, timestamp):
    option = {
        'Key': {
            'PartitionKey': SiteId,
            'SortKey': SiteId
        },
        'UpdateExpression': 'set #timestamp = :timestamp',
        'ExpressionAttributeNames': {
            '#timestamp': 'timestamp',
        },
        'ExpressionAttributeValues': {
            ':timestamp': timestamp,
        }
    }
    table.update_item(**option)
    logger.info(f'site: {SiteId} updated(latest timestamp)')


def verify_web_site(target_site):
    timestamp = int(time.time())
    result = requests.get(target_site['url'])
    hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

    print(f"latest hash {target_site['latest_data']['SortKey']}")
    if hash_result == target_site['latest_data']['SortKey']:
        pass
    else:
        update_dynammodb(SiteId=target_site["PartitionKey"],
                         hash_result=hash_result,
                         url=target_site['url'],
                         timestamp=timestamp)

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)


def verify_github_site(target_site):
    # https://qiita.com/nannany_hey/items/23f847e0a331da52ed77
    # https://api.github.com/repos/motya1121/web-update-test/commits
    # https://api.github.com/repos/motya1121/web-update-test/commits/deveropment

    last_modifed_dt = dt.utcfromtimestamp(target_site["timestamp"])

    timestamp = int(time.time())
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

        print(f"push {commit_d['html_url']}")
        update_dynammodb(SiteId=target_site["PartitionKey"],
                         hash_result=hash_result,
                         url=commit_d['html_url'],
                         timestamp=timestamp)

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)


def verify_rss_site(target_site):
    last_modifed_dt = dt.utcfromtimestamp(target_site["timestamp"])

    # parse rss
    timestamp = int(time.time())
    d = feedparser.parse(target_site['url'])
    for entry in d.entries:
        pubdate_dt = None
        if 'published_parsed' in entry.keys():
            pubdate_dt = dt(*entry['published_parsed'][:6])
        elif 'updated_parsed' in entry.keys():
            pubdate_dt = dt(*entry['updated_parsed'][:6])
        else:
            raise KeyError

        if last_modifed_dt < pubdate_dt:
            # get hash
            result = requests.get(entry['link'])
            hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

            print(f"push {entry['link']}")
            update_dynammodb(SiteId=target_site["PartitionKey"],
                             hash_result=hash_result,
                             url=entry['link'],
                             timestamp=timestamp)

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)


def lambda_handler(event, context):

    for Record in event['Records']:
        target_site = json.loads(Record["body"])
        logger.info(f'site: {target_site["PartitionKey"]}')

        if target_site['type'] == "web":
            _ = verify_web_site(target_site=target_site)
        elif target_site['type'] == "github":
            _ = verify_github_site(target_site=target_site)
        elif target_site['type'] == "rss":
            _ = verify_rss_site(target_site=target_site)
