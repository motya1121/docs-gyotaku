import boto3
import os
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup
import requests
import hashlib
import time
import logging
import feedparser
from datetime import datetime as dt, timedelta
import json
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)
DDB_TABLE_NAME = os.environ['DDBTablename']
db_session = boto3.Session(region_name='ap-northeast-1')
dynamodb = db_session.resource('dynamodb')
table = dynamodb.Table(DDB_TABLE_NAME)
log_info = {"siteId": "", "type": "", "last_modifed_dt": 0, "new_timestamp_dt": 0, "is_update": False}


def json_serial(obj):
    if isinstance(obj, (dt)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return int(obj)

    raise TypeError("Type %s not serializable" % type(obj))


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


def verify_web_site(target_site):
    timestamp = int(time.time())
    result = requests.get(target_site['url'])
    hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

    if hash_result == target_site['latest_data']['SortKey']:
        pass
    else:
        update_dynammodb(SiteId=target_site["PartitionKey"],
                         hash_result=hash_result,
                         url=target_site['url'],
                         timestamp=timestamp)

        log_info['is_update'] = True
        log_info['latest_hash_result'] = target_site['latest_data']['SortKey']
        log_info['hash_result'] = hash_result
        log_info['url'] = target_site['url']

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)

    log_info['last_modifed_dt'] = dt.utcfromtimestamp(target_site["timestamp"])
    log_info['new_timestamp_dt'] = dt.fromtimestamp(timestamp)


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

        update_dynammodb(SiteId=target_site["PartitionKey"],
                         hash_result=hash_result,
                         url=commit_d['html_url'],
                         timestamp=timestamp)

        log_info['is_update'] = True
        log_info['hash_result'] = hash_result
        log_info['url'] = commit_d['html_url']

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)

    log_info['last_modifed_dt'] = last_modifed_dt
    log_info['new_timestamp_dt'] = dt.fromtimestamp(timestamp)


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

            update_dynammodb(SiteId=target_site["PartitionKey"],
                             hash_result=hash_result,
                             url=entry['link'],
                             timestamp=timestamp)

            log_info['is_update'] = True
            log_info['hash_result'] = hash_result
            log_info['url'] = entry['link']

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)

    log_info['last_modifed_dt'] = last_modifed_dt
    log_info['new_timestamp_dt'] = dt.fromtimestamp(timestamp)


def lambda_handler(event, context):

    for Record in event['Records']:
        target_site = json.loads(Record["body"])
        log_info["siteId"] = target_site["PartitionKey"]
        log_info['type'] = target_site["type"]

        if target_site['type'] == "web":
            _ = verify_web_site(target_site=target_site)
        elif target_site['type'] == "github":
            _ = verify_github_site(target_site=target_site)
        elif target_site['type'] == "rss":
            _ = verify_rss_site(target_site=target_site)

        logger.info(json.dumps(log_info, default=json_serial))
