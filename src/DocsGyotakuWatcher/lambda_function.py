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


def verify_rss_site_prismacloud(result_text):
    soup = BeautifulSoup(result_text, "html.parser")
    topicsindex = soup.find('div', attrs={'class': 'featuredTilesPar parsys'})
    logger.info(topicsindex)
    hash_result = hashlib.sha224(topicsindex.encode('utf-8')).hexdigest()

    return hash_result


def verify_rss_site_prismacloud_releasenote(result_text):
    soup = BeautifulSoup(result_text, "html.parser")
    topicsindex = soup.find('div', attrs={'class': 'topic topic concept'})
    logger.info(topicsindex)
    hash_result = hashlib.sha224(topicsindex.encode('utf-8')).hexdigest()

    return hash_result


def verify_web_site_microsoft_learn(result_text):
    soup = BeautifulSoup(result_text, "html.parser")
    topicsindex = soup.find('div', attrs={'id': 'main-column'})
    logger.info(topicsindex)
    hash_result = hashlib.sha224(topicsindex.encode('utf-8')).hexdigest()

    return hash_result


def verify_web_site(target_site):
    log_info = {
        "siteId": target_site["PartitionKey"],
        "type": target_site["type"],
        "last_modifed_dt": 0,
        "new_timestamp_dt": 0,
        "is_update": False,
        "additional_info": {}
    }

    timestamp = int(time.time())
    result = requests.get(target_site['url'])
    if target_site['url'] == "https://docs.paloaltonetworks.com/prisma/prisma-cloud":
        log_info['additional_info']['type'] = 'prisma cloud'
        hash_result = verify_rss_site_prismacloud(result_text=result.text)
    elif target_site[
            'url'] == "https://docs.paloaltonetworks.com/prisma/prisma-cloud/prisma-cloud-release-notes/prisma-cloud-release-information/features-introduced-in-2022":
        log_info['additional_info']['type'] = 'prisma cloud'
        hash_result = verify_rss_site_prismacloud_releasenote(result_text=result.text)
    elif target_site['url'] == "https://learn.microsoft.com/en-us/connectors/common/outbound-ip-addresses":
        hash_result = verify_web_site_microsoft_learn(result_text=result.text)
    else:
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

    return log_info


def verify_github_site(target_site):

    log_info = {
        "siteId": target_site["PartitionKey"],
        "type": target_site["type"],
        "last_modifed_dt": 0,
        "new_timestamp_dt": 0,
        "is_update": False,
        "additional_info": {}
    }
    last_modifed_dt = dt.utcfromtimestamp(target_site["timestamp"])

    property_keys = target_site['property'].keys()
    if "owner" not in property_keys or "repo" not in property_keys or "path" not in property_keys:
        logger.error('property key Incorrect')
        logger.info(json.dumps(log_info, default=json_serial))
        return

    timestamp = int(time.time())
    url = "https://api.github.com/repos/{0[owner]}/{0[repo]}/commits?path={0[path]}".format(target_site['property'])
    result = requests.get(url=url)
    commit_list = json.loads(result.text)

    updated_commit_list = []
    for commit_d in commit_list:
        try:
            commit_dt = dt.strptime(commit_d['commit']['committer']['date'], '%Y-%m-%dT%H:%M:%SZ')
        except TypeError:
            continue
        if commit_dt <= last_modifed_dt:
            continue

        result = requests.get(url=commit_d['url'])
        hash_result = hashlib.sha224(result.text.encode('utf-8')).hexdigest()

        updated_commit_list.append({
            "url": commit_d['url'],
            "html_url": commit_d['html_url'],
            "commit_dt": commit_dt,
            "hash_result": hash_result
        })

        if 'is_aggregation' in property_keys and target_site['property']['is_aggregation'] is True:
            break

    updated_commit_list = sorted(updated_commit_list, key=lambda x: x['commit_dt'], reverse=False)

    for updated_commit in updated_commit_list:
        update_dynammodb(SiteId=target_site["PartitionKey"],
                         hash_result=updated_commit['hash_result'],
                         url=updated_commit['html_url'],
                         timestamp=timestamp)

        log_info['is_update'] = True
        log_info['hash_result'] = hash_result
        log_info['url'] = commit_d['html_url']

    # update timestamp
    update_latest_timestamp(SiteId=target_site["PartitionKey"], timestamp=timestamp)

    log_info['last_modifed_dt'] = last_modifed_dt
    log_info['new_timestamp_dt'] = dt.fromtimestamp(timestamp)

    return log_info


def verify_rss_atlassian_statuspage(result_text):
    soup = BeautifulSoup(result_text, "html.parser")
    topicsindex = soup.find('div', attrs={'class': 'layout-content status status-incident'})
    hash_result = hashlib.sha224(topicsindex.encode('utf-8')).hexdigest()

    return hash_result


def verify_rss_site(target_site):
    log_info = {
        "siteId": target_site["PartitionKey"],
        "type": target_site["type"],
        "last_modifed_dt": 0,
        "new_timestamp_dt": 0,
        "rss_url": target_site['url'],
        "is_update": False,
        "additional_info": {}
    }
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
            if target_site['url'] == "https://dropboxpublic.statuspage.io/history.rss":
                log_info['additional_info']['type'] = 'dropbox'
                hash_result = verify_rss_atlassian_statuspage(result_text=result.text)
            elif target_site['url'] == "https://status.lastpass.com/history.rss":
                log_info['additional_info']['type'] = 'lastpass'
                hash_result = verify_rss_atlassian_statuspage(result_text=result.text)
            else:
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

    return log_info


def lambda_handler(event, context):

    for Record in event['Records']:
        target_site = json.loads(Record["body"])
        logger.info(json.dumps({"siteId": target_site['PartitionKey'], "url": target_site['url']}, default=json_serial))

        if target_site['type'] == "web":
            log_info = verify_web_site(target_site=target_site)
        elif target_site['type'] == "github":
            log_info = verify_github_site(target_site=target_site)
        elif target_site['type'] == "rss":
            log_info = verify_rss_site(target_site=target_site)

        logger.info(json.dumps(log_info, default=json_serial))
