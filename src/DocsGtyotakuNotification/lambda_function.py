import os
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime as dt, timedelta

logger = logging.getLogger()

DDB_TABLE_NAME = os.environ['DDBTablename']
EMAIL_SENDER = os.environ['EMAIL_SENDER']
EMAIL_RECEIVERS = os.environ['EMAIL_RECEIVERS']
AWS_REAGION = os.getenv("AWS_DEFAULT_REGION")


def send_email(data):
    charset = "UTF-8"
    subject = '[{0[docs_type]}] {0[title]}'.format(data)
    email_body = """
    <html>
        <body>
            <ul>
                <li>アップデート時刻：{0[updated_dt_jst]}</li>
                <li>URL：<a href="{0[url]}">{0[url]}</a></li>
            </ul>
        </body>
    </html>
    """.format(data)

    # send email
    client = boto3.client('ses', region_name=AWS_REAGION)
    msg = MIMEMultipart('mixed')

    msg['Subject'] = subject
    msg['sender'] = EMAIL_SENDER
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_SENDER
    msg['Cc'] = ';'.join(data['receivers'])

    msg_body = MIMEMultipart('alternative')
    html_part = MIMEText(email_body.encode(charset), 'html', charset)
    msg_body.attach(html_part)
    msg.attach(msg_body)

    try:
        Destinations = data['receivers']
        Destinations.append(EMAIL_SENDER)
        Destinations = list(set(Destinations))
        responce = client.send_raw_email(Source=EMAIL_SENDER,
                                         Destinations=Destinations,
                                         RawMessage={'Data': msg.as_string()})
    except ClientError as e:
        print(e)
    else:
        print("Email sent! Message ID: ")
        print(responce['MessageId'])


def get_receivers(tags: list):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    tag_filter = Attr('tags').contains('all')
    scan_kwargs = {'FilterExpression': Attr('tags').contains('all')}
    receivers = []

    for tag in tags:
        tag_filter = tag_filter | Attr('tags').contains(tag)

    scan_kwargs['FilterExpression'] = (tag_filter) & Key('PartitionKey').begins_with('user-')

    responses = table.scan(**scan_kwargs)['Items']
    for receiver in responses:
        receivers.append(receiver['SortKey'])
    return receivers


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
        url = site_data['url']['S']
        if 'title' in site_data.keys():
            title = site_data['title']['S']
        else:
            title = target_site['title']
        updated_dt_jst = dt.fromtimestamp(int(site_data['timestamp']['N'])) + timedelta(hours=9)
        updated_dt_jst = updated_dt_jst.strftime('%Y-%m-%d %H:%M:%S')
        receivers = get_receivers(target_site['tags'])

        data = {
            "docs_type": target_site['type'],
            "title": title,
            "updated_dt_jst": updated_dt_jst,
            "url": url,
            "receivers": receivers
        }
        send_email(data)
