import os
import json
import boto3
# from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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
    print(email_body)

    # send email
    client = boto3.client('ses', region_name=AWS_REAGION)
    msg = MIMEMultipart('mixed')

    msg['Subject'] = subject
    msg['sender'] = EMAIL_SENDER
    msg['From'] = EMAIL_SENDER
    msg['To'] = EMAIL_SENDER
    msg['Cc'] = EMAIL_RECEIVERS

    msg_body = MIMEMultipart('alternative')
    html_part = MIMEText(email_body.encode(charset), 'html', charset)
    msg_body.attach(html_part)
    msg.attach(msg_body)

    try:
        responce = client.send_raw_email(Source=EMAIL_SENDER,
                                         Destinations=[EMAIL_SENDER, EMAIL_RECEIVERS],
                                         RawMessage={'Data': msg.as_string()})
    except ClientError as e:
        print(e)
    else:
        print("Email sent! Message ID: ")
        print(responce['MessageId'])


def lambda_handler(event, context):
    print(json.dumps(event))
    for record in event['Records']:
        url = record['dynamodb']['NewImage']['url']['S']

        data = {"docs_type": "", "title": "", "updated_dt_jst": "", "url": url}
        send_email(data)
