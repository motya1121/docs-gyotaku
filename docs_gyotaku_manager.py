import argparse
from ast import Or
import json
import random
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime as dt
from urllib.parse import urlparse

SSO_PRODILE = "main"
S3_BUCKET_NAME = "docs-gyotaku-532648218247"
DDB_TABLE_NAME = "docs-gyotaku"

session = boto3.Session(region_name='ap-northeast-1', profile_name=SSO_PRODILE)


def generate_site_id():
    while True:
        number = random.randint(1, 9999999999)
        WebSiteId = f'site-{number:0=10}'

        if exist_site_id(siteId=WebSiteId) is False:
            break

    return WebSiteId


def exist_site_id(siteId: str):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    responses = table.query(KeyConditionExpression=Key('PartitionKey').eq(siteId) & Key('SortKey').eq(siteId))

    if responses['Count'] == 0:
        return False
    else:
        return True


def verity_already_watched(url):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    scan_kwargs = {
        'FilterExpression': Key('url').eq(url),
    }
    responses = table.scan(**scan_kwargs)
    if responses['Count'] == 0:
        return False
    else:
        return True


def generate_user_id(mail_address):
    while True:
        number = random.randint(1, 9999999999)
        UserId = f'user-{number:0=10}'

        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table('docs-gyotaku')
        responses = table.query(KeyConditionExpression=Key('PartitionKey').eq(UserId) & Key('SortKey').eq(mail_address))

        if responses['Count'] == 0:
            break
    return UserId


def verity_already_submitted(mail_address):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    scan_kwargs = {
        'FilterExpression': Key('SortKey').eq(mail_address),
    }
    responses = table.scan(**scan_kwargs)
    if responses['Count'] == 0:
        return False
    else:
        return True


def site_list(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    if args.all is True:
        scan_kwargs = {
            'FilterExpression': Attr('is_watch').exists(),
        }
    else:
        scan_kwargs = {
            'FilterExpression': Key('is_watch').eq(True),
        }
    responses = table.scan(**scan_kwargs)

    if args.verbose is True:
        for result in responses['Items']:
            print(result)
    elif args.all is True:
        print('SiteId          |is_archive |is_watch |type\t|url|')
        for result in responses['Items']:
            print('{0[PartitionKey]} |{0[is_archive]}       |{0[is_watch]}     |{0[type]}\t|{0[url]}|'.format(result))
    else:
        print('SiteId          |is_archive |type\t|url|')
        for result in responses['Items']:
            print('{0[PartitionKey]} |{0[is_archive]}       |{0[type]}\t|{0[url]}|'.format(result))


def site_add(args):
    insert_datas = []
    if args.file is not None:
        print('file')

        with open(args.file, 'r') as f:
            insert_datas = json.load(f)

    if args.url is not None:
        o = urlparse(args.url)
        template = {
            "url": args.url,
            "type": "web",
            "is_archive": True,
            "is_watch": True,
            "property": {},
            "tags": ["test"],
            "title": o.netloc
        }
        insert_datas.append(template)

    for insert_data in insert_datas:
        # verify already wached
        if verity_already_watched(insert_data['url']) is True:
            print(f'[INFO] "{insert_data["url"]}" is already wached')
            continue

        site_id = generate_site_id()
        insert_data['PartitionKey'] = site_id
        insert_data['timestamp'] = int(time.time())
        insert_data['SortKey'] = site_id

        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table('docs-gyotaku')
        table.put_item(Item=insert_data)
        print(f'url: {insert_data["url"]} is success.')


def site_unwatch(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')

    site_id = args.siteId
    if exist_site_id(args.siteId) is False:
        print('error site id not found.')

    response = table.update_item(Key={
        'PartitionKey': site_id,
        'SortKey': site_id
    },
                                 UpdateExpression="set is_watch=:w",
                                 ExpressionAttributeValues={':w': False},
                                 ReturnValues="UPDATED_NEW")

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('error')
    else:
        print('success')


def site_watch(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')

    site_id = args.siteId
    if exist_site_id(args.siteId) is False:
        print('error site id not found.')

    response = table.update_item(Key={
        'PartitionKey': site_id,
        'SortKey': site_id
    },
                                 UpdateExpression="set is_watch=:w",
                                 ExpressionAttributeValues={':w': True},
                                 ReturnValues="UPDATED_NEW")

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('error')
    else:
        print('success')


def gyotaku_list(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')

    if args.siteId is not None:
        query_kwargs = {
            'IndexName': 'SiteData',
            'KeyConditionExpression': Key('PartitionKey').eq(args.siteId),
            'ScanIndexForward': False
        }
        responses = table.query(**query_kwargs)
        if responses['Count'] == 0:
            print(f'siteid:{args.siteId} is not found')
            return 0
        else:
            print('SiteId          | hash                                                     | timestamp           |')
            for item in responses['Items']:
                print('{0[PartitionKey]} | {0[SortKey]} | {1} |'.format(item, dt.fromtimestamp(item['timestamp'])))

    else:
        print('SiteId          |is_archive |type\t| latest hash                                              |url|')

        scan_kwargs = {'FilterExpression': Key('SortKey').begins_with('site-')}
        responses = table.scan(**scan_kwargs)

        # get latest site data
        for watch_data in responses['Items']:
            siteId = watch_data['PartitionKey']
            query_kwargs = {
                'IndexName': 'SiteData',
                'KeyConditionExpression': Key('PartitionKey').eq(siteId),
                'ScanIndexForward': False,
                'Limit': 1
            }
            site_hash = table.query(**query_kwargs)['Items'][0]['SortKey']

            query_kwargs = {
                'KeyConditionExpression': Key('PartitionKey').eq(siteId) & Key('SortKey').eq(site_hash),
                'Limit': 1
            }
            url = table.query(**query_kwargs)['Items'][0]['url']

            # print data
            print('{0[PartitionKey]} |{0[is_archive]}       |{0[type]}\t| {1} |{2}|'.format(watch_data, site_hash, url))


def gyotaku_get(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    siteId = args.siteId
    siteHash = args.hash

    query_kwargs = {'KeyConditionExpression': Key('PartitionKey').eq(siteId) & Key('SortKey').eq(siteHash)}
    responses = table.query(**query_kwargs)

    if responses['Count'] == 0:
        print(f'siteid:{siteId}, hash:{siteHash} is not found')
        return

    s3 = session.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_NAME)
    bucket.download_file(f"ArchiveData/{siteId}/{responses['Items'][0]['timestamp']}.html",
                         f"{responses['Items'][0]['timestamp']}.html")


def user_add(args):
    email_address = args.email
    if verity_already_submitted(mail_address=email_address) is True:
        print(f'[INFO] "{email_address}" is already submitted')
        return

    insert_data = {}
    insert_data['PartitionKey'] = generate_user_id(mail_address=email_address)
    insert_data['SortKey'] = email_address
    insert_data['tags'] = args.tags

    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    table.put_item(Item=insert_data)

    client = session.client('ses')
    _ = client.verify_email_identity(EmailAddress=email_address)


def test(args):
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DocsWatcher')
    subparsers = parser.add_subparsers()

    # *** site ***
    parser_site = subparsers.add_parser('site', help='docs gyotakuで監視するwebサイトの設定')
    parser_site_subparser = parser_site.add_subparsers()

    # site-list
    parser_site_list = parser_site_subparser.add_parser('list', help='docs gyotakuで監視中のwebサイト一覧')
    parser_site_list.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='verbose watching site data',
    )
    parser_site_list.add_argument(
        '-a',
        '--all',
        action='store_true',
        help='all site data',
    )
    parser_site_list.set_defaults(handler=site_list)

    # site-add
    parser_site_add = parser_site_subparser.add_parser('add', help='docs gyotakuで監視したいwebサイトつ追加')
    parser_site_add.add_argument(
        '-f',
        '--file',
        help='jsonファイルの内容から追加',
    )
    parser_site_add.add_argument(
        '-u',
        '--url',
        help='指定したWebサイトのURLを追加',
    )
    parser_site_add.set_defaults(handler=site_add)

    # site-unwatch
    parser_site_unwatch = parser_site_subparser.add_parser('unwatch', help='docs gyotakuでの監視を停止')
    parser_site_unwatch.add_argument(
        '--siteId',
        required=True,
        help='SiteId',
    )
    parser_site_unwatch.set_defaults(handler=site_unwatch)

    # site-watch
    parser_site_watch = parser_site_subparser.add_parser('watch', help='docs gyotakuでの監視を再開')
    parser_site_watch.add_argument(
        '--siteId',
        required=True,
        help='SiteId',
    )
    parser_site_watch.set_defaults(handler=site_watch)

    # *** gyotaku ***
    parser_gyotaku = subparsers.add_parser('gyotaku', help='魚拓データの管理')
    parser_gyotaku_subparser = parser_gyotaku.add_subparsers()

    # gyotaku-list
    parser_gyotaku_list = parser_gyotaku_subparser.add_parser('list', help='see `gyotaku list -h`')
    parser_gyotaku_list.add_argument(
        '-s',
        '--siteId',
        help='Web Site Id',
    )
    parser_gyotaku_list.set_defaults(handler=gyotaku_list)

    # gyotaku-get
    parser_gyotaku_get = parser_gyotaku_subparser.add_parser('get', help='see `gyotaku get -h`')
    parser_gyotaku_get.add_argument(
        '-s',
        '--siteId',
        required=True,
        help='Web Site Id',
    )
    parser_gyotaku_get.add_argument(
        '--hash',
        required=True,
        help='gyotaku hash',
    )
    parser_gyotaku_get.set_defaults(handler=gyotaku_get)

    # *** user ***
    parser_user = subparsers.add_parser('user', help='see `user -h`')
    parser_user_subparser = parser_user.add_subparsers()

    # user-add
    parser_user_add = parser_user_subparser.add_parser('add', help='see `user add -h`')
    parser_user_add.add_argument(
        '--email',
        required=True,
        help='Email address',
    )
    parser_user_add.add_argument(
        '--tags',
        required=True,
        nargs='*',
        help='document tags. ex) --tags test all',
    )
    parser_user_add.set_defaults(handler=user_add)

    # test
    parser_db = subparsers.add_parser('test', help='see `test -h`')
    parser_db.set_defaults(handler=test)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()
