import argparse
import os
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


def verity_already_watched(url, property: None):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    scan_kwargs = {
        'FilterExpression': Key('url').eq(url) & Key('SortKey').begins_with('site-'),
    }
    responses = table.scan(**scan_kwargs)
    if responses['Count'] == 0:
        return False

    for response in responses['Items']:
        if 'property' in response.keys() and property is not None:
            if len(response['property'].keys()) != len(property.keys()):
                return False

            for key, value in response['property'].items():
                if key in property.keys() and property[key] == value:
                    pass
                elif key not in property.keys():
                    return False
                else:
                    return False

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
        if verity_already_watched(insert_data['url'], insert_data['property']) is True:
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
        return

    update_kwargs = {
        'Key': {
            'PartitionKey': site_id,
            'SortKey': site_id
        },
        'UpdateExpression': "set is_watch=:w",
        'ExpressionAttributeValues': {
            ':w': False
        },
        'ReturnValues': "UPDATED_NEW"
    }
    response = table.update_item(**update_kwargs)

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
        return

    if args.now is True:
        timestamp = int(time.time())
        update_kwargs = {
            'Key': {
                'PartitionKey': site_id,
                'SortKey': site_id
            },
            'UpdateExpression': "set #timestamp = :timestamp, #is_watch = :is_watch",
            'ExpressionAttributeNames': {
                '#timestamp': 'timestamp',
                '#is_watch': 'is_watch'
            },
            'ExpressionAttributeValues': {
                ':timestamp': timestamp,
                ':is_watch': True
            },
            'ReturnValues': "UPDATED_NEW"
        }
    else:
        update_kwargs = {
            'Key': {
                'PartitionKey': site_id,
                'SortKey': site_id
            },
            'UpdateExpression': "set is_watch=:w",
            'ExpressionAttributeValues': {
                ':w': True
            },
            'ReturnValues': "UPDATED_NEW"
        }
    response = table.update_item(**update_kwargs)

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        print('error')
    else:
        print('success')


def gyotaku_list(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')

    if args.limit is not None and 1 < args.limit:
        limit = int(args.limit)
    else:
        limit = 100

    if args.siteId is not None:
        query_kwargs = {
            'IndexName': 'SiteData',
            'KeyConditionExpression': Key('PartitionKey').eq(args.siteId),
            'FilterExpression': Attr('SortKey').ne(args.siteId),
            'ScanIndexForward': False,
            'Limit': limit + 1
        }
        responses = table.query(**query_kwargs)
        if responses['Count'] == 0:
            print(f'siteid:{args.siteId} is not found')
            return 0
        else:
            count = 0
            print('SiteId          | hash                                                     | timestamp           |')
            for item in responses['Items']:
                print('{0[PartitionKey]} | {0[SortKey]} | {1} |'.format(item, dt.fromtimestamp(int(item['timestamp']))))
                count += 1
                if limit == count:
                    break

    else:
        print('SiteId          |is_archive |type\t| latest hash                                              |url|')

        # get site ids
        scan_kwargs = {'FilterExpression': Key('SortKey').begins_with('site-')}
        responses = table.scan(**scan_kwargs)

        # get latest site data
        for watch_data in responses['Items']:
            siteId = watch_data['PartitionKey']
            query_kwargs = {
                'IndexName': 'SiteData',
                'KeyConditionExpression': Key('PartitionKey').eq(siteId),
                'FilterExpression': Attr('SortKey').ne(siteId),
                'ScanIndexForward': False,
                'Limit': 2
            }
            # 初回取得が完了していない場合
            site_hash_responses = table.query(**query_kwargs)
            if site_hash_responses['Count'] == 0:
                continue
            site_hash = site_hash_responses['Items'][0]['SortKey']

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
    s3_prefix = f"ArchiveData/{siteId}/{responses['Items'][0]['timestamp']}"
    objs = bucket.meta.client.list_objects_v2(Bucket=bucket.name, Prefix=s3_prefix)
    for o in objs.get('Contents'):
        os.makedirs(str(responses['Items'][0]['timestamp']), exist_ok=True)
        file_name = os.path.basename(o.get('Key'))
        bucket.download_file(o.get('Key'), f"{responses['Items'][0]['timestamp']}/{file_name}")


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
    parser_site_watch.add_argument(
        '--now',
        action='store_true',
        help='現在の時刻から監視を再開する',
    )
    parser_site_watch.set_defaults(handler=site_watch)

    # *** gyotaku ***
    parser_gyotaku = subparsers.add_parser('gyotaku', help='魚拓データの管理')
    parser_gyotaku_subparser = parser_gyotaku.add_subparsers()

    # gyotaku-list
    parser_gyotaku_list = parser_gyotaku_subparser.add_parser(
        'list', help='魚拓情報のリスト。SiteIdを指定しない場合全てのサイトの最新情報、指定した場合そのサイトの全ての魚拓情報を表示')
    parser_gyotaku_list.add_argument(
        '-s',
        '--siteId',
        help='魚拓のリスト表示するSiteId',
    )
    parser_gyotaku_list.add_argument(
        '--limit',
        type=int,
        help='魚拓のリスト表示件数',
    )
    parser_gyotaku_list.set_defaults(handler=gyotaku_list)

    # gyotaku-get
    parser_gyotaku_get = parser_gyotaku_subparser.add_parser('get', help='魚拓を取得する')
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
