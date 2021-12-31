import argparse
import json
import random
import time
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime as dt

SSO_PRODILE = "main"
S3_BUCKET_NAME = "docs-gyotaku-532648218247"
DDB_TABLE_NAME = "docs-gyotaku"

session = boto3.Session(region_name='ap-northeast-1', profile_name=SSO_PRODILE)


def generate_site_id():
    while True:
        number = random.randint(1, 9999999999)
        WebSiteId = f'site-{number:0=10}'

        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table('docs-gyotaku')
        responses = table.query(KeyConditionExpression=Key('WebSiteId').eq(WebSiteId) & Key('SortId').eq(WebSiteId))

        if responses['Count'] == 0:
            break
    return WebSiteId


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


def db_list(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    scan_kwargs = {
        'FilterExpression': Key('is_watch').eq(True),
    }
    responses = table.scan(**scan_kwargs)

    if args.verbose is True:
        for result in responses['Items']:
            print(result)
    else:
        print('SiteId          |is_archive |type\t|url|')
        for result in responses['Items']:
            print('{0[WebSiteId]} |{0[is_archive]}       |{0[type]}\t|{0[url]}|'.format(result))


def db_add(args):
    insert_datas = None
    with open(args.file, 'r') as f:
        insert_datas = json.load(f)

    for insert_data in insert_datas:
        # verify already wached
        if verity_already_watched(insert_data['url']) is True:
            print(f'[INFO] "{insert_data["url"]}" is already wached')
            continue

        site_id = generate_site_id()
        insert_data['WebSiteId'] = site_id
        insert_data['timestamp'] = int(time.time())
        insert_data['SortId'] = site_id

        dynamodb = session.resource('dynamodb')
        table = dynamodb.Table('docs-gyotaku')
        table.put_item(Item=insert_data)


def gyotaku_list(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')

    if args.siteId is not None:
        query_kwargs = {
            'IndexName': 'SiteData',
            'KeyConditionExpression': Key('WebSiteId').eq(args.siteId),
            'ScanIndexForward': False
        }
        responses = table.query(**query_kwargs)
        if responses['Count'] == 0:
            print(f'siteid:{args.siteId} is not found')
            return 0
        else:
            print('SiteId          | hash                                                     | timestamp           |')
            for item in responses['Items']:
                print('{0[WebSiteId]} | {0[SortId]} | {1} |'.format(item, dt.fromtimestamp(item['timestamp'])))

    else:
        print('SiteId          |is_archive |type\t| latest hash                                              |url|')
        # get site list
        scan_kwargs = {
            'FilterExpression': Attr('timestamp').not_exists(),
        }
        responses = table.scan(**scan_kwargs)

        # get latest site data
        for watch_data in responses['Items']:
            siteId = watch_data['SortId']
            query_kwargs = {
                'IndexName': 'SiteData',
                'KeyConditionExpression': Key('WebSiteId').eq(siteId),
                'ScanIndexForward': False,
                'Limit': 1
            }
            site_data = table.query(**query_kwargs)['Items'][0]

            # print data
            print('{0[WebSiteId]} |{0[is_archive]}       |{0[type]}\t| {1} |{0[url]}|'.format(
                watch_data, site_data['SortId']))


def gyotaku_get(args):
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    siteId = args.siteId
    siteHash = args.hash

    query_kwargs = {'KeyConditionExpression': Key('WebSiteId').eq(siteId) & Key('SortId').eq(siteHash)}
    responses = table.query(**query_kwargs)

    if responses['Count'] == 0:
        print(f'siteid:{siteId}, hash:{siteHash} is not found')
        return

    s3 = session.resource('s3')
    bucket = s3.Bucket(S3_BUCKET_NAME)
    bucket.download_file(f"ArchiveData/{siteId}/{responses['Items'][0]['timestamp']}.html",
                         f"{responses['Items'][0]['timestamp']}.html")


def test(args):
    print('test')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DocsWatcher')
    subparsers = parser.add_subparsers()

    # *** db ***
    parser_db = subparsers.add_parser('db', help='see `db -h`')
    parser_db_subparser = parser_db.add_subparsers()

    # db-list
    parser_db_list = parser_db_subparser.add_parser('list', help='see `db list -h`')
    parser_db_list.add_argument(
        '-v',
        '--verbose',
        action='store_true',
        help='verbose watching site data',
    )
    parser_db_list.set_defaults(handler=db_list)

    # db-add
    parser_db_add = parser_db_subparser.add_parser('add', help='see `db add -h`')
    parser_db_add.add_argument(
        '-f',
        '--file',
        required=True,
        help='all files',
    )
    parser_db_add.set_defaults(handler=db_add)

    # *** gyotaku ***
    parser_gyotaku = subparsers.add_parser('gyotaku', help='see `gyotaku -h`')
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

    # test
    parser_db = subparsers.add_parser('test', help='see `test -h`')
    parser_db.set_defaults(handler=test)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()