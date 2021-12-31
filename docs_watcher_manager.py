import argparse
import json
import random
import time
import boto3
from boto3.dynamodb.conditions import Key

SSO_PRODILE = "main"

db_session = boto3.Session(region_name='ap-northeast-1', profile_name=SSO_PRODILE)


def generate_site_id():
    while True:
        number = random.randint(1, 9999999999)
        WebSiteId = f'site-{number:0=10}'

        dynamodb = db_session.resource('dynamodb')
        table = dynamodb.Table('docs-gyotaku')
        responses = table.query(KeyConditionExpression=Key('WebSiteId').eq(WebSiteId) & Key('SortId').eq(WebSiteId))

        if responses['Count'] == 0:
            break
    return WebSiteId


def verity_already_watched(url):
    dynamodb = db_session.resource('dynamodb')
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
    dynamodb = db_session.resource('dynamodb')
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

        dynamodb = db_session.resource('dynamodb')
        table = dynamodb.Table('docs-gyotaku')
        table.put_item(Item=insert_data)


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

    # test
    parser_db = subparsers.add_parser('test', help='see `test -h`')
    parser_db.set_defaults(handler=test)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()
