import argparse
import json
import boto3
from boto3.dynamodb.conditions import Key

SSO_PRODILE = "main"

db_session = boto3.Session(region_name='ap-northeast-1', profile_name=SSO_PRODILE)


def db_list(args):
    dynamodb = db_session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    responses = table.scan()
    for result in responses['Items']:
        print(result)


def db_add(args):
    insert_data = ""
    with open(args.file, 'r') as f:
        insert_data = json.load(f)
    print(insert_data)

    dynamodb = db_session.resource('dynamodb')
    table = dynamodb.Table('docs-gyotaku')
    table.put_item(Item=insert_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DocsWatcher')
    subparsers = parser.add_subparsers()

    # db
    parser_db = subparsers.add_parser('db', help='see `db -h`')
    parser_db_subparser = parser_db.add_subparsers()
    # db-list
    parser_db_list = parser_db_subparser.add_parser('list', help='see `db list -h`')
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

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()
