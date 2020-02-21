from pymongo import MongoClient
import os
import sys
import pprint
from bson.json_util import dumps
from json import loads, dump
import argparse


def get_mongo_session():
    """
    MONGO_USER and MONGO_PASSWORD = use the QA credentials
​
    MONGO_CERT = filepath to the RH-IT-pki-validation-chain.crt
    :return:
    """
    url = f'mongodb://{os.environ["MONGO_USER"]}:{os.environ["MONGO_PASSWORD"]}@dbamon.dev.redhat.com:60018/'
    params = ['authSource=admin', 'ssl=true', f'ssl_ca_certs={os.environ["MONGO_CERT"]}']
    params += ['tlsAllowInvalidHostnames=true', 'tlsAllowInvalidCertificates=true']
    url = ''.join([url, ''.join(["?", '&'.join(params)])])
    return MongoClient(url)['dwmops-db']


def search_collections(search_args):
    """
    Accepts 'topic', 'guid', and 'email'

    :param search_args:
    :return:
    """
    if search_args.topic:
        if search_args.guid:
            topic_records = db[search_args.topic].find({'headers.guid': search_args.guid})
            print_records(topic_records, search_args.topic)
        elif search_args.email:
            topic_records = db[search_args.topic].find({'value.email_address': search_args.email})
            print_records(topic_records, search_args.topic)
        else:
            print("  Use --email or --guid to search for specific records.  Use 'dump' to get entire collection")
            print()
    else:
        list_of_collections = db.list_collection_names()
        if search_args.guid:
            for collection in list_of_collections:
                topic_records = db[collection].find({'headers.guid': search_args.guid})
                print_records(topic_records, collection)
        elif search_args.email:
            for collection in list_of_collections:
                topic_records = db[collection].find({'value.email_address': search_args.email})
                print_records(topic_records, collection)
        else:
            print("  Use --email or --guid to search for specific records.  Use 'dump' to get entire collection")
            print()
            return


def print_records(topic_records, collection):
    records = [record for record in topic_records]
    if records:
        print("Messages found in collection", collection)
        pprint.pprint(records)
        print()
        print()


def get_list_of_collections(list_args):
    """
    Returns list of all collections in DB

    :param list_args:
    :return:
    """
    collections = db.list_collection_names()
    pprint.pprint(collections)


def dump_topic(dump_args):
    """
    Dumps entire collection to file

    :param dump_args:
    :return:
    """
    topic_records = db[dump_args.topic].find({})  # '{}' pulls all the records
    records = [record for record in topic_records]
    with open(os.path.join(os.getcwd(), dump_args.topic), 'w') as f:
        dump(loads(dumps(records)), f, indent=4)


def program_exit(exit_args):
    sys.exit()


db = get_mongo_session()
parser = argparse.ArgumentParser(description='Mongo forensics')
subparsers = parser.add_subparsers()
parser_list = subparsers.add_parser('list')
parser_list.set_defaults(func=get_list_of_collections)
parser_dump = subparsers.add_parser('dump')
parser_dump.add_argument('--topic', '-t', required=True)
parser_dump.set_defaults(func=dump_topic)
parser_search = subparsers.add_parser('search')
parser_search.add_argument('--guid', '-g')
parser_search.add_argument('--email', '-e')
parser_search.add_argument('--topic', '-t')
parser_search.set_defaults(func=search_collections)
parser_exit = subparsers.add_parser('exit')
parser_exit.set_defaults(func=program_exit)

while 1 == 1:
    cmd = input("> ")
    cmd_array = cmd.split()
    args = parser.parse_args(cmd_array)
    args.func(args)
