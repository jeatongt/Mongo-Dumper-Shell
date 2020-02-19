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


def search_collections(**kwargs):
    """
    Accepts 'topic', 'guid', and 'email'

    :param kwargs:
    :return:
    """
    if kwargs["topic"]:
        if kwargs["guid"]:
            topic_records = db[kwargs["topic"]].find({'headers.guid': kwargs["guid"]})
        elif kwargs["email"]:
            topic_records = db[kwargs["topic"]].find({'value.email_address': kwargs["email"]})
        else:
            print("  Use --email or --guid to search for specific records.  Use 'dump' to get entire collection")
            print()
    else:
        list_of_collections = db.list_collection_names()
        if kwargs["guid"]:
            for collection in list_of_collections:
                topic_records = db[collection].find({'headers.guid': kwargs["guid"]})
        elif kwargs["email"]:
            for collection in list_of_collections:
                topic_records = db[collection].find({'value.email_address': kwargs["email"]})
        else:
            print("  Use --email or --guid to search for specific records.  Use 'dump' to get entire collection")
            print()


def get_list_of_collections():
    """
    Returns list of all collections in DB

    :return:
    """
    collections = db.list_collection_names()
    pprint.pprint(collections)


def dump_topic(topic_name):
    """
    Dumps entire collection to file
    
    :param topic_name:
    :return:
    """
    topic_records = db[topic_name].find({})  # '{}' pulls all the records
    records = [record for record in topic_records]
    with open(os.path.join(os.getcwd(), topic_name), 'w') as f:
        dump(loads(dumps(records)), f, indent=4)


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

while 1 == 1:
    cmd = input("> ")
    cmd_array = cmd.split()
    args = parser.parse_args(cmd_array)
    args.func(args)

# while 1 == 1:
#     cmd = input("> ")
#     cmd_array = cmd.split()
#     if cmd_array[0] == "list":
#         get_list_of_collections()
#     elif cmd_array[0] == "dump":
#         if len(cmd_array) == 1:
#             print("Usage: > dump {collection name}")
#             break
#         else:
#             dump_topic(cmd_array[1])
#             print("Collection dumped to new file ", cmd_array[1])
#             break
#     elif cmd_array[0] == "guid":
#         if len(cmd_array) == 1:
#             print("Usage: > guid [-t topic_name] {guid}")
#         elif cmd_array[1] == "-t":
#             if cmd_array[2] == "":
#                 print("Usage: > guid [-t topic_name] {guid}")
#                 break
#             elif cmd_array[3] == "":
#                 print("Usage: > guid [-t topic_name] {guid}")
#                 break
#             else:
#                 find_messages_by_guid_single_topic(cmd_array[2], cmd_array[3])
#         else:
#             if cmd_array[1] == "":
#                 print("Usage: > guid [-t topic_name] {guid}")
#                 break
#             else:
#                 find_messages_by_guid(cmd_array[1])
#     elif cmd_array[0] == "test":
#         cre = db.CampaignResponse_Canon_12p
#         pprint.pprint(cre.find_one({'headers.guid': '82c0f8fe-4cff-11ea-9c77-0a58ac14b5c8'}))
#     elif cmd_array[0] == "exit":
#         break
#     else:
#         print("Unknown command")
