from pymongo import MongoClient
import os
import sys
import pprint
from bson.json_util import dumps
from json import loads, dump


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


def find_messages_by_guid_single_topic(topic_to_search, guid_to_find):
    topic_records = db[topic_to_search].find({'headers.guid': guid_to_find})
    records = [record for record in topic_records]
    pprint.pprint(records)


def find_messages_by_guid(guid_to_find):
    list_of_collections = db.list_collection_names()
    for collection in list_of_collections:
        topic_records = db[collection].find({'headers.guid': guid_to_find})
        records = [record for record in topic_records]
        if records != []:
            print("Messages found in collection", collection)
            pprint.pprint(records)
            print()
            print()


def get_list_of_collections():
    collections = db.list_collection_names()
    print(collections)


def dump_topic(topic_name):
    topic_records = db[topic_name].find({})  # '{}' pulls all the records
    records = [record for record in topic_records]
    with open(os.path.join(os.getcwd(), topic_name), 'w') as f:
        dump(loads(dumps(records)), f, indent=4)


db = get_mongo_session()

while 1 == 1:
    cmd = input("> ")
    cmd_array = cmd.split()
    if cmd_array[0] == "list":
        get_list_of_collections()
    elif cmd_array[0] == "dump":
        if len(cmd_array) == 1:
            print("Usage: > dump {collection name}")
            break
        else:
            dump_topic(cmd_array[1])
            print("Collection dumped to new file ", cmd_array[1])
            break
    elif cmd_array[0] == "guid":
        if len(cmd_array) == 1:
            print("Usage: > guid [-t topic_name] {guid}")
        elif cmd_array[1] == "-t":
            if cmd_array[2] == "":
                print("Usage: > guid [-t topic_name] {guid}")
                break
            elif cmd_array[3] == "":
                print("Usage: > guid [-t topic_name] {guid}")
                break
            else:
                find_messages_by_guid_single_topic(cmd_array[2], cmd_array[3])
        else:
            if cmd_array[1] == "":
                print("Usage: > guid [-t topic_name] {guid}")
                break
            else:
                find_messages_by_guid(cmd_array[1])
    elif cmd_array[0] == "test":
        cre = db.CampaignResponse_Canon_12p
        pprint.pprint(cre.find_one({'headers.guid': '82c0f8fe-4cff-11ea-9c77-0a58ac14b5c8'}))
    elif cmd_array[0] == "exit":
        break
    else:
        print("Unknown command")



