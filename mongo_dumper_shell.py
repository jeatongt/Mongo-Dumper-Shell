from pymongo import MongoClient
import os
import sys
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



"""
if sys.argv[1] == "-list":
    collections = db.list_collection_names()
    print(collections)
if sys.argv[1] == "-dump":
    topic_records = db[sys.argv[2]].find({})  # '{}' pulls all the records
    records = [record for record in topic_records]
    with open(os.path.join(os.getcwd(), sys.argv[2]), 'w') as f:
        dump(loads(dumps(records)), f, indent=4)

"""

def find_messages_by_guid(guid):
    printf("Not yet implemented for guid ", guid)

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
        else:
            dump_topic(cmd_array[1])
            print("Collection dumped to new file ", cmd_array[1])
    elif cmd_array[0] == "exit":
        break
    else:
        print("Unknown command")



