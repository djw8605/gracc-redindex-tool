
import json
import sys
from elasticsearch import Elasticsearch
from elasticsearch import TransportError
import traceback

import argparse




def add_args():
    parser = argparse.ArgumentParser(description='Sync Script')
    parser.add_argument('index', type=str, nargs='+', help='Indexes to sync')
    parser.add_argument("--output-index", "-o", help="Output index (not implemented)")
    parser.add_argument('--mapping', '-m', type=str, help='Mapping for index')
    parser.add_argument("--new-id", "-n", dest="newid", action='store_true', help="Create new _id for new documents")
    return parser.parse_args()


def main():
    args = add_args()

    es = Elasticsearch(timeout=300)
    print("Adding index", args.index)

    json_object = None
    if args.mapping:

        
        with open(args.mapping, 'r') as json2018:
            json_object=json.load(json2018)

        json_object = {"mappings": json_object}
        print(json_object)

    for index in args.index:
        es.indices.delete(index)

        try:
            if json_object:
                es.indices.create(index, body=json_object)
            else:
                es.indices.create(index)
        except TransportError as e:
            traceback.print_exc()
            print(e.info)
            sys.exit(0)



        reindex_commands = { "source": {
            "remote": {
            "host": "http://hcc-gracc-fe1.unl.edu:9200",
            "username": "gracc",
            "password": "graccitgood"
            },
            'index': index },
            "dest": {"index": index}
        }

        if args.newid:
            reindex_commands['script'] = {
                "source": "ctx._id=null",
                "lang": "painless"
            }


        reindex_return = es.reindex(body=reindex_commands, wait_for_completion=True)
        print(reindex_return)
        #task_id = reindex_return['task']
        #task_status = es.tasks.get(task_id)
        #print(json.dumps(task_status))


    
if __name__ == "__main__":
    main()

#es.indices.delete("gracc.osg.summary3-2018")

#reindex_commands = {'source': {'index': 'gracc.osg.summary3-2018-derektest'}, 'dest': {'index': 'gracc.osg.summary3-2018'}}
#print es.reindex(body=reindex_commands)

#es.indices.delete("gracc.osg.summary3-2018-derektest")

