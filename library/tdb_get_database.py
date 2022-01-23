from typedb.client import *
from loguru import logger
import json
import copy


@logger.catch
def get_database(db_connect):    
    g_uri = db_connect['url'] + ':' + db_connect['port']
    with TypeDB.core_client(g_uri) as client:
        return_list = []
        return_list = [db.name() for db in client.databases().all()]

    with open("db_list_output.json", "w") as outfile:  
        json.dump(return_list, outfile) 
    return return_list

    
if __name__ == '__main__':
    db_connect = {
            "url": "localhost",
            "port": "1729"
        }
    
    local_list = []
    local_list = get_database(db_connect)
    print(local_list)