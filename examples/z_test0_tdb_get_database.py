import sys
sys.path.append("..")
import json
from loguru import logger
from library.tdb_get_database import get_database


@logger.catch
def main():
    # load in json colagraph
    with open('..\\inputs\\input_test0_get_database.json') as json_file:
        db_connect = json.load(json_file)
    
    db_list = get_database(db_connect)

    

    logger.debug('==========colagraph output ====================')
    logger.debug(f'db_list is -> {db_list}')
    logger.debug('==============================')    
    

    with open("..\\outputs\\test0_output_db_list.json", "w") as outfile:  
        json.dump(db_list, outfile) 



if __name__ == '__main__':
    main()