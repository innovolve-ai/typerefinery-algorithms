import sys
sys.path.append("..")
import json
from loguru import logger
from library.tdb_query import get_data



@logger.catch
def main():
    # load in json colagraph
    with open('..\\inputs\\input_test1_connection.json') as json_file:
        db_query = json.load(json_file)
    
    colaGraph = get_data(db_query)

    

    logger.debug('==========colagraph output ====================')
    logger.debug(f'colaGraph is -> {colaGraph}')
    logger.debug('==============================')    
    

    with open("..\\outputs\\test1_output_Basic.json", "w") as outfile:  
        json.dump(colaGraph, outfile) 



if __name__ == '__main__':
    main()