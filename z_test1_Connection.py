import G_to_WebCola as g
import json
from loguru import logger


@logger.catch
def main():
    # load in json colagraph
    with open('input_test1_connection.json') as json_file:
        g_Connect = json.load(json_file)
    
    colaGraph = g.get_data(g_Connect)

    

    logger.debug('==========colagraph output ====================')
    logger.debug(f'colaGraph is -> {colaGraph}')
    logger.debug('==============================')    
    

    with open("test1_output_Basic.json", "w") as outfile:  
        json.dump(colaGraph, outfile) 



if __name__ == '__main__':
    main()