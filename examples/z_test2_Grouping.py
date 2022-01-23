import sys
sys.path.append("..")
from library.tdb_disj_group import group_Grakn
import json
from loguru import logger


@logger.catch
def main():

    # load in json colagraph
    with open('..\\inputs\\input_test2_colaGraph_sample.json') as json_file:
        data = json.load(json_file)
    
    # load in json colagraph test_definition
    with open('..\inputs\input_test2_definition.json') as json_file:
        def_list = json.load(json_file)

    data['defined_list'] = def_list

     

    colaGraph = group_Grakn(data, data['defined_list'])

    grouped = colaGraph['grouped']
    grp_nodes = grouped['nodes']
    grp_links = grouped['links']
    grp_groups = grouped['groups']

    logger.debug('==========colagraph output ====================')
    logger.debug(f'colaGraph is -> {colaGraph}')
    logger.debug(f'def list is -> {def_list}')
    logger.debug('==============================')    
    logger.debug(f'grp_nodes length is -> {len(grp_nodes)}') 
    logger.debug(f'grp_links length is -> {len(grp_links)}') 
    logger.debug(f'grp_groups length is -> {len(grp_groups)}') 

    with open("..\\outputs\\test2_output_Grouped.json", "w") as outfile:  
        json.dump(colaGraph, outfile) 



if __name__ == '__main__':
    main()