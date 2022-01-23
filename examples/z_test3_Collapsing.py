import sys
sys.path.append("..")
import json
from loguru import logger
from library.tdb_collapse_groups import collapse


with open('..\\inputs\\input_test3_Grouped.json') as t:
    test = json.load(t)

collapsed = collapse(test, "session")

logger.debug('===============================')


with open('..\\outputs\\test3_output_Collapsed.json', 'w') as json_file:
  json.dump(collapsed, json_file)