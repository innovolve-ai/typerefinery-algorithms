import json
from loguru import logger
from Collapse_Group import collapse


with open('input_test3_Grouped.json') as t:
    test = json.load(t)

collapsed = collapse(test, "session")

logger.debug('===============================')


with open('test3_output_Collapsed.json', 'w') as json_file:
  json.dump(collapsed, json_file)