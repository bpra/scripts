import json
from pprint import pprint
json_data=open('vertica.config')

data = json.load(json_data)
pprint(data)
json_data.close()

print "Hello"