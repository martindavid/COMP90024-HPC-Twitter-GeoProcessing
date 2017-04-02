import sys
import json
import re
from pprint import pprint

myData = []
count = 1
with open('data/tinyTwitter.json',errors='ignore') as f:
    for line in f:
        # print(" ")
        # print(count)
        # data = json.loads(line[0:len(line) - 2])
        # pprint(line[0:len(line) - 2])
        # count = count + 1
        # pprint(data)
        try:
            data = json.loads(line[0:len(line) - 2])
            coords = {}
            coords["lat"] = data["json"]["coordinates"]["coordinates"][1]
            coords["lng"] = data["json"]["coordinates"]["coordinates"][0]
            myData.append(coords)
            print(count)
            count = count + 1
        except Exception as inst:
            print(count)
            count = count + 1
            pprint(inst)
            continue

pprint(myData)