import sys
sys.path.append("lib/ijson-2.3")
import json
import ijson
from pprint import pprint
import time
import itertools
import operator


def load_twitter_json(file_name):
    with open(file_name) as f:
        parser = ijson.parse(f)
        i = 0
        for prefix, event, value in parser:
            if prefix == 'item.json.coordinates.coordinates.item':
                print('%d - %d' % (i, value))
            i = i + 1


def construct_melb_grid(file_name):
    """Parse melbGrid.json file and put the value inside dictionary"""
    melb_grid = []

    with open(file_name) as f:
        data = json.load(f)
        for val in data["features"]:
            grid_data = {}
            properties = val["properties"]
            grid_data["id"] = properties["id"]
            grid_data["xmax"] = properties["xmax"]
            grid_data["xmin"] = properties["xmin"]
            grid_data["ymin"] = properties["ymin"]
            grid_data["ymax"] = properties["ymax"]
            grid_data["count"] = 0
            grid_data["row_group"] = properties["id"][0:1]
            grid_data["column_group"] = properties["id"][1:2]
            melb_grid.append(grid_data)
    return melb_grid


# lat = y
# long = x
def match_tweets_coordinates(melb_grid, lat, lng):
    """Match individual tweet coordinates with the coordinates in melbGrid.json"""
    """lat -> y, long -> x"""
    for grid_data in melb_grid:
        if (lat >= grid_data["ymin"] and lat <= grid_data["ymax"]) \
            and (lng >= grid_data["xmin"] and lng <= grid_data["xmax"]):
            grid_data["count"] = grid_data["count"] + 1



print('Start Running The Program')
print('=========================')
start = time.time()
MELB_GRID_DATA = construct_melb_grid('data/melbGrid.json')
ROW_RANK = {"A":0, "B":0, "C":0, "D":0}
COLUMN_RANK = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
ROW_GROUP_TEXT = "row_group"
COLUMN_GROUP_TEXT = "column_group"
COUNT_TEXT = "count"

with open('data/smallTwitter.json') as f:
    PARSED_OBJ = ijson.items(f, 'item.json.coordinates.coordinates')
    for coordinates in PARSED_OBJ:
        match_tweets_coordinates(MELB_GRID_DATA, coordinates[1], coordinates[0])

# Group by Row
row_group = []
for key, items in itertools.groupby(MELB_GRID_DATA, operator.itemgetter(ROW_GROUP_TEXT)):
    row_group.append(list(items))

for data in row_group:
    if len(data) > 0:
        for val in data:
            ROW_RANK[val[ROW_GROUP_TEXT]] = ROW_RANK[val[ROW_GROUP_TEXT]] + int(val[COUNT_TEXT])

# Group by column
column_group = []
for key, items in itertools.groupby(MELB_GRID_DATA, operator.itemgetter(COLUMN_GROUP_TEXT)):
    column_group.append(list(items))

for data in row_group:
    if len(data) > 0:
        for val in data:
            COLUMN_RANK[val[COLUMN_GROUP_TEXT]] = COLUMN_RANK[val[COLUMN_GROUP_TEXT]] + int(val[COUNT_TEXT])


## Print all of the stuff here
print(" ")
print("Rank based on boxes")
GRID_RANKS = sorted(MELB_GRID_DATA, key=lambda k: k["count"], reverse=True)
for data in GRID_RANKS:
    pprint('%s: %d tweets' % (data["id"], data["count"]))

print(" ")
print("Order by rows")
for val in ROW_RANK:
    pprint('%s-Row: %d' % (val, ROW_RANK[val]))

print(" ")
print("Order by columns")
for val in COLUMN_RANK:
    pprint('Column %s: %d' % (val, COLUMN_RANK[val]))

done = time.time()
elapsed = round(done - start)
print(" ")
print("Program run for %d seconds" % (elapsed % 60))
