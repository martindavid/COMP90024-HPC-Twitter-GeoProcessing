import sys
sys.path.append("lib/ijson-2.3")
import json
import ijson
from pprint import pprint
import time
import itertools
import operator
from mpi4py import MPI
import numpy as np

# get start time
startTime = time.time()

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

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


def match_tweets_coordinates(melb_grid, lat, lng):
    """Match individual tweet coordinates with the coordinates in melbGrid.json"""
    """lat -> y, long -> x"""
    for grid_data in melb_grid:
        if (lat >= grid_data["ymin"] and lat <= grid_data["ymax"]) \
            and (lng >= grid_data["xmin"] and lng <= grid_data["xmax"]):
            grid_data["count"] = grid_data["count"] + 1


MELB_GRID = construct_melb_grid('data/melbGrid.json')
start = time.time()
if rank == 0:
    print('Start Running The Program')
    print('=========================')
    ROW_RANK = {"A":0, "B":0, "C":0, "D":0}
    COLUMN_RANK = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    ROW_GROUP_TEXT = "row_group"
    COLUMN_GROUP_TEXT = "column_group"
    COUNT_TEXT = "count"

    with open('data/bigTwitter.json') as f:
        try:
            PARSED_OBJ = ijson.items(f, 'item.json.coordinates.coordinates')
            coords_data = []
            for coordinates in PARSED_OBJ:
                coords = {}
                coords["lat"] = coordinates[0]
                coords["lng"] = coordinates[1]
                coords_data.append(coords)
        except:
            pass
    chunks = np.array_split(coords_data, size)
else:
    chunks = None

chunk = comm.scatter(chunks, root=0)

for data in chunk:    
    match_tweets_coordinates(MELB_GRID, data["lng"], data["lat"])

result = comm.allgather(MELB_GRID)


if rank == 0:
    RESULT_GRID = {
        "A1": 0, "A2": 0, "A3": 0, "A4": 0,
        "B1": 0, "B2": 0, "B3": 0, "B4": 0,
        "C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0,
        "D3": 0, "D4": 0, "D5": 0,
    }
    for grid_data in result:
        for single_grid_data in grid_data:
            RESULT_GRID[single_grid_data["id"]] = RESULT_GRID[single_grid_data["id"]] \
                                                    + single_grid_data["count"]

    # Group by Row
    ROW_GROUP = {"A": 0, "B": 0, "C": 0, "D": 0}
    for i in RESULT_GRID:
        ROW_GROUP[i[0:1]] = ROW_GROUP[i[0:1]] + RESULT_GRID[i]



    # Group by column
    COLUMN_GROUP = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    for i in RESULT_GRID:
        COLUMN_GROUP[i[1:2]] = COLUMN_GROUP[i[1:2]] + RESULT_GRID[i]

    # Print all of the stuff here
    print(" ")
    print("Rank based on boxes")
    GRID_RANKS = sorted(RESULT_GRID, key=RESULT_GRID.get, reverse=True)
    for i in GRID_RANKS:
        pprint('%s: %d tweets' % (i, RESULT_GRID[i]))

    print(" ")
    print("Order by rows")
    ROW_RANK = sorted(ROW_GROUP, key=ROW_GROUP.get, reverse=True)
    for val in ROW_RANK:
        pprint('%s-Row: %d' % (val, ROW_GROUP[val]))

    print(" ")
    print("Order by columns")
    COLUMN_RANK = sorted(COLUMN_GROUP, key=COLUMN_GROUP.get, reverse=True)
    for val in COLUMN_RANK:
        pprint('Column %s: %d' % (val, COLUMN_GROUP[val]))

    #print the total time it takes
    totalMinutes = time.time() - startTime
    minutes, seconds = divmod(totalMinutes, 60)
    print("\n\n Total time used for execution is %02d minutes and %02d seconds"%(minutes, seconds))