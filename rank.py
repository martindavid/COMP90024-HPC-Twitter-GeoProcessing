from __future__ import print_function
import sys
import json
from pprint import pprint
import time
import itertools
import operator
from mpi4py import MPI
import numpy as np


# get start time
start_time = time.time()
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
if rank == 0:
    print('Start Running The Program')
    print('=========================')
    ROW_RANK = {"A":0, "B":0, "C":0, "D":0}
    COLUMN_RANK = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}

    with open('data/smallTwitter.json') as f:
        coords_data = []
        #parse line by line in the file and ignore any error show up
        for line in f:
            try:
                coords = {}
                data = json.loads(line[0:len(line) - 2])
                coords["lat"] = data["json"]["coordinates"]["coordinates"][0]
                coords["lng"] = data["json"]["coordinates"]["coordinates"][1]
                coords_data.append(coords)
            except Exception as inst:
                continue
    chunks = np.array_split(coords_data, size)
else:
    chunks = None


chunk = comm.scatter(chunks, root=0)

for data in chunk:    
    match_tweets_coordinates(MELB_GRID, data["lng"], data["lat"])

# Gather all of results from child process
result = comm.gather(MELB_GRID)

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
    # Summarize the count by taking first character from the box name
    ROW_GROUP = {"A": 0, "B": 0, "C": 0, "D": 0}
    for i in RESULT_GRID:
        ROW_GROUP[i[0:1]] = ROW_GROUP[i[0:1]] + RESULT_GRID[i]

    # Group by column
    # Summarize the count by taking second character from the box name
    COLUMN_GROUP = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
    for i in RESULT_GRID:
        COLUMN_GROUP[i[1:2]] = COLUMN_GROUP[i[1:2]] + RESULT_GRID[i]

    # Print all of the stuff here
    # Print rank by boxes
    print("\nRank based on boxes")
    GRID_RANKS = sorted(RESULT_GRID, key=RESULT_GRID.get, reverse=True)
    for i in GRID_RANKS:
        pprint('%s: %d tweets' % (i, RESULT_GRID[i]))

    # Print rank by rows
    print("\nOrder by rows")
    ROW_RANK = sorted(ROW_GROUP, key=ROW_GROUP.get, reverse=True)
    for val in ROW_RANK:
        pprint('%s-Row: %d' % (val, ROW_GROUP[val]))

    # Print rank by column
    print("\nOrder by columns")
    COLUMN_RANK = sorted(COLUMN_GROUP, key=COLUMN_GROUP.get, reverse=True)
    for val in COLUMN_RANK:
        pprint('Column %s: %d' % (val, COLUMN_GROUP[val]))

    #print the total time it takes
    total_minutes = time.time() - start_time
    minutes, seconds = divmod(total_minutes, 60)
    print("\nTotal time used for execution is %02d minutes and %02d seconds" %(minutes, seconds))
