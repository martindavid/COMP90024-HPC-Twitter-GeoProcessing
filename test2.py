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


if not MPI.Is_initialized():
    MPI.Init()

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def match_tweets_coordinates(melb_grid, lat, lng):
    """Match individual tweet coordinates with the coordinates in melbGrid.json"""
    """lat -> y, long -> x"""
    for grid_data in melb_grid:
        if (lat >= grid_data["ymin"] and lat <= grid_data["ymax"]) \
            and (lng >= grid_data["xmin"] and lng <= grid_data["xmax"]):
            grid_data["count"] = grid_data["count"] + 1

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

MELB_GRID = construct_melb_grid("data/melbGrid.json")

if rank == 0:
    with open('data/tinyTwitter.json') as f:
        PARSED_OBJ = ijson.items(f, 'item.json.coordinates.coordinates')
        i = 0
        count = 0
        myData = []
        for coordinates in PARSED_OBJ:
            coords = {}
            coords["lat"] = coordinates[0]
            coords["lng"] = coordinates[1]
            myData.append(coords)
    chunks = np.array_split(myData, size)
else:
    chunks = None

chunk = comm.scatter(chunks, root=0)

for data in chunk:    
    match_tweets_coordinates(MELB_GRID, data["lng"], data["lat"])

result = comm.allgather(MELB_GRID)

if rank == 0:
    result_grid = {
        "A1": 0,
        "A2": 0,
        "A3": 0,
        "A4": 0,
        "B1": 0,
        "B2": 0,
        "B3": 0,
        "B4": 0,
        "C1": 0,
        "C2": 0,
        "C3": 0,
        "C4": 0,
        "C5": 0,
        "D3": 0,
        "D4": 0,
        "D5": 0,
    }
    for grid_data in result:
        for single_grid_data in grid_data:
            result_grid[single_grid_data["id"]] = result_grid[single_grid_data["id"]] + single_grid_data["count"]

    pprint(result_grid)