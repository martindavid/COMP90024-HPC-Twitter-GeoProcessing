import sys
sys.path.append("lib/ijson-2.3")
import ijson
import json
from pprint import pprint
from mpi4py import MPI
import time


WORKTAG = 1
DIETAG = 0

def match_tweets_coordinates(melb_grid, result_grid, lat, lng):
    """Match individual tweet coordinates with the coordinates in melbGrid.json"""
    # lat -> y, long -> x
    for grid_data in melb_grid:
        if (lat >= grid_data["ymin"] and lat <= grid_data["ymax"]) \
                and (lng >= grid_data["xmin"] and lng <= grid_data["xmax"]):
            result_grid[grid_data["id"]] = result_grid[grid_data["id"]] + 1

def construct_melb_grid(file_name):
    """Parse melbGrid.json file and put the value inside dictionary"""
    melb_grid = []

    with open(file_name) as json_file:
        data = json.load(json_file)
        for val in data["features"]:
            grid_data = {}
            properties = val["properties"]
            grid_data["id"] = properties["id"]
            grid_data["xmax"] = properties["xmax"]
            grid_data["xmin"] = properties["xmin"]
            grid_data["ymin"] = properties["ymin"]
            grid_data["ymax"] = properties["ymax"]
            melb_grid.append(grid_data)
    return melb_grid

MELB_GRID = construct_melb_grid("data/melbGrid.json")

class TweetData(object):
    "Shareable Data"
    def __init__(self, data):
        self.tweets = data

    def get_data(self):
        "return tweet coordinate data"
        return self.tweets


def process_melb_grid(result_grid, new_grid):
    "compare temp melb_grid data and final result grid"
    for data in new_grid:
        result_grid[data["id"]] = result_grid[data["id"]] + data["count"]


def master(comm):
    "a master function"
    start_time = time.time()
    size = comm.Get_size()
    status = MPI.Status()
    flag = 2000
    result_grid = {
        "A1": 0, "A2": 0, "A3": 0, "A4": 0,
        "B1": 0, "B2": 0, "B3": 0, "B4": 0,
        "C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0,
        "D3": 0, "D4": 0, "D5": 0,
    }


    with open('data/smallTwitter.json') as json_file:
        parsed_obj = ijson.items(json_file, 'item.json.coordinates.coordinates')
        i = 1
        count = 0
        my_data = []
        grid = []
        for coordinates in parsed_obj:
            coords = {}
            coords["lat"] = coordinates[0]
            coords["lng"] = coordinates[1]
            my_data.append(coords)
            count = count + 1
            if count % flag == 0:
                tweet = TweetData(my_data)
                comm.send(tweet, dest=i, tag=WORKTAG)
                my_data = []
                count = 0
                if i < size - 1:
                    i = i + 1
                else:
                    i = 1
                result = comm.recv(source=MPI.ANY_SOURCE,
                                   tag=MPI.ANY_TAG, status=status)
                grid.append(result)

        if my_data:
            comm.send(tweet, dest=i, tag=WORKTAG)
            result = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            grid.append(result)

        for i in range(1, size):
            comm.send(0, dest=i, tag=DIETAG)

        for data in grid:
            for i in result_grid:
                result_grid[i] = result_grid[i] + data[i]

        # Group by Row
        row_group = {"A": 0, "B": 0, "C": 0, "D": 0}
        for i in result_grid:
            row_group[i[0:1]] = row_group[i[0:1]] + result_grid[i]

        # Group by column
        column_group = {"1": 0, "2": 0, "3": 0, "4": 0, "5": 0}
        for i in result_grid:
            column_group[i[1:2]] = column_group[i[1:2]] + result_grid[i]

        # Print all of the stuff here
        print(" ")
        print("Rank based on boxes")
        grid_ranks = sorted(result_grid, key=result_grid.get, reverse=True)
        for i in grid_ranks:
            pprint('%s: %d tweets' % (i, result_grid[i]))

        print(" ")
        print("Order by rows")
        row_rank = sorted(row_group, key=row_group.get, reverse=True)
        for val in row_rank:
            pprint('%s-Row: %d' % (val, row_group[val]))

        print(" ")
        print("Order by columns")
        column_rank = sorted(column_group, key=column_group.get, reverse=True)
        for val in column_rank:
            pprint('Column %s: %d' % (val, column_group[val]))

        #print the total time it takes
        total_minutes = time.time() - start_time
        minutes, seconds = divmod(total_minutes, 60)
        print("\n\n Total time used for execution is %02d minutes and %02d seconds"\
                %(minutes, seconds))


def slave(comm):
    "a function for slave process"
    status = MPI.Status()
    map_grid = {
        "A1": 0, "A2": 0, "A3": 0, "A4": 0,
        "B1": 0, "B2": 0, "B3": 0, "B4": 0,
        "C1": 0, "C2": 0, "C3": 0, "C4": 0, "C5": 0,
        "D3": 0, "D4": 0, "D5": 0,
    }
    while True:
        data = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        if status.Get_tag() == DIETAG:
            break
        tweet = data.get_data()
        for i in tweet:
            match_tweets_coordinates(MELB_GRID, map_grid, i["lng"], i["lat"])

        comm.send(map_grid, dest=0)


def main():
    " main function "
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()

    if rank == 0:
        master(comm)
    else:
        slave(comm)

if __name__ == '__main__':
    main()
