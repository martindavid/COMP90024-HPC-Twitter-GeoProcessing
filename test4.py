import sys
sys.path.append("lib/ijson-2.3")
import json
from pprint import pprint
import time
import itertools
import operator
from mpi4py import MPI
import numpy as np
import ijson


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


if rank == 0:
    with open('data/sample-twitter.json') as f:
        PARSED_OBJ = ijson.items(f, 'item.json.coordinates.coordinates')
        myData = []
        count = 0
        flag = 2
        for coordinates in PARSED_OBJ:
            coords = {}
            coords["lat"] = coordinates[0]
            coords["lng"] = coordinates[1]
            myData.append(coords)
            if count % flag == 0:
                comm.send(myData, dest=(rank+1)%size)


data = comm.recv(source=0)
print('Rank %d' % rank)
pprint(data)


if rank == 0:
    print("Finish process")

    # if count >= 1:
    #     pprint(myData)

    #     count = 0
    #     myData = []
    # count = count + 1
