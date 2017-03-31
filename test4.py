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


if not MPI.Is_initialized():
    MPI.Init()

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()


with open('data/sample-twitter.json') as f:
    PARSED_OBJ = ijson.items(f, 'item.json.coordinates.coordinates')
    myData = []
    for coordinates in PARSED_OBJ:
        coords = {}
        coords["lat"] = coordinates[0]
        coords["lng"] = coordinates[1]
        myData.append(coords)


# tell the bastards that this is the data
for i in range(size):
	# print(rank," : rank and I is",i)
    if rank == i:
        dataInput = myData
        pprint(dataInput)
        chunks = [[] for _ in range(size)]
        for j, chunk in enumerate(dataInput):
            chunks[j % size].append(chunk)

dataInput = comm.scatter(chunks, 0)

for i in range(size):
    if rank == i:
         print('Rank %d' % rank)
         pprint(dataInput)
    comm.Barrier()

comm.gather(chunks, 0)