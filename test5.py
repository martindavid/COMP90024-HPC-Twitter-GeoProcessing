import json
import csv
from pprint import pprint
from mpi4py import MPI

# MPI initializing
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# get the input file
# with open('miniTwitter.csv','r') as fileName:
with open('data/miniTwitter.csv', 'r') as fileName:
    csvReader = csv.reader(fileName)
    header = next(fileName)
    myList = []
    for line in csvReader:
        try:
            # line = line.encode('ascii','ignore').decode()
            nm = json.loads(line[4])
            myList.append(nm['text'].strip('"').lower())
        except:
            pass

# tell the bastards that this is the data
for i in range(size):
	#print(rank," : rank and I is",i)
    if rank == i:
        dataInput = myList
        pprint(dataInput)
        chunks = [[] for _ in range(size)]
        for j, chunk in enumerate(dataInput):
            chunks[j % size].append(chunk)

dataInput = comm.scatter(chunks, 0)
print(" ")
print('Rank %d' % rank)
pprint(dataInput)