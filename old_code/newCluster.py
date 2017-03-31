from mpi4py import MPI
import csv
import re
import sys
import collections
from operator import itemgetter

#MPI initializing
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

#using regexs for finding the words and patterns
wordPattern = re.compile('[a-zA-Z]+[\w]*',re.IGNORECASE)
hashPattern = re.compile('[#]+[\w+]',re.IGNORECASE)
namePattern = re.compile('[@]+[\w+]',re.IGNORECASE)

#initializing empty dictionaries
wordDict = {}
namesDict = {}
hashDict = {}

def findWords(data):
    for row in data:
        for singleLine in row:
            singleLine = re.split("[\'|\"|:| |,|\n]", singleLine)

            for eachWord in singleLine:
                #for Word matching
                if wordPattern.match(eachWord):
                    if eachWord in wordDict:
                        wordDict[eachWord] += 1
                    else:
                        wordDict[eachWord] = 1
                #for username matching
                elif namePattern.match(eachWord):
                    if eachWord in namesDict:
                        namesDict[eachWord] += 1
                    else:
                        namesDict[eachWord] = 1
                #for hastag matching
                elif hashPattern.match(eachWord):
                    if eachWord in hashDict:
                        hashDict[eachWord] += 1
                    else:
                        hashDict[eachWord] = 1
                #default else
                else:
                    pass
    return (wordDict, hashDict, namesDict)

if rank == 0:
    dataInput = csv.reader(open('data/miniTwitter.csv','r'))
    chunks = [[]for _ in range(size)]
    for i, chunk in enumerate(dataInput):
        chunks[i%size].append(chunk)
else:
    dataInput = None
    chunks = None

dataInput = comm.scatter(chunks, root = 0)

for i in range(comm.size):
    if comm.rank == i:
        wordDict, hashDict, namesDict = findWords(dataInput)
    comm.barrier()

#if (rank == 0):
#print sorted dictionary
namesDict = sorted(namesDict.items(), key = itemgetter(1), reverse = True)
hashDict = sorted(hashDict.items(), key = itemgetter(1), reverse = True)
wordDict = sorted(wordDict.items(), key = itemgetter(1), reverse = True)

print ("Top 10 'Most tweets from a single username'\n")
count = 1
for key, value in namesDict[:10]:
    print ('%d -> %s : %d'%(count, key, value))
    count += 1

print ("\nTop 10 'Most hashtagged word'\n")
count = 1
for key, value in hashDict[:10]:
    print ('%d -> %s : %d'%(count, key, value))
    count += 1

for key, value in wordDict[:1]:
    print ('\nThe Most used word %s : %d'%(key, value))
