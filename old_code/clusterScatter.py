from mpi4py import MPI
import csv
import re
import sys
import collections
from operator import itemgetter
import json
import time
import datetime
from pprint import pprint

# get start time
startTime = time.time()
# MPI initializing
comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# using regexs for finding the words and patterns
wordPattern = re.compile('[a-zA-Z]+[\w]*', re.IGNORECASE)
hashPattern = re.compile('#+[\w_]+[\w\'_\-]*[\w_]+', re.IGNORECASE)
namePattern = re.compile('@+[\w_]', re.IGNORECASE)

# initializing empty dictionaries
wordDict = {}
namesDict = {}
hashDict = {}

# defining method to be invoked to find patterns corresponding
# to username, hashtags and terms in the csv


def findWords(dataInput):
    for singleLine in dataInput:
        singleLine = re.split("[\'|\"|:| |,|\n]", singleLine)

        for eachWord in singleLine:
            # for Word matching
            if wordPattern.match(eachWord):
                if eachWord in wordDict:
                    wordDict[eachWord] += 1
                else:
                    wordDict[eachWord] = 1
            # for username matching
            elif namePattern.match(eachWord):
                if eachWord in namesDict:
                    namesDict[eachWord] += 1
                else:
                    namesDict[eachWord] = 1
            # for hastag matching
            elif hashPattern.match(eachWord):
                if eachWord in hashDict:
                    hashDict[eachWord] += 1
                else:
                    hashDict[eachWord] = 1
            # default else
            else:
                pass
    return (wordDict, hashDict, namesDict)

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

#iterate throught the CSV file and get the bloody words into the dictionary
for i in range(size):
	if rank == i:
		wordDict, hashDict, namesDict = findWords(dataInput)
	comm.Barrier()

#gather every damn thing
comm.gather(chunks, comm)
#wordDict, hashDict, namesDict = findWords(newData)

#after gathering
namesDict = sorted(namesDict.items(), key = itemgetter(1), reverse = True)
hashDict = sorted(hashDict.items(), key = itemgetter(1), reverse = True)
wordDict = sorted(wordDict.items(), key = itemgetter(1), reverse = True)

#print the bloody thing
if(rank == 0):
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

#   word = raw_input("\nPlease input a word to know how many times it has been used: ")
    word = "India"
    count = 0
    for key, value in wordDict:
        if word.lower() == key:
            count = value
    print ("\'%s\' is used %s times"%(word,count))

#print the total time it takes
    totalMinutes = time.time() - startTime
    minutes, seconds = divmod(totalMinutes, 60)
    print("\n\n Total time used for execution is %02d minutes and %02d seconds"%(minutes, seconds))

