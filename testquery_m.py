#!/usr/bin/env python

import sys
import random
import datetime
import uuid
import threading
import time
import mmap
import os
from multiprocessing import Process
from pymongo import MongoClient
from pymongo.errors import InvalidStringData
from bson.objectid import ObjectId


def QueryFunc(collection, i, insertnum, mm, numtopicids):
  count = 0
  while count < insertnum:
    topicidstart = random.randint(0, numtopicids-1)*24
    topicidsend = topicidstart + 24
    rand_topicid = mm[topicidstart:topicidsend]
    collection.find_one({"_id":ObjectId(rand_topicid)})
    count += 1
        
if __name__  == '__main__':
  arglen = len(sys.argv)
  addr = "localhost"
  threadnum = 4
  insertnum = 20000
  if arglen >= 4:
    addr = sys.argv[1]
    threadnum = int(sys.argv[2])
    insertnum = int(sys.argv[3])

  contentsrc = "objids_0108.txt"
  f = open(contentsrc, "r")
  mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
  fsize = os.path.getsize(contentsrc)
  numtopicids = fsize/24

  client = MongoClient(addr, 57017)
  db = client.topicdbsharded
  collection = db.topiccolsharded

  threads = []
  start_time = time.time()
  print "start time:", time.ctime(start_time)
  for i in range(threadnum):
    #curthread = QueryThread(collection, i, insertnum)
    curthread = Process(target=QueryFunc, args=(collection, i, insertnum, mm, numtopicids))
    curthread.start()
    threads.append(curthread)

  for t in threads:
    t.join()
  mm.close()
  print "main thread over"
  print "total ", time.time() - start_time, "seconds"
