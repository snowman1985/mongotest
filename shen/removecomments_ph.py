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

def RemoveCommentFunc(collection, i, threadnum):
  teamidstart = (1000000/threadnum)*i
  teamidend = teamidstart + (1000000/threadnum)
  for teamid in range(teamidstart, teamidend):
    cursor = collection.find({"teamid":teamid})
    for c in cursor:
      collection.update({"_id":c["_id"]}, {"$pull": {"commentList": {"id":{"$gt":-1}}}})
        
if __name__  == '__main__':
  arglen = len(sys.argv)
  addr = "localhost"
  threadnum = 4
  insertnum = 20000
  if arglen >= 4:
    addr = sys.argv[1]
    threadnum = int(sys.argv[2])
    insertnum = int(sys.argv[3])

  client = MongoClient(addr, 57017)
  db = client.topicdbph
  collection = db.topiccolph

  threads = []
  start_time = time.time()
  print "start time:", time.ctime(start_time)

  for i in range(threadnum):
    curthread = Process(target=RemoveCommentFunc, args=(collection, i, threadnum))
    curthread.start()
    threads.append(curthread)

  for t in threads:
    t.join()
  print "main thread over"
  print "total ", time.time() - start_time, "seconds"
