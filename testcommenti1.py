#!/usr/bin/env python

import sys
import random
import datetime
import uuid
import threading
import time
import mmap
import os
from pymongo import MongoClient
from pymongo.errors import InvalidStringData

#contentsrc = "kexueguairen.txt"

class InsertCommentThread(threading.Thread):
  def __init__(self, collection, number, insertnum):
    threading.Thread.__init__(self)
    self.collection = collection
    self.tid = number
    self.count = 0
  def run(self):
    while self.count < insertnum:
      cursor = self.collection.find({"teamid":random.randint(0, 1000000)})
      for c in cursor:
#        print "insert teamid:", c["teamid"], "   id:", c["_id"]
  	commentsize = len(c["commentList"])
 	comment = {
	  "id": commentsize,
	  "userid":random.randint(0, 1000000),
	  "content": "This is my " + str(commentsize) + " comment , welcome mongodbtest",
	  "createtime":datetime.datetime.now(),
	  "parentid": commentsize
	}
        self.collection.update({"_id":c["_id"]}, {"$push": {"commentList": comment}}) 
      self.count += 1
#    print "thread-", self.tid, " over"
        

arglen = len(sys.argv)
addr = "localhost"
threadnum = 4
insertnum = 100000
if arglen >= 4:
  addr = sys.argv[1]
  threadnum = int(sys.argv[2])
  insertnum = int(sys.argv[3])
  


client = MongoClient(addr, 57017)
db = client.topicdbreplsharded
collection = db.topiccol

threads = []
start_time = time.time()
print "start time:", time.ctime(start_time)
for i in range(threadnum):
  curthread = InsertCommentThread(collection, i, insertnum)
  curthread.start()
  threads.append(curthread)

for t in threads:
  t.join()
print "main thread over"
print "total ", time.time() - start_time, "seconds"
