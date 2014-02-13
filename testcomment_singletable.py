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
from bson.objectid import ObjectId

#contentsrc = "kexueguairen.txt"
contentsrc = "shen/objids.txt"
f = open(contentsrc, "r")
mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
fsize = os.path.getsize(contentsrc)
numtopicids = fsize/24

def GenerateComment():
  createtime = datetime.datetime.now()
  topicidstart = random.randint(0, numtopicids-1)*24
  topicidsend = topicidstart + 24
  comment={
     "id" : ObjectId(),
     "topic_id" : mm[topicidstart:topicidsend],
     "userid":random.randint(0, 1000000),
     "content":"This is my comment please test insert comment performance",
     "createtime":datetime.datetime.now(),
     "parentid":0
  }
  return comment

class InsertThread(threading.Thread):
  def __init__(self, collection, number, insertnum):
    threading.Thread.__init__(self)
    self.collection = collection
    self.tid = number
    self.count = 0
  def run(self):
    while self.count < insertnum:
      post = GenerateComment()
      try:
        self.collection.insert(post)
      except InvalidStringData:
        #print "one invalid insert: utf-8 reason: thread-", self.tid
	continue
      self.count += 1
#    print "thread-", self.tid, " over"
        

arglen = len(sys.argv)
addr = "localhost"
threadnum = 4
insertnum = 20000
if arglen >= 4:
  addr = sys.argv[1]
  threadnum = int(sys.argv[2])
  insertnum = int(sys.argv[3])
  


client = MongoClient(addr, 27017)
#db = client.commentdbsharded
db = client.commentdbindex
#collection = db.shardtest1
collection = db.commentcol

threads = []
start_time = time.time()
print "start time:", time.ctime(start_time)
for i in range(threadnum):
  curthread = InsertThread(collection, i, insertnum)
  curthread.start()
  threads.append(curthread)

for t in threads:
  t.join()
mm.close()
print "main thread over"
print "total ", time.time() - start_time, "seconds"
