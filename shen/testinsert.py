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
contentsrc = "cs.txt"
f = open(contentsrc, "r")
mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
fsize = os.path.getsize(contentsrc)

def GenerateTopicPost():
  createtime = str(datetime.datetime.now())
  contentstart = random.randint(4, fsize-1)
  contentend = contentstart + random.randint(500,1000)
  if contentend >= fsize:
    contentend = fsize 
  topicpost={
	"userid":random.randint(0,1000000),
	#"teamid":str(uuid.uuid4()),
	"teamid":random.randint(0,1000000),
	"createtime":createtime,
	"refreshtime":str(datetime.datetime.now()),
	"title":"topic post test",
	"content":mm[contentstart:contentend],
	"source":"web",
	"type":"talk",
	"originid":0,
 	"haslocation":"no",
	"location":"",
	"longitude":"",
	"latitude":"",
	"hastag":"yes",
	"tagList":["culture","financial","cloud","chanjet"],
	"status":"active",
	"commentCount":0,
	"commentList":[],
	"favCount":0,
	"favList":[],
	"collectionList":[],
	"fileCount":2,
	"fileList":[{"name":"attach1.txt", "size":100, "url":"http://www.a.com/a.txt",},
		    {"name":"attach2.txt", "size":200, "url":"http://www.b.com/b.txt"}],
	"imageCount":2,
	"imageList":[{"name":"img1.img", "size":100, "width":200, "height":200, "uri":"http://www.a.com/a.img"},
		     {"name":"img2.img", "size":100, "width":200, "height":300, "uri":"http://www.b.com/b.img"}],
	"voiteList":[{"options":["a","b","c"], "type":"single", "voteusers":["user1","user2"]}]
	
  }
  return topicpost

class InsertThread(threading.Thread):
  def __init__(self, collection, number, insertnum):
    threading.Thread.__init__(self)
    self.collection = collection
    self.tid = number
    self.count = 0
  def run(self):
    while self.count < insertnum:
      post = GenerateTopicPost()
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
  


client = MongoClient(addr, 57017)
db = client.topicdb
collection = db.topiccolsharded

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
