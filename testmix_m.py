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


def GenerateTopicPost(mm, fsize):
  createtime = datetime.datetime.now()
  contentstart = random.randint(4, fsize-1)
  contentend = contentstart + random.randint(500,1000)
  if contentend >= fsize:
    contentend = fsize
  topicpost={
        "userid":random.randint(0,1000000),
        #"teamid":str(uuid.uuid4()),
        "teamid":random.randint(0,1000000),
        "createtime":createtime,
        "refreshtime":datetime.datetime.now(),
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


def QueryFunc(collection, i, insertnum, mm, numtopicids):
  count = 0
  totinsertnum = insertnum*2
  while count < totinsertnum:
    topicidstart = random.randint(0, numtopicids-1)*24
    topicidsend = topicidstart + 24
    rand_topicid = mm[topicidstart:topicidsend]
    collection.find_one({"_id":ObjectId(rand_topicid)})
    count += 1

def InsertFunc(collection, i, insertnum, mm, numtopicids, fsize):
  count = 0
  while count < insertnum:
    post = GenerateTopicPost(mm, fsize)
    try:
      collection.insert(post)
    except InvalidStringData:
      continue
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

  contentsrc = "shen/objids.txt"
  f = open(contentsrc, "r")
  mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
  fsize = os.path.getsize(contentsrc)
  numtopicids = fsize/24

  contentsrc1 = "cs.txt"
  f1 = open(contentsrc1, "r")
  mm1 = mmap.mmap(f1.fileno(), 0, prot=mmap.PROT_READ)
  fsize1 = os.path.getsize(contentsrc)

  client = MongoClient(addr, 57017)
  db = client.topicdbsharded
  collection = db.topiccolsharded

  threads = []
  start_time = time.time()
  print "start time:", time.ctime(start_time)
  for i in range(threadnum*2/3):
    curthread = Process(target=QueryFunc, args=(collection, i, insertnum, mm, numtopicids))
    curthread.start()
    threads.append(curthread)

  for i in range(threadnum/3):
    curthread = Process(target=InsertFunc, args=(collection, i, insertnum, mm1, numtopicids, fsize))
    curthread.start()
    threads.append(curthread)

  for t in threads:
    t.join()
  mm.close()
  mm1.close()
  print "main thread over"
  print "total ", time.time() - start_time, "seconds"
