#!/usr/bin/env python

import sys
import random
import datetime
import uuid
import threading
import time
import mmap
import os
import multiprocessing
from multiprocessing import Process
from multiprocessing import Pool
from multiprocessing import Queue
import pymongo
from pymongo.read_preferences import ReadPreference
from pymongo import MongoClient
from pymongo import MongoReplicaSetClient
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

def SayHello(addr, i):
  #client = MongoClient(addr, 57017)
  #db = client.topicdbsharded
  #collection = db.topiccolsharded
  #collection.insert({"mustbedelete":-7, "teamid":7})
  
  print "hello"


#def QueryFunc(addr, i, insertnum, mm, numtopicids):
def QueryFunc(addr, insertnum):
  print "query process"
  contentsrc = "objids_0116.txt"
  f = open(contentsrc, "r")
  mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
  fsize = os.path.getsize(contentsrc)
  numtopicids = fsize/24
  addrnew = addr + ":29002"
  #client = MongoReplicaSetClient(addrnew, replicaSet="rs0")
  client = MongoClient(addr, 27017)
  db = client.topicdb
  collection = db.topiccol
  count = 0
  totinsertnum = insertnum*3
  while count < totinsertnum:
    topicidstart = random.randint(0, numtopicids-1)*24
    topicidsend = topicidstart + 24
    rand_topicid = mm[topicidstart:topicidsend]
    try:
    #collection.find_one({"_id":ObjectId(rand_topicid)}, read_preference=ReadPreference.SECONDARY_PREFERRED)
      collection.find_one({"_id":ObjectId(rand_topicid)}, read_preference=ReadPreference.SECONDARY_PREFERRED)
      #print "q id:", rand_topicid
      #collection.find_one({"_id":ObjectId(rand_topicid)})
    except pymongo.errors.AutoReconnect:
      print "auto reconnect"
      time.sleep(2)
      continue
    count += 1
  mm.close()

def InsertFunc(addr, insertnum):
  contentsrc = "cs.txt"
  f = open(contentsrc, "r")
  mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
  fsize = os.path.getsize(contentsrc)
  addrnew= addr + ":29002"
  #client = MongoReplicaSetClient(addrnew, replicaSet="rs0")
  client = MongoClient(addr, 27017)
  db = client.topicdb
  collection = db.topiccol
  count = 0
  while count < insertnum:
    post = GenerateTopicPost(mm, fsize)
    try:
      collection.insert(post)
    except InvalidStringData:
      continue
    except pymongo.errors.AutoReconnect:
      print "insert auto reconnect", time.ctime(time.time())
      time.sleep(5)
      print "sleep over", time.ctime(time.time())
      continue
      print "continue:", count
    except Exception, e:
      print e
    count += 1
  print "insert process over"
  mm.close()
    
        
if __name__  == '__main__':
  arglen = len(sys.argv)
  addr = "localhost"
  threadnum = 4
  insertnum = 20000
  if arglen >= 4:
    addr = sys.argv[1]
    threadnum = int(sys.argv[2])
    insertnum = int(sys.argv[3])
  

  pool = Pool(threadnum*2)

  threads = []
  start_time = time.time()
  print "start time:", time.ctime(start_time)
  #pool = Pool(threadnum*2/3 + threadnum/3)
  for i in range(threadnum):
    #curthread = QueryThread(collection, i, insertnum)
    #curthread = Process(target=QueryFunc, args=(collection, i, insertnum, mm, numtopicids))
    #curthread.start()
    #threads.append(curthread)
    #pool.apply_async(QueryFunc, args=(addr, i, insertnum, mm, numtopicids))
    pool.apply_async(QueryFunc, args=(addr,insertnum))
    #pool.apply_async(QueryFunc, args=(client.topicdbsharded.topiccolsharded, i, insertnum, mm, numtopicids))
    #pool.apply_async(SayHello, args=(addr,i))

  for i in range(threadnum):
    #curthread = Process(target=InsertFunc, args=(collection, i, insertnum, mm1, numtopicids, fsize))
    #curthread.start()
    #threads.append(curthread)
    pool.apply_async(InsertFunc, args=(addr,insertnum))
    #pool.apply_async(InsertFunc, args=(addr, i, insertnum, numtopicids, fsize))

  #for t in threads:
  #  t.join()
  pool.close()
  pool.join()
  print "main thread over"
  print "total ", time.time() - start_time, "seconds"
