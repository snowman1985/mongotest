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
        "commentList":[{
          "id": 0,
          "userid": 0,
          "content": mm[contentstart:contentstart+2048],
          "createtime": datetime.datetime.now(),
          "parentid": 0
        }],
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

def InsertFunc(collection, i, insertnum, mm, fsize):
  count = 0
  while count < insertnum:
    post = GenerateTopicPost(mm, fsize)
    try:
      collection.insert(post)
    except InvalidStringData:
      continue
    count += 1

def InsertCommentFunc(collection, i, insertnum, mm, fsize):
  count = 0
  while count < insertnum:
    cursor = collection.find({"teamid":random.randint(0, 1000000)})
    for c in cursor:
#   print "insert teamid:", c["teamid"], "   id:", c["_id"]
      commentsize = 1024
      comment = {
        "id": commentsize,
        "userid":random.randint(0, 1000000),
        "content":'I am a fake commence',
        "createtime":datetime.datetime.now(),
        "parentid": commentsize
      }
      collection.update({"_id":c["_id"]}, {"$pull": {"commentList": {"id":{"$gt":0}}}})
    count += 1
  
def RemoveAllCommentFunc(collection):
  count = 0
  cursor = collection.find()
  for c in cursor:
    collection.update({"_id":c["_id"]}, {"$pull": {"commentList":{}}})
    count += 1
      
if __name__  == '__main__':
  addr = '172.18.17.14'
  client = MongoClient(addr, 57017)
  db = client.topicdbph
  collection = db.topiccolph

  start_time = time.time()
  print "start time:", time.ctime(start_time)
  RemoveAllCommentFunc(collection)
  print "total ", time.time() - start_time, "seconds"
