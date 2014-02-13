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

objidfile = "objids_repl2.txt"
f = open(objidfile, "w")

arglen = len(sys.argv)
addr = "localhost"
client = MongoClient(addr, 57017)
db = client.topicdbsharded
collection = db.topiccolsharded

ids = []
for topicid in collection.find(fileds=[], limit=10000000):
  ids.append(str(topicid['_id']))
f.writelines(ids)
f.close()
