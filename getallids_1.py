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

objidfile = "objids_0111.txt"
f = open(objidfile, "w")

arglen = len(sys.argv)
addr = "172.18.17.14"
client = MongoClient(addr, 27017)
db = client.topicdbsharded5
collection = db.topiccol

ids = []
for topicid in collection.find(fileds=[], limit=5000000):
    f.write(str(topicid['_id']))
f.close()
