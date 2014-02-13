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

objidfile = "objids_0116.txt"
f = open(objidfile, "w")

arglen = len(sys.argv)
addr = "172.18.4.240"
client = MongoClient(addr, 27017)
db = client.topicdb
collection = db.topiccol

ids = []
for topicid in collection.find(fileds=[], limit=10000):
    f.write(str(topicid['_id']))
f.close()
