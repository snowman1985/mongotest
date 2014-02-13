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
objids = "objids.txt"
f = open(objids, "r")
mm = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
fsize = os.path.getsize(objids)
print(fsize)
