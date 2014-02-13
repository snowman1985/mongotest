#-*-coding:utf-8-*-
#import MySQLdb  
import os
import commands
import csv
import subprocess
import os
import re
import time
from datetime import datetime
import hashlib
#import mysql.connector  
import random
import pg
import psycopg2
import time
import psycopg2.extras
import  sys
import  tty, termios
fetches=0
rectime=0
hlprdescribe = raw_input("请输入本次测试场景的描述:\n")

atemp=[]
dir = "."
type = "log"

#files = os.listdir( dir )
#rr = re.compile( "\.%s$" %type , re.I )
#for f in files:
#    if rr.search(f):
       #print f    
#       file_object = open(f)
#all_the_text = file_object.read( )
#       hmstrtemp=(file_object.read( )).split('once')
#print "len hmstrtemp:", len(hmstrtemp)
#hmstrtemp=all_the_text.split('\n')
#       atemp=hmstrtemp
#       for hmtempdata in atemp:
  #print hmtempdata
#           dostringnew(hmtempdata)
#file_object.close()
       #hmtempdata=atemp[0]

#save data
def hlsave():
 describe=hlprdescribe
 conn = psycopg2.connect(host='172.18.17.51', port=5432, user='x3', password='x3!W654', database='x3test')
 print 'conndata'

 cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

 print "%%%%%%%%%%%%%%%%%"

#print clientnum
 try:
   cursor.execute('insert into t_hlpr_data(rectime,fetches,describe) values(%s,%s,%s)RETURNING id;',(rectime,fetches,describe))
   conn.commit()
   cursor.close()

   conn.close()
 except Exception,e:
    print e
    print 'inert record into table failed'
    #pgdb_logger.error("insert record into table failed,ret=%s" %e.args[0])   
#finally:



tartime='2013'
targetstring='code'
#by line by line
def dostringnew(targetstring):
      print targetstring
      atemp=[]
      tempstr=targetstring.split('\n')
      atemp=tempstr
      global rectime
      rectime=atemp[1]
      print  8888
      print atemp[1]
     # print rectime
      if (len(atemp)>9):
        codestr=atemp[8]
      #print codestr
        atemp=codestr.split()
        global fetches
       # print atemp
        print "len atemp:", len(atemp)
      #print "afadfad", atemp[0]
      if (len(atemp)>3):
        fetches=atemp[3]
    #    print 9999999
    #    print fetches
    #  else:
      hlsave()


#by once
#for hmtempdata in atemp: 
  #print hmtempdata
  #dostringnew(hmtempdata)

files = os.listdir( dir )
rr = re.compile( "\.%s$" %type , re.I )
for f in files:
    if rr.search(f):
       #print f    
       file_object = open(f)
#all_the_text = file_object.read( )
       hmstrtemp=(file_object.read( )).split('once')
#print "len hmstrtemp:", len(hmstrtemp)
#hmstrtemp=all_the_text.split('\n')
       atemp=hmstrtemp
       for hmtempdata in atemp:
  #print hmtempdata
           dostringnew(hmtempdata)
