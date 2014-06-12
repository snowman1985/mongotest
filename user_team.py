#!/usr/bin/env python
import os
from pymongo import MongoClient
from bson import Binary, Code
from bson.son import SON
import ConfigParser

config_path = os.path.abspath(os.path.dirname(__file__))
config_file = config_path + '/config.ini'
config = ConfigParser.ConfigParser()
config.readfp(open(config_file, "rb"))

mongoaddr = config.get("mongo", "host")
mongoport = int(config.get("mongo", "port"))
topicdbuser = config.get("mongo", "topicdb_user")
topicdbpwd = config.get("mongo", "topicdb_pwd")

client = MongoClient(mongoaddr, mongoport)
topicdb = client.topicdb
#topicdb.authenticate(topicdbuser, topicdbpwd)

teamid_mapper_str = '''
function() {
    var teamid = this.teamid ? this.teamid : 0;
    var ttime = this.createtime ? this.createtime : 0;
    emit({'teamid': teamid},
        {'topic': 1, 'comment': 0, 'ttime': ttime, 'ctime': 0});
    for (var key in this.commentList) {
        var ct = this.commentList[key].createtime ? this.commentList[key].createtime : 0;
        emit({'teamid': teamid},
            {'topic': 0, 'comment': 1, 'ttime': 0, 'ctime': ct});
    }
};
'''

teamid_reducer_str = '''
 function(key, emits) {
    var topicCount = 0;
    var commentCount = 0;
    var lastTopicTime = 0;
    var lastCommentTime = 0;
    for (var i in emits) {
        topicCount += emits[i].topic;
        commentCount += emits[i].comment;
        if ((emits[i].ttime !== null)&&(lastTopicTime < emits[i].ttime))
            lastTopicTime = emits[i].ttime;
        if ((emits[i].ctime !== null)&&(lastCommentTime < emits[i].ctime))
            lastCommentTime = emits[i].ctime;
    }
    return {'topic': topicCount, 'comment': commentCount,
        'ttime': lastTopicTime, 'ctime': lastCommentTime}; };
'''

teamid_mapper = Code(teamid_mapper_str)
teamid_reducer = Code(teamid_reducer_str)

topicdb.topic.map_reduce(teamid_mapper, teamid_reducer, out=SON([("replace", "teamid_count")]))

collection_teamid_count = topicdb.teamid_count

for item in collection_teamid_count.find():
  print item['_id'], item['value']['topic'], item['value']['comment'], item['value']['ttime'], item['value']['ctime']
