var mapTopic = function() {
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

var reduceTopic = function(key, emits) {
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

db.runCommand({
    'mapreduce': 'topic',
    //'query': {teamid: {$lt: 0}},
    'map': mapTopic,
    'reduce': reduceTopic,
    'out': {replace : "teamid_count" }
});

