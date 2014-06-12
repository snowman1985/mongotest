var mapTopic = function() {
    var userid = this.userid ? this.userid : 0;
    var teamid = this.teamid ? this.teamid : 0;
    emit({'userid': userid, 'teamid': teamid},
        {'topic': 1, 'comment': 0});
    for (var key in this.commentList) {
        var cuid = this.commentList[key].userid ? this.commentList[key].userid : 0;
        emit({'userid': cuid, 'teamid': teamid},
            {'topic': 0, 'comment': 1});
    }
};

var reduceTopic = function(key, emits) {
    var topicCount = 0;
    var commentCount = 0;
    for (var i in emits) {
        topicCount += emits[i].topic;
        commentCount += emits[i].comment;
    }
    return {'topic': topicCount, 'comment': commentCount}; };

db.runCommand({
    //'mapreduce': 'topic',
    'mapreduce': 'topiccol',
    //'query': {teamid: {$lt: 0}},
    'map': mapTopic,
    'reduce': reduceTopic,
    'out': {replace : "userid_count" }
});

