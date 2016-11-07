var Sentinel = require('../lib/redis-sentinel');
var async = require('async');
var should = require('should');

function selectReadEndpoint(masterEndpoint, slaveEndpoints, readEndpoint) {
    if (sentinel.isSlaveEndpoint(readEndpoint.host, readEndpoint.port)) {
        return readEndpoint;
    } else if (slaveEndpoints.length) {
        return slaveEndpoints[0];
    } else {
        return masterEndpoint;
    }
}

// Test setup:
// sentinel named mymaster running on localhost:26379 and localhost:26380, master running on localhost:6379, slave running on localhost:6380

// The basic idea is to read/write all the time, and finding out if the redisClient doesn't error out and returns the expected value.
// In the mean time, redis/sentinel servers can be brought down/up.
// If slaves are brought up, no downtime should ever exist.
// On master change, the master/read may have some downtime but it should be picked up quickly.
// On slave down, the read could have some downtime but should be picket up quickly.
// Subscriptions should continue to work just as read operations.

var sentinel = new Sentinel("mymaster", [{host: 'localhost', port: '26379'}, {host: 'localhost', port: '26380'}], {debug: true, pollUpdateSlaveEndpointsInterval: 10000, selectReadEndpoint: selectReadEndpoint});
sentinel.init(function(err) {
    if (err) {
        return console.error(err);
    }

    var testPrefix = "__redis_sentinel_test";

    // We write the timestamp.
    var timestampKey = testPrefix + "_ts";
    var channelName = testPrefix + "_c";
    var getTimestamp = function() {
        return (new Date()).getTime();
    };

    // Write test.
    var read = sentinel.getReadClientById('read');
    read.__name = 'read';
    var sub = sentinel.getReadClientById('sub');
    sub.__name = 'sub';
    var write = sentinel.getMasterClientById('write');
    write.__name = 'write';

    var lastWritePublishSuccess = 0;
    var lastSet = 0;
    var lastGet = 0;
    var lastChannelUp = 0;

    var lastSetError = 0;
    var lastGetError = 0;
    var lastWritePublishError = 0;

    var highestChannelUp = 0;
    sub.subscribe(channelName, function(err) {
        if (err) {
            return console.error('channel', err);
        }
    });
    sub.on('message', function(channel, message) {
        lastChannelUp = getTimestamp();

        var channelUp = parseInt(message);
        if (highestChannelUp > channelUp) {
            console.error('OUT-OF-SYNC SUB message received');
        }
        highestChannelUp = channelUp;
    });

    var highestGet = 0;
    setInterval(function() {
        write.publish(channelName, '' + getTimestamp(), function(err) {
            if (!err) lastWritePublishSuccess = getTimestamp();
            if (err) lastWritePublishError = getTimestamp();
        });

        write.set(timestampKey, '' + getTimestamp(), function(err) {
            if (!err) lastSet = getTimestamp();
            if (err) lastSetError = getTimestamp();
        });

        read.get(timestampKey, function(err, res) {
            if (!err) lastGet = getTimestamp();
            if (err) lastGetError = getTimestamp();

            var _get = parseInt(res);
            if (highestGet > _get) {
                console.error('OUT-OF-SYNC GET message received');
            }
            highestGet = _get;
        });
    }, 5);

    var ago = function(ts) {
        if (!ts) return '-';
        return (getTimestamp() - ts) + 'ms ago';
    };

    setInterval(function() {
        console.log('MASTER SET    : ' + ago(lastSet) + ' ' + ago(lastSetError));
        console.log('READ GET      : ' + ago(lastGet) + ' ' + ago(lastGetError));
        console.log('MASTER PUBLISH: ' + ago(lastWritePublishSuccess) + ' ' + ago(lastWritePublishError));
        console.log('READ RECEIVE  : ' + ago(lastChannelUp));
        console.log('SENTINEL 1    : ' + sentinel.getSentinels()[0].address, sentinel.getSentinels()[0].connected);
        console.log('SENTINEL 2    : ' + sentinel.getSentinels()[1].address, sentinel.getSentinels()[1].connected);
        console.log('SENTINEL ACTIV: ' + sentinel.getActiveSentinel().address);
    }, 5000);

});
