var Sentinel = require('../lib/redis-sentinel');
var async = require('async');
var should = require('should');

// Test setup:
// sentinel named mymaster running on localhost:26379 and localhost:26380, master running on localhost:6379, slave running on localhost:6380

describe('redis-sentinel module', function() {
    var testPrefix = "__redis_sentinel_test";
    var sentinel;
    var master, slave, sub;
    var randomText;
    var receivedCb = null;

    describe('basic functionality', function() {
        it('should init', function(done) {
            sentinel = new Sentinel("mymaster", [{host: 'localhost', port: '26379'}, {host: 'localhost', port: '26380'}], {debug: true, pollUpdateSlaveEndpointsInterval: 10000, retryUnfulfilledCommands: false});
            sentinel.init(done);
        });
        it('should have 6379 as master', function(done) {
            master = sentinel.getMasterClientById('master');
            should(master.address).equal('127.0.0.1:6379');
            done();
        });
        it('should have 6380 as slave', function(done) {
            slave = sentinel.getReadClientById('slave');
            should(slave.address).equal('127.0.0.1:6380');

            sub = sentinel.getReadClientById('sub');
            should(sub.address).equal('127.0.0.1:6380');

            done();
        });
        it('set/get on master should work', function(done) {
            randomText = (new Date()).toISOString() + "_" + Math.floor(Math.random() * 10000);
            master.set(testPrefix + "_w", randomText);
            master.get(testPrefix + "_w", function(err, res) {
                should(err).equal(null);
                should(res).equal(randomText);
                done();
            });
        });

        it('subscribe on sub should work', function(done) {
            sub.on("message", function (channel, message) {
                if (channel == testPrefix + "_c") {
                    if (receivedCb) receivedCb(message);
                }
            });

            setTimeout(function() {
                sub.subscribe(testPrefix + "_c", done);
            }, 500);
        });

        it('publish should be received', function(done) {
            var timeout = setTimeout(function() {
                done("timeout");
            }, 5000);

            receivedCb = function(message) {
                clearTimeout(timeout);
                try {
                    should(message).equal(randomText);
                } catch(e) {
                    return done(e);
                }
                done();
            };

            master.publish(testPrefix + "_c", randomText);
        });
    });

    describe('force slave change', function() {
        after(function(done) {
            setTimeout(done, 8000);
        });
        it('sleep slave should force slave down', function(done) {
            var f = false;
            sentinel.onReadChange(function() {
                if (!f) {
                    f = true;
                    done();
                }
            });
            slave.debug("sleep",7);
        });

        it('should result in 6379 as slave', function(done) {
            should(slave.address).equal('127.0.0.1:6379');
            done();
        });

        it('should result in 6379 as sub', function(done) {
            should(sub.address).equal('127.0.0.1:6379');
            done();
        });

        it('slave should still get the previously set data', function(done) {
            slave.get(testPrefix + "_w", function(err, res) {
                should(err).equal(null);
                should(res).equal(randomText);
                done();
            });
        });

        it('publish should still work', function(done) {
            var timeout = setTimeout(function() {
                done("timeout");
            }, 5000);

            receivedCb = function(message) {
                clearTimeout(timeout);
                try {
                    should(message).equal(randomText);
                } catch(e) {
                    return done(e);
                }
                done();
            };

            master.publish(testPrefix + "_c", randomText);
        });

        it('slave should be gone', function() {
            should(sentinel.getSlaveEndpoints().length).equal(0);
        });

    });

    describe('check after slave is back', function() {
        it('should be back', function() {
            should(sentinel.getSlaveEndpoints().length).equal(1);
            should(sentinel.getSlaveEndpoints()[0].port).equal('6380');
            should(sentinel.getSlaveEndpoints()[0].disconnected).equal(false);
        });

        it('slave should still get the previously set data', function(done) {
            slave.get(testPrefix + "_w", function(err, res) {
                should(err).equal(null);
                should(res).equal(randomText);
                done();
            });
        });

        it('publish should still work', function(done) {
            var timeout = setTimeout(function() {
                done("timeout");
            }, 5000);

            receivedCb = function(message) {
                clearTimeout(timeout);
                try {
                    should(message).equal(randomText);
                } catch(e) {
                    return done(e);
                }
                done();
            };

            master.publish(testPrefix + "_c", randomText);
        });

    });

    describe('force master change', function() {
        after(function(done) {
            setTimeout(done, 8000);
        });

        it('pause master client should force failover', function(done) {
            sentinel.onMasterChange(function() {
                done();
            });
            master.debug("sleep", 7);
        });

        it('should result in 6380 as master', function(done) {
            should(master.address).equal('127.0.0.1:6380');
            done();
        });

        it('slave should stay 6380', function(done) {
            should(slave.address).equal('127.0.0.1:6380');
            done();
        });

        it('master should still get the previously set data', function(done) {
            master.get(testPrefix + "_w", function(err, res) {
                should(err).equal(null);
                should(res).equal(randomText);
                done();
            });
        });

        it('master set/get should work', function(done) {
            randomText = (new Date()).toISOString() + "_" + Math.floor(Math.random() * 10000);
            master.set(testPrefix + "_w", randomText);
            master.get(testPrefix + "_w", function(err, res) {
                should(err).equal(null);
                should(res).equal(randomText);
                done();
            });
        });

        it('publish should still work', function(done) {
            var timeout = setTimeout(function() {
                done("timeout");
            }, 5000);

            receivedCb = function(message) {
                clearTimeout(timeout);
                try {
                    should(message).equal(randomText);
                } catch(e) {
                    return done(e);
                }
                done();
            };

            master.publish(testPrefix + "_c", randomText);
        });
    });
});

