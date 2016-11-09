var redis = require('redis'),
    async = require('async'),
    _ = require('lodash');

function Sentinel(name, endpoints, options) {
    if (!options)
        options = {};

    if (!options.redisConnectOptions) {
        options.redisConnectOptions = {};
    }

    var debug = options.debug;
    var clusterName = name || 'mymaster';
    var sentinelEndpoints = endpoints || [];
    var redisConnectOptions = options.redisConnectOptions || {};
    var callbackMasterSwitched = [];
    var callbackReadSwitched = [];
    var retryUnfulfilledCommands = options.retryUnfulfilledCommands !== false;

    var defaultMergeOptions = options.defaultMergeOptions || {};

    // The endpoint of the (current) master in the cluster.
    var masterEndpoint = {};

    // The endpoints of the (current) up-to-date slaves in the cluster. A slave is up-to-date if its master-link-status is up.
    var slaveEndpoints = [];

    var pollUpdateSlaveEndpointsInterval = options.pollUpdateSlaveEndpointsInterval || 60000;

    // Polling will not stop before this timestamp in millis.
    var pollUpdateSlaveEndpointsUntil = 0;

    // The endpoint that's (currently) used by all read clients; could be a master of a slave endpoint.
    var readEndpoint = {};

    var activeSentinel;
    var sentinels = [];
    var sentinelPubSubs = [];

    // The actual assigned redis clients.
    var masterClients = {};
    var readClients = {};

    // Selects the endpoint to use for read (=slave) clients. Notice that a master may be returned, because it can be
    // used to read from as well.
    var selectReadEndpoint = options.selectReadEndpoint;

    this.init = function (callback) {
        if (typeof callback != 'function')
            throw new Error('init requires callback');

        var self = this;
        async.some(sentinelEndpoints, function(endpoint, cb) {
            self.initSentinel(endpoint, function(err, success) {
                cb(success);
            });
        }, function(success) {
            if (success === true) {
                var methods = [];
                methods.push(self.updateMasterEndpointFromSentinel.bind(self));
                methods.push(self.updateSlaveEndpointsFromSentinel.bind(self));
                async.series(methods, callback);
            } else {
                callback(new Error("Can't connect to any Sentinel endpoint."));
            }
        });
    };

    var sentinelRetryStrategy = function(options) {
        if (options.total_retry_time > 7 * 24 * 3600 * 1000) {
            // End reconnecting after a specific timeout and flush all commands with a individual error
            return new Error('Retry time exhausted');
        }

        // Try to reconnect every minute.
        return Math.min(options.attempt * 5000, 60000);
    };

    this.initSentinel = function (endpoint, callback) {
        if (!endpoint || !endpoint.host || !endpoint.port) {
            throw new Error('Sentinel endpoint " ' + endpoint + ' " is not valid');
        }

        // When one of the sentinels goes down, we keep retrying for a long time.
        var tempSentinel = this.createClient(endpoint, {retry_strategy: sentinelRetryStrategy});
        var tempPubSubRedis = this.createClient(endpoint, {retry_strategy: sentinelRetryStrategy});

        var self = this;
        tempPubSubRedis.on('message', function(channel, message) {
            // Prevent double messages.
            if (self.getActiveSentinel() === tempSentinel) {
                self.handleSentinelMessage(channel, message);
            }
        });

        this.getSentinels().push(tempSentinel);
        this.getSentinelPubSubs().push(tempPubSubRedis);

        var channels = ['+sdown', '-sdown', '+slave', '+switch-master'];
        var subs = [];
        for (var i = 0, n = channels.length; i < n; i++) {
            (function(channel) {
                subs.push(function(cb) {
                    tempPubSubRedis.send_command('subscribe', [channel], cb);
                });
            })(channels[i]);
        }

        var timeout = setTimeout(function() {
            self.log('timeout to sentinel ' + endpoint.host + ':' + endpoint.port);
            callback(null, false);
            timeout = null;
        }, 10000);

        async.series(subs, function(err) {
            if (timeout) {
                clearTimeout(timeout);
                if (!err) {
                    self.log('connection to sentinel ' + endpoint.host + ':' + endpoint.port);
                } else {
                    self.log('connection error, sentinel ' + endpoint.host + ':' + endpoint.port);
                }
                callback(null, !err);
            }
        });

    };

    this.getClusterName = function(){
        return clusterName;
    };

    this.getRedisConnectionOptions = function(){
        return redisConnectOptions;
    };

    this.getSentinels = function(){
        return sentinels;
    };

    this.getSentinelPubSubs = function(){
        return sentinelPubSubs;
    };

    this.getActiveSentinel = function() {
        if (activeSentinel && activeSentinel.connected) {
            return activeSentinel;
        }

        for (var i = 0, n = sentinels.length; i < n; i++) {
            if (sentinels[i].connected) {
                activeSentinel = sentinels[i];
                return activeSentinel;
            }
        }

        // When no more active sentinels are available: return null.
        return null;
    };

    this.updateMasterEndpointFromSentinel = function (callback) {
        if (typeof callback != 'function')
            throw new Error('getMasterEndpoint requires callback');

        var self = this;

        var activeSentinel = this.getActiveSentinel();
        if (!activeSentinel) {
            callback(new Error('no active sentinel'));
        }

        activeSentinel.send_command('sentinel', ['get-master-addr-by-name', clusterName], function (err, result) {
            if (err)
                return callback(err);

            if (!result || !(result instanceof Array) || result.length != 2) {
                return callback(new Error('No master found for cluster: ' + clusterName));
            }

            if (masterEndpoint.host !== result[0] && masterEndpoint.port !== result[1]) {
                masterEndpoint = {
                    host: result[0],
                    port: result[1]
                };
            }

            self.log('Found master', result[0], result[1]);
            callback(null);
        });
    };

    var pollingUpdateSlaveEndpoints = false;

    this.updateSlaveEndpointsFromSentinel = function (callback) {
        var hasPendingSlaves = false;

        var self = this;

        var activeSentinel = this.getActiveSentinel();
        if (!activeSentinel) {
            callback(new Error('no active sentinel'));
        }

        activeSentinel.send_command('sentinel', ['slaves', clusterName], function (err, result) {
            if (!err) {
                var newSlaveEndpoints = [];

                if (!result || !(result instanceof Array))
                    result = [];

                for (var i in result) {
                    var slaveEndpoint = self.getEndpointForSlave(result[i]);
                    if (!slaveEndpoint.disconnected) {
                        if (slaveEndpoint.masterLinkStatus) {
                            newSlaveEndpoints.push(slaveEndpoint);
                        } else {
                            hasPendingSlaves = true;
                            self.log('Ignore slave', slaveEndpoint);
                        }
                    }
                }

                self.log('Found slaves', newSlaveEndpoints);

                var changes = (!_.isEqual(newSlaveEndpoints, slaveEndpoints));

                slaveEndpoints = newSlaveEndpoints;

                if (changes || !readEndpoint.host) {
                    // Allow to select a new read endpoint.
                    self.selectReadEndpoint();
                }
            }

            if (err || hasPendingSlaves || ((new Date()).getTime() < pollUpdateSlaveEndpointsUntil)) {
                // Poll as long as there are still pending slaves (connected but syncing).

                if (pollingUpdateSlaveEndpoints) {
                    // Prevent double polling.
                    clearTimeout(pollingUpdateSlaveEndpoints);
                }

                self.log('Poll again after ' + pollUpdateSlaveEndpointsInterval + 'ms');

                pollingUpdateSlaveEndpoints = setTimeout(function() {
                    pollingUpdateSlaveEndpoints = null;
                    self.updateSlaveEndpointsFromSentinel();
                }, pollUpdateSlaveEndpointsInterval);
            } else {
                self.log('End polling loop');
            }

            if (callback) {
                callback(err);
            }
        });
    };

    this.getMasterEndpoint = function() {
        return masterEndpoint;
    };

    this.getSlaveEndpoints = function() {
        return slaveEndpoints;
    };

    this.isMasterEndpoint = function(host, port) {
        return masterEndpoint.host === host && masterEndpoint.port === port;
    };

    this.isSlaveEndpoint = function(host, port) {
        for (var i = 0, n = slaveEndpoints.length; i < n; i++) {
            if (slaveEndpoints[i].host === host && slaveEndpoints[i].port === port) {
                return true;
            }
        }
        return false;
    };

    this.isUpToDateEndpoint = function(host, port) {
        return this.isMasterEndpoint(host, port) || this.isSlaveEndpoint(host, port);
    };

    this.isReadEndpoint = function(host, port) {
        return readEndpoint.host === host && readEndpoint.port === port;
    };

    this.selectReadEndpoint = function() {
        var newReadEndpoint = this.getNewReadEndpoint();
        if (newReadEndpoint.host !== readEndpoint.host || newReadEndpoint.port !== readEndpoint.port) {
            this.switchReadEndpoint(newReadEndpoint.host, newReadEndpoint.port);
        }
    };

    this.getNewReadEndpoint = function() {
        if (!masterEndpoint) {
            // If there is no master endpoint, then the cluster is down and we shouldn't select any read endpoint.
            return null;
        }

        if (selectReadEndpoint) {
            return selectReadEndpoint(masterEndpoint, slaveEndpoints, readEndpoint);
        } else {
            // Default.
            if (readEndpoint && this.isUpToDateEndpoint(readEndpoint.host, readEndpoint.port)) {
                // Never switch when already having a working endpoint.
                return readEndpoint;
            } else {
                // Just select one randomly.
                if (slaveEndpoints.length) {
                    return slaveEndpoints[Math.floor(slaveEndpoints.length * Math.random())];
                } else {
                    return masterEndpoint;
                }
            }
        }
    };

    this.getMasterClients = function(){
        return masterClients;
    };

    this.getMasterClientById = function(id, overruleOptions){
        if (!id)
            throw new Error('id is required to get a slave client');

        if(!masterClients[id]) {
            masterClients[id] = this.createClient(masterEndpoint, overruleOptions, {retry_unfulfilled_commands: retryUnfulfilledCommands});
        }

        return masterClients[id];
    };

    this.getReadClients = function(){
        return readClients;
    };

    this.getReadClientById = function(id){
        if(!id)
            throw new Error('id is required to get a read client');

        if (!readClients[id])
            return this.createReadClient(id, readEndpoint);

        return readClients[id];
    };

    this.createReadClient = function(id, overruleOptions){
        if (!readEndpoint) {
            throw new Error("no read endpoint available");
        }
        readClients[id] = this.createClient(readEndpoint, overruleOptions, {retry_unfulfilled_commands: retryUnfulfilledCommands});
        return readClients[id];
    };

    this.createClient = function (redisObj, overruleOptions, mergeOptions) {
        var self = this;
        if (!redisObj.port || !redisObj.host)
            throw new Error('createClient requires object with host and port');

        var options = _.cloneDeep(overruleOptions || this.getRedisConnectionOptions());
        if (mergeOptions) {
            options = _.merge(options, mergeOptions);
        }

        if (defaultMergeOptions) {
            options = _.merge(options, defaultMergeOptions);
        }

        var temp = redis.createClient(redisObj.port, redisObj.host, options);
        temp.on('reconnecting', function (err, res) {
            self.log(redisObj.port, redisObj.host, ' is reconnecting');
        });

        temp.on('error', function (err, res) {
            self.log('redis connection error', err, res);
        });
        return temp;
    };

    this.handleSentinelMessage = function (channel, msg) {
        if (!msg.match(this.getClusterName())) {
            return this.log('Got a message for a different cluster. My cluster: ' + thisgetClusterName(), msg);
        }

        this.log('sentinel message received', channel, msg);
        switch (channel) {
            case '-sdown':
                if (msg.match('slave'))
                    this.handleSlaveUp(msg);
                break;
            case '+sdown':
                if (msg.match('slave'))
                    this.handleSlaveDown(msg);
                break;
            case '+slave':
                this.handleAddSlave(msg);
                break;
            case '+switch-master':
                this.handleSwitchMaster(msg);
                break;
        }
    };

    this.handleSlaveDown = function (sentinelMsg) {
        var hostDown = this.parseSlaveMessage(sentinelMsg);
        if (!hostDown)
            return this.log('Incorrect message from sentinel');

        // Remove from list of up-to-date slaves.
        this.removeFromSlaveEndpoints(hostDown.host, hostDown.port);

        if (this.isReadEndpoint(hostDown.host, hostDown.port)) {
            // Slave is down. Change to another one immediately.
            this.selectReadEndpoint();
        }
    };

    this.removeFromSlaveEndpoints = function(h, p) {
        // Remove from slave endpoints.
        slaveEndpoints = slaveEndpoints.filter(function(slaveEndpoint) {
            return (h !== slaveEndpoint.host) || (p !== slaveEndpoint.port);
        });
    };

    this.handleSlaveUp = function (sentinelMsg) {
        var hostDown = this.parseSlaveMessage(sentinelMsg);
        if (!hostDown)
            return this.log('Incorrect message from sentinel');

        // Get new list with slaves to confirm when the new slave is up.
        // Give some time for the slave to go out of 'disconnected' state.
        pollUpdateSlaveEndpointsUntil = (new Date()).getTime() + 10000;
        this.updateSlaveEndpointsFromSentinel();
    };

    this.handleAddSlave = function (sentinelMsg) {
        var hostDown = this.parseSlaveMessage(sentinelMsg);
        if (!hostDown)
            return this.log('Incorrect message from sentinel');

        // Get new list with slaves to confirm when the new slave is up.
        pollUpdateSlaveEndpointsUntil = (new Date()).getTime() + 10000;
        this.updateSlaveEndpointsFromSentinel();
    };

    this.handleSwitchMaster = function (sentinelMsg) {
        var splitted = sentinelMsg.split(' ');
        if (!splitted[3] || !splitted[4])
            return console.error('Switch master getting incorrect parameters', splitted);

        this.switchMasterEndpoint(splitted[3], splitted[4]);
    };

    this.switchMasterEndpoint = function(h, p) {
        if (this.isMasterEndpoint(h, p)) return;

        var readIsMaster = this.isReadEndpoint(masterEndpoint.host, masterEndpoint.port);

        masterEndpoint.host = h;
        masterEndpoint.port = p;

        this.log('Master down, switching master to ', h, p);

        var masters = this.getMasterClients();
        for(var i in masters){
            this.switchRedisConnection(masters[i], h, p);
        }

        if (readIsMaster) {
            // If read endpoint is master, then switch to new master as well ASAP.
            this.switchReadEndpoint(h, p);
        }

        // Remove from slave endpoints.
        this.removeFromSlaveEndpoints(h, p);

        this.masterChanged();

        // Re-check the slave endpoints. Because of the master change, some slaves may be syncing and are no longer up to date.
        pollUpdateSlaveEndpointsUntil = (new Date()).getTime() + 10000;
        this.updateSlaveEndpointsFromSentinel();
    };

    this.switchReadEndpoint = function(h, p){
        if (this.isReadEndpoint(h, p)) return;

        readEndpoint.host = h;
        readEndpoint.port = p;

        this.log('Switching read clients to ', readEndpoint.host, readEndpoint.port);
        var readClients = this.getReadClients();
        for(var i in readClients){
            this.switchRedisConnection(readClients[i], readEndpoint.host, readEndpoint.port);
        }

        this.readChanged();
    };

    this.masterChanged = function(){
        for(var i in callbackMasterSwitched)
            callbackMasterSwitched[i]();
    };

    this.readChanged = function(){
        for(var i in callbackReadSwitched)
            callbackReadSwitched[i]();
    };

    this.onMasterChange = function(method){
        if(typeof method != 'function')
            throw new Error('onMasterChange requires function');

        callbackMasterSwitched.push(method);
    };

    this.onReadChange = function(method){
        if(typeof method != 'function')
            throw new Error('onReadChange requires function');

        callbackReadSwitched.push(method);
    };

    this.log = function () {
        if (debug) {
            var params = ['redis-sentinel'].concat(Array.prototype.slice.call(arguments));
            console.log.apply(console, params);
        }
    };

    this.switchRedisConnection = function(redisClient, host, port) {
        if (redisClient.host == host && redisClient.port == port) {
            return this.log('client already switched to', host, port);
        }

        this.log('switch ' + redisClient.address + ' to: [' + host + ":" + port + "]");

        redisClient.address = host + ':' + port;
        var co = this.getConnectionOptions(redisClient);
        co.host = host;
        co.port = port;
        if (redisClient.options && redisClient.options.host) {
            redisClient.options.host = host;
            redisClient.options.port = port;
        }

        // We destroy the stream to make sure we don't get reponses for old packets.
        redisClient.stream.destroy();
    };

    this.getConnectionOptions = function(redisClient) {
        return redisClient.connectionOption || redisClient.connection_options;
    };

    this.quit = function() {
        // Quit all individual Redis clients.
        var sentinels = this.getSentinels();
        for (var i = 0; i < sentinels.length; i++) {
            sentinels[i].quit();
        }

        var sentinelPubSubs = this.getSentinelPubSubs();
        for (i = 0; i < sentinelPubSubs.length; i++) {
            sentinelPubSubs[i].quit();
        }

        var masterClients = this.getMasterClients();
        for (i in masterClients) {
            masterClients[i].quit();
        }

        var readClients = this.getReadClients();
        for (i in readClients) {
            readClients[i].quit();
        }

    };

    this.getEndpointForSlave = function(sentinelResult) {
        var obj = {};
        var ipIndex = sentinelResult.indexOf('ip');
        var portIndex = sentinelResult.indexOf('port');

        if (portIndex == -1 || ipIndex == -1)
            throw new Error('Ip or port not found for slave');

        obj.port = sentinelResult[portIndex + 1];
        obj.host = sentinelResult[ipIndex + 1];
        obj.disconnected = (sentinelResult[9].match('disconnected') || sentinelResult[9].match('s_down')) ? true : false;

        // Check for master link status.
        obj.masterLinkStatus = true;
        var i = sentinelResult.indexOf('master-link-status');
        if (i >= 0) {
            obj.masterLinkStatus = sentinelResult[i + 1] !== 'err';
        }

        return obj;
    };

    this.parseSlaveMessage = function(msg) {
        var splitted = msg.split(' ');

        if (!splitted[2] || !splitted[3]) {
            console.error('handleSlaveDown getting incorrect parameters', splitted);
            return false;
        }

        return {
            port: splitted[3],
            host: splitted[2]
        }
    };

}

module.exports = Sentinel;

