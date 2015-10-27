var redis = require('redis'),
    async = require('async');

function Sentinel(name, endpoints, options) {
    if (!options)
        options = {};

    if (!options.redisConnectOptions) {
        options.redisConnectOptions = {};
    }

    var debug = options.debug;
    var useSlave = options.useSlave;
    var clusterName = name || 'mymaster';
    var sentinelEndpoints = endpoints || [];
    var redisConnectOptions = options.redisConnectOptions || {};
    var callbackMasterSwitched = [];
    var callbackSlaveSwitched = [];
    var self = this;
    var masterAddress = {};
    var slaveEndpoints = [];
    var activeSentinel;
    var sentinels = [];
    var sentinelPubSubs = [];
    var masterClients = {};
    var slaveClients = {};


    this.onMasterChange = function(method){
        if(typeof method != 'function')
            throw new Error('onMasterChange requires function');

        callbackMasterSwitched.push(method);
    };

    this.onSlaveChange = function(method){
        if(typeof method != 'function')
            throw new Error('onSlaveChange requires function');

        callbackSlaveSwitched.push(method);
    };

    this.masterChanged = function(){
        for(var i in callbackMasterSwitched)
            callbackMasterSwitched[i]();
    };

    this.slaveChanged = function(){
        for(var i in callbackSlaveSwitched)
            callbackSlaveSwitched[i]();
    };

    this.getMasterAddress = function (callback) {
        if (typeof callback != 'function')
            throw new Error('getMasterAddress requires callback');

        this.getActiveSentinel().send_command('sentinel', ['get-master-addr-by-name', clusterName], function (err, result) {
            if (err)
                return callback(err);

            if (!result || !(result instanceof Array) || result.length != 2) {
                return callback(new Error('No master found for cluster: ' + clusterName));
            }

            masterAddress = {
                host: result[0],
                port: result[1]
            };

            callback(null, masterAddress);
            self.log('Found master', masterAddress);
        });
    };

    this.getSlaveAddresses = function (callback) {
        if (typeof callback != 'function')
            throw new Error('getSlaveAddresses requires callback');

        this.getActiveSentinel().send_command('sentinel', ['slaves', clusterName], function (err, result) {
            if (err)
                return callback(err);

            if (!result || !(result instanceof Array))
                return callback(null, []);


            for (var i in result)
                slaveEndpoints.push(getIpAndPortForSlave(result[i]));

            callback();
            self.log('Found slaves', slaveEndpoints);
        });
    };

    this.log = function () {
        if (debug)
            console.log(arguments);
    };

    this.init = function (callback) {
        if (typeof callback != 'function')
            throw new Error('init requires callback');

        var methods = [];
        for (var i in sentinelEndpoints)
            methods.push(this.initSentinel.bind(this, sentinelEndpoints[i]));

        methods.push(this.getMasterAddress.bind(this));
        methods.push(this.getSlaveAddresses.bind(this));
        async.series(methods, callback);
    };

    this.getMasterHost = function(){
        return masterAddress.host;
    };

    this.getMasterPort = function(){
        return masterAddress.port;
    };

    this.setMasterPort = function(p){
        masterAddress.port = p;
    };

    this.setMasterHost = function(h){
        masterAddress.host = h;
    };

    this.getClusterName = function(){
        return clusterName;
    };

    this.getRedisConnectionOptions = function(){
        return redisConnectOptions;
    };

    this.getMasterClients = function(){
        return masterClients;
    };

    this.getMasterClientById = function(id){
        if (!id)
            throw new Error('id is required to get a slave client');

        if(!masterClients[id])
            masterClients[id] = this.createClient(masterAddress);

        return masterClients[id];
    };

    this.getSlaveClients = function(){
        return slaveClients;
    };

    this.getSlaveClientById = function(id){
        if(!id)
            throw new Error('id is required to get a slave client');

        if(!useSlave)
            throw new Error('Slaves not enabled. Set option useSlave: true');

        if (!slaveClients[id])
            return this.setSlaveByEndpoint(id, this.getNextSlaveHost());

        return slaveClients[id];
    };

    this.setSlaveByEndpoint = function(id, endpoint){
        slaveClients[id] = this.createClient(endpoint);
        return slaveClients[id];
    };

    this.usingSlaves = function(){
        return useSlave;
    };

    this.getSlaveEndpoints = function(){
        return slaveEndpoints;
    };

    this.getSentinels = function(){
        return sentinels;
    };

    this.getSentinelPubSubs = function(){
        return sentinelPubSubs;
    };

    this.setActiveSentinel = function(s){
        activeSentinel = s;
    };

    this.getActiveSentinel = function(){
        if (this.getSentinels().length == 0)
            throw new Error('No sentinels registered');

        if (!activeSentinel)
            throw new Error('Active sentinel does not exist');


        return activeSentinel;
    }

}

Sentinel.prototype.createClient = function (redisObj) {
    var self = this;
    if (!redisObj.port || !redisObj.host)
        throw new Error('createClient requires object with host and port');

    var temp = redis.createClient(redisObj.port, redisObj.host, this.getRedisConnectionOptions());
    temp.on('reconnecting', function (err, res) {
        self.log(redisObj.port, redisObj.host, ' is reconnecting');
    });

    temp.on('error', function (err, res) {
        self.log('redis connection error', err, res);
    });
    return temp;
};

Sentinel.prototype.switchMaster = function (sentinelMsg) {
    var splitted = sentinelMsg.split(' ');
    if (!splitted[3] || !splitted[4])
        return console.error('Switch master getting incorrect parameters', splitted);

    this.log('Master down, switching master');
    var masters = this.getMasterClients();
    this.setMasterHost(splitted[3]);
    this.setMasterPort(splitted[4]);
    for(var i in masters){
        this.switchRedisConnection(masters[i], splitted[3], splitted[4]);
    }

    if(this.isSlave(splitted[3], splitted[4])){
        var slaveEndpoints = this.getSlaveEndpoints();
        for(var i in slaveEndpoints)
            if(slaveEndpoints[i].host == splitted[3] && slaveEndpoints[i].port == splitted[4]){
                slaveEndpoints.splice(i ,1);
                this.log('Removed new master from slave endpoints');
                break;
            }
    }

    this.masterChanged();
};

Sentinel.prototype.handleSlaveDown = function (sentinelMsg) {

    if (this.getSlaveEndpoints().length == 0)
        return this.log('Got slave down, but not using slaves.');

    var hostDown = parseSlaveDownMessage(sentinelMsg);
    if (!hostDown)
        return this.log('Incorrect message from sentinel');

    if (this.isSlave(hostDown.host, hostDown.port)) {

        this.changeSlaveState(hostDown, true);
        var slaves = this.getSlaveClients();
        var host = this.getNextSlaveHost();
        var switchedSlave = false;
        for(var i in slaves){
            if (slaves[i].connectionOption.host != hostDown.host || slaves[i].connectionOption.port != hostDown.port){
                this.log('Got notified slave is down. We\'re not using that slave. So ignoring message');
            } else {
                switchedSlave = true;
                this.switchRedisConnection(slaves[i], host.host, host.port);
            }
        }
        if(switchedSlave)
            this.slaveChanged();
    }


};

Sentinel.prototype.handleSlaveUp = function (sentinelMsg) {
    if (this.getSlaveEndpoints().length == 0)
        return this.log('Got slave up, but not using slaves.');

    var hostDown = parseSlaveDownMessage(sentinelMsg);
    if (!hostDown)
        return this.log('Incorrenct message from sentinel');

    if (this.isSlave(hostDown.host, hostDown.port)) {

        this.changeSlaveState(hostDown, false);
        var host = this.getNextSlaveHost();

        var slaveClients = this.getSlaveClients();
        var switchedSlave = false;

        for(var i in slaveClients){

            if (this.isSlave(slaveClients[i].connectionOption.host, slaveClients[i].connectionOption.port)){
                this.log('Got notified slave is up. We\'re allready using a slave');
            } else {
                switchedSlave = true;
                this.switchRedisConnection(slaveClients[i], host.host, host.port);
            }
        }

        if(switchedSlave)
            this.slaveChanged();
    }
};

Sentinel.prototype.isSlave = function (host, port) {
    var slaveEndpoints = this.getSlaveEndpoints();
    for (var i in slaveEndpoints)
        if (slaveEndpoints[i].port == port && slaveEndpoints[i].host == host)
            return true;

    return false;
};

Sentinel.prototype.switchRedisConnection = function (client, host, port) {
    if (!(client instanceof redis.RedisClient))
        throw new Error('switchRedisConnection requires a RedisClient.');

    if (client.host == host && client.port == port) {
        return this.log('client allready switched to', host, port);
    }

    client.address = host + ':' + port;
    client.connectionOption.host = host;
    client.connectionOption.port = port;
    client.connection_gone("Force refresh connection");
};

Sentinel.prototype.initSentinel = function (endpoint, callback) {
    if (!endpoint || !endpoint.host || !endpoint.port) {
        throw new Error('Sentinel endpoint " ' + endpoint + ' " is not valid');
    }

    var tempSentinel = this.createClient(endpoint);
    var tempPubSubRedis = this.createClient(endpoint);
    tempPubSubRedis.send_command('subscribe', ['+sdown'], handleSubMessage);
    tempPubSubRedis.send_command('subscribe', ['-sdown'], handleSubMessage);
    tempPubSubRedis.send_command('subscribe', ['+slave'], handleSubMessage);
    tempPubSubRedis.send_command('subscribe', ['+switch-master'], handleSubMessage);
    tempPubSubRedis.on('message', this.handleSentinelMessage.bind(this));

    this.getSentinels().push(tempSentinel);
    this.getSentinelPubSubs().push(tempPubSubRedis);
    this.setActiveSentinel(tempSentinel);

    if (typeof callback == 'function')
        callback();
};

Sentinel.prototype.addSlave = function(sentinelMsg){
    var newSlave = parseSlaveDownMessage(sentinelMsg);

    if(newSlave && !this.isSlave(newSlave.host, newSlave.port)){
        this.getSlaveEndpoints().push(newSlave);
        this.log('added new slave');
    }
};

Sentinel.prototype.handleSentinelMessage = function (channel, msg) {
    if (!msg.match(this.getClusterName())) {
        return this.log('Got a message for a different cluster. My cluster: ' + this.getClusterName(), msg);
    }

    switch (channel) {
        case '+switch-master':
            return this.switchMaster(msg);
        case '-sdown':
            if(this.usingSlaves() && msg.match('slave'))
                this.handleSlaveUp(msg);
            return;
        case '+sdown':
            if(this.usingSlaves() && msg.match('slave'))
                this.handleSlaveDown(msg);
            return;
        case '+slave':
            if(this.usingSlaves())
                return this.addSlave(msg);
    }
};

Sentinel.prototype.changeSlaveState = function(slave, disconnected){
    var slaveEndpoints = this.getSlaveEndpoints();
    for(var i = 0; i < slaveEndpoints.length; i++)
        if(slaveEndpoints[i].host == slave.host && slaveEndpoints[i].port == slave.port){
            slaveEndpoints[i].disconnected = disconnected;
            this.log('Changed slave state', slave, disconnected);
            break;
        }
};


Sentinel.prototype.getNextSlaveHost = function(){
    var slaveEndpoints = this.getSlaveEndpoints();
    for(var i = 0; i < slaveEndpoints.length; i++)
        if(!slaveEndpoints[i].disconnected)
            return slaveEndpoints[i];

    this.log('No slave host found. Use master');
    return {
        host: this.getMasterHost(),
        port: this.getMasterPort()
    };
};

Sentinel.prototype.quit = function() {
    // Quit all individual Redis clients.

    var i;

    var sentinels = this.getSentinels();
    for (i = 0; i < sentinels.length; i++) {
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

    var slaveClients = this.getSlaveClients();
    for (i in slaveClients) {
        slaveClients[i].quit();
    }

};

module.exports = Sentinel;


function getIpAndPortForSlave(sentinelResult) {
    var obj = {};
    var ipIndex = sentinelResult.indexOf('ip');
    var portIndex = sentinelResult.indexOf('port');

    if (portIndex == -1 || ipIndex == -1)
        throw new Error('Ip or port not found for slave');


    obj.port = sentinelResult[portIndex + 1];
    obj.host = sentinelResult[ipIndex + 1];
    obj.disconnected = (sentinelResult[9].match('disconnected') || sentinelResult[9].match('s_down')) ? true : false ;
    return obj;
}

function handleSubMessage(err, res) {
    if (err)
        throw err;
}


function parseSlaveDownMessage(msg) {
    var splitted = msg.split(' ');

    if (!splitted[2] || !splitted[3]) {
        console.error('handleSlaveDown getting incorrect parameters', splitted);
        return false;
    }


    return {
        port: splitted[3],
        host: splitted[2]
    }
}
