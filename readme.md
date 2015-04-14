# Redis sentinel

```sh
$ npm install simple-redis-sentinel
```

```javascript
    var Sentinel = require('simple-redis-sentinel');
    var clusterName = 'mymaster';
    var sentinelEndpoints = [
        {
            host: '127.0.0.1',
            port: 26379
        },
        {
            host: '127.0.0.1',
            port: 26380
        }
    ];

    var redisOptions = {
        debug:true,
        useSlave: true,
        redisConnectOptions: {
            retry_max_delay: 10000
        }
    };
    var redisManager = new Sentinel(clusterName, sentinelEndpoints, redisOptions);
    redisManager.init(function(err,res){
        var masterConnection = redisManager.getMasterClientById('master');
        var masterForSubscription = redisManager.getMasterClientById('masterForSub');
        var slave = redisManager.getSlaveClientById('slave');
    });

    redisManager.onMasterChange(function(){
        console.error('Redis master changed');
    });

    redisManager.onSlaveChange(function(){
        console.error('Redis slave changed');
    });
```