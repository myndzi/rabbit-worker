'use strict';

var fs = require('fs'),
    util = require('util'),
    PATH = require('path'),
    amqp = require('amqplib'),
    Logger = require('logger'),
    extend = require('jquery-extend'),
    msgpack = require('msgpack'),
    Promise = require('bluebird'),
    Consumer = require('./consumer'),
    EventEmitter = require('events').EventEmitter;

var log = new Logger('Rabbit-worker');

module.exports = Worker;

function Worker(config) {
    EventEmitter.call(this);
    
    if (config.ca) {
        var caCert = fs.readFileSync(PATH.join(__dirname, config.ca));
        config.secure = true;
        this.caCert = caCert;
    }

    this.connectString = util.format('%s://%s:%s@%s:%d/%s?heartbeat=60',
        config.secure ? 'amqps' : 'amqp',
        encodeURIComponent(config.user),
        encodeURIComponent(config.pass),
        config.host,
        config.port,
        encodeURIComponent(config.path)
    );
    log.trace('connectString: ' + this.connectString);
    this.controlKeys = config.controlKeys || [ ];
    this.reconnect = config.reconnect || {
        factor: 2,
        max: 30000
    };
    this.reconnect.current = 0;
    this.prefetch = config.prefetch || 0;
    this.log = config.log || log;
    this.exchange = config.exchange || '';
    this.bindings = config.bindings || { };
    this.consumers = [ ];
    this.connection = null;
    this.channel = null;
    this.error = null; // set when setup() fails to prevent hammering
    this.asserted = false;
}
util.inherits(Worker, EventEmitter);

var jobRE = /\.job$/;
Worker.prototype.consume = Promise.method(function (queue, opts, fn) {
    if (typeof opts === 'function') { fn = opts; opts = { }; }
    var self = this;
    return self.getChannel().then(function (ch) {
        self.log.silly('binding consumer: ', queue);
        
        var consumer = new Consumer(fn, {
            opts: opts.consumeOpts,
            channel: ch,
            exchange: self.exchange,
            replyKey: opts.replyKey,
            log: self.log
        });
        return consumer.bind(queue).then(function () {
            self.consumers.push(consumer);
        });
    });
});
Worker.prototype.bind = Promise.method(function (keys, fn, ch) {
    var self = this;
    if (!Array.isArray(keys)) { keys = [ keys ]; }
    self.log.silly('Binding temporary consumer: ', keys);
    return Promise.try(function () {
        return ch || self.getChannel();
    }).then(function (ch) {
        return ch.assertQueue(null, {
            exclusive: true,
            autoDelete: true
        }).tap(function (res) {
            self.log.trace('Created temporary queue: ' + res.queue);
            return Promise.map(keys, function (key) {
                self.log.trace('Binding: ' +  key);
                return ch.bindQueue(res.queue, self.exchange, key);
            });
        }).then(function (res) {
            return ch.consume(res.queue, function (msg) {
                var headers = msg.properties.headers,
                    content = msgpack.unpack(msg.content),
                    fields = msg.fields;
                
                self.log.trace('.bind callback:', {
                    content: content,
                    headers: headers,
                    fields: fields
                });
                    
                fn(content, headers, fields);
                
                ch.ack(msg);
            });
        });
    });
});
Worker.prototype.publish = Promise.method(function (key, data, headers) {
    var self = this;
    
    var msgOpts = {
        contentType: 'application/x-msgpack',
        contentEncoding: 'binary',
        headers: headers
    };
    self.log.trace('Worker.publish');
    
    return self.getChannel().then(function (ch) {
        self.log.trace({
            exchange: self.exchange,
            key: key,
            data: data,
            msgOpts: msgOpts
        });
        return ch.publish(
            self.exchange,
            key,
            msgpack.pack(data),
            msgOpts
        );
    });
});
Worker.prototype.reset = function () {
    this.error = null;
    this.asserted = false;
};
Worker.prototype.setup = Promise.method(function (channel) {
    var self = this,
        bindings = self.bindings,
        consumers = self.consumers;
    
    if (self.error) {
        self.log && self.log.warn('Attempting to set up previously failed connection', self.error);
        throw self.error;
    }
    
    self.log.info('worker.setup');
    self.log.silly(bindings);
    return Promise.map(Object.keys(bindings), function (exchg) {
        var queues = bindings[exchg] || [ ];
        self.log.silly('asserting exchange', exchg);
        self.log.trace('queues: ', queues);
        
        if (self.prefetch) { channel.prefetch(self.prefetch); }
        return channel
            .assertExchange(exchg, 'topic')
            .thenReturn(queues)
            .map(function (queue) {
                self.log.trace('asserting queue', queue);
                return channel.assertQueue(queue);
            })
            .thenReturn(queues)
            .map(function (queue) {
                self.log.trace('binding queue', {
                    exchange: exchg,
                    key: queue,
                    queue: queue
                });
                return channel.bindQueue(queue, exchg, queue);
            });
    })
    .then(function () {
        if (self.controlKeys) {
            self.log.trace('control keys:', self.controlKeys);
            return self.setupControlChannel(channel);
        }
    })
    .then(function () {
        self.asserted = true;
        self.log.silly('consumers: ', consumers.map(function (c) { return c[0]; }));
        return Promise.settle(consumers.map(function (args) {
            self.log.silly('re-binding');
            return self.consume.apply(self, args);
        }));
    })
    .return(channel)
    .catch(function (err) {
        self.log.warn('error', err);
        self.error = err;
        throw err;
    });
});
Worker.prototype.setupControlChannel = Promise.method(function (ch) {
    var self = this;
    self.log.trace('setupControlChannel()');
    if (!Array.isArray(self.controlKeys) || !self.controlKeys.length) { return; }

    var doEvent = function (content, headers, fields) {
        if (headers.event) {
            log.silly('Emitting event from message', {
                headers: headers,
                content: content,
                fields: fields
            });
            self.emit(headers.event, content, headers, fields);
        }
    };
    return ch.assertExchange(self.exchange, 'topic').then(function () {
        return self.bind(self.controlKeys, doEvent, ch);
    });
});
Worker.prototype.getConnection = Promise.method(function (force) {
    var self = this;

    var connecting = !!self.connection, // or connected
        connected = connecting && self.connection.isResolved();
    
    if (connecting && !connected || connected && !force) {
        return self.connection;
    }
    
    function _error(err) {
        self.log.error('Disconnected from RabbitMQ server:', err);
    }
    function _close() {
        // if the connection closes, we want to reconnect and re-bind any
        // consumers. Re-binding happens when a channel opens, so force
        // a channel to open
        self.log.trace('Connection closed unexpectedly, reconnecting');
        self.getConnection(true);
    }
        
    if (connected && force) {
        self.log.trace('Forcing reconnection');
        
        var conn = self.connection.value();
        
        conn.removeListener('error', _error);
        conn.removeListener('close', _close);
        
        self.connection = null;
        self.asserted = false;
        
        conn.close()
        .catch(function (err) {
            log.error('Couldn\'t close connection:', err);
        });
        
        return self.getConnection();
    }
    
    var r = this.reconnect, delay = r.current;
    r.current = Math.min(Math.max(delay,1) * r.factor, r.max);

    if (delay) { self.log.silly('Delaying connection for ' + (delay/1000) + 's'); }
    
    self.connection = Promise.delay(delay).then(function () {
        return amqp.connect(self.connectString, { ca: [self.caCert] })
    }).tap(function (conn) {
        self.log.silly('Connected successfully');
        r.current = 0;
        
        conn.once('error', _error);
        conn.once('close', _close);
    }).catch(function (err) {
        self.log.warn('Connection failed:', err);
        // no forcing getChannel here because it's in the normal code flow
        // on the way to that very thing
        return self.getConnection(true);
    });
    return self.connection;
});
Worker.prototype.getChannel = Promise.method(function (force) {
    var self = this;
    
    var opening = !!self.channel, // or opened
        opened = self.channel.isResolved();
    
    if (opening && !opened || opened && !force) {
        return self.channel;
    }
    
    function _error(err) {
        self.log.error('Channel error:', err);
        self.channel = null;
        self.getChannel();
    }
    function _close() {
        self.log.trace('Channel closed unexpectedly, reopening');
        self.getChannel(true);
    }
    
    if (opened && force) {
        self.log.trace('Forcing new channel');
        
        var ch = self.channel.value();
        
        ch.removeListener('error', _error);
        ch.removeListener('close', _close);
        
        self.channel = null;
        
        ch.close()
        .catch(function (err) {
            log.error('Couldn\'t close channel:', err);
        });
        
        return self.getChannel();
    }
    
    self.channel = self.getConnection(force).then(function (conn) {
        return conn.createChannel();
    }).then(function (ch) {
        ch.once('error', _error);
        ch.once('close', _close);
        
        if (self.asserted) { return ch; }
        return self.setup(ch);
    }).catch(function (err) {
        self.log.warn('Create channel failed:', err);
        
        return self.getConnection(true)
        .then(function () {
            return self.getChannel();
        });
    });
    
    return self.channel;
});
