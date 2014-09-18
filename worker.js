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

    if (self.connection) {
        if (!force || self.connection.isPending()) {
            // already trying to connect, or already have a connection
            // don't create multiple connections
            return self.connection;
        }
    }
    
    function _reconnect() {
        self.log.info('Reconnecting');
        
        var staleConnection = self.connection;
        self.connection = null;
        
        return Promise.try(function () {
            // first ensure the previous connection is closed
            if (!staleConnection.isFulfilled()) {
                return;
            }
            
            var conn = staleConnection.value();
        
            conn.removeListener('error', _reconnect);
            conn.removeListener('close', _reconnect);
            
            return Promise.try(function () {
                return conn.close();
            }).catch(function (err) {
                self.log.warn('Failed to cleanly close connection: ' + err.message);
            });
        })
        .then(function () {
            // then create a new one, with an accumulating delay for retries
            
            var r = self.reconnect,
                delay = r.current;

            // set the delay for next time
            r.current = Math.floor(
                Math.min(
                    r.max,
                    Math.max( 
                        delay,
                        Math.random() * 4000 + 1000 + r.current
                    )
                )
            );
            
            if (delay) { self.log.silly('Delaying connection for ' + (delay/1000) + 's'); }
            
            return Promise.delay(delay).then(function () {
                return self.getConnection();
            });
        });
    }
    
    if (force) {
        self.log.silly('Forcing new connection');
        return _reconnect();
    }
    
    // we're starting from a new connection from here on, so assertions have not been run
    self.asserted = false;
    
    // here, self.connection reflects the state of this single attempt to connect
    // while the actual promise returned reflects the eventual connection to the server
    // more specifically, self.connection can be pending, fulfilled, or rejected
    // but the promise returned from Worker.getConnection() can only ever be
    // pending or fulfilled
    
    self.connection = amqp.connect(self.connectString, { ca: [self.caCert] });

    return self.connection.tap(function (conn) {
        self.reconnect.current = 0;
        
        conn.once('error', _reconnect);
        conn.once('close', _reconnect);
    }).catch(_reconnect);
});
Worker.prototype.getChannel = Promise.method(function (force) {
    var self = this;

    if (self.channel) {
        if (!force || self.channel.isPending()) {
            // already trying to get a channel, or already have a channel
            // don't create multiple channels
            return self.channel;
        }
    }

    function _reconnect() {
        self.log.info('Reopening channel');
        
        var staleCh = self.channel;
        self.channel = null;
        
        return Promise.try(function () {
            // first ensure the previous channel is closed
            if (!staleCh.isFulfilled()) {
                return;
            }
            
            var ch = staleCh.value();
        
            ch.removeListener('error', _reconnect);
            ch.removeListener('close', _reconnect);
            
            return Promise.try(function () {
                return ch.close();
            }).catch(function (err) {
                self.log.warn('Failed to cleanly close channel: ' + err.message);
            });
        })
        .then(function () {
            // no delay logic for channels
            return self.getChannel();
        });
    }
    
    if (force) {
        self.log.silly('Forcing new channel');
        return _reconnect();
    }

    self.channel = self.getConnection()
        .then(function (conn) {
            return conn.createChannel();
        });
        
    return self.channel
        .tap(function (ch) {
            ch.once('error', _reconnect);
            ch.once('close', _reconnect);
        
            if (self.asserted) { return ch; }
            return self.setup(ch);
        }).catch(function (err) {
            self.log.warn('Couldn\'t establish channel: ' + err.message);
            return self.getConnection(true).then(_reconnect);
        });
});
