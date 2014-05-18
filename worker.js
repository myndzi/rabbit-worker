'use strict';

var fs = require('fs'),
    util = require('util'),
    PATH = require('path'),
    amqp = require('amqplib'),
    Logger = require('logger'),
    extend = require('jquery-extend'),
    msgpack = require('msgpack'),
    Promise = require('bluebird');

var log = new Logger('Rabbit-worker');

module.exports = Worker;

function Worker(config) {
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
    this.reconnect = config.reconnect || {
        factor: 2,
        max: 30000
    };
    this.reconnect.current = 0;
    this.log = config.log;
    this.exchange = config.exchange || '';
    this.bindings = config.bindings || { };
    this.consumers = [ ];
    this.connection = null;
    this.channel = null;
    this.error = null; // set when setup() fails to prevent hammering
    this.asserted = false;
    
    // keeping track of retry queues we've asserted so we don't
    // spam asserts a bunch of messages fall through to the retry queue(s)
    this._asserted = { };
}
var jobRE = /\.job$/;
Worker.prototype.consume = Promise.method(function (queue, fn, opts) {
    var self = this;
    return self.getChannel().then(function (ch) {
        log.silly('binding consumer: ', queue);
        self.consumers.push([queue, fn, opts]);
        return ch.consume(queue, function (msg) {
            var deferred = Promise.defer(),
                cancelled = null,
                routingKey = msg.fields.routingKey,
                resolveKey, rejectKey;
            
            
            if (jobRE.test(routingKey)) {
                resolveKey = routingKey.slice(0, -3) + 'success';
                rejectKey = routingKey.slice(0, -3) + 'failure';
            }
            
            var ctx = {
                fields: msg.fields,
                content: msgpack.unpack(msg.content),
                headers: msg.properties.headers,
                cancel: function (reason) { return Promise.reject(cancelled = reason); }
            };
            log.silly('consume callback');
            
            Promise.try(function () {
                return fn.call(ctx, ctx.content);
            }).then(function (res) {
                // in case they called this.cancel but didn't return it
                if (cancelled !== null) { throw new Error(cancelled); }
                
                if (resolveKey) {
                    return self.publish(
                        resolveKey,
                        res,
                        ctx.headers
                    );
                }
            }).catch(function (err) {
                var retry = ctx.headers.retry;
                // if there is a retry header, it should be an array of
                // delays to implement, in seconds, in ascending order
                // e.g. [30, 300, 3600, 28800, 86400]
                if (Array.isArray(retry) && retry.length) {
                    log.warn('Consumer failed, retrying', err);
                    return self.redeliver(msg, ctx)
                } else if (rejectKey) {
                    log.error('Consumer failed, rejecting', err);
                    return self.publish(
                        rejectKey,
                        { message: err.message,
                          content: ctx.content},
                        ctx.headers
                    );
                } else {
                    throw err;
                }
            }).catch(function (err) {
                // no reject key specified, or publish rejection failed
                delete ctx.cancel;
                log.error('Error processing message', err);
                log.trace('Message:', ctx);
            }).finally(function () {
                // allow the message to be removed from the queue, we've done everything we can
                ch.ack(msg);
            });
        }, opts);
    });
});
// keeps track of queues we've asserted so as not to re-assert
// them every time. this is in support of the 'redeliver' method
// which works from a message header, so it is impossible to
// assert the queues beforehand
Worker.prototype.assertOnce = Promise.method(function (queueName, opts) {
    var self = this;
    if (self._asserted[queueName]) { return; }
    return self.getChannel().then(function (ch) {
        return ch.assertQueue(queueName, opts);
    });
});
Worker.prototype.redeliver = Promise.method(function (msg, ctx) {
    log.trace('Worker.redeliver()');
    var self = this,
        dly = ctx.headers.retry.shift(),
        retryKey = 'retry.' + dly;
    
    // ensure the redelivery queue exists
    return self.assertOnce('redelivery')
    .then(function () {
        // assert a queue for this specific delay length
        // (ensures long delays don't hold up slow ones)
        // the message will expire and be delivered to 
        // the 'redelivery' queue
        return self.assertOnce(retryKey, {
            deadLetterExchange: '',
            deadLetterRoutingKey: 'redelivery',
            messageTtl: dly*1000
        });
    }).then(function () {
        log.silly('Publishing to ' + retryKey);
        return self.publish(
            { exchange: '' },
            retryKey,
            { deliverTo: msg.fields.routingKey,
              headers: ctx.headers,
              content: ctx.content }
        );  
    });
});
Worker.prototype.publish = Promise.method(function (opts, key, data, headers) {
    var self = this;
    
    if (typeof opts === 'string') {
        headers = data;
        data = key;
        key = opts;
        opts = {
            exchange: self.exchange
        };
    }
    
    var msgOpts = {
        contentType: 'application/x-msgpack',
        contentEncoding: 'binary',
        headers: headers
    };
    log.trace('Worker.publish opts:', opts);
    
    return self.getChannel().then(function (ch) {
        log.silly('publish', {
            exchange: opts.exchange,
            key: key,
            data: data,
            msgOpts: msgOpts
        });
        return ch.publish(
            opts.exchange,
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
    
    log.info('worker.setup');
    log.silly(bindings);
    return Promise.map(Object.keys(bindings), function (exchg) {
        var queues = bindings[exchg] || [ ];
        log.silly('asserting exchange', exchg);
        log.trace('queues: ', queues);
        
        return channel
            .assertExchange(exchg, 'topic')
            .thenReturn(queues)
            .map(function (queue) {
                log.trace('asserting queue', queue);
                return channel.assertQueue(queue);
            })
            .thenReturn(queues)
            .map(function (queue) {
                log.trace('binding queue', {
                    exchange: exchg,
                    key: queue,
                    queue: queue
                });
                return channel.bindQueue(queue, exchg, queue);
            });
    })
    .then(function () {
        self.asserted = true;
        log.silly('consumers: ', consumers.map(function (c) { return c[0]; }));
        return Promise.settle(consumers.map(function (args) {
            log.silly('re-binding');
            return self.consume.apply(self, args);
        }));
    })
    .return(channel)
    .catch(function (err) {
        log.warn('error', err);
        self.error = err;
        throw err;
    });
});
Worker.prototype.getConnection = Promise.method(function (force) {
    var self = this;
    
    if (self.connection) {
        if (!force) { return self.connection; }
        
        log.trace('Forcing reconnection');
        if (self.connection.isResolved()) {
            Promise.try(self.connection.value().close).catch(function () {});;
        }
        self.connection = null;
        self.asserted = false;
    }
    
    var r = this.reconnect, delay = r.current;
    r.current = Math.min(Math.max(delay,1) * r.factor, r.max);

    if (delay) { log.silly('Delaying connection for ' + (delay/1000) + 's'); }
    
    self.connection = Promise.delay(delay).then(function () {
        return amqp.connect(self.connectString, { ca: [self.caCert] })
    }).tap(function (conn) {
        log.silly('Connected successfully');
        r.current = 0;
        
        conn.once('error', function (err) {
            log.error('Disconnected from RabbitMQ server:', err);
        });
        conn.once('close', function () {
            log.trace('Connection closed');
            // if the connection closes, we want to reconnect and re-bind any
            // consumers. Re-binding happens when a channel opens, so force
            // a channel to open
            self.getChannel(true);
        });
    }).catch(function (err) {
        log.warn('Connection failed:', err);
        // no forcing getChannel here because it's in the normal code flow
        // on the way to that very thing
        return self.getConnection(true);
    });
    return self.connection;
});
Worker.prototype.getChannel = Promise.method(function (force) {
    var self = this;
    if (self.channel) {
        if (!force) { return self.channel; }
        
        log.trace('Forcing new channel');
        if (self.channel.isResolved()) {
            Promise.try(self.channel.value().close).catch(function () {});;
        }
        self.channel = null;
    }
    
    var errFn = function (err) {
        self.channel = null;
        self.log && self.log.error('amqp channel error:', err);
        throw err;
    };
    
    self.channel = self.getConnection(force).then(function (conn) {
        return conn.createChannel();
    }).then(function (ch) {
        ch.once('error', function (err) {
            log.error('Channel error:', err);
        });
        ch.once('close', function () {
            log.trace('Channel closed');
            self.getChannel(true);
        });
        if (self.asserted) { return ch; }
        return self.setup(ch);
    }).catch(function (err) {
        log.warn('Create channel failed:', err);
        return self.getChannel(true);
    });
    
    return self.channel;
});
