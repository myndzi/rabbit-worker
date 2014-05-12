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
    this.log = config.log;
    this.exchange = config.exchange || '';
    this.bindings = config.bindings || { };
    this.consumers = [ ];
    this.connection = null;
    this.channel = null;
    this.error = null; // set when setup() fails to prevent hammering
    this.asserted = false;
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
                log.warn('Consumer failed:', err);
                if (rejectKey) {
                    return self.publish(
                        rejectKey,
                        err,
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
Worker.prototype.publish = Promise.method(function (key, data, headers) {
    var self = this;
    
    var opts = {
        contentType: 'application/x-msgpack',
        contentEncoding: 'binary',
        headers: extend(true, { 'retries': 0 }, headers)
    };
    log.trace('Worker.publish opts:', opts);
    
    
    return self.getChannel().then(function (ch) {
        log.silly('publish', {
            exchange: self.exchange,
            key: key,
            data: data,
            opts: opts
        });
        return ch.publish(
            self.exchange,
            key,
            msgpack.pack(data),
            opts
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
Worker.prototype.getConnection = Promise.method(function () {
    var self = this;
    if (self.connection) { return self.connection; }
    
    var errFn = function (err) {
        self.connection = null;
        self.asserted = false;
        //self.log && self.log.error('amqp connection error:', err);
        throw err;
    };

    self.connection = amqp.connect(self.connectString, {
        ca: [self.caCert]
    }).then(function (conn) {
        conn.on('error', errFn);
        return conn;
    }, errFn);
    return self.connection;
});
Worker.prototype.getChannel = Promise.method(function () {
    var self = this;
    if (self.channel) { return self.channel; }
    
    var errFn = function (err) {
        self.channel = null;
        self.log && self.log.error('amqp channel error:', err);
        throw err;
    };
    
    self.channel = self.getConnection().then(function (conn) {
        return conn.createChannel();
    }).then(function (ch) {
        ch.on('error', errFn);
        
        if (self.asserted) { return ch; }
        return self.setup(ch);
    }, errFn);
    
    return self.channel;
});
