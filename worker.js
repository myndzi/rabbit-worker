'use strict';

var util = require('util'),
    amqp = require('amqplib'),
    Logger = require('logger'),
    extend = require('jquery-extend'),
    msgpack = require('msgpack'),
    Promise = require('bluebird');

var log = new Logger('Rabbit-worker', 'trace');

module.exports = Worker;

function Worker(config) {
    this.connectString = util.format('amqp://%s:%s@%s:%d/%s',
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
Worker.prototype.consume = Promise.method(function (queue, fn, opts) {
    var self = this;
    return self.getChannel().then(function (ch) {
        log.silly('binding consumer: ', queue);
        self.consumers.push([queue, fn, opts]);
        return ch.consume(queue, function (msg) {
            var deferred = Promise.defer(),
                cancelled = null;
            
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
                
                if (ctx.headers.resolveKey) {
                    return self.publish(
                        ctx.headers.resolveKey,
                        res,
                        { headers: ctx.headers }
                    );
                }
            }).catch(function (err) {
                log.warn('Consumer failed:', err);
                if (ctx.headers.rejectKey) {
                    return self.publish(
                        ctx.headers.rejectKey,
                        err,
                        { headers: ctx.headers }
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
        headers: extend(true, {
            'retries': 0,
            'resolveKey': 'resolved',
            'rejectKey': 'rejected'
        }, headers)
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
};
Worker.prototype.setup = Promise.method(function () {
    var self = this,
        channel = self.channel,
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
        
        return channel.then(function (ch) {
            return ch
                .assertExchange(exchg, 'topic')
                .thenReturn(queues)
                .map(function (queue) {
                    log.trace('asserting queue', queue);
                    return ch.assertQueue(queue);
                })
                .thenReturn(queues)
                .map(function (queue) {
                    log.trace('binding queue', {
                        exchange: exchg,
                        key: queue,
                        queue: queue
                    });
                    return ch.bindQueue(queue, exchg, queue);
                });
        });
    })
    .then(function () {
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
    if (this.connection) { return this.connection; }
    var self = this,
        config = self.config;
    
    var errFn = function (err) {
        self.connection = null;
        self.asserted = false;
        self.log && self.log.error('amqp connection error:', err);
    };
    
    return amqp.connect(
    ).then(function (conn) {
        conn.on('error', errFn);
        self.connection = conn;
        return conn;
    }, errFn);
});
Worker.prototype.getChannel = Promise.method(function () {
    if (this.channel) { return this.channel; }
    
    var self = this;
    var errFn = function (err) {
        self.channel = null;
        self.log && self.log.error('amqp channel error:', err);
    };
    
    return this.channel = self.getConnection().then(function (conn) {
        return conn.createChannel();
    }).then(function (ch) {
        ch.on('error', errFn);
        
        if (self.asserted) { return ch; }
        return self.setup().return(ch);
    }, errFn);
});
