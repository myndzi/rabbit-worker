'use strict';

var util = require('util'),
    amqp = require('amqplib'),
    extend = require('jquery-extend'),
    msgpack = require('msgpack'),
    Promise = require('bluebird'),
    EventEmitter = require('events').EventEmitter;

module.exports = Worker;

function Worker(config) {
    EventEmitter.call(this);
    
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
util.inherits(Worker, EventEmitter);
Worker.prototype.consume = function (queue, fn, opts) {
    var self = this;
    self.consumers.push([queue, fn, opts]);
    return self.getChannel().then(function (ch) {
        return ch.consume(queue, function (msg) {
            var deferred = Promise.defer(),
                cancelled = null;
            
            var ctx = {
                fields: msg.fields,
                content: msgpack.unpack(msg.content),
                headers: msg.properties.headers,
                cancel: function (reason) { return Promise.reject(cancelled = reason); }
            };
            
            Promise.try(function () {
                return fn.call(ctx, content);
            }).then(function (res) {
                // in case they called this.cancel but didn't return it
                if (cancelled !== null) { throw new Error(cancelled); }
                
                if (headers.resolveKey) {
                    return self.publish(
                        headers.resolveKey,
                        res,
                        { headers: headers }
                    );
                }
            }).catch(function (err) {
                if (headers.rejectKey) {
                    return self.publish(
                        headers.rejectKey,
                        err,
                        { headers: headers }
                    );
                } else {
                    throw err;
                }
            }).catch(function (err) {
                // no reject key specified, or publish rejection failed
                delete ctx.cancel;
                log.error('Error processing message', ctx);
            }).finally(function () {
                // allow the message to be removed from the queue, we've done everything we can
                ch.ack(msg);
            });
        }, opts);
    });
};
Worker.prototype.publish = function (key, data, opts) {
    var self = this;
    opts = extend({ }, {
        contentType: 'application/x-msgpack',
        contentEncoding: 'binary',
        retries: 0
    }, opts);
    return self.getChannel().then(function (ch) {
        return ch.publish(self.exchange, key, msgpack.pack(data), opts);
    });
};
Worker.prototype.reset = function () {
    this.error = null;
};
Worker.prototype.setup = function () {
    var self = this,
        ch = self.channel,
        bindings = self.bindings,
        consumers = self.consumers;
    
    if (self.error) { return Promise.reject(self.error); }
        
    return Promise.map(bindings, function (exchg) {
        var queues = bindings[exchg].queues || [ ];
        
        return ch
            .assertExchange(exchg, bindings[exchg].type || 'direct')
            .thenReturn(queues)
            .map(function (queue) {
                return ch.assertQueue(queue).thenReturn(queue);
            })
            .map(function (queue) {
                return ch.bindQueue(queue, exchg, queues[queue]);
            });
    })
    .thenReturn(consumers)
    .map(self.consume.bind(self))
    .then(ch.recover.bind(ch))
    .catch(function (err) {
        self.error = err;
    })
    .return(self);
};
Worker.prototype.getConnection = function () {
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
};
Worker.prototype.getChannel = function () {
    if (this.channel) { return this.channel; }
    
    var self = this;
    var errFn = function (err) {
        self.channel = null;
        self.log && self.log.error('amqp channel error:', err);
    };
    
    return self.getConnection().then(function (conn) {
        return conn.createChannel();
    }).then(function (ch) {
        ch.on('error', errFn);
        self.channel = ch;
        
        if (self.asserted) { return ch; }
        return self.setup().return(ch);
    }, errFn);
};
