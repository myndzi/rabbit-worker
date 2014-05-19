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
    self.log.trace('Worker.publish opts:', opts);
    
    return self.getChannel().then(function (ch) {
        self.log.silly('publish', {
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
    
    self.log.info('worker.setup');
    self.log.silly(bindings);
    return Promise.map(Object.keys(bindings), function (exchg) {
        var queues = bindings[exchg] || [ ];
        self.log.silly('asserting exchange', exchg);
        self.log.trace('queues: ', queues);
        
        if (self.prefetch) { channel.prefetch(self.prefetch); }
        return channel
            .assertExchange(exchg, 'topic')
            .then(function () {
                self.log.trace('control keys:', self.controlKeys);
                return self.setupControlChannel(channel);
            })
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

    return ch.assertQueue(null, {
        exclusive: true,
        autoDelete: true
    }).tap(function (res) {
        return Promise.map(self.controlKeys, function (key) {
            log.trace('Bound queue: ', [self.exchange, key, res.queue]);
            return ch.bindQueue(res.queue, self.exchange, key);
        });
    }).then(function (res) {
        ch.consume(res.queue, function (msg) {
            var headers = msg.properties.headers,
                content = msgpack.unpack(msg.content),
                fields = msg.fields;
            
            if (headers.event) {
                log.silly('Emitting event from message', {
                    headers: headers,
                    content: content,
                    fields: fields
                });
                self.emit(headers.event, content, headers, fields);
            }
            ch.ack(msg);
        });
    });
});
Worker.prototype.getConnection = Promise.method(function (force) {
    var self = this;
    
    if (self.connection) {
        if (!force) { return self.connection; }
        
        self.log.trace('Forcing reconnection');
        if (self.connection.isResolved()) {
            Promise.try(self.connection.value().close).catch(function () {});;
        }
        self.connection = null;
        self.asserted = false;
    }
    
    var r = this.reconnect, delay = r.current;
    r.current = Math.min(Math.max(delay,1) * r.factor, r.max);

    if (delay) { self.log.silly('Delaying connection for ' + (delay/1000) + 's'); }
    
    self.connection = Promise.delay(delay).then(function () {
        return amqp.connect(self.connectString, { ca: [self.caCert] })
    }).tap(function (conn) {
        self.log.silly('Connected successfully');
        r.current = 0;
        
        conn.once('error', function (err) {
            self.log.error('Disconnected from RabbitMQ server:', err);
        });
        conn.once('close', function () {
            self.log.trace('Connection closed');
            // if the connection closes, we want to reconnect and re-bind any
            // consumers. Re-binding happens when a channel opens, so force
            // a channel to open
            self.getChannel(true);
        });
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
    if (self.channel) {
        if (!force) { return self.channel; }
        
        self.log.trace('Forcing new channel');
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
            self.log.error('Channel error:', err);
        });
        ch.once('close', function () {
            ch.removeAllListeners();
            self.log.trace('Channel closed');
            self.getChannel(true);
        });
        if (self.asserted) { return ch; }
        return self.setup(ch);
    }).catch(function (err) {
        self.log.warn('Create channel failed:', err);
        return self.getChannel(true);
    });
    
    return self.channel;
});
