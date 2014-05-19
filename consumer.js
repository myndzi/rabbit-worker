'use strict';

var Promise = require('bluebird'),
    msgpack = require('msgpack'),
    util = require('util');

module.exports = Consumer;

function NackError(message) {
    Error.call(this, message);
    Error.captureStackTrace(this, this.constructor);
    
    this.name = this.constructor.name;
    this.message = message;
}
util.inherits(NackError, Error);

function Consumer(fn, opts) {
    opts = opts || { };
    
    this.fn = fn;
    
    this.consumerTag = '';
    this.state = 'unbound';
    this.channel = opts.channel;
    
    this.log = opts.log || new require('logger')('Consumer');
    
    // ch.consume options
    this.opts = opts.opts;
    
    // exchange and key to publish responses to
    this.exchange = opts.exchange;
    this.replyKey = opts.replyKey;
    
    this.asserted = { };
}

Consumer.prototype.bind = Promise.method(function (queue) {
    var self = this, ch = self.channel;
    
    if (self.state !== 'unbound') {
        throw new Error('Consumer.bind(): already bound');
    }
    
    ch.once('error', function (err) {
        log.warn('Channel error:', err);
        self.state = 'error';
    });
    ch.once('close', function () {
        log.warn('Channel closed');
        self.reset();
    });
    
    self.state = 'binding';
    return ch.consume(queue, self.consumeHandler.bind(self), self.opts)
    .then(function (data) {
        self.consumerTag = data.consumerTag;
        self.state = 'bound';
    });
});
Consumer.prototype.reset = function () {
    this.log.trace('Consumer resetting');
    this.state = 'unbound';
    this.consumerTag = '';
};
Consumer.prototype.consumeHandler = function (msg) {
    var self = this, ch = self.channel;

    var headers = msg.properties.headers,
        content = msgpack.unpack(msg.content);
    
    var cancelled = false, nack = null,
        reply = (this.exchange !== void 0 && this.replyKey !== void 0);
    
    var thisArg = {
        /* stop this consumer from listening for further messages */
        cancel: function () { cancelled = true; },
        /* return this message to the queue */
        nack: function (message) {
            reply = false;
            nack = new NackError(message);
        }
    };
    var ctx = { msg: msg, headers: headers, content: content };
    
    return Promise.try(function () {
        // run the supplied callback handler, it may return a promise
        // or throw/return a value
        if (content.value && content.state) {
            // if it's a deferred message, convert it to a resolved or rejected promise
            if (content.state === 'resolved') { return self.fn.call(Promise.resolve(content.value)); }
            if (content.state === 'rejected') { return self.fn.call(Promise.reject(content.value)); }
        }

        return self.fn.call(thisArg, content, ctx);
    }).then(function (res) {
        self.log.trace('Consumer callback resolved: ' + res);
        return { state: 'resolved', value: res };
    }).catch(function (err) {
        self.log.warn('Problem:', err);
        // if failed but message is set for retries, do that
        if (Array.isArray(headers.retry) && headers.retry.length) {
            self.log.trace('Message queued for redelivery');
            reply = false;
            return self.redeliver(msg);
        } else {
            self.log.trace('Consumer callback rejected:', err);
            return { state: 'rejected', value: err };
        }
    }).then(function (res) {
        if (nack) { throw nack; }
        if (!reply) {
            self.log.trace('Reply disabled');
            return;
        }
        
        self.log.trace('Publishing reply');
        return ch.publish(
            self.exchange,
            self.replyKey,
            msgpack.pack(res),
            {   contentType: 'application/x-msgpack',
                contentEncoding: 'binary',
                request: msg.content    }
        );
    }).then(function () {
        if (!cancelled) { return; }
        self.log.trace('Consumer cancelled');
        return self.unbind();
    }).then(function () {
        self.log.trace('Acking message');
        ch.ack(msg);
    }).catch(NackError, function (err) {
        self.log.warn('Nacked: ' + err.message);
    }).catch(function (err)  {
        self.log.error('Other error:', err);
    });
};
Consumer.prototype.assertOnce = Promise.method(function (queueName, opts) {
    var self = this;
    if (self.asserted[queueName]) { return; }
    self.channel.assertQueue(queueName, opts).then(function () {
        self.asserted[queueName] = true;
    });
});
Consumer.prototype.redeliver = Promise.method(function (msg, ctx) {
    var self = this, headers = msg.properties.headers;
    var dly = headers.retry.shift(), retryKey = 'retry.' + dly;
    
    self.log.trace('Consumer.redeliver()');
    
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
        self.log.silly('Publishing to ' + retryKey);
        return self.channel.publish('', retryKey, msg.content, {
            deliverTo: msg.fields.routingKey,
            origHeaders: JSON.stringify(headers),
            contentType: 'application/x-msgpack',
            contentEncoding: 'binary'
        });
    });
});
Consumer.prototype.unbind = Promise.method(function () {
    var self = this;
    
    if (self.state !== 'bound') { throw new Error('unbind(): Not bound!'); }
    var ch = self.channel, tag = self.consumerTag;
    
    if (!tag) { throw new Error('Unbind called but no consumer tag is set!'); }
    
    self.reset();
    
    self.log.trace('Consumer unbinding: ' + tag);
    return ch.cancel(tag).then(function () {
        self.log.trace('unbound');
    });
});
