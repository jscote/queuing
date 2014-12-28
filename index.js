/**
 * Created by jean-sebastiencote on 12/20/14.
 */
(function (_, q, qu, jb, Buffer, serviceMessage) {

    'use strict';

    var channel = {};

    function Queue(options) {

    }

    Queue.send = function (msgType, msg) {

        var response = new serviceMessage.ServiceResponse();
        var message = new serviceMessage.ServiceMessage();

        if (msg instanceof serviceMessage.ServiceMessage) {
            response.correlationId = msg.correlationId;
            message.correlationId = msg.correlationId;
            message.data = msg.data;
        } else {
            message.data = msg;
            message.setCorrelationId();
        }

        if (channel[msgType]) {
            try {
                var str = JSON.stringify(message.toJSON());


                response.isSuccess = channel[msgType].channel.publish(msgType, channel[msgType].routingKey, new Buffer(str), {persistent: channel[msgType].persistent});
                if (!response.isSuccess) {
                    response.errors.push('The message could not be sent');
                }
            } catch (e) {
                response.isSuccess = false;
                response.errors.push('The message could not be sent due to an exception: ' + e.message + ' ', +e.stack);
            }

        } else {
            response.isSuccess = false;
            response.errors.push('There are no channels open for this message type');
        }

        return response;
    };

    function listen(parameters) {
        var promises = parameters.promises;
        if (!_.isUndefined(parameters.listener)) {
            promises.push(parameters.channel.consume(parameters.messageType, function (msg) {
                console.log(msg);
                if(msg.content.length > 0) {
                    var obj = JSON.parse(msg.content.toString());
                    var message = new serviceMessage.ServiceMessage();
                    message.fromJSON(obj);
                }
                if (_.isFunction(parameters.listener)) {
                    q.when(parameters.listener(message)).then(function (result) {
                        channel[parameters.messageType].channel.ack(msg);
                    });
                } else if (_.isString(parameters.listener)) {

                }
            }));
        }
    }

    Queue.setup = function (config) {

        qu.connect(config.connection.url).then(function (connection) {
            var dfd = q.defer();
            var promises = [];
            process.nextTick(function () {
                for (var i = 0; i < config.types.length; i++) {

                    (function iterate(currentConfig) {
                        var ok = connection.createChannel();
                        promises.push(ok);
                        ok = ok.then(function (ch) {

                            channel[currentConfig.type] = {
                                channel: ch,
                                persistent: currentConfig.pattern == 'topic',
                                routingKey: currentConfig.pattern == 'topic' ? currentConfig.type : ''
                            };

                            if (currentConfig.pattern == 'topic') {
                                promises.push(ch.assertExchange(currentConfig.type, 'topic'));
                                promises.push(ch.assertQueue(currentConfig.type));
                                promises.push(ch.bindQueue(currentConfig.type, currentConfig.type, currentConfig.type));
                                listen({
                                    messageType: currentConfig.type,
                                    listener: currentConfig.listener,
                                    promises: promises,
                                    channel: ch
                                });
                            } else if (currentConfig.pattern == 'fanout') {
                                promises.push(ch.assertExchange(currentConfig.type, 'fanout'));
                                if (!_.isUndefined(currentConfig.listener)) {
                                    promises.push(ch.assertQueue('', {
                                        durable: false,
                                        autoDelete: true
                                    }).then(function (qq) {
                                        ch.bindQueue(qq.name, currentConfig.type);
                                        listen({
                                            messageType: qq.name,
                                            listener: currentConfig.listener,
                                            promises: promises,
                                            channel: ch
                                        });
                                    }));
                                }
                            }
                        });
                    })(config.types[i]);


                }
                q.all(promises).then(function () {
                    dfd.resolve();
                });

            });
            return dfd.promise;


        }).then(function () {
                config.startupHandler();
            },
            console.warn);


    };

    module.exports = Queue;

})(
    require('lodash'),
    require('q'),
    require('amqplib'),
    require('json-buffer'),
    require('buffer').Buffer,
    require('jsai-serviceMessage')
);
