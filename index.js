/**
 * Created by jean-sebastiencote on 12/20/14.
 */
(function (_, q, qu, Buffer, serviceMessage) {

    'use strict';

    var channels = {};
    var connected = false;

    function Queue(options) {

    }

    function getRoutingKey(rouingKeyPrefix, routingKeyBody) {
        if(!_.isUndefined(routingKeyPrefix) && !_.isUndefined(routingKeyBody)) {
            return routingKeyPrefix + '.' + routingKeyBody;
        }
        return routingKeyBody;
    }

    function getRoutingKeyFromChannel(channel) {
        if(!_.isUndefined(channel) && !_.isNull(channel)) {
            return getRoutingKey(channel.routingKeyPrefix, channel.routingKey);
        }
        throw Error('Channel is not defined');
    }

    Queue.send = function (msgType, msg, withDelay) {

        var response = null;
        var message = null;
        withDelay = withDelay || -1;

        if (msg instanceof serviceMessage.ServiceMessage) {
            response = msg.createServiceResponseFrom();
            message = msg;
        } else {
            message = new serviceMessage.ServiceMessage({data: msg});
            message.setCorrelationId();
            response = message.createServiceResponseFrom();
        }

        if (channels[msgType]) {
            try {
                var str = JSON.stringify(message.toJSON());

                if (withDelay < 1) {
                    response.isSuccess = channels[msgType].channel.publish(msgType,
                        getRoutingKeyFromChannel(channels[msgType]),
                        new Buffer(str), {persistent: channels[msgType].persistent});
                } else {
                    response.isSuccess = channels[msgType].channel.publish(msgType + "-delayed",
                        getRoutingKeyFromChannel(channels[msgType]), new Buffer(str), {
                            persistent: channels[msgType].persistent,
                            headers: {'x-delay': withDelay}
                        });
                }
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

    function handleConsumeError(error, msg, parameters) {
        //TODO track completion of the message in error

        //put the message in an error queue
        Queue.send('error', {
            error: error,
            originalMessage: msg,
            exchange: msg.fields.exchange,
            routingKey: msg.fields.routingKey
        });

        channels[parameters.messageType].channel.ack(msg);
    }

    function listen(parameters) {
        var dfd = q.defer();
        if (!_.isUndefined(parameters.listener)) {
            parameters.channel.consume(parameters.queueName, function (msg) {
                try {
                    if (msg.content.length > 0) {
                        var obj = JSON.parse(msg.content.toString());
                        var message = new serviceMessage.ServiceMessage();
                        message.fromJSON(obj);
                    }
                    if (_.isFunction(parameters.listener)) {
                        q.fcall(parameters.listener, message).then(function (result) {

                            channels[parameters.messageType].channel.ack(msg);
                        }).fail(function (error) {
                            handleConsumeError(error, msg, parameters);
                        }).done();
                    } else {
                        throw('Invalid listener on : ' + parameters.messageType);
                    }
                } catch (error) {

                    handleConsumeError(error, msg, parameters);
                }

            }).then(function () {
                dfd.resolve();
            });
        }
        return dfd.promise;
    }

    function handleError(err) {

        console.log('disconnected');
        console.log(err);

        connected = false;

        var interval = setInterval(function () {

            console.log('attempting reconnection...');

            configureQueues(channels.config).then(function () {
                if (connected) {
                    clearInterval(interval);
                    console.log('reconnected');
                }
            });


        }, 10000)

    }

    function configureQueues(config) {


        var deferred = q.defer();

        if (_.isUndefined(config) || _.isUndefined(config.connection)) {
            deferred.resolve();
            return deferred.promise;
        }

        var dfd = q.defer();

        qu.connect(config.connection.url).then(function (connection) {
            connected = true;
            connection.on('error', handleError);

            var promises = [];
            process.nextTick(function () {

                //create the error queue
                var errorChannel = connection.createChannel();
                promises.push(errorChannel);
                errorChannel.then(function (ch) {
                    channels['error'] = {channel: ch, persistent: true, routingKey: 'error'};
                    var ok = ch.assertExchange('error', 'topic');
                    promises.push(ok);
                    ok = ok.then(ch.assertQueue('errorQueue'));
                    promises.push(ok);
                    ok = ok.then(ch.bindQueue('errorQueue', 'error', 'error'));
                    promises.push(ok);
                });

                for (var i = 0; i < config.types.length; i++) {

                    (function iterate(currentConfig) {
                        var ok = connection.createChannel();
                        promises.push(ok);
                        ok = ok.then(function (ch) {

                            channels[currentConfig.type] = {
                                channel: ch,
                                persistent: currentConfig.pattern == 'topic',
                                routingKeyPrefix: currentConfig.pattern == 'durableEvent' ? 'all' : undefined,
                                routingKey: (currentConfig.pattern == 'topic' || currentConfig.pattern == 'durableEvent') ? currentConfig.type : ''
                            };

                            if (currentConfig.pattern == 'topic' || currentConfig.pattern == 'durableEvent') {
                                ok = ok.then(ch.assertExchange(currentConfig.type, 'topic', {durable: true}));
                                promises.push(ok);

                                if (currentConfig.supportDelay == true) {
                                    ok = ok.then(ch.assertExchange(currentConfig.type + "-delayed", "x-delayed-message", {
                                        durable: true,
                                        arguments: {'x-delayed-type': 'topic'}
                                    }));
                                    promises.push(ok);
                                }

                                if(!_.isUndefined(currentConfig.listener)) {
                                    var queueName = currentConfig.type;
                                    var routingKey = currentConfig.type;

                                    if (currentConfig.pattern == 'durableEvent') {
                                        queueName = (currentConfig.serviceName || 'unknownn') + '_' + queueName;
                                        routingKey = '*.' + currentConfig.type;
                                    }


                                    ok = ok.then(ch.assertQueue(queueName));
                                    promises.push(ok);

                                    //bind the queue
                                    ok = ok.then(ch.bindQueue(queueName, currentConfig.type, routingKey));
                                    promises.push(ok);

                                    if (currentConfig.supportDelay == true) {
                                        //if there is a delayed exchange, add another binding to the queue for it
                                        ok = ok.then(ch.bindQueue(queueName, currentConfig.type + "-delayed", routingKey));
                                        promises.push(ok);
                                    }
                                    ok = ok.then(listen({
                                        messageType: currentConfig.type,
                                        queueName: queueName,
                                        listener: currentConfig.listener,
                                        channel: ch
                                    }));
                                    promises.push(ok);
                                }

                            } else if (currentConfig.pattern == 'fanout') {
                                ok = ok.then(ch.assertExchange(currentConfig.type, 'fanout'));
                                promises.push(ok);
                                if (!_.isUndefined(currentConfig.listener)) {
                                    var d = q.defer();
                                    ok = ok.then(
                                        ch.assertQueue('', {
                                            durable: false,
                                            autoDelete: true
                                        }).then(function (qq) {
                                            return ch.bindQueue(qq.queue, currentConfig.type).then(function () {
                                                listen({
                                                    messageType: currentConfig.type,
                                                    queueName: qq.queue,
                                                    listener: currentConfig.listener,
                                                    channel: ch
                                                }).then(function () {
                                                    d.resolve();
                                                });
                                            })
                                        }));

                                    //});

                                    promises.push(ok);
                                    promises.push(d.promise);
                                }
                            }
                        });
                    })(config.types[i]);


                }
                q.all(promises).then(function () {

                    dfd.resolve();
                });

            });


        }).then(function () {
                dfd.promise.then(function () {
                    if (config.startupHandler) {
                        config.startupHandler(channels);
                    }
                })
            },
            console.warn).done(function () {
                deferred.resolve();
            }
        );

        return deferred.promise;
    }

    Queue.setup = function (config) {

        if (_.isUndefined(config) || _.isNull(config)) return;

        channels.config = config;

        configureQueues(config);

    };

    module.exports = Queue;

})(
    require('lodash'),
    require('q'),
    require('amqplib'),
    require('buffer').Buffer,
    require('jsai-servicemessage')
);
