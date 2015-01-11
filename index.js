/**
 * Created by jean-sebastiencote on 12/20/14.
 */
(function (_, q, qu, Buffer, serviceMessage) {

    'use strict';

    var channels = {};
    var connected = false;

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
            response.correlationId = message.correlationId;
        }

        if (channels[msgType]) {
            try {
                var str = JSON.stringify(message.toJSON());

                response.isSuccess = channels[msgType].channel.publish(msgType, channels[msgType].routingKey, new Buffer(str), {persistent: channels[msgType].persistent});
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
                if (msg.content.length > 0) {
                    var obj = JSON.parse(msg.content.toString());
                    var message = new serviceMessage.ServiceMessage();
                    message.fromJSON(obj);
                }
                if (_.isFunction(parameters.listener)) {
                    q.fcall(parameters.listener, message).then(function (result) {

                        //TODO track completion of the message

                        channels[parameters.messageType].channel.ack(msg);
                    }).fail(function(error){

                        //TODO track completion of the message in error

                        //put the message in an error queue
                        Queue.send('error', {error: error, originalMessage: msg, exchange: msg.fields.exchange, routingKey: msg.fields.routingKey});

                        channels[parameters.messageType].channel.ack(msg);

                    }).done();
                } else if (_.isString(parameters.listener)) {

                }
            }));
        }
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

        if(_.isUndefined(config) || _.isUndefined(config.connection)) {
            deferred.resolve();
            return deferred.promise;
        }

        qu.connect(config.connection.url).then(function (connection) {
            connected = true;
            connection.on('error', handleError);

            var dfd = q.defer();
            var promises = [];
            process.nextTick(function () {

                //create the error queue
                var errorChannel = connection.createChannel();
                promises.push(errorChannel);
                errorChannel.then(function(ch) {
                   channels['error'] = {channel: ch, persistent: true, routingKey: 'error'};
                    promises.push(ch.assertExchange('error', 'topic'));
                    promises.push(ch.assertQueue('errorQueue'));
                    promises.push(ch.bindQueue('errorQueue', 'error', 'error'));
                });

                for (var i = 0; i < config.types.length; i++) {

                    (function iterate(currentConfig) {
                        var ok = connection.createChannel();
                        promises.push(ok);
                        ok = ok.then(function (ch) {

                            channels[currentConfig.type] = {
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
                if(config.startupHandler) {
                    config.startupHandler(channels);
                }
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
