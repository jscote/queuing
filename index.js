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

        if (msg instanceof serviceMessage.ServiceMessage) {
            response.correlationId = msg.correlationId;
        }

        if (channel[msgType]) {
            try {
                var str = JSON.stringify(msg);

                response.isSuccess = channel[msgType].channel.publish(msgType, channel[msgType].routingKey, new Buffer(str), {persistent: channel[msgType].persistent});
                if (!response.isSuccess) {
                    response.errors.push('The message could not be sent');
                }
            } catch(e) {
                response.isSuccess = false;
                response.errors.push('The message could not be sent due to an exception: ' + e.message + ' ', + e.stack);
            }

        } else {
            response.isSuccess = false;
            response.errors.push('There are no channels open for this message type');
        }

        return response;
    };

    Queue.listen = function (msgType, callback) {

    };

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
                                persistent: currentConfig.pattern == 'topic' ? true : false,
                                routingKey: currentConfig.pattern == 'topic' ? currentConfig.type : ''
                            };

                            if (currentConfig.pattern == 'topic') {
                                promises.push(ch.assertExchange(currentConfig.type, 'topic'));
                                promises.push(ch.assertQueue(currentConfig.type));
                                promises.push(ch.bindQueue(currentConfig.type, currentConfig.type, currentConfig.type));
                                promises.push(ch.consume(currentConfig.type, function (msg) {
                                    console.log(msg);
                                    var obj = JSON.parse(msg.content.toString());
                                    channel[currentConfig.type].channel.ack(msg);

                                }));
                            } else if (currentConfig.pattern == 'fanout') {
                                promises.push(ch.assertExchange(currentConfig.type, 'fanout'));
                                promises.push(ch.assertQueue('', {
                                    durable: false,
                                    autoDelete: true
                                }).then(function (qq) {
                                    ch.bindQueue(qq.name, currentConfig.type);
                                    ch.consume(qq.name, function (msg) {
                                        console.log(msg);
                                        channel[currentConfig.type].channel.ack(msg);
                                    });
                                }));
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
                console.log('are we done yet');
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

config = {
    connection: {url: '127.0.0.1'},
    types: [
        {
            type: 'CustomerUpdate', pattern: 'topic', receiveHandler: function () {
        }
        },
        {type: 'CustomerUpdated', pattern: 'fanout'},
        {type: 'CustomerCreated', pattern: 'fanout'}
    ]
};