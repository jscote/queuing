/**
 * Created by jean-sebastiencote on 12/20/14.
 */
module.exports = {
    setUp: function (callback) {
        callback();
    },
    tearDown: function (callback) {
        // clean up
        callback();
    },
    t2estTaskNodeCanOnlyHaveANodeObjectSuccessor: function (test) {

        var queue = require('../index.js');

        var p = queue.setup({
            connection: {url: 'amqp://127.0.0.1?heartbeat=10'},
            startupHandler: function() {
                queue.send('CustomerUpdate', {toto: 'hello'});
            },
            types: [
                {
                    type: 'CustomerUpdate', pattern: 'topic', receiveHandler: function () {
                }
                },
                {type: 'CustomerUpdated', pattern: 'fanout'},
                {type: 'CustomerCreated', pattern: 'fanout'}
            ]
        });

        test.done();
    },
    testSendMessage: function(test) {

        var queue = require('../index.js');

        queue.send('CustomerUpdate', {toto: 'hello'});

        test.done();
    }
};