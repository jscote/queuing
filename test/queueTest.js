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
            connection: {url: 'amqp://127.0.0.1?heartbeat=300'},
            startupHandler: function () {
                queue.send('CustomerUpdated', {toto: 'hello'});
                queue.send('CustomerUpdate', {toto: 'hello'});
                queue.send('CustomerUpdate', {toto: 'hello'}, 5000);
                queue.send('CustomerUpdate', {toto: 'hello'}, 7000);


            },
            types: [
                {
                    type: 'CustomerUpdated', pattern: 'fanout', listener: function (msg) {
                    console.log('updated')
                }
                },
                {
                    type: 'CustomerUpdate', pattern: 'topic', supportDelay: true, listener: function (msg) {
                    console.log("inner function");
                    console.log(msg);
                }
                }

                //,
                //{type: 'CustomerCreated', pattern: 'fanout', listener: ''}
            ]
        });

        test.done();
    }/*,
     testSendMessage: function (test) {

     var queue = require('../index.js');

     queue.send('CustomerUpdate', {toto: 'hello'}, 30000);

     test.done();
     }*/
};