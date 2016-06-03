'use strict';

var _ = require('lodash');
var async = require('async');
var Chance = require('chance');
var gcloud = require('gcloud');
var googleAuth = require('google-auto-auth');
var googleProtos = require('google-proto-files');
var grpc = require('grpc');
var relative = require('path').relative;

// Environment variables to set:
//      GCLOUD_PROJECT: Your project ID
//      USE_GCLOUD: If set, uses gcloud-node to create topics and publish.
var GCLOUD_PROJECT = process.env.GCLOUD_PROJECT;
var USE_GCLOUD = process.env.USE_GCLOUD;

var NUM_TOPICS_TO_CREATE = 40;
var NUM_PUBLISH_THREADS = 50;
var NUM_MESSAGES_TO_PUBLISH_PER_THREAD = 1000;
var MEMORY_WATCH_INTERVAL_MS = 10000;
var TEST_TOPIC_PREFIX = 'GCLOUD_NODE_ISSUE_1356_';

var CFG = {
    // keyFilename: '/path/to/key.json',
    //
    // // or...
    //
    // credentials: {
    //   client_email: '',
    //   private_key: ''
    // },
    scopes: ['https://www.googleapis.com/auth/cloud-platform']
};

var chance = new Chance();
var pubsub = gcloud.pubsub(CFG);
pubsub.maxRetries = 0;

var topicNames = [];
var gcloudTopics = {};
var publisherService;

if (USE_GCLOUD) {
    console.log('Using gcloud-node...');
}

printMemoryUsage();
setInterval(printMemoryUsage, MEMORY_WATCH_INTERVAL_MS);

async.series([
    createGrpcService,
    print('gRPC service created'),

    createTopics,
    print('Topics found or created'),

    publishToRandomTopic,
    print('Messages published')
], function(err) {
    printMemoryUsage();
    if (err) throw err;
    console.log('Done!');
});

function createGrpcService(callback) {
    if (USE_GCLOUD) return callback();

    googleAuth(CFG).getAuthClient(function(err, authClient) {
        if (err) throw err;

        var proto = grpc.load({
            root: googleProtos('..'),
            file: relative(googleProtos('..'), googleProtos.pubsub.v1)
        }, 'proto', { binaryAsBase64: true, convertFieldsToCamelCase: true });

        var credentials = grpc.credentials.combineChannelCredentials(
            grpc.credentials.createSsl(),
            grpc.credentials.createFromGoogleCredential(authClient)
        );

        publisherService = new proto.google.pubsub.v1.Publisher(
            'pubsub.googleapis.com',
            credentials
        );

        callback();
    });
}

function createTopics(callback) {
    async.times(NUM_TOPICS_TO_CREATE, function(index, next) {
        var topicName = 'projects/' + GCLOUD_PROJECT + '/topics/' + TEST_TOPIC_PREFIX + index;
        topicNames.push(topicName);

        if (USE_GCLOUD) {
            var topic = pubsub.topic(topicName);
            gcloudTopics[topicName] = topic;
            topic.get({ autoCreate: true }, next);
        } else {
            publisherService.getTopic({ topic: topicName }, function(err) {
                if (err && err.code === 5) {
                    // "5" === "404"
                    publisherService.createTopic({ name: topicName }, next);
                    return;
                }

                next(err);
            });
        }
    }, callback);
}

function publishToRandomTopic(callback) {
    async.times(NUM_PUBLISH_THREADS, function(_, threadDone) {
        async.times(NUM_MESSAGES_TO_PUBLISH_PER_THREAD, function(_, next) {
            var randomTopicName = topicNames[chance.integer({
                min: 0,
                max: topicNames.length - 1
            })];

            if (USE_GCLOUD) {
                var topic = gcloudTopics[randomTopicName];
                topic.publish({ data: 'data' }, function() {
                    // Ignore errors.
                    next();
                });
            } else {
                publisherService.publish({
                    topic: randomTopicName,
                    messages: [{ data: new Buffer('data').toString('base64') }]
                }, function() {
                    // Ignore errors.
                    next();
                });
            }
        }, threadDone);
    }, callback);
}

function printMemoryUsage() {
    var obj = process.memoryUsage();
    console.log('Date: ' + new Date() + ', Memory: ' + _.ceil((obj.heapUsed / 1000 / 1000), 2) + 'Mb');
}

function print(message) {
    return function(next) {
        console.log(message);
        next();
    };
}
