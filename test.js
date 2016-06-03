'use strict'

var assert = require('assert')
var async = require('async')
var exec = require('methmeth')
var gcloud = require('gcloud')
var pubsub = gcloud.pubsub()

var PREFIX = 'gcloud-node-issue-1356-'

var topic = pubsub.topic(generateName())
var subscription = topic.subscription(generateName())

describe('memory leak', function () {
  before(deleteTopics)
  before(createSubscription)
  after(deleteTopics)
  beforeEach(logMemory)

  var TIME_BETWEEN_TESTS_MS = 60000
  var timesToRun = 120
  while (timesToRun--) {
    it('should not leak', function (done) {
      async.times(10, publishMessage, function (err) {
        assert.ifError(err)
        setTimeout(done, TIME_BETWEEN_TESTS_MS)
      })
    })
  }
})

function generateName() {
  return PREFIX + Date.now()
}

function logMemory() {
  console.log(new Date(), 'memory usage', getMemory(), 'MB')
}

function getMemory() {
  return Math.round(process.memoryUsage().heapUsed / 1000000)
}

function deleteTopics(callback) {
  pubsub.getTopics(function (err, topics) {
    if (err) throw err

    var testTopics = topics.filter(function (topic) {
      return topic.name.indexOf(PREFIX) === 0
    })

    async.each(testTopics, exec('delete'), callback)
  })
}

function createSubscription(callback) {
  topic.get({ autoCreate: true }, function (err) {
    if (err) return callback(err)
    subscription.get({ autoCreate: true }, callback)
  })
}

function publishMessage(_, callback) {
  topic.publish({ data: 'message' }, callback)
}