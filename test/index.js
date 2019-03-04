"use strict";

let Promise;

if (process.env.Q_PROMISE)
  Promise = require('q').Promise;
else
  Promise = require('promise');

const redis = require('redis');
const Redlock = require('redlock');
const redisClient = redis.createClient(6379, 'localhost');

const options = {
  delayMs: 100,
  maxStaleRetries: 2
};
const watchdog = require('../index');

const should = require('should');

var redlockConfig = {
  // the expected clock drift; for more details
  // see http://redis.io/topics/distlock
  driftFactor: 0.01,

  // the max number of times Redlock will attempt
  // to lock a resource before erroring
  retryCount: 3,

  // the time in ms between attempts
  retryDelay: 100
};

const testTimeoutMs = 30000; // 30sec

const denodeify = function (fn) {
  return new Promise((resolve, reject) =>
    fn((err, res) => {
      if (err) reject(err);
      else resolve(res);
    }));
};

function deleteRedisKey(key) {
  return denodeify(redisClient.del.bind(redisClient, key));
}

function stopAndReleaseWatchdog() {
  return watchdog.stop()
    .then(() => watchdog.release());
}

describe("single", function () {
  before(function () {
    // previous test process could be killed without free of redis key "a"
    return deleteRedisKey("a")
      .then(() => deleteRedisKey("redlock_list"));
  });
  beforeEach(function () {
    watchdog.init(redisClient, Promise, options);
  });
  afterEach(function () {
    return stopAndReleaseWatchdog()
      .then(() => deleteRedisKey("a"));
  });
  it("without any redlock - watchdog should check empty redlock_list without an error", function() {
    this.timeout(testTimeoutMs);
    return new Promise((resolve) => {
      watchdog.start();
      setTimeout(() => resolve(), 1000);
    });
  });
  it("with local heartbeat - watchdog should not remove it", function () {
    this.timeout(testTimeoutMs);
    let redlock = new Redlock([redisClient], redlockConfig);
    return redlock.lock('a', testTimeoutMs)
      .then(() => {
        return new Promise((resolve, reject) => {
          watchdog.listen('removeStaled', (key) =>
            reject(new Error("lock " + key + " should not be removed")));

          watchdog.addHeartbeat("a");

          watchdog.start();

          setTimeout(() => resolve(), 1000);
        });
      });
  });
  it("not send heartbeat - watchdog should remove redlock", function () {
    this.timeout(testTimeoutMs);

    // add redlock (not add heartbeat)
    // check if redis key exists
    // run watchdog and wait until removeKey listener will fired
    // check if redis key not exists
    let redlock = new Redlock([redisClient], redlockConfig);
    return redlock.lock('a', testTimeoutMs) // allocate resource during test working time
      .then(() =>
        denodeify((redisClient.get.bind(redisClient, 'a'))))
        
      .then((res) => {
        should(res).be.ok();

        // add key "a" to redlock_list; not append it to local "heartbeat"
        // it may do other process in real system
        // @see watchdog.setHeartbeat
        
        return denodeify(redisClient.hset.bind(redisClient, "redlock_list", "a", "0"))
      })
      
      .then(() => {
        return new Promise((resolve, reject) => {
          let to;
          watchdog.listen('removeStaled', (key) => {
            key.should.be.equal('a');
            to && clearTimeout(to);
            resolve();
          });

          // start without heartbeat of "a"
          watchdog.start();

          to = setTimeout(reject.bind(null, Error('key "a" not removed')), 1000);
        });
      })

      .then(() =>
        denodeify((redisClient.get.bind(redisClient, 'a'))))

      .then((res) =>
        should(res).be.null());
  });
});