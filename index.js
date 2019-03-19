"use strict";

/**
 *
 * @type {redis.RedisClient}
 */
let redisClient = null;

/**
 * Promise/A+ instance.
 * @type {Promise|null}
 */
let Promise = null;

let onlyHeartbeat;
let delayMs;
let redlockHashKey;
let redlockInfoKey;
let maxStaleRetries;
let debug;

function reset() {
  onlyHeartbeat = false;
  delayMs = 60000; // 1min
  redlockHashKey = 'redlock_list';
  redlockInfoKey = 'redlock_info';
  maxStaleRetries = 5; // wait 5min until remove zombie redlock
  debug = false;
}

reset();

/**
 * Initialize module.
 *
 * It is possible to initialize this module only once.
 *
 * @param {{}} rc
 * @param {Promise} pr
 * @param {{}} options
 */
let ns = (rc, pr, options) => {
  if (redisClient) {
    throw new Error("redis-watchdog already initialized!");
  }

  redisClient = rc;
  Promise = pr;
  options = options || {};
  if (typeof options.onlyHeartbeat === 'boolean') onlyHeartbeat = options.onlyHeartbeat;
  if (options.delayMs) delayMs = options.delayMs;
  if (options.redlockHashKey) redlockHashKey = options.redlockHashKey;
  if (options.maxStaleRetries) {
    if (options.maxStaleRetries < 2) {
      throw new Error("you should assign to maxStaleRetries value 2 or more");
    }
    maxStaleRetries = options.maxStaleRetries;
  }
  if (typeof options.debug === 'boolean') debug = options.debug;
  return ns;
};

ns.init = ns;
ns.release = () => {
  redisClient = null;
  Promise = null;
  reset();
  heartbeats = {};
};

const denodeify = function (fn) {
  return new Promise((resolve, reject) =>
    fn((err, res) => {
      if (err) reject(err);
      else resolve(res);
    }));
};

// This process items.
// redlock_key, value - {redlock_key, counter}
let heartbeats = {};

/**
 * 
 * @param {string} redlock_key
 * @param {{server:string, pid:number}} info
 * @return {Promise}
 */
ns.addHeartbeat = function (redlock_key, info) {
  heartbeats[redlock_key] = 1;
  let job = denodeify(redisClient.hset.bind(redisClient, redlockHashKey, redlock_key, "0"));
  if (info) {
    job.then(() =>
      redisClient.hset.bind(redisClient, redlockInfoKey, redlock_key, 's:' + info.server + '|p:' + info.pid));
  }
  return job;
};

ns.removeHeartbeat = function (redlock_key) {
  delete heartbeats[redlock_key];
  return denodeify(redisClient.hdel.bind(redisClient, redlockHashKey, redlock_key))
    .then(() => 
      redisClient.hdel.bind(redisClient, redlockInfoKey, redlock_key));
};

/**
 * Update counters of heartbeats.
 * @return {Promise[]}
 */
const heartbeat = () =>
  Object.keys(heartbeats).map((key) =>
    denodeify(redisClient.hincrby.bind(redisClient, 'redlock_list', key, 1)));

ns.heartbeat = heartbeat;

let listeners = {
  removeStaled: null
};

ns.listen = (eventType, fn) => {
  if (eventType in listeners)
    listeners[eventType] = fn;
};

let previousItems = {};

// key => retry count
let stales = {};

/**
 * The "check" promise.
 * @type {null|Promise}
 */
let checkPromise = null;

/**
 * Check if redlock is not zombie (process that captured it was killed).
 *
 * Every process should increase counter every minute.
 * Any other process can remove it when counter is not increased during minute.
 *
 * Lets update counter every call and check if counter is expired if it not updated twice.
 *
 * Format: redlock_key, counter, retry_counter
 *
 * @return {Promise}
 * @private
 */
const check = function () {
  let jobs = heartbeat();
  
  debug && console.debug('redlock_watchdog heartbeats %j', heartbeats);
  
  // Check if there are expired redlocks, remove redlocks by key and after that remove keys from list
  checkPromise = Promise.all(jobs);
  
  if (onlyHeartbeat)
    return checkPromise;
  
  return checkPromise
    .then(() => denodeify(redisClient.hgetall.bind(redisClient, redlockHashKey)))
    .then(function (items) {
      // Если текущий счетчик НЕ отличается от предыдущего, то увеличиваем количество таких проверок по ключу.
      // Если текущий счетчик отличается от предыдущего, то устанавливаем количество проверок равное 0.
      // Если количество проверок больше некоторого значения MAX_STALE_RETRIES, то удаляем этот лок и, затем ключ.
      // Поскольку для локальных ключей мы только что увеличили счетчик, они заведомо не будут протухшими.
      if (items === null)
        items = {};
      
      let jobs = [];

      for (let key in items) {
        if (!items.hasOwnProperty(key)) continue;
        let val = items[key];

        let prev = previousItems[key] || "0";
        if (+prev !== +val) {
          stales[key] = 0;
        } else {
          if (!stales[key]) stales[key] = 1;
          else ++stales[key];
        }

        if (stales[key] >= maxStaleRetries) {
          jobs.push(new Promise(function(resolve, reject) {
            return denodeify(redisClient.del.bind(redisClient, key))
              
              .then(() =>
                denodeify(redisClient.hdel.bind(redisClient, redlockHashKey, key)))

              .then(() => 
                denodeify(redisClient.hdel.bind(redisClient, redlockInfoKey, key)))
              
              .then(() => {
                // after successful removing the key also remove it from "stales" object
                // and fire listener
                delete stales[key];
                listeners.removeStaled && listeners.removeStaled(key);
                resolve(key);
              })
              
              .catch((err) => {
                console.error("redlock_watchdog delete key %s failed with error " + err, key); 
                reject(err);
              });
          }));
        }
      }

      // удаляем из stales ключи, которых уже нет в текущем списке
      // их удалил другой watchdog или проснулся процесс-владелец
      let removed = [];
      for (let key in stales) {
        if (!stales.hasOwnProperty(key)) continue;
        if (typeof items[key] === 'undefined')
          removed.push(key);
      }

      for (let i = 0; i < removed.length; ++i) {
        delete stales[removed[i]];
      }

      debug && console.debug('redlock_watchdog stales %j', stales);
      
      previousItems = items;
      return Promise.all(jobs);
    })
    .catch((err) =>
      console.error("redlock_watchdog check failed with error ", err));
};

let runned = false;
let timeout = null;

const watchdog = function () {
  check().then(() =>
    runned && (timeout = setTimeout(watchdog, delayMs)));
};

ns.start = function() {
  if (runned) return;
  runned = true;
  watchdog();
};

ns.stop = () => {
  timeout && clearTimeout(timeout);
  runned = false;

  // if "check" is running then wait until it finished
  let _promise = checkPromise || Promise.resolve();
  
  checkPromise = null;
  return _promise;
};

module.exports = ns;