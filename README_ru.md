# node-redlock-watchdog

NodeJS модуль для освобождения `redlock` ключей внутри `redis`, которые были потеряны процессами "владельцами".

## Использование

Данный модуль может использоваться как внутри процесса, который аллоцирует `redlock` ключи, так и в отдельном процессе.

Пример подключения

```javascript
// где-то выше в коде
// const redisClient = redis.createClient(CONFIG.redis.port, CONFIG.redis.host);

// ...
// Q.Promise - используем модуль Q для старых версий nodejs
watchdog.init(redisClient, Q.Promise, {
  // 
  onlyHeartbeat: false,
  // 
  delayMs: 60000, // 1min
  // 
  redlockHashKey: "redlock_list",
  // wait 5min until remove zombie redlock
  maxStaleRetries: 5,
  // write debug logs
  debug: true
});

watchdog.start();
```

 Пример использования

```javascript
// когда вы аллоцируете `redlock`
watchdog.addHeartbeat(lockKey, {server:CONFIG.hostname, pid:process.pid});

// ...
// когда вы освобождаете `redlock`
watchdog.removeHeartbeat(lockKey);
```
