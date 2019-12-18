 This library implements the ghetto lock described [here](https://github.com/memcached/memcached/wiki/ProgrammingTricks#ghetto-central-locking). The lock isn't resistant to server failures and should be used only in situations where strong locking guarantees are not required.

 A popular use case for this lock is to avoid the [stampeding herd problem](https://en.wikipedia.org/wiki/Thundering_herd_problem) caused by a cache miss.

 # Example:

 ```rust
 use ghetto_lock::{LockOptions, LockError};
 use memcache::Client;
 use std::borrow::Cow;

 fn expensive_computation() -> u64 {
     2 * 2
 }

 fn main() {
     let mut client = Client::connect("memcache://localhost:11211").expect("error creating client");
     let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner-1"))
                     .with_expiry(1)
                     .build()
                     .expect("failed to build client");
     let value = client.get("key").expect("failed to get key");
     let v = if value.is_none() {
         lock.try_acquire()
             .and_then(|_guard| {
                 // compute and update cache for other instances to consume
                 let v = expensive_computation();
                 client.set("key", v, 5).expect("failed to set key");
                 Ok(v)
             })
             .or_else(|_| loop {
                 // poll cache key until it is updated.
                 let v = client.get("key").expect("failed to get key");
                 if v.is_none() {
                     continue;
                 }
                 break Ok::<_, LockError>(v.unwrap());
             }).unwrap()
     } else { value.unwrap() };
     assert_eq!(4, v);
}
```
