//! This library implements the ghetto lock described
//! [here](https://github.com/memcached/memcached/wiki/ProgrammingTricks#ghetto-central-locking).
//! The lock isn't resistant to server failures and should be used only in situations where
//! strong locking guarantees are not required.
//!
//! A popular use case for this library is to avoid the [stampeding herd
//! problem](https://en.wikipedia.org/wiki/Thundering_herd_problem) caused by a cache
//! miss.
//!
//! # Example:
//!
//! ```rust
//! use ghetto_lock::{LockOptions, LockError};
//! use memcache::Client;
//! use std::borrow::Cow;
//!
//! fn expensive_computation() -> u64 {
//!     2 * 2
//! }
//!
//! fn main() {
//!     let mut client = Client::connect("memcache://localhost:11211").expect("error creating client");
//!     let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner-1"))
//!                     .with_expiry(1)
//!                     .build()
//!                     .expect("failed to build client");
//!     let value = client.get("key").expect("failed to get key");
//!     let v = if value.is_none() {
//!         lock.try_acquire()
//!             .and_then(|_guard| {
//!                 // compute and update cache for other instances to consume
//!                 let v = expensive_computation();
//!                 client.set("key", v, 5).expect("failed to set key");
//!                 Ok(v)
//!             })
//!             .or_else(|_| loop {
//!                 // poll cache key until it is updated.
//!                 let v = client.get("key").expect("failed to get key");
//!                 if v.is_none() {
//!                     continue;
//!                 }
//!                 break Ok::<_, LockError>(v.unwrap());
//!             }).unwrap()
//!     } else { value.unwrap() };
//!     assert_eq!(4, v);
//!}
#[macro_use]
extern crate log;
use memcache::{Client, Connectable, MemcacheError};
use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, Instant},
};

const MEMCACHE_DEFAULT_URL: &'static str = "memcache://localhost:11211";

/// The lock configuration along with the connection to memcache server.
pub struct GhettoLock<'a> {
    /// Number of times to retry when before giving up to acquire a lock.
    tries: usize,
    /// Name of the lock. Also used as the key in memcached.
    name: Cow<'a, str>,
    /// Memcache client instance.
    memcache: Client,
    /// Expiry in seconds.
    expiry: u32,
    /// The value set in the key to identify the current owner of the key.
    owner: Cow<'a, str>,
}

// TODO:
// * auto-renewal option
// * tests

/// Lock guard returned after successfully acquiring a lock.
/// The lock is automatically released when the `Guard` is dropped.
pub struct Guard<'a, 'b> {
    released: bool,
    lock: &'a mut GhettoLock<'b>,
}

impl<'a, 'b> Guard<'a, 'b> {
    /// Releases the lock by deleting the key in memcache. This function is *NOT* safe
    /// as of now because the operations used are not atomic. The CAS command required
    /// to safely release the lock is not implemented by the memcache library yet.
    ///
    /// Returns `Ok(true)` when the lock is successfully released.
    pub unsafe fn try_release(&mut self) -> Result<bool, LockError> {
        if !self.released {
            let result = self.lock.release().map_err(Into::into);
            if result.is_ok() {
                self.released = true;
            }
            return result;
        }
        Err(LockError::AlreadyReleased)
    }
}

impl<'a, 'b> Drop for Guard<'a, 'b> {
    fn drop(&mut self) {
        if !self.released {
            let _ = unsafe { self.lock.release() };
        }
    }
}

/// Result type used by the `try_acquire` function.
pub type LockResult<'a, 'b> = Result<Guard<'a, 'b>, LockError>;

impl<'a> GhettoLock<'a> {
    pub fn try_acquire<'b>(&'b mut self) -> LockResult<'a, 'b> {
        for i in 0..self.tries {
            debug!(target: "ghetto-lock", "trying to acquire lock: {}, try: {}", &self.name, i);
            let instant = Instant::now();
            match self.memcache.add(&self.name, &*self.owner, self.expiry) {
                Ok(()) => {
                    // if lock expired before the call could return, we don't have the lock, so
                    // retry.
                    if instant.elapsed().as_secs() >= u64::from(self.expiry) {
                        debug!(
                            target: "ghetto-lock",
                            "failed to acquire lock: {}, try: {}, memcache.add latency: {}",
                            &self.name, i, instant.elapsed().as_secs()
                        );
                        continue;
                    }
                    debug!(target: "ghetto-lock", "acquired lock: {}, try: {}", &self.name, i);
                    return Ok(Guard {
                        released: false,
                        lock: self,
                    });
                }
                Err(e) => match e {
                    // key already exists in server, so retry
                    MemcacheError::ServerError(0x02) => {
                        debug!(target: "ghetto-lock", "failed to acquire lock: {}, try: {}, already taken", &self.name, i);
                        // TODO: may be add a configurable sleep here
                        continue;
                    }
                    // fail early on unrecoverable errors
                    e => return Err(LockError::MemcacheError(e)),
                },
            }
        }
        Err(LockError::TriesExceeded)
    }

    /// Releases the lock by deleting the key in memcache. This is NOT safe as of now
    /// because cas command is not supported by rust-memcache. Safe only if expiry time
    /// has not exceeded.
    unsafe fn release(&mut self) -> Result<bool, LockError> {
        // TODO: Use a gets(&self.name) + cas(&self.name, "", 1576412321, cas_id)
        // when its supported by rust-memcache because get + delete is not atomic
        let result: HashMap<String, String> = self.memcache.gets(vec![&self.name])?;
        if let Some(current_owner) = result.get(&*self.name) {
            if current_owner == &*self.owner {
                debug!(target: "ghetto-lock", "trying to release: {}", &self.name);
                self.memcache.delete(&self.name).map_err(Into::into)
            } else {
                debug!(target: "ghetto-lock", "trying to release: {}, not owned", &self.name);
                Err(LockError::NotOwned)
            }
        } else {
            debug!(target: "ghetto-lock", "trying to release: {}, already released", &self.name);
            Err(LockError::AlreadyReleased)
        }
    }
}

/// Error type
#[derive(Debug)]
pub enum LockError {
    /// Acquiring lock failed even after the number of configured retries.
    TriesExceeded,
    /// When trying to release a lock, it was found to be already released.
    /// This could be because the key has expired.
    AlreadyReleased,
    /// Tried to release the lock but it wasn't owned by the current instance.
    NotOwned,
    /// Other errors returned by the underlying memcache client.
    MemcacheError(MemcacheError),
}

impl From<MemcacheError> for LockError {
    fn from(error: MemcacheError) -> Self {
        Self::MemcacheError(error)
    }
}

/// Builder for `GhettoLock`
pub struct LockOptions<'a, C> {
    /// Name of the lock. Also used as the memcache key
    name: Cow<'a, str>,
    /// Connectable which can resolve to a list of memcache servers.
    connectable: C,
    /// Expiry to use for the memcache lock key in seconds.
    expiry: Option<u32>,
    /// Unique identifier representing the current owner of the lock.
    owner: Cow<'a, str>,
    /// Read timeout for the underlying connection.
    read_timeout: Option<Duration>,
    /// Write timeout for the underlying connection.
    write_timeout: Option<Duration>,
    /// The number of times to try acquiring the lock before giving up.
    tries: usize,
}

impl<'a> LockOptions<'a, &str> {
    /// Initialize the lock builder.
    /// `name` is used as the memcache key while trying to acquire the lock.
    /// `owner` is used to uniquely identify the current owner of the lock.
    /// Each individual user trying to acquire the lock should have a unique owner value.
    pub fn new(name: Cow<'a, str>, owner: Cow<'a, str>) -> Self {
        Self {
            name,
            connectable: MEMCACHE_DEFAULT_URL,
            expiry: None,
            owner: owner,
            read_timeout: None,
            write_timeout: None,
            tries: 1,
        }
    }
}

impl<'a, C: Connectable> LockOptions<'a, C> {
    /// Set the expiry in seconds for the the lock key. The lock will be
    /// released after this number of seconds elapse.
    /// Defaults to `10` seconds.
    ///
    /// Note: A value of `0` denotes no expiry. If a user holding the lock panics, it could lead to
    /// a deadlock when there is no expiry.
    pub fn with_expiry(mut self, expiry: u32) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// The memcache servers to use.
    pub fn with_connectable<K: Connectable>(self, connectable: K) -> LockOptions<'a, K> {
        LockOptions {
            connectable: connectable,
            name: self.name,
            expiry: self.expiry,
            owner: self.owner,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            tries: self.tries,
        }
    }

    /// Set the read timeout of the underlying connection to the server. The value should be very
    /// small compared to the key expiry time. Otherwise, we might never acquire the lock in case
    /// of network delays.
    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    /// Set the write timeout of the underlying connection to the server. The value should be very
    /// small compared to the expiry seconds. Otherwise, we might never acquire of the lock in case
    /// of network delays.
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }

    /// The number of retry attempts that should be made while trying to acquire the lock before
    /// giving up.
    ///
    /// # Panics
    ///
    /// Panics if tries is equal to 0.
    pub fn with_tries(mut self, tries: usize) -> Self {
        assert!(tries > 0);
        self.tries = tries;
        self
    }

    /// Build the lock.
    pub fn build(self) -> Result<GhettoLock<'a>, LockError> {
        let mut memcache = Client::connect(self.connectable)?;
        memcache.set_read_timeout(self.read_timeout)?;
        memcache.set_write_timeout(self.write_timeout)?;
        Ok(GhettoLock {
            tries: self.tries,
            name: self.name,
            memcache,
            owner: self.owner,
            expiry: self.expiry.unwrap_or(10),
        })
    }
}
