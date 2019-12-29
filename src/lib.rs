//! This library implements the lock described
//! [here](https://github.com/memcached/memcached/wiki/ProgrammingTricks#ghetto-central-locking).
//! The lock isn't resistant to server failures and should be used only in situations where
//! strong locking guarantees are not required.
//!
//! A popular use case for this lock is to avoid the [stampeding herd
//! problem](https://en.wikipedia.org/wiki/Thundering_herd_problem) caused by a cache
//! miss.
//!
//! # Example:
//!
//! ```rust
//! use macgyver_lock::{LockOptions, LockError};
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
//!```
#[macro_use]
extern crate log;
use memcache::Client;
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub use memcache::{Connectable, MemcacheError};

const MEMCACHE_DEFAULT_URL: &'static str = "memcache://localhost:11211";

/// The lock configuration along with the connection to memcache server.
pub struct MacGyverLock<'a> {
    /// Name of the lock. Also used as the key in memcached.
    name: Cow<'a, str>,
    /// Memcache client instance.
    memcache: Client,
    /// Expiry in seconds.
    expiry: u32,
    /// The value set in the key to identify the current owner of the key.
    owner: Cow<'a, str>,
}

/// Lock guard returned after successfully acquiring a lock.
/// The lock is automatically released when the `Guard` is dropped.
///
/// Note: The `PartialEq` impl only exists to make error handling simple.
/// `Guard`s should never be compared. It always returns `false`.
pub struct Guard<'l, 'b> {
    released: bool,
    lock: &'l mut MacGyverLock<'b>,
}

impl<'l, 'b> PartialEq for Guard<'l, 'b> {
    // There can never be two mutable references to MacGyverLock.
    fn eq(&self, _: &Self) -> bool {
        false
    }
}

impl<'l, 'b> fmt::Debug for Guard<'l, 'b> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Guard {{ released: {} }}", self.released)
    }
}

impl<'a, 'b> Guard<'a, 'b> {
    /// Releases the lock by deleting the key in memcache.
    ///
    /// Returns `Ok(())` when the lock is successfully released.
    pub fn try_release(mut self) -> Result<(), LockError> {
        if !self.released {
            let result = self.lock.release().map_err(Into::into);
            self.released = result.is_ok();
            result
        } else {
            Err(LockError::AlreadyReleased)
        }
    }
}

impl<'a, 'b> Drop for Guard<'a, 'b> {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.lock.release();
        }
    }
}

/// Result type used by the `try_acquire` function.
pub type LockResult<'a, 'b> = Result<Guard<'a, 'b>, LockError>;

impl<'a> MacGyverLock<'a> {
    fn is_expired(&self, lock_time: Instant) -> bool {
        self.expiry != 0 // denotes no expiry
            && if self.expiry <= 60 * 60 * 24 * 30 { // memcache interprets these values as seconds
                lock_time.elapsed().as_secs() >= u64::from(self.expiry)
            } else {
                // check unix time.
                UNIX_EPOCH + Duration::from_secs(u64::from(self.expiry)) <= SystemTime::now()
            }
    }

    // 'l is the scope for which the lock should live.
    /// Try to acquire the lock, returning the guard if the lock was successfully acquired.
    /// Otherwise returns an error denoting why the lock couldn't be acquired.
    pub fn try_acquire<'l>(&'l mut self) -> LockResult<'l, 'a> {
        debug!(target: "macgyver-lock", "trying to acquire lock: {} for user: {}", &self.name, &self.owner);
        let instant = Instant::now();
        match self.memcache.add(&self.name, &*self.owner, self.expiry) {
            Ok(()) => {
                // if lock expired before the call could return, we don't have the lock, so error.
                if self.is_expired(instant) {
                    debug!(
                        target: "macgyver-lock",
                        "failed to acquire lock: {} for user: {}, memcache.add latency: {}",
                        &self.name, &self.owner, instant.elapsed().as_secs()
                    );
                    Err(LockError::TimedOut)
                } else {
                    debug!(target: "macgyver-lock", "acquired lock: {} for user: {}", &self.name, &self.owner);
                    Ok(Guard {
                        released: false,
                        lock: self,
                    })
                }
            }
            Err(e) => match e {
                // key already exists in server, so retry
                MemcacheError::ServerError(0x02) => {
                    debug!(target: "macgyver-lock", "failed to acquire lock: {}, already taken", &self.name);
                    Err(LockError::FailedToAcquire)
                }
                e => Err(LockError::MemcacheError(e)),
            },
        }
    }

    /// Using CAS with an expiry value in the past to atomically delete the lock as
    /// `delete` doesn't accept `cas_id`.
    ///
    /// Returns `true` if the key has been successfully deleted. `false` if key expired.
    fn delete(&mut self, cas: u64) -> Result<(), LockError> {
        match self.memcache.cas(&self.name, "", 1576412321, cas)? {
            true => Ok(()),
            false => Err(LockError::AlreadyReleased),
        }
    }

    /// Releases the lock by deleting the key in memcache.
    fn release(&mut self) -> Result<(), LockError> {
        let result: HashMap<String, (Vec<u8>, u32, Option<u64>)> =
            self.memcache.gets(&[&self.name])?;
        if let Some((current_owner, _, cas)) = result.get(&*self.name) {
            if *current_owner == &*self.owner.as_bytes() {
                debug!(target: "macgyver-lock", "trying to release: {}", &self.name);
                self.delete(cas.expect("cas should be present"))
            } else {
                debug!(target: "macgyver-lock", "trying to release: {}, not owned", &self.name);
                Err(LockError::NotOwned)
            }
        } else {
            debug!(target: "macgyver-lock", "trying to release: {}, already released", &self.name);
            Err(LockError::AlreadyReleased)
        }
    }
}

/// Error type
#[derive(Debug)]
pub enum LockError {
    /// When trying to release a lock, it was found to be already released.
    /// This could be because the key has expired.
    AlreadyReleased,
    /// Tried to release the lock but it wasn't owned by the current instance.
    NotOwned,
    /// An other user succeeded in acquiring the lock.
    FailedToAcquire,
    /// The `add` call was successful, but a possible latency in network/process
    /// scheduling caused the lock to be released.
    TimedOut,
    /// Other errors returned by the underlying memcache client.
    MemcacheError(MemcacheError),
}

impl PartialEq for LockError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LockError::AlreadyReleased, LockError::AlreadyReleased) => true,
            (LockError::NotOwned, LockError::NotOwned) => true,
            (LockError::FailedToAcquire, LockError::FailedToAcquire) => true,
            (LockError::TimedOut, LockError::TimedOut) => true,
            (LockError::MemcacheError(l), LockError::MemcacheError(r)) => match (l, r) {
                (MemcacheError::Io(l), MemcacheError::Io(r)) => l.kind() == r.kind(),
                (MemcacheError::ClientError(l), MemcacheError::ClientError(r)) => l == r,
                (MemcacheError::ServerError(l), MemcacheError::ServerError(r)) => l == r,
                (MemcacheError::FromUtf8(_), MemcacheError::FromUtf8(_)) => true,
                (MemcacheError::ParseIntError(_), MemcacheError::ParseIntError(_)) => true,
                (MemcacheError::ParseFloatError(_), MemcacheError::ParseFloatError(_)) => true,
                (MemcacheError::ParseBoolError(_), MemcacheError::ParseBoolError(_)) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

impl From<MemcacheError> for LockError {
    fn from(error: MemcacheError) -> Self {
        Self::MemcacheError(error)
    }
}

/// Builder for `MacGyverLock`
pub struct LockOptions<'a, C> {
    /// Name of the lock. Also used as the memcache key
    name: Cow<'a, str>,
    /// `Connectable` which can resolve to a list of memcache servers.
    connectable: C,
    /// Expiry to use for the memcache lock key in seconds.
    expiry: Option<u32>,
    /// Unique identifier representing the current owner of the lock.
    owner: Cow<'a, str>,
    /// Read timeout for the underlying connection.
    read_timeout: Option<Duration>,
    /// Write timeout for the underlying connection.
    write_timeout: Option<Duration>,
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
        }
    }
}

impl<'a, C: Connectable> LockOptions<'a, C> {
    /// Set the expiry in seconds for the the lock key. The lock will be
    /// released after this number of seconds elapse.
    /// Defaults to `10` seconds.
    ///
    /// Note: A value of `0` denotes no expiry and a value greater than 24*60*60*30
    /// is interpreted as a unix timestamp.
    ///
    /// Note 2: `std::time::SystemTime::now` is used to check if the lock has expired
    /// when expiry is interepreted as UNIX timestamp. The check might fail if the system
    /// time has been tampered with.
    ///
    /// If a user holding the lock panics, it could lead to a deadlock when there is no
    /// expiry.
    pub fn with_expiry(mut self, expiry: u32) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// The memcache servers to use.
    ///
    /// Defaults to `memcache://localhost:11211`
    ///
    /// Note: The `Connectable` shouldn't use ASCII protocol because the underlying memcache
    /// library doesn't give a way to detect if acquiring the lock succeeded.
    pub fn with_connectable<K: Connectable>(self, connectable: K) -> LockOptions<'a, K> {
        LockOptions {
            connectable: connectable,
            name: self.name,
            expiry: self.expiry,
            owner: self.owner,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
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

    /// Build the lock.
    pub fn build(self) -> Result<MacGyverLock<'a>, LockError> {
        let mut memcache = Client::connect(self.connectable)?;
        memcache.set_read_timeout(self.read_timeout)?;
        memcache.set_write_timeout(self.write_timeout)?;
        Ok(MacGyverLock {
            name: self.name,
            memcache,
            owner: self.owner,
            expiry: self.expiry.unwrap_or(10),
        })
    }
}
