#[macro_use]
extern crate log;
use memcache::{Client, Connectable, MemcacheError};
use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, Instant},
};

const MEMCACHE_DEFAULT_URL: &'static str = "memcache://localhost:11211";

pub struct GhettoLock<'a> {
    tries: usize,
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
// * write docs: assumptions, failure cases, limitations
// * tests
// * what should be clone, send semantics?
// * should ghetto_lock have internal mutability?

/// Lock guard returned after successfully acquiring a lock. The lock is automatically unlocked
/// when the `Guard` is dropped.
pub struct Guard<'a, 'b> {
    unlocked: bool,
    lock: &'a mut GhettoLock<'b>,
}

impl<'a, 'b> Guard<'a, 'b> {
    pub unsafe fn unlock(&mut self) -> Result<bool, LockError> {
        if !self.unlocked {
            let result = self.lock.unlock().map_err(Into::into);
            if result.is_ok() {
                self.unlocked = true;
            }
            return result;
        }
        Err(LockError::AlreadyUnlocked)
    }
}

impl<'a, 'b> Drop for Guard<'a, 'b> {
    fn drop(&mut self) {
        if !self.unlocked {
            let _ = unsafe { self.lock.unlock() };
        }
    }
}

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
                        unlocked: false,
                        lock: self,
                    });
                }
                Err(e) => match e {
                    // key already exists in server, so retry
                    MemcacheError::ServerError(0x02) => {
                        debug!(target: "ghetto-lock", "failed to acquire lock: {}, try: {}, already taken", &self.name, i);
                        continue;
                    }
                    // TODO: may be add a configurable sleep here
                    // fail early on unrecoverable errors
                    e => return Err(LockError::MemcacheError(e)),
                },
            }
        }
        Err(LockError::TriesExceeded)
    }

    /// Unlocks the lock by deleting the key in memcache. This is NOT safe as of now
    /// because cas command is not supported by rust-memcache. Safe only if expiry time
    /// has not exceeded.
    unsafe fn unlock(&mut self) -> Result<bool, LockError> {
        // TODO: Use a gets(&self.name) + cas(&self.name, "", 1576412321, cas_id)
        // when its supported by rust-memcache because get + delete is not atomic
        let result: HashMap<String, String> = self.memcache.gets(vec![&self.name])?;
        if let Some(current_owner) = result.get(&*self.name) {
            if current_owner == &*self.owner {
                debug!(target: "ghetto-lock", "trying to unlock: {}", &self.name);
                self.memcache.delete(&self.name).map_err(Into::into)
            } else {
                debug!(target: "ghetto-lock", "trying to unlock: {}, not owned", &self.name);
                Err(LockError::NotOwned)
            }
        } else {
            debug!(target: "ghetto-lock", "trying to unlock: {}, already unlocked", &self.name);
            Err(LockError::AlreadyUnlocked)
        }
    }
}

pub enum LockError {
    TriesExceeded,
    AlreadyUnlocked,
    NotOwned,
    MemcacheError(MemcacheError),
}

impl From<MemcacheError> for LockError {
    fn from(error: MemcacheError) -> Self {
        Self::MemcacheError(error)
    }
}

/// Builder for GhettoLock
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
    /// automatically unlocked after this number of seconds elapse.
    /// Defaults to 10 seconds.
    ///
    /// Note: A value of 0 denotes no expiry. If a user holding the lock panics, it could lead to
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
