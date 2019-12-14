use memcache::{Client, Connectable, MemcacheError};
use std::{
    borrow::Cow,
    time::{Duration, Instant},
};

const MEMCACHE_DEFAULT_URL: &'static str = "memcache://localhost:11211";

pub struct GhettoLock<'a> {
    retries: usize,
    name: Cow<'a, str>,
    /// Memcache client instance.
    memcache: Client,
    /// Expiry in seconds.
    expiry: u32,
    /// The value set in the key to identify the current owner of the key.
    owner: Cow<'a, str>,
}

// TODO:
// * add logging
// * auto-renewal option
// * check cluster support
// * failure cases
// * tests
// * what should be clone, send semantics?
// * should ghetto_lock have internal mutability?
// * when unlocking, check if we are the owner

/// Lock guard returned after successfully acquiring a lock. The lock is automatically unlocked
/// when the `Guard` is dropped.
pub struct Guard<'a, 'b> {
    unlocked: bool,
    lock: &'a mut GhettoLock<'b>,
}

impl<'a, 'b> Guard<'a, 'b> {
    pub fn unlock(&mut self) -> Result<bool, Error> {
        if !self.unlocked {
            self.unlocked = true;
            return self.lock.unlock()
        }
        Err(Error::AlreadyUnlocked)
    }
}

impl<'a, 'b> Drop for Guard<'a, 'b> {
    fn drop(&mut self) {
        if !self.unlocked {
            let _ = self.lock.unlock();
        }
    }
}

pub type LockResult<'a, 'b> = Result<Guard<'a, 'b>, Error>;

impl<'a> GhettoLock<'a> {
    pub fn try_lock<'b>(&'b mut self) -> LockResult<'a, 'b> {
        for _ in 0..self.retries {
            let instant = Instant::now();
            match self.memcache.add(&self.name, &*self.owner, self.expiry) {
                Ok(()) => {
                    // if lock expired before the call could return, we don't have the lock, so
                    // retry.
                    if instant.elapsed().as_secs() >= u64::from(self.expiry) {
                        continue;
                    }
                    return Ok(Guard { unlocked: false, lock: self });
                }
                _ => {
                    // TODO: handle memcache errors
                    // TODO: maybe add sleep before retrying
                }
            }
        }
        Err(Error::RetriesExceeded)
    }

    fn unlock(&mut self) -> Result<bool, Error> {
        self.memcache.delete(&self.name).map_err(Into::into)
    }
}

pub enum Error {
    Memcache(MemcacheError),
    RetriesExceeded,
    AlreadyUnlocked
}

impl From<MemcacheError> for Error {
    fn from(error: MemcacheError) -> Self {
        Self::Memcache(error)
    }
}

pub struct LockOptions<'a, C> {
    name: Cow<'a, str>,
    connectable: C,
    expiry: Option<u32>,
    owner: Cow<'a, str>,
    read_timeout: Option<Duration>,
    write_timeout: Option<Duration>,
    retries: usize,
}

impl<'a> LockOptions<'a, &str> {
    pub fn new(name: Cow<'a, str>, owner: Cow<'a, str>) -> Self {
        Self {
            name,
            connectable: MEMCACHE_DEFAULT_URL,
            expiry: None,
            owner: owner,
            read_timeout: None,
            write_timeout: None,
            retries: 5,
        }
    }
}

impl<'a, C: Connectable> LockOptions<'a, C> {
    pub fn with_expiry(mut self, expiry: u32) -> Self {
        self.expiry = Some(expiry);
        self
    }

    pub fn with_connectable<K: Connectable>(self, connectable: K) -> LockOptions<'a, K> {
        LockOptions {
            connectable: connectable,
            name: self.name,
            expiry: self.expiry,
            owner: self.owner,
            read_timeout: self.read_timeout,
            write_timeout: self.write_timeout,
            retries: self.retries,
        }
    }

    pub fn with_read_timeout(mut self, timeout: Duration) -> Self {
        self.read_timeout = Some(timeout);
        self
    }

    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = Some(timeout);
        self
    }

    pub fn with_retries(mut self, retries: usize) -> Self {
        self.retries = retries;
        self
    }

    pub fn build(self) -> Result<GhettoLock<'a>, Error> {
        let mut memcache = Client::connect(self.connectable)?;
        memcache.set_read_timeout(self.read_timeout)?;
        memcache.set_write_timeout(self.write_timeout)?;
        Ok(GhettoLock {
            retries: self.retries,
            name: self.name,
            memcache,
            owner: self.owner,
            expiry: self.expiry.unwrap_or(10),
        })
    }
}
