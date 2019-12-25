use std::{borrow::Cow, thread::sleep, time::Duration};

use ghetto_lock::{LockError, LockOptions};
use memcache::Client;

fn setup() {
    let mut client = Client::connect("memcache://localhost:11211").expect("error creating client");
    client.flush().expect("flush failed");
}

#[test]
fn test_lock_release() {
    setup();
    let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner-1"))
        .with_expiry(1)
        .build()
        .expect("failed to build client");
    let guard = lock.try_acquire().expect("failed to acquire lock");
    let result = guard.try_release();
    assert!(result.is_ok(), format!("{:?}", result.err()));
    assert_eq!(result.unwrap(), ());
}

#[test]
fn test_long_running_job() {
    setup();
    let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner-1"))
        .with_expiry(1)
        .build()
        .expect("failed to build client");
    let guard = lock.try_acquire().expect("failed to acquire lock");
    sleep(Duration::new(2, 0));
    assert_eq!(guard.try_release(), Err(LockError::AlreadyReleased));
}

#[test]
fn test_dropping_guard_releases_lock() {
    setup();
    let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner-1"))
        .with_expiry(1)
        .build()
        .expect("failed to build client");
    {
        let _guard = lock.try_acquire().expect("failed to acquire lock");
    }

    let guard = lock.try_acquire();
    assert!(guard.is_ok(), format!("{:?}", guard.err()));
}

#[test]
fn test_lock_fails_when_expiry_is_in_the_past() {
    setup();
    let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner"))
        .with_expiry(1576412321) // set expiry in unix time
        .build()
        .expect("failed to build client");
    assert_eq!(Err(LockError::TimedOut), lock.try_acquire());
}

#[test]
fn test_rust_memcache_bug() {
    setup();
    let mut lock = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner"))
        .with_expiry(1000)
        .build()
        .expect("failed to build client");
    let mut lock2 = LockOptions::new(Cow::Borrowed("db-lock"), Cow::Borrowed("owner-2"))
        .with_expiry(1000)
        .build()
        .expect("failed to build client");
    let _guard = lock.try_acquire().expect("can't acquire lock: owner");
    {
        let result = lock2.try_acquire();
        assert_eq!(Err(LockError::FailedToAcquire), result);
    }
    let result = lock2.try_acquire();
    assert_eq!(Err(LockError::FailedToAcquire), result);
}
