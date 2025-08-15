extern crate proc_macro;

use std::thread::{sleep, spawn};
use std::time::{Duration, Instant};
use r#macro::limited;

#[test]
fn test_within() {
    test_100ms()
}

#[test]
#[should_panic]
fn test_too_long() {
    test_5s()
}

#[test]
#[should_panic]
fn test_within_but_error() {
    test_error_within()
}

#[test]
fn test_instant_panic() {
    let now = Instant::now();
    let handle = spawn(instant_panic);
    let res = handle.join();

    assert!(res.is_err());

    assert!(now.elapsed() < Duration::from_secs(19)) // just shorter than if the test would run for the full timed time
}

#[limited(s = 5)]
fn test_error_within() {
    sleep(Duration::from_millis(100));
    panic!("Lead to panic");
}

#[limited(s = 20)]
fn instant_panic() {
    panic!("instant panic!")
}

#[limited(ms=100)]
fn test_5s() {
    sleep(Duration::from_secs(5));
}

#[limited(s = 5)]
fn test_100ms() {
    sleep(Duration::from_millis(100));
}




