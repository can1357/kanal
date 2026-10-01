use super::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

/// Counts its drops. `N` pads it past pointer size, so `N == 0` takes the
/// inline `KanalPtr` path and `N > 0` the by-address path.
struct Counted<'a, const N: usize>(&'a AtomicUsize, [usize; N]);

impl<const N: usize> Drop for Counted<'_, N> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Spins until a sender is parked in the wait list, so the send under test
/// takes the blocking path instead of handing off to a waiting receiver.
fn wait_for_sender<T>(receiver: &Receiver<T>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while acquire_internal(&receiver.internal).wait_list.is_empty() {
        assert!(Instant::now() < deadline, "sender never queued");
        thread::yield_now();
    }
}

fn blocked_send_transfers_ownership_once<const N: usize>() {
    let drops = AtomicUsize::new(0);
    let (sender, receiver) = bounded(0);
    thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let mut payload = Some(Counted(&drops, [0; N]));
            sender
                .send_option_timeout(&mut payload, Duration::from_secs(10))
                .unwrap();
            assert!(payload.is_none());
        });
        wait_for_sender(&receiver);
        let payload = receiver.recv().unwrap();
        worker.join().unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0, "sender dropped sent data");
        drop(payload);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

fn failed_send_returns_ownership<const N: usize>() {
    let drops = AtomicUsize::new(0);
    let (sender, receiver) = bounded(0);

    let mut payload = Some(Counted(&drops, [0; N]));
    assert_eq!(
        sender.send_option_timeout(&mut payload, Duration::ZERO),
        Err(SendErrorTimeout::Timeout)
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(payload.expect("timed out send keeps its payload"));
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let mut payload = Some(Counted(&drops, [0; N]));
            assert_eq!(
                sender.send_option_timeout(&mut payload, Duration::from_secs(10)),
                Err(SendErrorTimeout::Closed)
            );
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            drop(payload.expect("closed send keeps its payload"));
        });
        wait_for_sender(&receiver);
        drop(receiver);
        worker.join().unwrap();
    });
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn send_option_timeout_blocked_transfer_small() {
    blocked_send_transfers_ownership_once::<0>();
}

#[test]
fn send_option_timeout_blocked_transfer_large() {
    blocked_send_transfers_ownership_once::<4>();
}

#[test]
fn send_option_timeout_failure_small() {
    failed_send_returns_ownership::<0>();
}

#[test]
fn send_option_timeout_failure_large() {
    failed_send_returns_ownership::<4>();
}
