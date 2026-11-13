//! # tokio-debouncer
//!
//! A lightweight, cancel-safe async debouncer for [Tokio](https://tokio.rs/) tasks.
//!
//! ## Overview
//!
//! This crate provides a simple, robust, and deterministic debouncer for batching signals or jobs in async workflows.
//! It is especially suited for job queues, event batching, and select-based async workers where you want to coalesce bursts of work and process them efficiently.
//!
//! - Supports both **leading** and **trailing** debounce modes.
//! - Designed for use with `tokio::select!` for robust, cancel-safe batching.
//! - Can be triggered from any task.
//! - Fully tested with simulated time.
//!
//! ## Example
//!
//! ```rust
//! use tokio_debouncer::{Debouncer, DebounceMode};
//! use tokio::time::Duration;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Create a debouncer with a 100ms cooldown in trailing mode
//!     let debouncer = Debouncer::new(Duration::from_millis(100), DebounceMode::Trailing);
//!     debouncer.trigger(); // Signal an event
//!     let _guard = debouncer.ready().await; // Wait until ready; debounce is finalized on drop
//!     // Do your work here
//! }
//! ```
//!
//! ## Select-based Job Queue Example
//!
//! ```rust
//! use tokio::{select, time::{sleep, Duration}};
//! use tokio_debouncer::{Debouncer, DebounceMode};
//!
//! #[tokio::main]
//! async fn main() {
//!     let debouncer = Debouncer::new(Duration::from_secs(1), DebounceMode::Trailing);
//!     let debouncer2 = debouncer.clone();
//!     tokio::spawn(async move {
//!         loop {
//!             debouncer2.trigger();
//!             sleep(Duration::from_millis(200)).await;
//!         }
//!     });
//!    let mut iterations = 10;
//!     loop {
//!          iterations -= 1;
//!          if iterations == 0 {
//!              break;
//!          }
//!        // Wait for the debouncer to be ready
//!         select! {
//!             _guard = debouncer.ready() => {
//!                 // Debounce is finalized automatically on drop
//!                 println!("Processing job batch!");
//!             }
//!             _ = sleep(Duration::from_millis(100)) => {
//!                 // Handle other events
//!             }
//!         }
//!     }
//! }
//! ```
//!
//! ## Best Practice
//!
//! The debounce state is now finalized automatically when the guard is dropped. You do not need to call any method to commit the debounce; simply let the guard go out of scope after acquiring it. This ensures robust, cancellation-safe batching, even if your task is cancelled or panics after acquiring the guard.
//!
//! If you need to do work after acquiring the guard, do it after awaiting `ready()` and let the guard drop naturally.
//! The guard holds the state lock until it is dropped. `trigger` does not wait on that lock; those triggers are applied when the guard drops. Do not call `is_triggered` from the same task while holding the guard.

use std::marker::PhantomData;
use std::sync::Arc;

use parking_lot::Mutex as BlockingMutex;
use tokio::sync::{Mutex, Notify, OwnedMutexGuard};
use tokio::time::{Duration, Instant};

/// The debounce mode: Leading or Trailing.
#[derive(Debug)]
pub enum DebounceMode {
    /// Fires immediately, then cools down.
    Leading,
    /// Fires after the last trigger and cooldown (default).
    Trailing,
}

/// Internal state for the debouncer.
struct DebouncerState {
    has_run: bool,
    last_run: Instant,
    triggered: bool,
}

/// Shared inner struct for Debouncer.
struct DebouncerInner {
    mode: DebounceMode,
    notifier: Notify,
    cooldown: Duration,
    state: Arc<Mutex<DebouncerState>>,
    /// Trigger instants recorded while `state` was already locked.
    /// A std mutex so [`DebouncerGuard`]'s synchronous `Drop` can drain it.
    pending: BlockingMutex<Vec<Instant>>,
}

impl DebouncerInner {
    /// Apply one trigger to `state`, as [`Debouncer::trigger`] would if it held the lock.
    /// Returns whether a waiter should be notified.
    fn apply_trigger(&self, state: &mut DebouncerState, at: Instant) -> bool {
        if matches!(self.mode, DebounceMode::Trailing) {
            state.last_run = at;
        }
        if state.triggered {
            false
        } else {
            state.triggered = true;
            true
        }
    }

    /// Replay triggers that could not lock `state`, in arrival order.
    fn apply_pending(&self, state: &mut DebouncerState) -> bool {
        let pending = std::mem::take(&mut *self.pending.lock());
        let mut notify = false;
        for at in pending {
            notify |= self.apply_trigger(state, at);
        }
        notify
    }

    fn enqueue_trigger(&self, at: Instant) {
        self.pending
            .lock()
            .push(at);
    }
}

/// Guard returned by [`Debouncer::ready`].
///
/// Holds the state lock acquired by `ready` and finalizes the debounce with that
/// lock when dropped. [`Debouncer::trigger`] does not wait on this lock; triggers
/// that arrive while it is held are applied here on drop. Do not call
/// [`Debouncer::is_triggered`] from the same task while this guard is held.
#[must_use = "if unused the debounce state will be finalized immediately"]
pub struct DebouncerGuard<'a> {
    state: OwnedMutexGuard<DebouncerState>,
    inner: Arc<DebouncerInner>,
    _not_static: PhantomData<&'a ()>,
}

impl DebouncerGuard<'_> {
    fn new(inner: Arc<DebouncerInner>, state: OwnedMutexGuard<DebouncerState>) -> Self {
        Self {
            inner,
            state,
            _not_static: PhantomData,
        }
    }

    /// Check if the debouncer is currently triggered (for diagnostics/testing).
    ///
    /// Waits if a [`DebouncerGuard`] currently holds the state lock.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        self.state.triggered
    }
}

impl Drop for DebouncerGuard<'_> {
    /// Finalizes the debounce state with the lock acquired by [`Debouncer::ready`].
    ///
    /// This ensures cancel-safety: if your task is cancelled or panics after acquiring the guard,
    /// the debounce state is still committed and the next batch can proceed.
    fn drop(&mut self) {
        // Replay the work of triggers that observed this lock as contended
        let notify_anyway = self.inner.apply_pending(&mut self.state);
        if self.state.triggered {
            self.state.has_run = true;
            self.state.triggered = false;
            self.state.last_run = Instant::now();
            self.inner.notifier.notify_one();
        } else if notify_anyway {
            self.inner.notifier.notify_one();
        }
    }
}

/// Debouncer struct for batching events or jobs.
/// Can be cloned and shared between tasks.
#[derive(Clone)]
pub struct Debouncer {
    inner: Arc<DebouncerInner>,
}

impl Debouncer {
    /// Create a new Debouncer with a cooldown time and mode (Leading or Trailing).
    /// Cooldown is the minimum time between triggers.
    #[must_use]
    pub fn new(cooldown: Duration, mode: DebounceMode) -> Self {
        let inner = Arc::new(DebouncerInner {
            notifier: Notify::new(),
            cooldown,
            state: Arc::new(Mutex::new(DebouncerState {
                has_run: !matches!(mode, DebounceMode::Leading),
                last_run: tokio::time::Instant::now(),
                triggered: false,
            })),
            pending: BlockingMutex::new(Vec::new()),
            mode,
        });
        Self { inner }
    }

    /// Check if the debouncer is currently triggered (for diagnostics/testing).
    ///
    /// Waits if a [`DebouncerGuard`] currently holds the state lock.
    #[must_use]
    pub async fn is_triggered(&self) -> bool {
        let mut state = self.inner.state.lock().await;
        let notify = self.inner.apply_pending(&mut state);
        let triggered = state.triggered;
        drop(state);
        if notify {
            self.inner.notifier.notify_one();
        }
        triggered
    }

    /// Trigger the debouncer. Can be called from any task.
    /// Notifies the worker if not already pending.
    ///
    /// Does not wait if a [`DebouncerGuard`] currently holds the state lock.
    /// That trigger is stored and applied when the guard (or the next state-lock
    /// holder) runs, as if this call had taken the lock itself.
    pub fn trigger(&self) {
        let at = Instant::now();
        let notify = if let Ok(mut guard) = self.inner.state.try_lock() {
            self.inner.apply_pending(&mut guard) |
            self.inner.apply_trigger(&mut guard, at)
        } else {
            self.inner.enqueue_trigger(at);
            false
        };
        if notify {
            self.inner.notifier.notify_one();
        }
    }

    /// Lock state and either keep that lock or report how long to wait.
    ///
    /// `Err(None)` means wait for a trigger. `Err(Some(instant))` means sleep
    /// until the cooldown elapses. The lock is dropped before either wait.
    async fn try_lock_ready(
        inner: &Arc<DebouncerInner>,
    ) -> Result<OwnedMutexGuard<DebouncerState>, Option<Instant>> {
        let mut locked = Arc::clone(&inner.state).lock_owned().await;
        if inner.apply_pending(&mut locked) {
            inner.notifier.notify_one();
        }

        if !locked.triggered {
            return Err(None);
        }

        let now = Instant::now();
        let next_allowed = locked.last_run + inner.cooldown;
        let next_allowed = Some(next_allowed).filter(|&next_allowed| now < next_allowed);
        let wait_until = match inner.mode {
            DebounceMode::Leading => next_allowed.filter(|_| locked.has_run),
            DebounceMode::Trailing => next_allowed,
        };

        match wait_until {
            Some(instant) => Err(Some(instant)),
            None => Ok(locked),
        }
    }

    /// Wait until the debouncer is ready to run.
    ///
    /// Returns a guard that keeps the state lock acquired here and finalizes the
    /// debounce with that lock when dropped.
    ///
    /// # Cancel Safety
    /// This method is cancel-safe. Debounce flags are not changed until the returned
    /// guard is dropped, so cancelling before then — including inside `select!` —
    /// does not mutate them. The state lock is a [`tokio::sync::Mutex`]: dropping
    /// this future drops any guard it holds and releases the lock. The lock is still
    /// dropped before the wait awaits so a cancelled `ready` does not pin the mutex
    /// for the whole cooldown.
    pub async fn ready<'a>(&self) -> DebouncerGuard<'a> {
        // Flags stay unchanged until `DebouncerGuard` drops. The state lock is
        // released before either wait so cancellation does not hold it meanwhile.
        loop {
            match Self::try_lock_ready(&self.inner).await {
                Ok(locked) => break DebouncerGuard::new(self.inner.clone(), locked),
                Err(Some(wait)) => tokio::time::sleep_until(wait).await,
                Err(None) => self.inner.notifier.notified().await,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>(_: &T) {}

    #[test]
    fn ready_future_is_send() {
        let debouncer = Debouncer::new(Duration::from_millis(1), DebounceMode::Trailing);
        let fut = debouncer.ready();
        assert_send(&fut);
    }

    #[tokio::test(start_paused = true)]
    async fn guard_holds_trigger_until_drop() {
        let debouncer = Debouncer::new(Duration::from_secs(10), DebounceMode::Leading);
        debouncer.trigger();
        let guard = debouncer.ready().await;
        assert!(guard.state.triggered);
        drop(guard);
        assert!(!debouncer.is_triggered().await);
    }
}
