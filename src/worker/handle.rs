use super::AtomicWorkerState;
use crate::worker::WorkerState;
use std::{
    sync::{atomic::Ordering, Arc},
    thread::{self, JoinHandle},
};

/// Handle for managing a worker
pub(crate) struct WorkerHandle {
    thread_handle: JoinHandle<()>,
    state: Arc<AtomicWorkerState>,
}

impl WorkerHandle {
    pub(crate) fn new(thread_handle: JoinHandle<()>, state: Arc<AtomicWorkerState>) -> Self {
        Self {
            thread_handle,
            state,
        }
    }

    /// Check if the worker is idle
    pub(crate) fn is_idle(&self) -> bool {
        matches!(self.state.load(Ordering::Acquire), WorkerState::Idle)
    }

    /// Unpark the worker from idle state
    pub(crate) fn unpark_idle(&self) {
        if self
            .state
            .compare_exchange(
                WorkerState::Idle,
                WorkerState::Busy,
                Ordering::Release,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.thread_handle.thread().unpark();
        };
    }

    /// Wait for the worker to shut down gracefully
    pub(crate) fn join(self) -> thread::Result<()> {
        self.state.store(WorkerState::Shutdown, Ordering::Release);
        self.thread_handle.thread().unpark();
        self.thread_handle.join()
    }
}
