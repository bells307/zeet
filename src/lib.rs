mod builder;
mod inner;
mod worker;

pub use builder::WorkStealThreadPoolBuilder;

use inner::ThreadPoolInner;
use std::{any::Any, num::NonZeroUsize, sync::Arc, thread};

pub struct WorkStealThreadPool(ThreadPoolInner);

impl WorkStealThreadPool {
    pub fn builder() -> WorkStealThreadPoolBuilder {
        WorkStealThreadPoolBuilder::default()
    }

    /// Spawn new job for thread pool
    pub fn spawn(&self, job: impl FnOnce() + Send + 'static) {
        self.0.spawn(Box::new(job))
    }

    /// Wait for thread pool to complete all jobs
    pub fn join(self) -> thread::Result<()> {
        self.0.join()
    }

    pub(crate) fn new(max_threads: NonZeroUsize, panic_handler: Option<PanicHandler>) -> Self {
        Self(ThreadPoolInner::new(max_threads, panic_handler))
    }
}

/// Job for worker
pub type Job = Box<dyn FnOnce() + Send>;

type PanicHandler = Arc<dyn Fn(Box<dyn Any + Send>) + Send + Sync>;