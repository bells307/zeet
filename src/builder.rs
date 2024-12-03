use crate::{PanicHandler, WorkStealThreadPool};
use std::{any::Any, num::NonZeroUsize, sync::Arc};

#[derive(Default)]
pub struct WorkStealThreadPoolBuilder {
    max_threads: Option<NonZeroUsize>,
    panic_handler: Option<PanicHandler>,
}

impl WorkStealThreadPoolBuilder {
    /// Maximum number of operating system threads
    pub fn max_threads(mut self, val: NonZeroUsize) -> Self {
        self.max_threads = Some(val);
        self
    }

    /// Panic handler
    pub fn panic_handler(
        mut self,
        f: impl Fn(Box<dyn Any + Send>) + Send + Sync + 'static,
    ) -> Self {
        self.panic_handler = Some(Arc::new(f));
        self
    }

    pub fn build(self) -> WorkStealThreadPool {
        let max_threads = self.max_threads.unwrap_or_else(default_thread_count);
        WorkStealThreadPool::new(max_threads, self.panic_handler)
    }
}

fn default_thread_count() -> NonZeroUsize {
    num_cpus::get().try_into().expect("can't define num cpus")
}
