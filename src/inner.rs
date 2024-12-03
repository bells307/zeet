use crate::{
    worker::{handle::WorkerHandle, Worker},
    Job, PanicHandler,
};
use crossbeam::deque::Injector;
use rand::seq::IteratorRandom;
use std::{
    num::NonZeroUsize,
    sync::Arc,
    thread::{self},
};

pub(crate) struct ThreadPoolInner {
    global: Arc<Injector<Job>>,
    workers: Vec<WorkerHandle>,
}

impl ThreadPoolInner {
    pub(crate) fn new(thread_count: NonZeroUsize, panic_handler: Option<PanicHandler>) -> Self {
        let thread_count = thread_count.get();

        let global = Arc::new(Injector::new());

        let mut workers = Vec::with_capacity(thread_count);
        let mut stealers = Vec::with_capacity(thread_count);

        for idx in 0..thread_count {
            let worker = Worker::new(idx, Arc::clone(&global), panic_handler.clone());
            let stealer = worker.stealer();
            workers.push(worker);
            stealers.push(stealer);
        }

        let stealers = stealers.into();

        let workers = workers
            .into_iter()
            .map(|w| w.run(Arc::clone(&stealers)))
            .collect();

        ThreadPoolInner { global, workers }
    }

    /// Wait for all threads to finish their tasks
    pub(crate) fn join(self) -> thread::Result<()> {
        for wh in self.workers {
            wh.join()?;
        }

        Ok(())
    }

    /// Add a job to the queue. If it fails to be added to a local thread queue,
    /// it will be placed in the global queue.
    pub(crate) fn spawn(&self, job: Job) {
        let idx = match Worker::spawn_job(job) {
            Ok(idx) => Some(idx),
            Err(job) => {
                self.global.push(job);
                None
            }
        };

        self.notify_one_waiter(idx);
    }

    /// Notify one worker about new tasks. If `idx` is `None`, any idle worker
    /// will be notified.
    fn notify_one_waiter(&self, idx: Option<usize>) {
        let iter = self.workers.iter().filter(|w| w.is_idle());

        let worker = match idx {
            Some(idx) => iter
                .enumerate()
                .find_map(|(i, w)| if i == idx { Some(w) } else { None }),
            None => iter.choose(&mut rand::thread_rng()),
        };

        if let Some(worker) = worker {
            worker.unpark_idle();
        }
    }
}
