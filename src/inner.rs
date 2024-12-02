use crate::{
    worker::{Worker, WorkerHandle},
    Job, PanicHandler,
};
use crossbeam::deque::Injector;
use rand::seq::IteratorRandom;
use std::{
    num::NonZeroUsize,
    sync::{atomic::Ordering, Arc},
    thread::{self},
};

pub(crate) struct ThreadPoolInner {
    injector: Arc<Injector<Job>>,
    workers: Vec<WorkerHandle>,
}

impl ThreadPoolInner {
    pub(crate) fn new(thread_count: NonZeroUsize, panic_handler: Option<PanicHandler>) -> Self {
        let thread_count = thread_count.get();

        let injector = Arc::new(Injector::new());

        let mut workers = Vec::with_capacity(thread_count);
        let mut stealers = Vec::with_capacity(thread_count);

        for idx in 0..thread_count {
            let worker = Worker::new(idx, Arc::clone(&injector), panic_handler.clone());
            let stealer = worker.stealer();
            workers.push(worker);
            stealers.push(stealer);
        }

        let stealers = stealers.into();

        let workers = workers
            .into_iter()
            .map(|w| w.run(Arc::clone(&stealers)))
            .collect();

        ThreadPoolInner { injector, workers }
    }

    /// Ожидание завершения работы всех потоков
    pub(crate) fn join(self) -> thread::Result<()> {
        for wh in self.workers {
            wh.thread_handle.join()?;
        }

        Ok(())
    }

    /// Постановка задачи в очередь. Если не удалось поставить задачу в очередь локального потока,
    /// то она будет установлена в общую очередь
    pub(crate) fn spawn(&self, job: Job) {
        let idx = match Worker::spawn_job(job) {
            Ok(idx) => Some(idx),
            Err(job) => {
                self.injector.push(job);
                None
            }
        };

        self.notify_one_waiter(idx);
    }

    /// Оповестить одного воркера о появлении новых задач. Если в `idx`, передано `None`, то будет
    /// оповещен любой воркер в спящем состоянии `idle`.
    fn notify_one_waiter(&self, idx: Option<usize>) {
        let iter = self
            .workers
            .iter()
            .filter(|w| w.idle.load(Ordering::Acquire));

        let worker = match idx {
            Some(idx) => iter
                .enumerate()
                .find_map(|(i, w)| if i == idx { Some(w) } else { None }),
            None => iter.choose(&mut rand::thread_rng()),
        };

        if let Some(worker) = worker {
            if worker
                .idle
                .compare_exchange(true, false, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                worker.thread_handle.thread().unpark();
            };
        }
    }
}
