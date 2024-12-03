pub(crate) mod handle;

use crate::{Job, PanicHandler};
use atomic_enum::atomic_enum;
use crossbeam::deque::{Injector, Stealer, Worker as WorkerQueue};
use handle::WorkerHandle;
use std::{
    cell::RefCell,
    iter,
    panic::{self, AssertUnwindSafe},
    sync::{atomic::Ordering, Arc},
    thread::{self},
};

thread_local! {
    /// Local task queue for the worker
    static WORKER_QUEUE: RefCell<Option<(usize, WorkerQueue<Job>)>> = const { RefCell::new(None) };
}

#[atomic_enum]
pub(crate) enum WorkerState {
    Idle,
    Busy,
    Shutdown,
}

/// A worker executing tasks on a dedicated thread
pub(crate) struct Worker {
    idx: usize,
    queue: WorkerQueue<Job>,
    global: Arc<Injector<Job>>,
    panic_handler: Option<PanicHandler>,
    state: Arc<AtomicWorkerState>,
}

impl Worker {
    pub(crate) fn new(
        idx: usize,
        global: Arc<Injector<Job>>,
        panic_handler: Option<PanicHandler>,
    ) -> Self {
        Self {
            idx,
            queue: WorkerQueue::new_fifo(),
            global,
            panic_handler,
            state: Arc::new(AtomicWorkerState::new(WorkerState::Idle)),
        }
    }

    /// Start the worker
    pub(crate) fn run(self, stealers: Arc<[Stealer<Job>]>) -> WorkerHandle {
        let state = Arc::clone(&self.state);

        let thread_handle = thread::spawn(move || {
            self.worker_loop(&stealers);
        });

        WorkerHandle::new(thread_handle, state)
    }

    /// Add a task to the local queue
    pub(crate) fn spawn_job(job: Job) -> Result<usize, Job> {
        WORKER_QUEUE.with_borrow(|maybe_queue| match maybe_queue {
            Some((idx, queue)) => {
                queue.push(job);
                Ok(*idx)
            }
            None => Err(job),
        })
    }

    /// Worker component that allows others to steal tasks
    pub(crate) fn stealer(&self) -> Stealer<Job> {
        self.queue.stealer()
    }

    /// Worker execution loop
    fn worker_loop(self, stealers: &[Stealer<Job>]) {
        WORKER_QUEUE.with_borrow_mut(|maybe_queue| *maybe_queue = Some((self.idx, self.queue)));

        loop {
            if matches!(self.state.load(Ordering::Acquire), WorkerState::Shutdown) {
                break;
            };

            // Fetch a task to execute
            let Some(job) = find_job(&self.global, stealers, self.idx) else {
                // No tasks available, park until further instructions
                park_idle(&self.state);
                continue;
            };

            // Execute the task and handle potential panics
            if let Err(err) = panic::catch_unwind(AssertUnwindSafe(job)) {
                if let Some(ph) = &self.panic_handler {
                    ph(err)
                }
            };
        }
    }
}

/// Search for a task for the worker
fn find_job(global: &Injector<Job>, stealers: &[Stealer<Job>], idx: usize) -> Option<Job> {
    WORKER_QUEUE.with_borrow(|local| {
        local.as_ref().and_then(|(_, local)| {
            // Pop a task from the local queue, if not empty
            local.pop().or_else(|| {
                // Otherwise, we need to look for a task elsewhere
                iter::repeat_with(|| {
                    // Try stealing a batch of tasks from the global queue
                    global
                        .steal_batch_and_pop(local)
                        // Or try stealing a task from one of the other threads
                        .or_else(|| {
                            stealers
                                .iter()
                                .enumerate()
                                .filter_map(|(i, s)| if i == idx { Some(s) } else { None })
                                .map(|s| s.steal_batch_and_pop(local))
                                .collect()
                        })
                })
                // Loop while no task was stolen and any steal operation needs to be retried.
                .find(|s| !s.is_retry())
                // Extract the stolen task, if there is one.
                .and_then(|s| s.success())
            })
        })
    })
}

/// Park the worker thread until a signal is received
fn park_idle(state: &AtomicWorkerState) {
    state.store(WorkerState::Idle, Ordering::Release);

    loop {
        thread::park();

        let state = state.load(Ordering::Acquire);
        if matches!(state, WorkerState::Busy) || matches!(state, WorkerState::Shutdown) {
            break;
        };
    }
}
