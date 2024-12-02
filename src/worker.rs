use crate::{Job, PanicHandler};
use crossbeam::deque::{Injector, Steal, Stealer, Worker as WorkerQueue};
use rand::Rng;
use std::{
    cell::RefCell,
    panic::{self, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

thread_local! {
    /// Локальная очередь задач воркера
    static WORKER_QUEUE: RefCell<Option<(usize, WorkerQueue<Job>)>> = const { RefCell::new(None) };
}

/// Воркер, исполняющий задачи на выделенном потоке
pub(crate) struct Worker {
    idx: usize,
    queue: WorkerQueue<Job>,
    injector: Arc<Injector<Job>>,
    panic_handler: Option<PanicHandler>,
    idle: Arc<AtomicBool>,
}

impl Worker {
    pub(crate) fn new(
        idx: usize,
        injector: Arc<Injector<Job>>,
        panic_handler: Option<PanicHandler>,
    ) -> Self {
        Self {
            idx,
            queue: WorkerQueue::new_fifo(),
            injector,
            panic_handler,
            idle: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Запустить воркера
    pub(crate) fn run(self, stealers: Arc<[Stealer<Job>]>) -> WorkerHandle {
        let idle = Arc::clone(&self.idle);

        let thread_handle = thread::spawn(move || {
            self.worker_loop(&stealers);
        });

        WorkerHandle::new(thread_handle, idle)
    }

    /// Запуск задачи на локальной очереди
    pub(crate) fn spawn_job(job: Job) -> Result<usize, Job> {
        WORKER_QUEUE.with_borrow(|maybe_queue| match maybe_queue {
            Some((idx, queue)) => {
                queue.push(job);
                Ok(*idx)
            }
            None => Err(job),
        })
    }

    /// Часть воркера, позволяющая другим воркерам "украсть" задачи
    pub(crate) fn stealer(&self) -> Stealer<Job> {
        self.queue.stealer()
    }

    /// Цикл работы воркера
    fn worker_loop(self, stealers: &[Stealer<Job>]) {
        WORKER_QUEUE.with_borrow_mut(|maybe_queue| *maybe_queue = Some((self.idx, self.queue)));

        loop {
            // Получаем задачу на выполнение
            let Some(job) = WORKER_QUEUE
                .with_borrow(|maybe_queue| {
                    // Сначала пытаемся взять задачу из своей очереди
                    maybe_queue.as_ref().and_then(|(_, queue)| queue.pop())
                })
                // Если ничего не нашлось, то пробуем взять задачу у другого воркера
                .or_else(|| steal_from_worker(stealers, self.idx))
                // Если и там не удалось ничего найти, пробуем взять задачу из глобальной
                // очереди
                .or_else(|| steal_from_injector(&self.injector))
            else {
                // Задач на выполнение нет, засыпаем до дальнейших указаний
                park(&self.idle);
                continue;
            };

            // Запускаем задачу на выполнение и отлавливаем возможную панику
            if let Err(err) = panic::catch_unwind(AssertUnwindSafe(job)) {
                if let Some(ph) = &self.panic_handler {
                    ph(err)
                }
            };
        }
    }
}

/// Ручка для управления воркером
pub(crate) struct WorkerHandle {
    thread_handle: JoinHandle<()>,
    idle: Arc<AtomicBool>,
}

impl WorkerHandle {
    pub(crate) fn new(thread_handle: JoinHandle<()>, idle: Arc<AtomicBool>) -> Self {
        Self {
            thread_handle,
            idle,
        }
    }

    /// Флаг, находится ли воркер в ожидании
    pub(crate) fn is_idle(&self) -> bool {
        self.idle.load(Ordering::Acquire)
    }

    /// Снять воркера с режима ожидания
    pub(crate) fn unpark(&self) {
        if self
            .idle
            .compare_exchange(true, false, Ordering::Release, Ordering::Relaxed)
            .is_ok()
        {
            self.thread_handle.thread().unpark();
        };
    }

    /// Дождаться корректного завершения работы воркера
    pub(crate) fn join(self) -> thread::Result<()> {
        self.thread_handle.join()
    }
}

/// Украсть задачу у одного из соседних воркеров
fn steal_from_worker(stealers: &[Stealer<Job>], idx: usize) -> Option<Job> {
    let mut rng = rand::thread_rng();
    let worker_count = stealers.len();

    loop {
        let mut retry = false;
        let start = rng.gen_range(0..worker_count);

        let job = (start..worker_count)
            .chain(0..start)
            .filter(move |&i| i != idx)
            .find_map(|victim_index| match stealers[victim_index].steal() {
                Steal::Success(job) => Some(job),
                Steal::Empty => None,
                Steal::Retry => {
                    retry = true;
                    None
                }
            });

        if job.is_some() || !retry {
            return job;
        }
    }
}

/// Украсть задачу из глобальной очереди
fn steal_from_injector(injector: &Injector<Job>) -> Option<Job> {
    loop {
        match injector.steal() {
            Steal::Success(job) => break Some(job),
            Steal::Empty => break None,
            Steal::Retry => {}
        }
    }
}

/// Запарковать поток воркера, пока не поступит сигнал
fn park(idle: &AtomicBool) {
    idle.store(true, Ordering::Release);

    loop {
        thread::park();

        if !idle.load(Ordering::Acquire) {
            break;
        }
    }
}
