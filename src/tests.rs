use crate::WorkStealThreadPool;
use std::sync::mpsc;

#[test]
fn test_thread_pool() {
    let thread_count = num_cpus::get();

    let tp = WorkStealThreadPool::builder()
        .max_threads(thread_count.try_into().unwrap())
        .build();

    let (tx, rx) = mpsc::channel();

    for _ in 0..thread_count {
        let tx = tx.clone();
        tp.spawn(move || {
            tx.send(1).unwrap();
        });
    }

    assert_eq!(rx.iter().take(thread_count).sum::<usize>(), thread_count);

    tp.join().unwrap();
}
