//! ID pre-allocation logic tests
//!
//! Tests verify ID allocation correctness:
//! - Sequential allocation
//! - Batch range allocation
//! - Concurrent allocation without collisions

use std::collections::HashSet;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::{
    BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig, RaftLog,
};

fn setup_memory() -> Arc<BufferedRaftLog<MockTypeConfig>> {
    let storage = Arc::new(MockStorageEngine::new());
    let (raft_log, _receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );

    Arc::new(raft_log)
}

fn is_empty_range(range: &RangeInclusive<u64>) -> bool {
    range.start() == &u64::MAX && range.end() == &u64::MAX
}

#[tokio::test]
async fn test_pre_allocate_next_index_sequential() {
    let raft_log = setup_memory();

    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 1);
    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 2);
    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 3);

    assert_eq!(raft_log.next_id().load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn test_pre_allocate_id_range_exact_batch() {
    let raft_log = setup_memory();

    let range = raft_log.pre_allocate_id_range(100);
    assert_eq!(*range.start(), 1);
    assert_eq!(*range.end(), 100);

    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 101);
}

#[tokio::test]
async fn test_pre_allocate_id_range_zero_count() {
    let raft_log = setup_memory();
    let initial_id = raft_log.next_id().load(Ordering::SeqCst);

    let range = raft_log.pre_allocate_id_range(0);
    assert!(is_empty_range(&range));
    assert_eq!(raft_log.next_id().load(Ordering::SeqCst), initial_id);
}

#[tokio::test]
async fn test_pre_allocate_id_range_after_zero() {
    let raft_log = setup_memory();

    let empty_range = raft_log.pre_allocate_id_range(0);
    assert!(is_empty_range(&empty_range));

    let range = raft_log.pre_allocate_id_range(5);
    assert_eq!(*range.start(), 1);
    assert_eq!(*range.end(), 5);
}

#[tokio::test]
async fn test_pre_allocate_id_range_concurrent() {
    let raft_log = setup_memory();
    let raft_log_clone = Arc::clone(&raft_log);

    let handle = std::thread::spawn(move || raft_log_clone.pre_allocate_id_range(50));

    let main_range = raft_log.pre_allocate_id_range(30);
    let thread_range = handle.join().unwrap();

    let main_count = main_range.end() - main_range.start() + 1;
    let thread_count = thread_range.end() - thread_range.start() + 1;
    assert_eq!(main_count, 30);
    assert_eq!(thread_count, 50);

    let points = [
        *main_range.start(),
        *main_range.end(),
        *thread_range.start(),
        *thread_range.end(),
    ];

    let min_id = *points.iter().min().unwrap();
    let max_id = *points.iter().max().unwrap();

    assert_eq!(main_count + thread_count, max_id - min_id + 1);
    assert_eq!(raft_log.next_id().load(Ordering::SeqCst), max_id + 1);
}

#[derive(Debug)]
enum AllocationResult {
    Single(u64),
    Range(RangeInclusive<u64>),
}

#[tokio::test]
async fn test_concurrent_mixed_allocations() {
    let raft_log = setup_memory();
    let mut handles = vec![];

    for _ in 0..10 {
        let log_clone = Arc::clone(&raft_log);
        handles.push(std::thread::spawn(move || {
            let id = log_clone.pre_allocate_raft_logs_next_index();
            AllocationResult::Single(id)
        }));
    }

    for _ in 0..5 {
        let log_clone = Arc::clone(&raft_log);
        handles.push(std::thread::spawn(move || {
            let range = log_clone.pre_allocate_id_range(10);
            AllocationResult::Range(range)
        }));
    }

    let mut results = vec![];
    for handle in handles {
        results.push(handle.join().unwrap());
    }

    let mut all_ids = HashSet::new();
    for result in &results {
        match result {
            AllocationResult::Single(id) => {
                assert!(!all_ids.contains(id), "Duplicate ID: {id}");
                all_ids.insert(*id);
            }
            AllocationResult::Range(range) => {
                if !range.is_empty() {
                    for id in *range.start()..=*range.end() {
                        assert!(!all_ids.contains(&id), "Duplicate ID: {id}");
                        all_ids.insert(id);
                    }
                }
            }
        }
    }

    let min_id = *all_ids.iter().min().unwrap_or(&0);
    let max_id = *all_ids.iter().max().unwrap_or(&0);

    if !all_ids.is_empty() {
        assert_eq!(
            all_ids.len() as u64,
            max_id - min_id + 1,
            "IDs not contiguous"
        );
        assert_eq!(raft_log.next_id().load(Ordering::SeqCst), max_id + 1);
    }
}

#[tokio::test]
async fn test_pre_allocate_id_range_multiple_batches() {
    let raft_log = setup_memory();

    let range1 = raft_log.pre_allocate_id_range(150);
    let range2 = raft_log.pre_allocate_id_range(50);
    let empty_range = raft_log.pre_allocate_id_range(0);
    let range3 = raft_log.pre_allocate_id_range(100);

    assert_eq!(*range1.start(), 1);
    assert_eq!(*range1.end(), 150);

    assert_eq!(*range2.start(), 151);
    assert_eq!(*range2.end(), 200);

    assert!(is_empty_range(&empty_range));

    assert_eq!(*range3.start(), 201);
    assert_eq!(*range3.end(), 300);
}

#[tokio::test]
async fn test_mixed_allocation_strategies() {
    let raft_log = setup_memory();

    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 1);

    let empty = raft_log.pre_allocate_id_range(0);
    assert!(is_empty_range(&empty));

    let range = raft_log.pre_allocate_id_range(5);
    assert_eq!(*range.start(), 2);
    assert_eq!(*range.end(), 6);

    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 7);
    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 8);

    let range = raft_log.pre_allocate_id_range(250);
    assert_eq!(*range.start(), 9);
    assert_eq!(*range.end(), 258);

    assert_eq!(raft_log.pre_allocate_raft_logs_next_index(), 259);
}

#[tokio::test]
async fn test_edge_cases() {
    let raft_log = setup_memory();

    // Test: Single ID allocation
    let single = raft_log.pre_allocate_id_range(1);
    assert_eq!(*single.start(), 1);
    assert_eq!(*single.end(), 1);

    // Test: Allocation near u64::MAX boundary (successful)
    raft_log.next_id().store(u64::MAX - 5, Ordering::SeqCst);
    let range = raft_log.pre_allocate_id_range(5);
    assert_eq!(*range.start(), u64::MAX - 5);
    assert_eq!(*range.end(), u64::MAX - 1);
}

/// Test ID allocation overflow protection
///
/// # Scenario
/// - Set next_id to u64::MAX - 5
/// - Attempt to allocate 10 IDs (would overflow)
/// - Expected: Panics with "ID overflow" message
#[tokio::test]
#[should_panic(expected = "ID overflow")]
async fn test_id_allocation_overflow_panics() {
    let raft_log = setup_memory();

    // Arrange: Set next_id close to max value
    raft_log.next_id().store(u64::MAX - 5, Ordering::SeqCst);

    // Act: Try to allocate more IDs than available (10 > 5 remaining)
    // Expected: Panics due to overflow protection
    raft_log.pre_allocate_id_range(10);
}
