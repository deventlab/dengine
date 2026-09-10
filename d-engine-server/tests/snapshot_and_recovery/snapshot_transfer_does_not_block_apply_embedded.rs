//! Snapshot transfer does not block ApplyEntries (Embedded mode)
//!
//! Guards the architecture invariant that a large, slow InstallSnapshot push to a
//! lagging Learner never stalls the Leader's own commit/apply path. The slow work
//! (streaming chunks + decompression) lives in the role layer and the final install
//! lives in the Worker, so healthy nodes keep applying new writes while one peer is
//! being snapshotted.

#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;

use d_engine_core::capture_logs_globally_filtered;
use d_engine_core::logs_contain_globally_since;
use d_engine_server::DefaultEmbeddedEngine;
use d_engine_server::RocksDBStateMachine;
use d_engine_server::RocksDBUnifiedEngine;
use d_engine_server::StateMachine;
use d_engine_server::install_snapshot_transfer_gate;
use serial_test::serial;
use tracing::info;

use crate::common::get_available_ports;

/// Deterministic, incompressible payload so the snapshot does not shrink under gzip —
/// keeps the transferred snapshot representative of a real, non-trivial payload.
fn deterministic_value(
    seed: u64,
    len: usize,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut s = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for _ in 0..len {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        out.push((s & 0xFF) as u8);
    }
    out
}

/// Test: a snapshot push to a lagging Learner does not block Leader apply.
///
/// ## Why this is meaningful
///
/// A Learner that joins after the log has been purged must receive a full snapshot.
/// While that transfer is in flight, the Leader writes new entries and must apply them
/// without waiting for the Learner — otherwise every snapshot transfer would freeze the
/// cluster's write path for its full duration.
///
/// ## How "in flight" is observed
///
/// The test does not rely on bandwidth throttling or wall-clock timing to catch the
/// transfer mid-flight — both are racy (transfer speed depends on machine load and isn't
/// reliably controllable). Instead, a test-only [`install_snapshot_transfer_gate`] freezes
/// the real push transfer right after it finishes setup and right before it sends its
/// first chunk. This gives a deterministic "started but not completed" window instead of
/// a timing guess.
///
/// ## An assumption this test relies on
///
/// The leader's per-tick replication logic (deciding what to send each peer, and
/// updating `PeerReplicationState`/`next_index`) runs to completion synchronously,
/// one tick at a time, with no overlapping concurrent execution. That is what makes
/// "a peer is in Snapshot state" a reliable, race-free signal that no Append task
/// will be generated for it — see the peer-replication-state unit tests colocated
/// with `LeaderState` for the scenarios that assumption protects. If the leader's
/// main loop is ever changed to process replication concurrently across ticks, this
/// assumption — and the correctness argument behind it — needs to be re-examined.
///
/// ## Flow
///
/// 1. Start a 3-node cluster; write enough large entries to trigger snapshot + purge.
/// 2. Record the Leader's applied index.
/// 3. Register a transfer gate for the Learner's node id, then start the Learner (behind
///    the purge boundary → needs a snapshot).
/// 4. Wait for the gate to report the transfer has started (deterministic handshake).
/// 5. Write `CONCURRENT_ENTRIES` small entries to the Leader.
/// 6. Assert the Leader's applied index advances past the concurrent writes **while the
///    transfer gate confirms the snapshot transfer has still not completed**.
/// 7. Release the gate, assert the Learner eventually catches up, then stop all engines.
#[tokio::test]
#[serial]
async fn test_snapshot_transfer_does_not_block_apply() -> Result<(), Box<dyn std::error::Error>> {
    // PR #442 review: a snapshot *file* existing on disk doesn't prove the Leader has
    // purged the log entries it covers — snapshot generation and log purge are separate
    // steps (see `schedule_and_execute_purge`). If purge lags behind the file appearing,
    // node 4 could still receive normal AppendEntries instead of a snapshot, silently
    // testing nothing about the InstallSnapshot path this test exists to guard. Capturing
    // the leader's own purge log lets the test wait for the real boundary instead of
    // guessing from file existence.
    let logs = capture_logs_globally_filtered(
        "info,d_engine_core=debug,d_engine_core::state_machine_handler=trace,d_engine_server=debug,h2=off,tonic=warn,hyper=warn",
    );

    // Recorded before any baseline writes start, not after. Snapshot capture + purge for
    // entry 64 runs as a background task, fully concurrent with the remaining baseline
    // writes (65..80) — it does not wait for the write loop below to finish. Verified by
    // log inspection: `purge_upto_index=` was observed appearing while the write loop was
    // still around entry ~74, well before `commit_index` reached 80. Placing `since` after
    // the write loop (as before) raced against that background completion: if purge won
    // the race, its log line landed before `since` and the poll below could never see it,
    // failing the test even though purge genuinely happened. Placing `since` here, before
    // any write can possibly trigger a purge, removes the race regardless of which side wins.
    let since = logs.lock().unwrap().len();

    const SNAPSHOT_THRESHOLD: u64 = 64;
    const RETAINED_LOGS: u64 = 8;
    const BASELINE_ENTRIES: u64 = 80;
    const VALUE_SIZE: usize = 16 * 1024;
    const CONCURRENT_ENTRIES: u64 = 20;
    const CHUNK_SIZE: usize = 64 * 1024;

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("db");

    let mut port_guard = get_available_ports(4).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let mut engines = Vec::new();
    let mut voter_sms = Vec::new();

    for node_id in 1u64..=3 {
        let config = format!(
            r#"
[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 1, status = 3 }}
]

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
chunk_size = {CHUNK_SIZE}
push_queue_size = 1
"#,
            ports[node_id as usize - 1],
            ports[0],
            ports[1],
            ports[2],
        );

        let config_path = temp_dir.path().join(format!("node{node_id}.toml"));
        tokio::fs::write(&config_path, &config).await?;

        let db_path = data_dir.join(format!("node{node_id}/db"));
        tokio::fs::create_dir_all(&db_path).await?;

        let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
        let sm = Arc::new(sm);
        voter_sms.push(Arc::<RocksDBStateMachine>::clone(&sm));

        let engine = DefaultEmbeddedEngine::start_custom(
            &db_path,
            Arc::new(storage),
            sm,
            Some(config_path.to_str().unwrap()),
        )
        .await?;
        engines.push(engine);
    }

    let leader_info = engines[0].wait_ready(Duration::from_secs(15)).await?;
    let leader_idx = engines
        .iter()
        .position(|e| e.node_id() == leader_info.leader_id)
        .expect("leader must be one of the 3 engines");
    let leader_client = engines[leader_idx].client().clone();
    let leader_sm = &voter_sms[leader_idx];

    info!("Writing {BASELINE_ENTRIES} large entries to trigger snapshot + purge");
    for i in 0..BASELINE_ENTRIES {
        leader_client
            .put(
                format!("key_{i}").into_bytes(),
                deterministic_value(i, VALUE_SIZE),
            )
            .await?;
    }

    // This used to gate `since` on a `wait_for_snapshot` directory scan for the leader's
    // `.gz` file first, on the theory that "file exists" proves the snapshot is built.
    // It doesn't: `compress_directory` (default_state_machine_handler.rs) calls
    // `File::create` on the *final* snapshot path before writing a single tar/gzip byte
    // into it, so the scan can observe an empty, still-being-written file milliseconds
    // into the build. That gate returned almost immediately in practice, contributing
    // none of its 15s budget to the real wait — it just added false confidence while the
    // actual bottleneck (the fixed window below) stayed exactly as tight. That's what
    // made this test flake under CI load: RocksDB checkpoint export + tar/gzip
    // compression + metadata persist is genuinely sequential disk+CPU work that slows
    // down under contention.

    // The retained-log purge boundary — the actual signal this test needs, not just "a
    // snapshot file exists" (see comment above for why those differ). Emitted by
    // `schedule_and_execute_purge` as `info!("purge_upto_index={purge_upto_index}")`
    // whenever ANY node completes a local snapshot capture, unconditionally — including
    // the `purge_upto_index=0` case where nothing is actually purged (retained_log_entries
    // >= last_included.index). A bare substring match on "purge_upto_index=" would wrongly
    // accept that no-op case. It can't happen here, though: with
    // SNAPSHOT_THRESHOLD=64 > RETAINED_LOGS=8, the earliest possible snapshot on this
    // cluster already has last_included.index >= 64, so purge_upto_index is always > 0
    // by the time this log line can appear at all.
    // 60 x 500ms = 30s, not 15s: this test lives in the `multi-node-cluster-local`
    // nextest group (throttled but not serialized, see .config/nextest.toml), and the
    // log line polled below shares a process-global Mutex<Vec<String>> with every other
    // concurrently-running test in this binary (see log_capture.rs). Under full-suite
    // load, RocksDB checkpoint export + tar/gzip (genuinely sequential CPU+disk work,
    // see comment above) plus that shared-mutex contention can push real completion past
    // 15s even though nothing is actually wrong — same root cause already documented in
    // stress_test.rs's 30s bound.
    let mut purged = false;
    for _ in 0..60 {
        if logs_contain_globally_since(&logs, since, "purge_upto_index=") {
            purged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !purged {
        eprintln!("=== DEBUG: captured logs since baseline writes ===");
        for line in logs.lock().unwrap()[since..].iter() {
            eprintln!("{line}");
        }
        eprintln!("=== END DEBUG ===");
    }
    assert!(
        purged,
        "Leader never logged a completed log purge — node 4 joining now would prove \
         nothing about the InstallSnapshot path (it could legitimately catch up via \
         ordinary AppendEntries if the retained log still reaches its starting index)"
    );

    let baseline_applied = leader_sm.last_applied().index;
    info!("Leader applied index before learner joins: {baseline_applied}");

    // Start an empty Learner (behind the purge boundary → needs a full snapshot).
    let learner_config = format!(
        r#"
[cluster]
node_id = 4
listen_address = '127.0.0.1:{}'
initial_cluster = [
    {{ id = 1, name = 'n1', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 2, name = 'n2', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 3, name = 'n3', address = '127.0.0.1:{}', role = 1, status = 3 }},
    {{ id = 4, name = 'n4', address = '127.0.0.1:{}', role = 4, status = 2 }}
]

[raft]
general_raft_timeout_duration_in_ms = 5000

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000

[raft.snapshot]
max_log_entries_before_snapshot = {SNAPSHOT_THRESHOLD}
retained_log_entries = {RETAINED_LOGS}
chunk_size = {CHUNK_SIZE}
push_queue_size = 1
"#,
        ports[3], ports[0], ports[1], ports[2], ports[3],
    );

    let learner_config_path = temp_dir.path().join("node4.toml");
    tokio::fs::write(&learner_config_path, &learner_config).await?;

    let learner_db_path = data_dir.join("node4/db");
    tokio::fs::create_dir_all(&learner_db_path).await?;

    let (learner_storage, learner_sm) = RocksDBUnifiedEngine::open(&learner_db_path)?;
    let learner_sm = Arc::new(learner_sm);

    // Register the transfer gate for the Learner's node id *before* starting it, so the
    // Leader can never race ahead and start (or finish) the push before the gate exists.
    let learner_id: u32 = 4;
    let (transfer_gate, _gate_guard) = install_snapshot_transfer_gate(learner_id);

    let learner_engine = DefaultEmbeddedEngine::start_custom(
        &learner_db_path,
        Arc::new(learner_storage),
        Arc::<RocksDBStateMachine>::clone(&learner_sm),
        Some(learner_config_path.to_str().unwrap()),
    )
    .await?;
    info!("Learner Node 4 started, snapshot transfer triggered");

    // Deterministic handshake: wait for the real transfer to have entered the push loop
    // (all setup done, about to send its first chunk) and frozen there, instead of guessing
    // how long that takes.
    tokio::time::timeout(Duration::from_secs(15), transfer_gate.wait_started())
        .await
        .expect("timed out waiting for snapshot transfer to start");
    info!("Snapshot transfer started and is frozen mid-flight (gate not released yet)");

    info!("Writing {CONCURRENT_ENTRIES} entries while the snapshot transfer is frozen");
    for i in 0..CONCURRENT_ENTRIES {
        leader_client
            .put(
                format!("post_{i}").into_bytes(),
                format!("value_{i}").into_bytes(),
            )
            .await?;
    }

    // Core non-blocking assertion: the Leader must have applied the concurrent writes
    // while the snapshot transfer to the Learner is still frozen mid-flight (started,
    // not completed).
    let target = baseline_applied + CONCURRENT_ENTRIES;
    let mut leader_advanced = false;
    for _ in 0..20 {
        if leader_sm.last_applied().index >= target {
            leader_advanced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        leader_advanced,
        "Leader applied index did not reach {target} (stuck at {}) while snapshot was in flight",
        leader_sm.last_applied().index
    );

    assert!(
        !transfer_gate.is_completed(),
        "snapshot transfer unexpectedly completed before the test released it — the Leader's \
         apply must not depend on completing the transfer, but this transfer really was still \
         in flight and this assertion only holds if it stayed frozen"
    );
    info!(
        "Leader applied {CONCURRENT_ENTRIES} entries while the snapshot transfer was still frozen"
    );

    // Release the transfer and let it proceed normally.
    transfer_gate.release();

    // Eventually the Learner must catch up (full snapshot + the concurrent tail).
    let mut caught_up = false;
    for _ in 0..40 {
        if learner_sm.last_applied().index >= target {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        caught_up,
        "Learner failed to catch up (applied {}, expected >= {target})",
        learner_sm.last_applied().index
    );
    info!(
        "Learner caught up to applied index {}",
        learner_sm.last_applied().index
    );

    let mut stop_err: Option<Box<dyn std::error::Error>> = None;
    for engine in &engines {
        if let Err(e) = engine.stop().await {
            stop_err = Some(e.into());
        }
    }
    if let Err(e) = learner_engine.stop().await {
        stop_err = Some(e.into());
    }
    if let Some(e) = stop_err {
        return Err(e);
    }

    Ok(())
}
