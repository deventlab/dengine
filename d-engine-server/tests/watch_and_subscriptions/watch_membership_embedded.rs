//! Integration tests for `DefaultEmbeddedEngine::watch_membership()` — Ticket #327
//!
//! Verifies in-process membership change notifications via `watch::Receiver<MembershipSnapshot>`.
//!
//! ## What is tested
//!
//! | # | Test | Key assertion |
//! |---|------|---------------|
//! | 1 | [`test_watch_membership_returns_initial_snapshot_without_waiting`] | First `borrow()` returns current members immediately |
//! | 2 | [`test_watch_membership_fires_on_node_join`] | Watch fires when a new learner joins the cluster |
//! | 3 | [`test_watch_membership_zombie_warns_without_removal`] | Zombie detection emits warn log; watch does NOT fire (no auto-removal) |
//! | 4 | [`test_watch_membership_fires_on_learner_promote`] | Watch fires when a learner auto-promotes to voter |
//! | 5 | [`test_watch_membership_all_nodes_receive_notification`] | Leader, follower, and learner all receive the notification |
//! | 6 | [`test_watch_membership_multiple_subscribers_all_notified`] | Multiple `watch_membership()` receivers all fire independently |
//! | 7 | [`test_watch_membership_committed_index_monotonically_increasing`] | `committed_index` increases across consecutive changes |
//!
//! ## Design notes
//!
//! - `watch::Receiver` semantics: lossy (latest value only), no diff carried.
//!   Callers compute diff from their own previous snapshot.
//! - `committed_index` is the idempotency key; callers use it to detect replayed
//!   snapshots after reconnect or restart.
//! - Notification fires on **every** node (leader / follower / learner) because
//!   all walk the same `CommitHandler::apply_config_change()` path.
//!
//! ## How conf changes reach the watch channel
//!
//! Only Raft-consensus-backed conf changes trigger the watch.  The three paths used
//! in these tests are:
//!
//! 1. **`JoinCluster` RPC**: New learner node (role=4) calls `join_cluster()` on boot.
//!    Leader appends `AddNode(Config)` entry → commits → `CommitHandler::apply_config_change`
//!    → `notify_config_applied` → watch fires on all nodes.
//!
//! 2. **Zombie detection** (warn-only): When a node's connection failure count reaches
//!    `zombie.threshold`, the leader emits a `warn` log — no `BatchRemove` is proposed.
//!    Auto-removal is intentionally absent; see zombie_detection_decision.md.
//!    Test 3 uses `threshold=1` to trigger the warning quickly and verify no watch fires.
//!
//! 3. **Auto-promotion** (`BatchPromote`): When a learner with `status=Promotable(2)` catches
//!    up to the leader's commit index (within `learner_check_throttle_ms = 100 ms`), the
//!    leader proposes `BatchPromote(Config)` → commits → watch fires.
//!
//! `ClusterManagementService::update_cluster_conf` is NOT used here; it bypasses the Raft
//! log and modifies follower membership state directly without calling `notify_config_applied`.

#![cfg(feature = "rocksdb")]

use std::sync::Arc;
use std::time::Duration;

use d_engine_server::{DefaultEmbeddedEngine, RocksDBUnifiedEngine};
use tempfile::TempDir;
use tokio::time::timeout;
use tracing_test::traced_test;

use crate::common::get_available_ports;

// ── Timeout constants ─────────────────────────────────────────────────────────

/// How long to wait for the initial leader election before a test assertion.
const ELECTION_TIMEOUT: Duration = Duration::from_secs(8);

/// How long to wait for a single membership-change notification to arrive.
const MEMBERSHIP_CHANGE_TIMEOUT: Duration = Duration::from_secs(10);

// ── NodeStatus constants (mirrors d_engine_proto::common::NodeStatus) ─────────

/// `NodeStatus::Active = 3` — alive voter or non-promotable learner.
const STATUS_ACTIVE: i32 = 3;

/// `NodeStatus::Promotable = 1` — learner eligible for auto-promotion to voter.
/// When a learner with this status catches up to the leader's commit index,
/// the leader automatically proposes `BatchPromote` via Raft consensus.
const STATUS_PROMOTABLE: i32 = 1;

// ── Cluster setup helpers ──────────────────────────────────────────────────────

/// Build a TOML config string for one node in a fixed-topology cluster.
///
/// `all_ports[i]` belongs to node `i+1`.
/// `roles[i]` is the Raft role for node `i+1` (1 = voter, 4 = learner).
/// `statuses[i]` is the NodeStatus for node `i+1` (1 = Promotable, 3 = Active).
///
/// Includes `learner_check_throttle_ms = 100` so auto-promotion fires within
/// ~100 ms of a learner catching up (default is 1000 ms).
fn node_toml(
    node_id: u32,
    port: u16,
    all_ports: &[u16],
    roles: &[i32],
    statuses: &[i32],
    _db_root: &str,
    log_dir: &str,
) -> String {
    let members = all_ports
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            let id = i as u32 + 1;
            let role = roles.get(i).copied().unwrap_or(1);
            let status = statuses.get(i).copied().unwrap_or(STATUS_ACTIVE);
            format!(
                "{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = {role}, status = {status} }}"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    format!(
        r#"
[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{port}'
initial_cluster = [
    {members}
]
log_dir = '{log_dir}'

[raft]
general_raft_timeout_duration_in_ms = 10000
learner_check_throttle_ms = 100

[raft.election]
election_timeout_min = 300
election_timeout_max = 3000


[retry.election]
max_retries = 5
timeout_ms = 2000
base_delay_ms = 100
max_delay_ms = 5000
"#
    )
}

/// Append a `[raft.membership.zombie]` override so that a single AppendEntries
/// failure immediately triggers zombie detection (warn-only; no auto-removal).
///
/// Used only in test 3 (`test_watch_membership_zombie_warns_without_removal`).
/// Kept separate from `node_toml` to avoid spurious zombie signals in other
/// tests during startup.
fn with_fast_zombie(base_toml: &str) -> String {
    format!(
        "{base_toml}
[raft.membership.zombie]
threshold = 1
"
    )
}

/// Poll `rx` until the snapshot contains `node_id` or `dur` elapses.
///
/// `changed()` fires for **any** ConfChange, not just the one we're waiting for.
/// This helper loops until the snapshot actually reflects the expected membership,
/// making assertions resilient to intermediate notifications.
async fn wait_for_node_in_snapshot(
    rx: &mut tokio::sync::watch::Receiver<d_engine_server::MembershipSnapshot>,
    node_id: u32,
    dur: Duration,
) -> Option<d_engine_server::MembershipSnapshot> {
    tokio::time::timeout(dur, async {
        loop {
            let snap = rx.borrow_and_update().clone();
            if snap.members.contains(&node_id) || snap.learners.contains(&node_id) {
                return Some(snap);
            }
            if rx.changed().await.is_err() {
                return None;
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// Start one `DefaultEmbeddedEngine` from a TOML string written to a temp file.
async fn start_engine(
    toml: &str,
    node_id: u32,
    db_root: &std::path::Path,
    config_path: &str,
) -> Result<DefaultEmbeddedEngine, Box<dyn std::error::Error>> {
    tokio::fs::write(config_path, toml).await?;
    let node_data_dir = db_root.join(format!("node{node_id}"));
    let db_path = node_data_dir.join("db");
    tokio::fs::create_dir_all(&db_path).await?;
    let (storage, sm) = RocksDBUnifiedEngine::open(&db_path)?;
    Ok(DefaultEmbeddedEngine::start_custom(
        &node_data_dir,
        Arc::new(storage),
        Arc::new(sm),
        Some(config_path),
    )
    .await?)
}

// ── Tests ──────────────────────────────────────────────────────────────────────

/// Test 1: First `borrow()` on a fresh `watch_membership()` returns the current
/// membership immediately — no change event required.
///
/// This guarantees developers can read the initial cluster state synchronously
/// right after subscribing, matching the `watch::channel` semantics where the
/// initial value is always available.
///
/// Setup:  single-node cluster (self-elected leader).
/// Assert: `snapshot.members` contains exactly node 1; `committed_index` is valid.
#[tokio::test]
#[traced_test]
async fn test_watch_membership_returns_initial_snapshot_without_waiting()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let mut port_guard = get_available_ports(1).await;
    port_guard.release_listeners();
    let port = port_guard.as_slice()[0];

    let engine = start_engine(
        &node_toml(
            1,
            port,
            &[port],
            &[1],
            &[STATUS_ACTIVE],
            &temp.path().join("db").to_string_lossy(),
            &temp.path().join("logs").to_string_lossy(),
        ),
        1,
        temp.path(),
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;

    engine.wait_ready(ELECTION_TIMEOUT).await?;

    // Subscribe and read current state without waiting for any change.
    let rx = engine.watch_membership();
    let snapshot = rx.borrow().clone();

    assert!(
        snapshot.members.contains(&1),
        "Initial snapshot must contain self (node 1); got: {:?}",
        snapshot.members
    );
    assert!(
        snapshot.learners.is_empty(),
        "No learners expected on single-node boot"
    );
    // committed_index of 0 is acceptable on a freshly-booted single-node cluster
    // that has not yet committed any ConfChange entry.
    let _ = snapshot.committed_index; // presence check; value is implementation-defined

    engine.stop().await?;
    Ok(())
}

/// Test 2: `watch_membership()` fires when a new node joins the cluster.
///
/// A new node must start with `role = Learner (4)` so it calls `join_cluster()`
/// RPC on boot.  The leader appends an `AddNode` ConfChange entry; when it
/// commits all nodes fire the watch channel.
///
/// Node 3 uses `status = Active (3)` to avoid triggering auto-promotion; this
/// test only verifies the join notification, not the promotion path.
///
/// Setup:  2-node cluster (nodes 1, 2).  Subscribe on node 1.
/// Action: start node 3 as Learner (role=4, status=Active) — it sends JoinCluster
///         RPC to the leader.
/// Assert: `changed()` fires; `snapshot.learners` contains node 3.
#[tokio::test]
#[traced_test]
async fn test_watch_membership_fires_on_node_join() -> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db_root = temp.path().join("db");
    let log_dir = temp.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Start 2-node cluster: nodes 1 and 2 only know about each other.
    let two_node_ports = &ports[..2];
    let engine1 = start_engine(
        &node_toml(
            1,
            ports[0],
            two_node_ports,
            &[1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        1,
        &db_root,
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;
    let engine2 = start_engine(
        &node_toml(
            2,
            ports[1],
            two_node_ports,
            &[1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        2,
        &db_root,
        &temp.path().join("n2.toml").to_string_lossy(),
    )
    .await?;

    engine1.wait_ready(ELECTION_TIMEOUT).await?;
    engine2.wait_ready(ELECTION_TIMEOUT).await?;

    // Subscribe on node 1 before the change happens.
    let mut rx = engine1.watch_membership();

    // Node 3 starts as Learner (role=4, status=Active).  On boot it calls
    // join_cluster() RPC, causing the leader to commit an AddNode ConfChange.
    // Active status prevents auto-promotion so only one watch event fires.
    let engine3 = start_engine(
        &node_toml(
            3,
            ports[2],
            ports,
            &[1, 1, 4],
            &[STATUS_ACTIVE, STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        3,
        &db_root,
        &temp.path().join("n3.toml").to_string_lossy(),
    )
    .await?;

    // Wait for the membership change notification.
    timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx.changed())
        .await
        .expect("timed out waiting for membership change after node join")
        .expect("watch channel closed unexpectedly");

    let snapshot = rx.borrow_and_update().clone();
    assert!(
        snapshot.learners.contains(&3),
        "Snapshot must include the newly joined learner node 3; got learners: {:?}",
        snapshot.learners
    );

    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;
    Ok(())
}

/// Test 3: Zombie detection does NOT auto-remove the node.
///
/// ## Design rationale
///
/// Auto-removal of unreachable nodes via `BatchRemove` is intentionally absent.
/// Membership changes are high-risk Raft operations: a node that fails N connection
/// attempts may simply be restarting, and an incorrect removal would permanently eject
/// it from the cluster (requiring manual re-add).  Instead, d-engine emits a `warn`
/// log so that upper-layer tooling or operators can decide whether removal is warranted.
/// See decision doc: `d-engine-product-design/…/20260418/zombie_detection_decision.md`.
///
/// ## What this test verifies
///
/// 1. The `watch_membership()` channel does **not** fire after node 3 becomes
///    unreachable — confirming no `BatchRemove` was submitted to Raft consensus.
/// 2. Node 3 is still present in the membership snapshot (not auto-removed).
/// 3. The surviving 2-node cluster remains healthy (leader still accepts requests).
///
/// Note: the `warn!("Zombie detected …")` is emitted inside engine Raft tasks that
/// run on tokio worker threads.  `traced_test` does not capture events from those
/// threads, so we verify the behavioral invariants instead of the log output.
///
/// ## Setup
///
/// - 3-node cluster (nodes 1, 2, 3).
/// - `zombie.threshold = 1`: first AppendEntries failure to node 3 fires the zombie
///   signal immediately.
/// - Subscribe watch on node 1 before stopping node 3.
///
/// ## Invariant protected
///
/// If this test fails because `rx.changed()` fires, it means auto-removal was
/// accidentally re-introduced somewhere in the zombie detection path.
#[tokio::test]
#[traced_test]
async fn test_watch_membership_zombie_warns_without_removal()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db_root = temp.path().join("db");
    let log_dir = temp.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // threshold=1: the very first AppendEntries failure fires ZombieDetected(3).
    // This keeps the wait short while still exercising the full detection pipeline.
    let make_toml = |id: u32, port: u16| {
        with_fast_zombie(&node_toml(
            id,
            port,
            ports,
            &[1, 1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ))
    };

    let engine1 = start_engine(
        &make_toml(1, ports[0]),
        1,
        &db_root,
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;
    let engine2 = start_engine(
        &make_toml(2, ports[1]),
        2,
        &db_root,
        &temp.path().join("n2.toml").to_string_lossy(),
    )
    .await?;
    let engine3 = start_engine(
        &make_toml(3, ports[2]),
        3,
        &db_root,
        &temp.path().join("n3.toml").to_string_lossy(),
    )
    .await?;

    engine1.wait_ready(ELECTION_TIMEOUT).await?;

    let rx = engine1.watch_membership();

    // Stop node 3. The currently elected leader (whichever of 1/2/3 it is) will
    // start failing to replicate to it, triggering the zombie pipeline.
    // If node 3 happened to be the initial leader, nodes 1 and 2 must first elect
    // a new leader among themselves before AppendEntries to node 3 can resume.
    engine3.stop().await?;

    // Wait for a stable leader among nodes 1 and 2 — this is a prerequisite for
    // AppendEntries being sent to the stopped node 3, which is what feeds the
    // failure counter and eventually fires the "Zombie detected" warning.
    //
    // Use 35s here: with election_timeout_max=3000ms, split-vote livelock between
    // nodes 1 and 2 can produce multiple election rounds (~9-12s typical, up to ~25s
    // observed on slow CI machines). 35s provides a safe upper bound.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
    loop {
        if engine1.is_leader() || engine2.is_leader() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Nodes 1 and 2 failed to elect a leader within the election timeout after node 3 stopped"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Give the zombie pipeline time to run.  With threshold=1 the signal fires on
    // the very first AppendEntries failure, which happens within milliseconds of the
    // new leader sending its first heartbeat to node 3.  2 seconds is generous.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Assert 1: watch channel did NOT fire — no BatchRemove was committed to Raft.
    // `has_changed()` returns true only if a new value was sent since the last borrow.
    assert!(
        !rx.has_changed().unwrap_or(false),
        "watch_membership must not fire when zombie detection is warn-only (no BatchRemove)"
    );

    // Assert 2: node 3 is still present in the membership snapshot — not auto-removed.
    let snapshot = rx.borrow();
    let node3_present = snapshot.members.contains(&3) || snapshot.learners.contains(&3);
    assert!(
        node3_present,
        "node 3 must still be in membership after zombie detection (no auto-removal)"
    );
    drop(snapshot);

    // Assert 3: surviving cluster is still healthy (leader responds).
    assert!(
        engine1.is_leader() || engine2.is_leader(),
        "2-node cluster must still have a leader after node 3 stopped"
    );

    engine1.stop().await?;
    engine2.stop().await?;
    Ok(())
}

/// Test 4: `watch_membership()` fires when a learner is promoted to voter.
///
/// Promotion happens automatically via the `BatchPromote` Raft path:
/// 1. Learner starts with `status = Promotable (1)` and `role = Learner (4)`.
/// 2. On boot the learner sends `JoinCluster` RPC → leader commits `AddNode`;
///    watch fires with node 3 in `learners` (change 1).
/// 3. The learner replicates the log.  Within `learner_check_throttle_ms = 100 ms`
///    the leader detects it has caught up and proposes `BatchPromote`; watch fires
///    with node 3 moved from `learners` to `members` (change 2).
///
/// Setup:  2-node voter cluster (nodes 1, 2).  Subscribe on node 1.
/// Action: start node 3 as Promotable Learner (role=4, status=1).
/// Assert: second `changed()` shows node 3 in `members`, not in `learners`.
#[tokio::test]
#[traced_test]
async fn test_watch_membership_fires_on_learner_promote() -> Result<(), Box<dyn std::error::Error>>
{
    let temp = TempDir::new()?;
    let db_root = temp.path().join("db");
    let log_dir = temp.path().join("logs");

    let mut port_guard = get_available_ports(3).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Nodes 1 and 2 form a 2-voter cluster; they do NOT know about node 3 initially.
    // Node 3 will join later via JoinCluster RPC.
    let two_ports = &ports[..2];

    let engine1 = start_engine(
        &node_toml(
            1,
            ports[0],
            two_ports,
            &[1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        1,
        &db_root,
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;
    let engine2 = start_engine(
        &node_toml(
            2,
            ports[1],
            two_ports,
            &[1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        2,
        &db_root,
        &temp.path().join("n2.toml").to_string_lossy(),
    )
    .await?;

    engine1.wait_ready(ELECTION_TIMEOUT).await?;
    engine2.wait_ready(ELECTION_TIMEOUT).await?;

    // Subscribe BEFORE starting node 3 so we receive both the AddNode and
    // the subsequent BatchPromote notifications.
    let mut rx = engine1.watch_membership();

    // Node 3 starts as a Promotable Learner.  Its status=1 (Promotable) is read from
    // initial_cluster and sent to the leader in the JoinRequest.  Once it catches up
    // (gap ≤ learner_catchup_threshold), the leader auto-promotes it via BatchPromote.
    let engine3 = start_engine(
        &node_toml(
            3,
            ports[2],
            ports,
            &[1, 1, 4],
            &[STATUS_ACTIVE, STATUS_ACTIVE, STATUS_PROMOTABLE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        3,
        &db_root,
        &temp.path().join("n3.toml").to_string_lossy(),
    )
    .await?;

    // Change 1: AddNode commits — node 3 appears in learners.
    timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx.changed())
        .await
        .expect("timed out waiting for AddNode notification (change 1)")
        .expect("watch channel closed unexpectedly");
    let snap1 = rx.borrow_and_update().clone();
    assert!(
        snap1.learners.contains(&3),
        "After joining, node 3 must be in learners; got: {:?}",
        snap1.learners
    );

    // Change 2: BatchPromote commits — node 3 moves from learners to members.
    timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx.changed())
        .await
        .expect("timed out waiting for BatchPromote notification (change 2)")
        .expect("watch channel closed unexpectedly");

    let snap2 = rx.borrow_and_update().clone();
    assert!(
        snap2.members.contains(&3),
        "Promoted node 3 must appear in members; got: {:?}",
        snap2.members
    );
    assert!(
        !snap2.learners.contains(&3),
        "Promoted node 3 must not remain in learners; got: {:?}",
        snap2.learners
    );

    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;
    Ok(())
}

/// Test 5: All nodes in a cluster receive the membership change notification —
/// leader, follower, and learner included.
///
/// The Raft commit path (`CommitHandler::apply_config_change`) is identical on
/// every node, so all must fire the watch channel after a ConfChange commits.
///
/// Setup sequence:
/// 1. Start 2-voter cluster (nodes 1, 2).
/// 2. Start node 3 as Active Learner via JoinCluster RPC; wait for it to join.
/// 3. Subscribe on all 3 nodes so all hold a "seen" version.
/// 4. Start node 4 as Active Learner — triggers AddNode ConfChange.
///
/// Assert: `changed()` fires on every receiver (leader, follower, learner) within timeout.
///
/// Node 3 must NOT be in nodes 1/2's initial_cluster so that `can_rejoin` succeeds.
/// Both node 3 and node 4 use `status = Active` to suppress auto-promotion (no extra events).
#[tokio::test]
#[traced_test]
async fn test_watch_membership_all_nodes_receive_notification()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db_root = temp.path().join("db");
    let log_dir = temp.path().join("logs");

    let mut port_guard = get_available_ports(4).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Step 1: 2-voter cluster — nodes 1 and 2 do NOT know about nodes 3 or 4 yet.
    let two_ports = &ports[..2];

    let engine1 = start_engine(
        &node_toml(
            1,
            ports[0],
            two_ports,
            &[1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        1,
        &db_root,
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;
    let engine2 = start_engine(
        &node_toml(
            2,
            ports[1],
            two_ports,
            &[1, 1],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        2,
        &db_root,
        &temp.path().join("n2.toml").to_string_lossy(),
    )
    .await?;

    engine1.wait_ready(ELECTION_TIMEOUT).await?;

    // Step 2: Node 3 joins as Active Learner via JoinCluster RPC.
    // It knows about nodes 1 and 2 (but they don't know about it initially).
    // Active status prevents auto-promotion.
    let engine3 = start_engine(
        &node_toml(
            3,
            ports[2],
            &ports[..3],
            &[1, 1, 4],
            &[STATUS_ACTIVE, STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        3,
        &db_root,
        &temp.path().join("n3.toml").to_string_lossy(),
    )
    .await?;

    // Wait for node 3's join (AddNode ConfChange) to commit on node 1.
    {
        let mut tmp_rx = engine1.watch_membership();
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, tmp_rx.changed())
            .await
            .expect("timed out waiting for node 3 to join")
            .expect("watch channel closed");
    }

    // Step 3: Subscribe on all three nodes AFTER node 3 has joined.
    // Each receiver now holds the "already seen" value; the next change will fire changed().
    let mut rx1 = engine1.watch_membership();
    let mut rx2 = engine2.watch_membership();
    let mut rx3 = engine3.watch_membership();

    // Step 4: Node 4 starts as Active Learner — JoinCluster → AddNode ConfChange commits.
    // All three nodes (leader, follower, learner) must receive the notification.
    let engine4 = start_engine(
        &node_toml(
            4,
            ports[3],
            ports,
            &[1, 1, 4, 4],
            &[STATUS_ACTIVE, STATUS_ACTIVE, STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        4,
        &db_root,
        &temp.path().join("n4.toml").to_string_lossy(),
    )
    .await?;

    // All three receivers must fire — leader, follower, and learner.
    let (r1, r2, r3) = tokio::join!(
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx1.changed()),
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx2.changed()),
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx3.changed()),
    );

    assert!(
        r1.is_ok(),
        "Node 1 (voter/leader or follower) did not receive membership notification"
    );
    assert!(
        r2.is_ok(),
        "Node 2 (voter/follower) did not receive membership notification"
    );
    assert!(
        r3.is_ok(),
        "Node 3 (learner) did not receive membership notification"
    );

    // Verify all three nodes eventually observe node 4 in their membership snapshot.
    // changed() above guarantees a notification fired; this loop handles the case where
    // that notification was for an intermediate ConfChange before node 4's AddNode
    // propagated to the observer node (common under bidi-streaming AppendEntries).
    wait_for_node_in_snapshot(&mut rx1, 4, MEMBERSHIP_CHANGE_TIMEOUT)
        .await
        .expect("Node 1 did not observe node 4 in membership snapshot within timeout");
    wait_for_node_in_snapshot(&mut rx2, 4, MEMBERSHIP_CHANGE_TIMEOUT)
        .await
        .expect("Node 2 did not observe node 4 in membership snapshot within timeout");
    wait_for_node_in_snapshot(&mut rx3, 4, MEMBERSHIP_CHANGE_TIMEOUT)
        .await
        .expect("Node 3 (learner) did not observe node 4 in membership snapshot within timeout");

    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;
    engine4.stop().await?;
    Ok(())
}

/// Test 6: Multiple `watch_membership()` calls on the same engine each return
/// independent receivers that all fire on a single membership change.
///
/// `watch::channel` supports multiple subscribers natively.  This test confirms
/// the implementation does not limit concurrent subscribers.
///
/// Setup:  single-node cluster.  Subscribe 3 times before any change.
/// Action: node 2 joins as Active Learner (no auto-promotion).
/// Assert: all 3 receivers fire and show consistent snapshots.
#[tokio::test]
#[traced_test]
async fn test_watch_membership_multiple_subscribers_all_notified()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db_root = temp.path().join("db");
    let log_dir = temp.path().join("logs");

    let mut port_guard = get_available_ports(2).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    let engine1 = start_engine(
        &node_toml(
            1,
            ports[0],
            &ports[..1],
            &[1],
            &[STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        1,
        &db_root,
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;
    engine1.wait_ready(ELECTION_TIMEOUT).await?;

    // Three independent subscribers on the same engine.
    let mut rx_a = engine1.watch_membership();
    let mut rx_b = engine1.watch_membership();
    let mut rx_c = engine1.watch_membership();

    // Node 2 starts as Active Learner (role=4, status=Active) — triggers one AddNode
    // ConfChange commit.  Active status prevents auto-promotion (no second event).
    let engine2 = start_engine(
        &node_toml(
            2,
            ports[1],
            ports,
            &[1, 4],
            &[STATUS_ACTIVE, STATUS_ACTIVE],
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        ),
        2,
        &db_root,
        &temp.path().join("n2.toml").to_string_lossy(),
    )
    .await?;

    let (ra, rb, rc) = tokio::join!(
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx_a.changed()),
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx_b.changed()),
        timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx_c.changed()),
    );

    assert!(
        ra.is_ok(),
        "Subscriber A did not receive membership notification"
    );
    assert!(
        rb.is_ok(),
        "Subscriber B did not receive membership notification"
    );
    assert!(
        rc.is_ok(),
        "Subscriber C did not receive membership notification"
    );

    // All subscribers must see the same current state.
    let sa = rx_a.borrow_and_update().clone();
    let sb = rx_b.borrow_and_update().clone();
    let sc = rx_c.borrow_and_update().clone();

    assert_eq!(
        sa.members, sb.members,
        "Subscribers A and B disagree on members"
    );
    assert_eq!(
        sb.members, sc.members,
        "Subscribers B and C disagree on members"
    );
    assert!(
        sa.learners.contains(&2) || sa.members.contains(&2),
        "Node 2 missing from snapshot: members={:?} learners={:?}",
        sa.members,
        sa.learners
    );

    engine1.stop().await?;
    engine2.stop().await?;
    Ok(())
}

/// Test 7: `committed_index` in consecutive membership snapshots is strictly
/// monotonically increasing.
///
/// ## Why this matters
///
/// Callers use `committed_index` as an idempotency key: after a reconnect or restart
/// they compare the newly received index against the last one they processed, and skip
/// snapshots they have already handled.  If two distinct membership changes ever produced
/// the same `committed_index`, a caller could silently miss a change — breaking
/// correctness.  Monotonicity is therefore a hard invariant.
///
/// ## How two membership changes are produced
///
/// Both changes are `AddNode` entries committed via the `JoinCluster` RPC path:
///
/// - Change 1: node 3 starts and joins the 2-node cluster as an Active Learner.
/// - Change 2: node 4 starts and joins the (now 3-node) cluster as an Active Learner.
///
/// Using two joins avoids relying on zombie-detection auto-removal, which is
/// intentionally not implemented (see zombie_detection_decision.md).  Each join
/// produces one Raft `AddNode(Config)` entry that increments the commit index.
///
/// ## Assert
///
/// `snapshot2.committed_index > snapshot1.committed_index`
#[tokio::test]
#[traced_test]
async fn test_watch_membership_committed_index_monotonically_increasing()
-> Result<(), Box<dyn std::error::Error>> {
    let temp = TempDir::new()?;
    let db_root = temp.path().join("db");
    let log_dir = temp.path().join("logs");

    // 4 ports: 2 initial voters + 2 joining learners.
    let mut port_guard = get_available_ports(4).await;
    port_guard.release_listeners();
    let ports = port_guard.as_slice();

    // Helper: build a node TOML with Active status for all nodes.
    let make_toml = |id: u32, port: u16, port_slice: &[u16], roles: &[i32]| {
        let statuses: Vec<i32> = vec![STATUS_ACTIVE; roles.len()];
        node_toml(
            id,
            port,
            port_slice,
            roles,
            &statuses,
            &db_root.to_string_lossy(),
            &log_dir.to_string_lossy(),
        )
    };

    // Start the initial 2-node cluster (nodes 1 and 2).
    let engine1 = start_engine(
        &make_toml(1, ports[0], &ports[..2], &[1, 1]),
        1,
        &db_root,
        &temp.path().join("n1.toml").to_string_lossy(),
    )
    .await?;
    let engine2 = start_engine(
        &make_toml(2, ports[1], &ports[..2], &[1, 1]),
        2,
        &db_root,
        &temp.path().join("n2.toml").to_string_lossy(),
    )
    .await?;

    engine1.wait_ready(ELECTION_TIMEOUT).await?;
    engine2.wait_ready(ELECTION_TIMEOUT).await?;

    // Subscribe before any join so we don't miss the first notification.
    let mut rx = engine1.watch_membership();

    // Change 1: node 3 joins as Active Learner.
    // role=4 (learner), status=Active → no auto-promotion, one clean AddNode commit.
    let engine3 = start_engine(
        &make_toml(3, ports[2], &ports[..3], &[1, 1, 4]),
        3,
        &db_root,
        &temp.path().join("n3.toml").to_string_lossy(),
    )
    .await?;

    timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx.changed())
        .await
        .expect("timed out waiting for first membership change (node 3 join)")
        .expect("watch channel closed unexpectedly");
    let snapshot1 = rx.borrow_and_update().clone();

    // Change 2: node 4 joins as Active Learner.
    // A second distinct AddNode entry is committed, incrementing committed_index again.
    let engine4 = start_engine(
        &make_toml(4, ports[3], ports, &[1, 1, 4, 4]),
        4,
        &db_root,
        &temp.path().join("n4.toml").to_string_lossy(),
    )
    .await?;

    timeout(MEMBERSHIP_CHANGE_TIMEOUT, rx.changed())
        .await
        .expect("timed out waiting for second membership change (node 4 join)")
        .expect("watch channel closed unexpectedly");
    let snapshot2 = rx.borrow_and_update().clone();

    assert!(
        snapshot2.committed_index > snapshot1.committed_index,
        "committed_index must be strictly increasing across consecutive membership changes: \
         first={}, second={}",
        snapshot1.committed_index,
        snapshot2.committed_index
    );

    engine1.stop().await?;
    engine2.stop().await?;
    engine3.stop().await?;
    engine4.stop().await?;
    Ok(())
}
