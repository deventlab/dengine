use crate::ClientCmd;
use crate::Error;
use crate::InboundEvent;
use crate::InternalEvent;
use crate::MaybeCloneOneshot;
use crate::MockBuilder;
use crate::MockMembership;
use crate::MockPurgeExecutor;
use crate::MockStateMachineHandler;
use crate::NewCommitData;
use crate::RaftOneshot;
use crate::SnapshotApplyResult;
use crate::StateMachineCommand;
use crate::StateMachineCommandSender;
use crate::client::ClientReadRequest;
use crate::client::ClientResponse;
use crate::client::ClientResponsePayload;
use crate::client::ClientWriteRequest;
use crate::client::ErrorCode;
use crate::client::KvEntry;
use crate::config::ReadConsistencyPolicy;
use crate::raft_role::candidate_state::CandidateState;
use crate::raft_role::follower_state::FollowerState;
use crate::raft_role::learner_state::LearnerState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;
use crate::test_utils::mock::mock_raft_context;
use crate::test_utils::mock::mock_raft_context_with_temp;
use crate::test_utils::mock::mock_raft_log;
use crate::test_utils::node_config;
use bytes::Bytes;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::NodeMeta;
use d_engine_proto::server::cluster::cluster_conf_update_response;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::snapshot_ack::ChunkStatus;
use mockall::predicate::eq;
use prost::Message;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tonic::Code;

/// Test: LearnerState drain_read_buffer returns NotLeader error
///
/// Scenario: Call drain_read_buffer() on Learner
/// Expected: Returns NotLeader error (Learner doesn't buffer reads)
#[tokio::test]
async fn test_learner_drain_read_buffer_returns_error() {
    let mut state =
        LearnerState::<MockTypeConfig>::new(1, Arc::new(node_config("/tmp/test_learner_drain")));

    // Action: Call drain_read_buffer()
    let result = state.drain_read_buffer();

    // Verify: Returns NotLeader error
    assert!(
        result.is_err(),
        "Learner drain_read_buffer should return error"
    );

    if let Err(e) = result {
        let error_str = format!("{e:?}");
        assert!(
            error_str.contains("NotLeader"),
            "Error should be NotLeader, got: {error_str}"
        );
    }
}

// ============================================================================
// Basic Operations Tests
// ============================================================================

/// Test: LearnerState tick behavior
///
/// Scenario:
/// - Learner receives tick event
/// - Learner doesn't participate in elections (no election timeout)
///
/// Expected:
/// - tick() returns Ok()
/// - No role change events
/// - Learner remains passive
///
/// This validates that learners don't initiate elections.
///
/// Original: test_tick
#[tokio::test]
async fn test_learner_tick_succeeds() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let (event_tx, _event_rx) = mpsc::channel(1);

    // Action: Tick
    assert!(
        state.tick(&internal_event_tx, &event_tx, &context).await.is_ok(),
        "Learner tick should succeed"
    );
}

/// Test: LearnerState rejects VoteRequest but updates term
///
/// Scenario:
/// - Learner (term=1) receives VoteRequest from candidate (term=11)
/// - Learners cannot vote (not voters)
/// - But must update term to latest
///
/// Expected:
/// - Returns response with vote_granted=false
/// - Updates current_term to request term (11)
/// - handle_inbound_event returns Ok()
///
/// This validates Raft rule: all nodes update term when seeing higher term,
/// but learners never grant votes.
///
/// Original: test_handle_inbound_event_case1
#[tokio::test]
async fn test_learner_rejects_vote_request_updates_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let term_before = state.current_term();
    let request_term = term_before + 10;

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ReceiveVoteRequest(
        VoteRequest {
            term: request_term,
            candidate_id: 2,
            last_log_index: 11,
            last_log_term: 0,
        },
        resp_tx,
    );

    // Action: Handle VoteRequest
    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    // Verify: Term updated
    assert_eq!(
        state.current_term(),
        request_term,
        "Should update to request term"
    );

    // Verify: Vote rejected
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.vote_granted, "Learner should never grant votes");
}

/// Test: LearnerState rejects ClusterConf metadata request
///
/// Scenario:
/// - Learner receives ClusterConf (metadata request)
/// - Learners cannot serve cluster metadata (not full members)
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_inbound_event returns Ok()
///
/// This validates that learners redirect metadata requests to leader.
///
/// Original: test_handle_inbound_event_case2
#[tokio::test]
async fn test_learner_rejects_cluster_conf_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConf(MetadataRequest {}, resp_tx);

    // Action: Handle ClusterConf
    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    // Verify: PermissionDenied
    let status = resp_rx.recv().await.unwrap().unwrap_err();
    assert_eq!(
        status.code(),
        Code::PermissionDenied,
        "Should return PermissionDenied"
    );
}

/// Test: LearnerState handles ClusterConfUpdate successfully
///
/// Scenario:
/// - Learner receives ClusterConfUpdate from leader
/// - Membership update succeeds
///
/// Expected:
/// - Returns response with success=true
/// - error_code = Unspecified
/// - handle_inbound_event returns Ok()
///
/// This validates that learners can receive and apply cluster
/// configuration updates from leader.
///
/// Original: test_handle_inbound_event_case3
#[tokio::test]
async fn test_learner_handles_cluster_conf_update_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock membership to accept update
    let mut membership = MockMembership::new();
    membership
        .expect_update_cluster_conf_from_leader()
        .times(1)
        .returning(|_, _, _, _, _| {
            Ok(ClusterConfUpdateResponse {
                id: 1,
                term: 1,
                version: 1,
                success: true,
                error_code: cluster_conf_update_response::ErrorCode::Unspecified.into(),
            })
        });
    membership.expect_get_cluster_conf_version().returning(|| 1);
    context.membership = Arc::new(membership);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let inbound_event = InboundEvent::ClusterConfUpdate(
        ClusterConfChangeRequest {
            id: 2, // Leader ID
            term: 1,
            version: 1,
            change: None,
        },
        resp_tx,
    );

    // Action: Handle ClusterConfUpdate
    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    // Verify: Success response
    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.success, "Update should succeed");
    assert_eq!(
        response.error_code,
        cluster_conf_update_response::ErrorCode::Unspecified as i32
    );
}

// ============================================================================
// AppendEntries Tests
// ============================================================================

/// Test: LearnerState successfully handles AppendEntries from leader
///
/// Scenario:
/// - Learner (term=1) receives AppendEntries from leader (term=2)
/// - replication_handler.handle_append_entries returns success
///
/// Expected:
/// - Sends LeaderDiscovered event
/// - Sends NotifyNewCommitIndex event
/// - Updates current_term to leader's term
/// - Updates commit_index
/// - Returns AppendEntriesResponse with success=true
///
/// Original: test_handle_inbound_event_case4_1
#[tokio::test]
async fn test_learner_handles_append_entries_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let learner_term = 1;
    let leader_term = learner_term + 1;
    let expected_commit = 2;

    // Mock replication handler
    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler.expect_handle_append_entries().returning(move |_, _, _| {
        Ok(crate::AppendResponseWithUpdates {
            response: d_engine_proto::server::replication::AppendEntriesResponse::success(
                1,
                leader_term,
                Some(LogId {
                    term: leader_term,
                    index: 1,
                }),
            ),
            commit_index_update: Some(expected_commit),
        })
    });

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(learner_term);

    let append_request = d_engine_proto::server::replication::AppendEntriesRequest {
        term: leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_request, vec![resp_tx]);
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    // Verify: LeaderDiscovered event
    assert!(matches!(
        internal_event_rx.try_recv().unwrap(),
        crate::InternalEvent::LeaderDiscovered(5, _)
    ));

    // Verify: NotifyNewCommitIndex event
    assert!(matches!(
        internal_event_rx.try_recv().unwrap(),
        crate::InternalEvent::NotifyNewCommitIndex(_)
    ));

    assert_eq!(state.current_term(), leader_term);
    assert_eq!(state.commit_index(), expected_commit);

    // RPO=0 (#446): the success ACK is withheld until durable_index reaches the claimed index.
    assert!(
        resp_rx.try_recv().is_err(),
        "ACK must be withheld until durable_index catches up"
    );

    // Simulate LogFlushed: durable_index advances to 1, releasing the withheld ACK.
    let (flush_tx, _flush_rx) = mpsc::unbounded_channel();
    state.handle_log_flushed(1, &context, &flush_tx).await;

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_success());
}

/// Test: LearnerState rejects AppendEntries with stale term
///
/// Scenario:
/// - Learner (term=2) receives AppendEntries with stale term (term=1)
///
/// Expected:
/// - No events sent
/// - Term unchanged
/// - Returns AppendEntriesResponse with is_higher_term=true
///
/// Original: test_handle_inbound_event_case4_2
#[tokio::test]
async fn test_learner_rejects_append_entries_stale_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let learner_term = 2;
    let stale_term = learner_term - 1;

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(learner_term);

    let append_request = d_engine_proto::server::replication::AppendEntriesRequest {
        term: stale_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_request, vec![resp_tx]);
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    assert!(
        internal_event_rx.try_recv().is_err(),
        "No events should be sent"
    );
    assert_eq!(state.current_term(), learner_term);

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(response.is_higher_term());
}

/// Test: LearnerState handles AppendEntries failure
///
/// Scenario:
/// - Learner receives AppendEntries
/// - replication_handler returns error
///
/// Expected:
/// - Sends LeaderDiscovered event (leader is valid)
/// - Updates term
/// - Returns AppendEntriesResponse with success=false
/// - handle_inbound_event returns Err()
///
/// Original: test_handle_inbound_event_case4_3
#[tokio::test]
async fn test_learner_handles_append_entries_handler_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let learner_term = 1;
    let leader_term = learner_term + 1;

    let mut replication_handler = crate::MockReplicationCore::new();
    replication_handler
        .expect_handle_append_entries()
        .returning(|_, _, _| Err(crate::Error::Fatal("test".to_string())));

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);
    context.handlers.replication_handler = replication_handler;

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(learner_term);

    let append_request = d_engine_proto::server::replication::AppendEntriesRequest {
        term: leader_term,
        leader_id: 5,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: vec![],
        leader_commit_index: 0,
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::AppendEntries(append_request, vec![resp_tx]);
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_err(),
        "handle_inbound_event should return error"
    );

    assert!(matches!(
        internal_event_rx.try_recv().unwrap(),
        crate::InternalEvent::LeaderDiscovered(5, _)
    ));
    assert!(internal_event_rx.try_recv().is_err());

    assert_eq!(state.current_term(), leader_term);

    let response = resp_rx.recv().await.unwrap().unwrap();
    assert!(!response.is_success());
}

// ============================================================================
// Client Request Tests
// ============================================================================

/// Test: LearnerState rejects ClientWriteRequest
///
/// Scenario:
/// - Learner receives ClientWriteRequest
/// - Learners cannot process writes
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
///
/// Original: test_handle_inbound_event_case5
#[tokio::test]
async fn test_learner_rejects_client_write_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Propose(
        ClientWriteRequest {
            client_id: 1,
            command: Some(Bytes::from(
                WriteCommand::delete(Bytes::new()).encode_to_vec(),
            )),
        },
        resp_tx,
    );

    // Non-leader: push_client_cmd will immediately reject
    state.push_client_cmd(cmd, &context);

    let result = resp_rx.recv().await.expect("channel should not be closed");
    match result {
        Ok(ClientResponse { error, .. }) => assert_eq!(error, ErrorCode::NotLeader),
        Err(status) => panic!(
            "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
        ),
    }
}

/// Test: LearnerState rejects ClientReadRequest
///
/// Scenario:
/// - Learner receives ClientReadRequest
/// - Learners cannot serve reads directly
///
/// Expected:
/// - Returns response with error_code = NOT_LEADER
///
/// Original: test_handle_inbound_event_case6
#[tokio::test]
async fn test_learner_rejects_client_read_request() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let client_read_request = ClientReadRequest {
        client_id: 1,
        consistency_policy: None,
        keys: vec![],
    };

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let cmd = ClientCmd::Read(client_read_request, resp_tx);

    // Non-leader: push_client_cmd will immediately reject
    state.push_client_cmd(cmd, &context);

    let result = resp_rx.recv().await.expect("channel should not be closed");
    match result {
        Ok(ClientResponse { error, .. }) => assert_eq!(error, ErrorCode::NotLeader),
        Err(status) => panic!(
            "expected Ok(ClientResponse {{ error: NotLeader, .. }}), got bare Err(Status): {status:?}"
        ),
    }
}
///
/// Original: test_handle_inbound_event_case10
#[tokio::test]
async fn test_learner_rejects_join_cluster() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = d_engine_proto::server::cluster::JoinRequest {
        status: d_engine_proto::common::NodeStatus::Promotable as i32,
        node_id: 2,
        node_role: d_engine_proto::common::NodeRole::Learner.into(),
        address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::JoinCluster(request, resp_tx);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_err(),
        "handle_inbound_event should return error"
    );

    let response = resp_rx.recv().await.expect("should receive response");
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Test: LearnerState rejects LeaderDiscovery
///
/// Scenario:
/// - Learner receives LeaderDiscoveryRequest
/// - Learner cannot serve discovery requests
///
/// Expected:
/// - Returns Status error with Code::PermissionDenied
/// - handle_inbound_event returns Ok()
///
/// Original: test_handle_inbound_event_case11
#[tokio::test]
async fn test_learner_rejects_leader_discovery() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let request = d_engine_proto::server::cluster::LeaderDiscoveryRequest {
        node_id: 2,
        requester_address: "127.0.0.1:9090".to_string(),
    };
    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let inbound_event = InboundEvent::DiscoverLeader(request, resp_tx);
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    assert!(
        state
            .handle_inbound_event(inbound_event, &context, internal_event_tx)
            .await
            .is_ok(),
        "handle_inbound_event should succeed"
    );

    let response = resp_rx.recv().await.expect("should receive response");
    assert!(response.is_err());
    let status = response.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
}

// ============================================================================
// Discovery & Selection Tests
// ============================================================================

/// Test: LearnerState broadcast_discovery succeeds on first attempt
///
/// Scenario:
/// - Learner broadcasts discovery to find leader
/// - Transport returns valid LeaderDiscoveryResponse
///
/// Expected:
/// - Returns Ok with leader info
///
/// Original: test_broadcast_discovery_case1_success
#[tokio::test]
async fn test_broadcast_discovery_succeeds_first_attempt() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 5,
                leader_address: "127.0.0.1:5005".to_string(),
                term: 3,
            },
        ])
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let result = state.broadcast_discovery(context.membership.clone(), &context).await;

    assert!(result.is_ok(), "Should return leader info");
}

/// Test: LearnerState broadcast_discovery fails after retry exhaustion
///
/// Scenario:
/// - Learner broadcasts discovery
/// - Transport always returns empty responses
///
/// Expected:
/// - Returns NetworkError::RetryTimeoutError
///
/// Original: test_broadcast_discovery_case2_retry_exhaustion
#[tokio::test]
async fn test_broadcast_discovery_fails_after_retries() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| Ok(vec![]));
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let result = state.broadcast_discovery(context.membership.clone(), &context).await;

    assert!(result.is_err(), "Should error after retries");
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::System(crate::SystemError::Network(
            crate::NetworkError::RetryTimeoutError(_)
        ))
    ));
}

/// Test: LearnerState select_valid_leader chooses highest term
///
/// Scenario:
/// - Multiple valid responses with different terms
/// - Leader with highest term should be selected
///
/// Expected:
/// - Returns leader_id with highest term
///
/// Original: test_select_valid_leader_case1_priority
#[tokio::test]
async fn test_select_valid_leader_prioritizes_highest_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let responses = vec![
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 3,
            term: 5,
            leader_address: "127.0.0.1:5003".to_string(),
        },
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 5,
            term: 7,
            leader_address: "127.0.0.1:5005".to_string(),
        }, // Highest term
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 4,
            term: 7,
            leader_address: "127.0.0.1:5004".to_string(),
        }, // Same term
    ];

    let result = state.select_valid_leader(responses).await;

    assert!(result.is_some());
    assert_eq!(result.unwrap(), 5, "Should select highest term");
}

/// Test: LearnerState select_valid_leader filters invalid responses
///
/// Scenario:
/// - Responses with invalid leader_id (0) or invalid term (0)
///
/// Expected:
/// - Returns None (all responses filtered)
///
/// Original: test_select_valid_leader_case2_invalid_responses
#[tokio::test]
async fn test_select_valid_leader_filters_invalid_responses() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let responses = vec![
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 0,
            term: 5,
            leader_address: "127.0.0.1:5003".to_string(),
        }, // Invalid ID
        d_engine_proto::server::cluster::LeaderDiscoveryResponse {
            leader_id: 3,
            term: 0,
            leader_address: "127.0.0.1:5003".to_string(),
        }, // Invalid term
    ];

    let result = state.select_valid_leader(responses).await;

    assert!(result.is_none(), "Should filter invalid responses");
}

// ============================================================================
// Join Cluster Tests
// ============================================================================

/// Test: LearnerState join_cluster succeeds with known leader
///
/// Scenario:
/// - Learner has known leader in shared_state
/// - Transport join_cluster succeeds
///
/// Expected:
/// - Returns Ok
///
/// Original: test_join_cluster_case1_success_known_leader
#[tokio::test]
async fn test_join_cluster_succeeds_with_known_leader() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_ok(), "Join should succeed with known leader");
}

/// Test: LearnerState join_cluster succeeds after discovery
///
/// Scenario:
/// - No known leader initially
/// - Discovery succeeds
/// - Join succeeds
///
/// Expected:
/// - Returns Ok
///
/// Original: test_join_cluster_case2_success_after_discovery
#[tokio::test]
async fn test_join_cluster_succeeds_after_discovery() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 5,
                leader_address: "127.0.0.1:5005".to_string(),
                term: 3,
            },
        ])
    });
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 0,
            snapshot_metadata: None,
            leader_id: 2,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    let result = state.join_cluster(&context).await;

    assert!(result.is_ok(), "Join should succeed after discovery");
}

/// Test: LearnerState join_cluster fails on discovery timeout
///
/// Scenario:
/// - No known leader
/// - Discovery returns empty responses (timeout)
///
/// Expected:
/// - Returns RetryTimeoutError
///
/// Original: test_join_cluster_case3_discovery_timeout
#[tokio::test]
async fn test_join_cluster_fails_discovery_timeout() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| Ok(vec![]));
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    let result = state.join_cluster(&context).await;

    assert!(result.is_err(), "Should timeout during discovery");
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::System(crate::SystemError::Network(
            crate::NetworkError::RetryTimeoutError(_)
        ))
    ));
}

/// Test: LearnerState join_cluster fails on RPC failure
///
/// Scenario:
/// - Known leader exists
/// - Join RPC fails with ServiceUnavailable
///
/// Expected:
/// - Returns ServiceUnavailable error
///
/// Original: test_join_cluster_case4_join_rpc_failure
#[tokio::test]
async fn test_join_cluster_fails_on_rpc_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Err(crate::NetworkError::ServiceUnavailable("Service unavailable".to_string()).into())
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::System(crate::SystemError::Network(
            crate::NetworkError::ServiceUnavailable(_)
        ))
    ));
}

/// Test: LearnerState join_cluster fails on invalid response
///
/// Scenario:
/// - Known leader exists
/// - Join response has success=false
///
/// Expected:
/// - Returns JoinClusterFailed error
///
/// Original: test_join_cluster_case5_invalid_join_response
#[tokio::test]
async fn test_join_cluster_fails_invalid_response() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: false,
            error: "Node rejected".to_string(),
            config: None,
            config_version: 0,
            snapshot_metadata: None,
            leader_id: 0,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        crate::Error::Consensus(crate::ConsensusError::Membership(
            crate::MembershipError::JoinClusterFailed(_)
        ))
    ));
}

/// Test: LearnerState join_cluster handles redirect
///
/// Scenario:
/// - Known leader exists
/// - First join attempt fails (ServiceUnavailable)
/// - Retry mechanism should handle redirect
///
/// Expected:
/// - Returns error (single attempt fails)
///
/// Original: test_join_cluster_case6_leader_redirect
#[tokio::test]
async fn test_join_cluster_handles_redirect() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Err(crate::NetworkError::ServiceUnavailable("Not leader".to_string()).into())
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());
    state.shared_state.set_current_leader(5);

    let result = state.join_cluster(&context).await;

    assert!(result.is_err(), "Should handle redirect scenario");
}

/// Test: LearnerState join_cluster succeeds in large cluster
///
/// Scenario:
/// - Large cluster (simulated with 100 node_id)
/// - Discovery and join succeed
///
/// Expected:
/// - Returns Ok
///
/// Original: test_join_cluster_case7_large_cluster
#[tokio::test]
async fn test_join_cluster_succeeds_large_cluster() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let membership = MockMembership::new();
    context.membership = Arc::new(membership);

    let mut transport = crate::MockTransport::new();
    transport.expect_discover_leader().returning(|_, _, _| {
        Ok(vec![
            d_engine_proto::server::cluster::LeaderDiscoveryResponse {
                leader_id: 5,
                leader_address: "127.0.0.1:5005".to_string(),
                term: 3,
            },
        ])
    });
    transport.expect_join_cluster().returning(|_, _, _, _| {
        Ok(d_engine_proto::server::cluster::JoinResponse {
            success: true,
            error: "".to_string(),
            config: None,
            config_version: 1,
            snapshot_metadata: None,
            leader_id: 3,
        })
    });
    context.transport = Arc::new(transport);

    let state = LearnerState::<MockTypeConfig>::new(100, context.node_config.clone());

    let result = state.join_cluster(&context).await;

    assert!(result.is_ok(), "Should handle large cluster");
}

// ============================================================================
// Membership Applied Tests
// ============================================================================

/// Test: LearnerState detects promotion to Follower on MembershipApplied
///
/// Scenario:
/// - Learner receives MembershipApplied event
/// - Membership shows node has been promoted to Follower role
///
/// Expected:
/// - Sends BecomeFollower event
/// - handle_inbound_event returns Ok()
///
/// This validates learner promotion mechanism.
///
/// Original: test_learner_promotion_on_membership_applied
#[tokio::test]
async fn test_learner_promotion_on_membership_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut mock_membership = MockMembership::<MockTypeConfig>::new();

    let promoted_node = NodeMeta {
        id: 3,
        address: "127.0.0.1:8003".to_string(),
        role: NodeRole::Follower as i32,
        status: NodeStatus::Active as i32,
    };

    mock_membership
        .expect_retrieve_node_meta()
        .with(eq(3))
        .times(1)
        .return_once(move |_| Some(promoted_node));

    context.membership = Arc::new(mock_membership);

    let mut state = LearnerState::<MockTypeConfig>::new(3, context.node_config.clone());
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_membership_applied(&context, &internal_event_tx).await;
    assert!(result.is_ok(), "MembershipApplied should succeed");

    let internal_event = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await
    .expect("Should receive event within timeout")
    .expect("Channel should not be closed");

    match internal_event {
        crate::InternalEvent::BecomeFollower(leader_id) => {
            assert_eq!(leader_id, None, "Should not specify leader on promotion");
        }
        other => panic!("Expected BecomeFollower event, got: {other:?}"),
    }
}

/// Test: LearnerState remains Learner on MembershipApplied
///
/// Scenario:
/// - Learner receives MembershipApplied event
/// - Membership still shows node as Learner
///
/// Expected:
/// - No role transition event
/// - handle_inbound_event returns Ok()
///
/// Original: test_learner_stays_learner_on_membership_applied
#[tokio::test]
async fn test_learner_stays_learner_on_membership_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut mock_membership = MockMembership::<MockTypeConfig>::new();

    let learner_node = NodeMeta {
        id: 3,
        address: "127.0.0.1:8003".to_string(),
        role: NodeRole::Learner as i32,
        status: NodeStatus::Promotable as i32,
    };

    mock_membership
        .expect_retrieve_node_meta()
        .with(eq(3))
        .times(1)
        .return_once(move |_| Some(learner_node));

    context.membership = Arc::new(mock_membership);

    let mut state = LearnerState::<MockTypeConfig>::new(3, context.node_config.clone());
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_membership_applied(&context, &internal_event_tx).await;
    assert!(result.is_ok(), "MembershipApplied should succeed");

    let timeout_result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await;

    if let Ok(Some(event)) = timeout_result {
        panic!("Should not send role transition when still Learner, got: {event:?}");
    }
}

/// Test: LearnerState handles node not found in membership
///
/// Scenario:
/// - Learner receives MembershipApplied event
/// - Node not found in membership (edge case)
///
/// Expected:
/// - No role transition event
/// - handle_inbound_event returns Ok()
///
/// Original: test_learner_node_not_found_on_membership_applied
#[tokio::test]
async fn test_learner_node_not_found_on_membership_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut mock_membership = MockMembership::<MockTypeConfig>::new();

    mock_membership
        .expect_retrieve_node_meta()
        .with(eq(3))
        .times(1)
        .return_once(move |_| None);

    context.membership = Arc::new(mock_membership);

    let mut state = LearnerState::<MockTypeConfig>::new(3, context.node_config.clone());
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();

    let result = state.handle_membership_applied(&context, &internal_event_tx).await;
    assert!(
        result.is_ok(),
        "MembershipApplied should succeed even when node not found"
    );

    let timeout_result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        internal_event_rx.recv(),
    )
    .await;

    if let Ok(Some(event)) = timeout_result {
        panic!("Should not send role transition when node not found, got: {event:?}");
    }
}

// ============================================================================
// Snapshot Tests Module
// ============================================================================

mod snapshot_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    /// Test: Learner gracefully ignores stale leader-only internal events
    ///
    /// Protocol scenario: a leader steps down or a stale event arrives.
    /// LogPurgeCompleted, PromoteReadyLearners, StepDownSelfRemoved must all be
    /// silently ignored by a learner — they carry no meaning here and must not error.
    ///
    /// Expected: all return Ok(()) — no panic, no state change.
    #[tokio::test]
    async fn test_learner_ignores_stale_leader_internal_events() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

        state.pending_purge_upto = Some(LogId { term: 1, index: 1 });
        assert!(
            state.handle_log_purge_completed(LogId { term: 1, index: 1 }).is_ok(),
            "Stale LogPurgeCompleted should be silently ignored"
        );
        assert!(
            state.pending_purge_upto.is_none(),
            "handle_log_purge_completed must clear scheduled_purge_upto"
        );
        assert!(
            state.handle_promote_ready_learners(&context, &internal_event_tx).await.is_ok(),
            "Stale PromoteReadyLearners should be silently ignored"
        );
        assert!(
            state.handle_self_removed(&internal_event_tx).is_ok(),
            "Stale StepDownSelfRemoved should be silently ignored"
        );
    }

    /// Test: Learner ignores duplicate CreateSnapshot while one is already in progress
    ///
    /// The `snapshot_in_progress` flag guards against concurrent snapshot creation.
    ///
    /// Expected:
    /// - First call: Ok(), sets snapshot_in_progress = true, spawns background task
    /// - Second call: Ok(), skips (flag already set), flag remains true
    #[tokio::test]
    async fn test_learner_ignores_duplicate_create_snapshot_event() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

        // First trigger — starts background snapshot
        let result1 = state.handle_create_snapshot(&context, &internal_event_tx).await;
        assert!(result1.is_ok(), "First CreateSnapshotEvent should succeed");
        assert!(
            state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress should be true after first event"
        );

        // Second trigger while first is still running — must be a no-op
        let result2 = state.handle_create_snapshot(&context, &internal_event_tx).await;
        assert!(
            result2.is_ok(),
            "Second CreateSnapshotEvent should return Ok (ignored)"
        );
        assert!(
            state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress should remain true"
        );
    }

    /// Test: Learner resets snapshot_in_progress and updates last_purged_index on success
    ///
    /// Per Raft §7, learners independently purge logs after a successful snapshot.
    ///
    /// Scenario:
    /// - snapshot_in_progress pre-set to true (simulating in-flight snapshot)
    /// - SnapshotCreated arrives with successful result (last_included = index 50)
    ///
    /// Expected:
    /// - snapshot_in_progress reset to false
    /// - last_purged_index updated to Some(LogId { term: 1, index: 50 })
    #[tokio::test]
    async fn test_learner_resets_snapshot_flag_on_success() {
        let (_graceful_tx, graceful_rx) = watch::channel(());

        // retained_log_entries = 1, last_included.index = 50 → purge_upto_index = 49
        let mut raft_log = mock_raft_log();
        raft_log
            .expect_entry_term()
            .withf(|&idx| idx == 49)
            .times(1)
            .returning(|_| Some(1));
        let _temp_dir = tempfile::tempdir().unwrap();
        let mut nc = node_config(_temp_dir.path().to_str().unwrap());
        nc.raft.snapshot.retained_log_entries = 1;
        let context = MockBuilder::new(graceful_rx)
            .with_raft_log(raft_log)
            .with_node_config(nc)
            .build_context();

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
        state.snapshot_in_progress.store(true, Ordering::SeqCst);
        state.update_commit_index(100).unwrap();

        let last_included = LogId { term: 1, index: 50 };
        let metadata = d_engine_proto::server::storage::SnapshotMetadata {
            last_included: Some(last_included),
            checksum: bytes::Bytes::new(),
        };
        let snapshot_result = Ok((metadata, std::path::PathBuf::from("/tmp/test_snapshot.bin")));

        let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_snapshot_created(snapshot_result, &context, &internal_event_tx)
            .await;

        assert!(result.is_ok(), "SnapshotCreated should succeed");
        assert!(
            !state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress should be false after SnapshotCreated"
        );
        // last_purged_index is updated via LogPurgeCompleted event (processed by event loop).
        // Verify the event was dispatched with purge_upto = last_included - retained_log_entries.
        let event = internal_event_rx
            .try_recv()
            .expect("LogPurgeCompleted event must be dispatched after successful purge");
        assert!(
            matches!(
                event,
                InternalEvent::LogPurgeCompleted(LogId { index: 49, .. })
            ),
            "purge boundary must be last_included.index(50) - retained(1) = 49, got: {event:?}"
        );
    }

    /// Test: Learner resets snapshot_in_progress on failure but does NOT purge logs
    ///
    /// A failed snapshot must not advance the purge boundary.
    /// The flag must still clear so the next ApplyCompleted can retry.
    ///
    /// Expected:
    /// - snapshot_in_progress reset to false
    /// - last_purged_index remains None
    /// - handler returns Ok() (error logged, not propagated)
    #[tokio::test]
    async fn test_learner_resets_snapshot_flag_on_failure() {
        let (_graceful_tx, graceful_rx) = watch::channel(());
        let (context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

        let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
        state.snapshot_in_progress.store(true, Ordering::SeqCst);

        let snapshot_result = Err(crate::Error::Fatal("Snapshot creation failed".to_string()));

        let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
        let result = state
            .handle_snapshot_created(snapshot_result, &context, &internal_event_tx)
            .await;

        assert!(
            result.is_ok(),
            "SnapshotCreated with error should return Ok"
        );
        assert!(
            !state.snapshot_in_progress.load(Ordering::SeqCst),
            "snapshot_in_progress should be false after failed SnapshotCreated"
        );
        assert_eq!(
            state.last_purged_index, None,
            "last_purged_index must not advance when snapshot failed"
        );
    }
}

/// Test: Learner handles FatalError and returns error
///
/// Verifies that when Learner receives FatalError from any component,
/// it returns Error::Fatal and stops further processing.
///
/// # Test Scenario
/// Learner receives FatalError event from state machine while in learner role.
/// Learner should recognize the fatal error and return Error::Fatal.
///
/// # Given
/// - Learner in normal state (catching up on logs)
/// - FatalError event from StateMachine component
///
/// # When
/// - Learner handles FatalError event via handle_inbound_event()
///
/// # Then
/// - handle_inbound_event() returns Error::Fatal
/// - Error message contains source and error details
/// - No role transition events are sent (log replication is aborted)
#[tokio::test]
async fn test_learner_handles_fatal_error_returns_error() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let context = mock_raft_context(
        "/tmp/test_learner_handles_fatal_error_returns_error",
        graceful_rx,
        None,
    );

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Create FatalError event
    let fatal_error = InboundEvent::FatalError {
        source: "StateMachine".to_string(),
        error: "Disk failure".to_string(),
    };

    // Create internal event channel
    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // Handle the FatalError event
    let result = learner.handle_inbound_event(fatal_error, &context, internal_event_tx).await;

    // VERIFY 1: handle_inbound_event() returns Error::Fatal
    assert!(
        result.is_err(),
        "Expected handle_inbound_event to return Err, got: {result:?}"
    );

    // VERIFY 2: Error is Fatal and contains source information
    match result.unwrap_err() {
        Error::Fatal(msg) => {
            assert!(
                msg.contains("StateMachine"),
                "Error message should mention source, got: {msg}"
            );
        }
        other => panic!("Expected Error::Fatal, got: {other:?}"),
    }

    // VERIFY 3: No internal events sent
    assert!(
        internal_event_rx.try_recv().is_err(),
        "No role transition events should be sent during FatalError handling"
    );
}

// ============================================================================
// Role-Specific Behavior Tests
// ============================================================================

/// Learner - Eventual Read Support
///
/// **Objective**: Verify Learner can serve EventualConsistency reads locally
///
/// **Scenario**:
/// - Learner node receives EventualConsistency read request
/// - State machine returns valid data
///
/// **Expected**:
/// - Read processed immediately in push_client_cmd()
/// - No NOT_LEADER error
/// - Response latency < 10ms
/// - Data served from local state machine
#[tokio::test]
async fn test_learner_serves_eventual_read_locally() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Setup state machine mock for eventual read
    let mut state_machine_handler = MockStateMachineHandler::new();
    state_machine_handler
        .expect_read_from_state_machine()
        .times(1)
        .withf(|keys| keys.len() == 1 && keys[0] == "eventual_key")
        .returning(|_| {
            Some(vec![KvEntry {
                key: bytes::Bytes::from("eventual_key"),
                value: bytes::Bytes::from("eventual_value"),
            }])
        });
    context.handlers.state_machine_handler = Arc::new(state_machine_handler);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    // Send EventualConsistency read request
    let (response_tx, mut response_rx) = MaybeCloneOneshot::new();
    let read_req = ClientReadRequest {
        client_id: 1,
        keys: vec![bytes::Bytes::from("eventual_key")],
        consistency_policy: Some(ReadConsistencyPolicy::EventualConsistency),
    };

    let start = tokio::time::Instant::now();

    // Push read command (should process immediately)
    state.push_client_cmd(ClientCmd::Read(read_req, response_tx), &context);

    // Verify: Response ready immediately
    let result = response_rx.recv().await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Eventual read should return response");

    // Verify: Latency < 10ms
    assert!(
        elapsed.as_millis() < 10,
        "Eventual read latency should be <10ms, got {:?}ms",
        elapsed.as_millis()
    );

    // Verify: Response is success (not NOT_LEADER error)
    if let Ok(response) = result {
        match response {
            Ok(read_response) => {
                // Check result for ReadResults
                match read_response.result {
                    Some(ClientResponsePayload::Read(read_data)) => {
                        assert!(!read_data.entries.is_empty(), "Should have read results");
                        assert_eq!(
                            read_data.entries[0].value,
                            bytes::Bytes::from("eventual_value")
                        );
                    }
                    other => panic!("Expected Read variant, got: {other:?}"),
                }
            }
            Err(e) => {
                panic!("Eventual read should succeed on Learner, got error: {e:?}");
            }
        }
    }
}

/// Test: Learner ApplyCompleted triggers snapshot when condition is met
///
/// Purpose: Verify that learners independently create snapshots per Raft §7.
/// This ensures learner snapshot progress allows leader to advance its purge_safe_index
/// and prevent unbounded log growth.
///
/// Scenario:
/// - Learner receives ApplyCompleted event after state machine apply
/// - Snapshot is enabled in config
/// - State machine handler indicates snapshot should be taken
///
/// Expected:
/// - InternalEvent::CreateSnapshotEvent is sent directly on internal_event_tx (P2 unbounded)
/// - No ReprocessEvent wrapper — direct send eliminates the bounded event_tx deadlock path
#[tokio::test]
async fn test_apply_completed_triggers_snapshot_when_condition_met() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler that returns true for should_snapshot
    let mut mock_sm_handler = MockStateMachineHandler::new();
    mock_sm_handler
        .expect_should_snapshot()
        .with(eq(NewCommitData {
            new_commit_index: 100,
            role: NodeRole::Learner as i32,
            current_term: 1,
        }))
        .times(1)
        .returning(|_| true);

    // Build context with mock state machine handler before context creation
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .build_context();

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = learner.handle_apply_completed(100, vec![], &context, &internal_event_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully, got: {result:?}"
    );

    // VERIFY 2: CreateSnapshotEvent is sent directly on internal_event_tx (P2 unbounded)
    let event = internal_event_rx.try_recv().expect("Should receive snapshot trigger event");
    assert!(
        matches!(event, InternalEvent::CreateSnapshotEvent),
        "Expected InternalEvent::CreateSnapshotEvent, got: {event:?}"
    );

    // VERIFY 3: No additional events queued
    assert!(
        internal_event_rx.try_recv().is_err(),
        "Should only send one snapshot event"
    );
}

/// Test: Learner ApplyCompleted does NOT trigger snapshot when condition is not met
///
/// Purpose: Verify that learners respect snapshot conditions and don't create unnecessary snapshots.
///
/// Scenario:
/// - Learner receives ApplyCompleted event
/// - Snapshot is enabled in config
/// - State machine handler indicates snapshot should NOT be taken (returns false)
///
/// Expected:
/// - No CreateSnapshotEvent is sent
/// - ApplyCompleted is processed normally without side effects
#[tokio::test]
async fn test_apply_completed_does_not_trigger_snapshot_when_condition_not_met() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler that returns false for should_snapshot
    let mut mock_sm_handler = MockStateMachineHandler::new();
    mock_sm_handler
        .expect_should_snapshot()
        .with(eq(NewCommitData {
            new_commit_index: 50,
            role: NodeRole::Learner as i32,
            current_term: 1,
        }))
        .times(1)
        .returning(|_| false);

    // Build context with mock state machine handler before context creation
    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .build_context();

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = learner.handle_apply_completed(50, vec![], &context, &internal_event_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent
    assert!(
        internal_event_rx.try_recv().is_err(),
        "Should not send snapshot event when condition is not met"
    );
}

/// Test: Learner ApplyCompleted respects snapshot config disabled state
///
/// Purpose: Verify that snapshots are not triggered when snapshot feature is disabled.
///
/// Scenario:
/// - Learner receives ApplyCompleted event
/// - Snapshot is DISABLED in config (enable = false)
/// - State machine handler would indicate snapshot (returns true)
///
/// Expected:
/// - No CreateSnapshotEvent is sent (config takes precedence)
/// - ApplyCompleted is processed without attempting snapshot
#[tokio::test]
async fn test_apply_completed_respects_snapshot_disabled_config() {
    let (_graceful_tx, graceful_rx) = watch::channel(());

    // Create a mock state machine handler
    let mock_sm_handler = MockStateMachineHandler::new();

    // Build context with snapshot disabled and mock handler
    let mut node_config = node_config("/tmp/test_learner_snapshot_disabled");
    node_config.raft.snapshot.enable = false;

    let context = MockBuilder::new(graceful_rx)
        .with_state_machine_handler(mock_sm_handler)
        .with_node_config(node_config)
        .build_context();

    let mut learner = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (internal_event_tx, mut internal_event_rx) = mpsc::unbounded_channel::<InternalEvent>();

    // ACTION: Handle ApplyCompleted event
    let result = learner.handle_apply_completed(100, vec![], &context, &internal_event_tx).await;

    // VERIFY 1: Event handling succeeds
    assert!(
        result.is_ok(),
        "ApplyCompleted should be handled successfully"
    );

    // VERIFY 2: No snapshot event is sent (snapshot disabled in config)
    assert!(
        internal_event_rx.try_recv().is_err(),
        "Should not send snapshot event when snapshot is disabled in config"
    );
}

/// Spawns a fake Worker that answers exactly one `InstallSnapshot` command with `result`,
/// then exits. Returns a `StateMachineCommandSender` wired to it — stands in for the real
/// `StateMachineWorker`, which isn't running in these role-layer unit tests.
fn fake_command_sink_install_snapshot(
    result: crate::Result<SnapshotApplyResult>
) -> StateMachineCommandSender {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        if let Some(StateMachineCommand::InstallSnapshot { response, .. }) = rx.recv().await {
            let _ = response.send(result);
        }
    });
    StateMachineCommandSender::new(tx)
}

// ============================================================================
// InstallSnapshotChunk Tests
// ============================================================================

/// #436-adjacent: same protection as `FollowerState` (see
/// `test_follower_rejects_install_snapshot_from_stale_leader_term`) — a learner
/// must also reject InstallSnapshotChunk from a stale-term leader before it ever
/// reaches `prepare_snapshot_stream`.
#[tokio::test]
async fn test_learner_rejects_install_snapshot_from_stale_leader_term() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().never();
    context.handlers.state_machine_handler = Arc::new(sm_handler);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    let higher_term = 5;
    state.update_current_term(higher_term);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: higher_term - 1, // stale
        leader_id: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);

    state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await
        .unwrap();

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s")
        .expect("recv must not fail")
        .expect("response must be Ok");

    assert!(
        !response.success,
        "must reject a snapshot pushed by a stale-term leader"
    );
    assert_eq!(
        response.term, higher_term,
        "rejection response must carry the learner's own (higher) term"
    );
    assert_eq!(
        state.current_term(),
        higher_term,
        "rejecting a stale leader must not change the learner's own term"
    );
}

/// Learner reports success only after all chunks are applied.
///
/// The ACK handler must drain the full ACK channel (not stop at the first ACK)
/// and wait for `prepare_snapshot_stream` to complete before replying.
/// This test uses 3 ACKs to confirm the last one is used for the final response.
#[tokio::test]
async fn test_learner_install_snapshot_reports_success_after_all_chunks_applied() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Mock: prepare_snapshot_stream sends 3 ACKs (simulating 3 chunks), all Accepted,
    // then returns a prepared snapshot ready for the Worker.
    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, ack_tx, _config| {
            drop(tokio::spawn(async move {
                for i in 0u32..3 {
                    let _ = ack_tx
                        .send(SnapshotAck {
                            seq: i,
                            status: ChunkStatus::Accepted as i32,
                            next_requested: i + 1,
                        })
                        .await;
                }
            }));
            Ok(crate::PreparedSnapshot {
                metadata: Default::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            })
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);
    context.handlers.state_machine_commands =
        fake_command_sink_install_snapshot(Ok(SnapshotApplyResult::Applied {
            last_included: LogId { index: 1, term: 2 },
        }));

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await
        .unwrap();

    // Response must arrive (apply is done before reply)
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s") // timeout
        .expect("recv must not fail") // RecvError
        .expect("response must be Ok"); // Status error

    assert!(
        response.success,
        "Learner must report success after all chunks applied"
    );
}

/// C0 baseline (#436): same purge coverage gap as the follower side —
/// none of the existing learner install-snapshot tests exercise the purge branch,
/// since they all mock get_latest_snapshot_metadata() to return None.
#[tokio::test]
async fn test_learner_install_snapshot_purges_to_snapshot_boundary_on_success() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);
    let expected_boundary = LogId { index: 42, term: 3 };

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, ack_tx, _config| {
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            Ok(crate::PreparedSnapshot {
                metadata: Default::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            })
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);
    context.handlers.state_machine_commands =
        fake_command_sink_install_snapshot(Ok(SnapshotApplyResult::Applied {
            last_included: expected_boundary,
        }));

    // Boundary must come from the Worker's own Applied result, not from re-querying
    // get_latest_snapshot_metadata() after the fact (the race #436 fixes).
    let mut purge_executor = MockPurgeExecutor::new();
    purge_executor
        .expect_execute_purge()
        .withf(move |boundary| *boundary == expected_boundary)
        .times(1)
        .returning(|_| Ok(()));
    context.handlers.purge_executor = Arc::new(purge_executor);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await
        .unwrap();

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s");
    // mock drop verifies execute_purge was called exactly once with expected_boundary
}

/// Regression test (PR #442 review): same commit_index gap as the follower side
/// (see `test_follower_install_snapshot_advances_commit_index_to_boundary`). On a
/// successful InstallSnapshotChunk, the Worker advances `last_applied` to
/// `last_included.index` and this handler schedules a log purge up to that same
/// boundary — but neither step advances `commit_index`. A learner that receives a
/// pushed snapshot before its `commit_index` has caught up via AppendEntries is left
/// with `last_applied` / `last_purged_index` > `commit_index`, breaking the invariant
/// that nothing is ever applied or purged ahead of what `commit_index` confirms as
/// committed.
///
/// # Given
/// - Learner's commit_index starts at 10 (stale, below the snapshot boundary)
/// - Worker reports SnapshotApplyResult::Applied with last_included.index = 42
///
/// # When
/// - Leader pushes a snapshot (InstallSnapshotChunk event)
///
/// # Then
/// - commit_index must advance to at least 42 (the snapshot boundary), not stay at 10
#[tokio::test]
async fn test_learner_install_snapshot_advances_commit_index_to_boundary() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);
    let snapshot_boundary = LogId { index: 42, term: 3 };

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, ack_tx, _config| {
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            Ok(crate::PreparedSnapshot {
                metadata: Default::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            })
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);
    context.handlers.state_machine_commands =
        fake_command_sink_install_snapshot(Ok(SnapshotApplyResult::Applied {
            last_included: snapshot_boundary,
        }));

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);
    state.update_commit_index(10).unwrap();

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await
        .unwrap();

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s");

    assert_eq!(
        state.commit_index(),
        snapshot_boundary.index,
        "commit_index must advance to the installed snapshot boundary — otherwise \
         last_applied/last_purged_index run ahead of commit_index"
    );
}

/// Learner must NOT report success when a mid-stream chunk fails.
///
/// # Note on this rewrite
/// Pre-#436, `apply_snapshot_stream_from_leader` combined receiving the stream (per-chunk
/// ACKs) and applying it into a single method, and the bug this pinned down was about
/// draining *all* ACKs rather than replying on the first one. That draining is now entirely
/// internal to `prepare_snapshot_stream` (the receive/decompress phase) — the role layer
/// only sees its final `Result`, not individual chunk ACKs. A mid-stream chunk failure now
/// surfaces as `prepare_snapshot_stream` returning `Err`, before the Worker is ever involved.
///
/// # Then
/// - Response MUST be success: false
/// - The Worker (`state_machine_commands`) must NOT be invoked — no fake sink is wired up,
///   so a call there would panic on the closed channel / never resolve.
#[tokio::test]
async fn test_learner_install_snapshot_does_not_report_success_on_mid_chunk_failure() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, ack_tx, _config| {
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 1,
                status: ChunkStatus::Failed as i32,
                next_requested: 1,
            });
            Err(crate::Error::Fatal("simulated mid-chunk failure".into()))
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    // handle_inbound_event returns Err (prepare failed) — that is expected
    let _ = state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    // The ACK handler may or may not have sent a response (depends on whether
    // the spawned task ran). If it did respond, it must NOT be success:true.
    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv()).await;
    if let Ok(Ok(Ok(r))) = response {
        assert!(
            !r.success,
            "Learner must NOT report success when apply failed mid-stream (got success:true — pre-fix bug)"
        );
    }
    // If no response arrived or it was an error — acceptable here.
    // The primary assertion: success:true must never be sent on failure.
}

/// Learner must NOT report success when apply fails after transfer succeeds (#308).
///
/// # Raft §7 + #308
/// The existing test above covers chunk-level failures (Failed ACK sent before Err).
/// This test covers the distinct #308 scenario: ALL chunks are accepted (transfer ok),
/// but apply_snapshot_from_file then fails. The last per-chunk ACK is still Accepted,
/// so the buggy ACK-handler sends success:true — causing the leader to advance
/// match_index and stop retrying, leaving the learner permanently behind.
///
/// # Given
/// - prepare_snapshot_stream: sends Accepted ACKs (transfer succeeded),
///   then returns Err (apply_snapshot_from_file failed)
///
/// # When
/// - Leader pushes a snapshot (InstallSnapshotChunk event)
///
/// # Then
/// - Response MUST be success: false
#[tokio::test]
async fn test_learner_install_snapshot_reports_failure_when_apply_fails_after_transfer() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, ack_tx, _config| {
            // Transfer phase succeeds: all chunks accepted
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            Ok(crate::PreparedSnapshot {
                metadata: Default::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            })
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);
    // Apply phase fails inside the Worker (apply_snapshot_from_file returned Err)
    context.handlers.state_machine_commands = fake_command_sink_install_snapshot(Err(
        crate::Error::Fatal("apply_snapshot_from_file failed".into()),
    ));

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    // Learner propagates apply errors — ignore the return value here
    let _ = state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s")
        .expect("recv must not fail")
        .expect("response must be Ok(SnapshotResponse)");

    assert!(
        !response.success,
        "Learner must NOT report success when apply failed after transfer (got success:true — #308 bug)"
    );
}

/// Regression test (PR #442 review, point A): learner_state.rs's InstallSnapshotChunk arm
/// was copied from follower_state.rs but dropped a safeguard — when the snapshot stream
/// closes before any chunk arrives (a transient transport hiccup), the follower replies
/// with an explicit `SnapshotResponse{success: false}` before returning `Ok(())`; the
/// learner instead drops `sender` unused and returns `Err(SnapshotError::TransferFailed)`.
/// The leader then has to infer failure from a dropped RPC channel instead of the typed
/// response every other failure path in this handler sends, and — since this Err is
/// currently non-fatal — the node also logs it one level too alarmingly (`error!` instead
/// of `warn!`) for something that's meant to be routine and recoverable.
///
/// # Given
/// - Leader opens an InstallSnapshotChunk stream but it closes before any chunk arrives
///
/// # Then
/// - handle_inbound_event must return Ok(()) (this is a transient hiccup, not fatal)
/// - the leader must receive an explicit SnapshotResponse{success: false}
#[tokio::test]
async fn test_learner_install_snapshot_replies_before_returning_when_stream_closes_before_first_chunk()
 {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    // Must never be reached — the handler must bail out before touching the Worker.
    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().never();
    context.handlers.state_machine_handler = Arc::new(sm_handler);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();
    let (tx, rx) = mpsc::channel::<SnapshotChunk>(32);
    drop(tx); // stream closes before any chunk arrives

    let result = state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    assert!(
        result.is_ok(),
        "an empty/closed snapshot stream is a transient transport hiccup, not fatal — \
         must not propagate an error"
    );

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s")
        .expect("recv must not fail")
        .expect("response must be Ok(SnapshotResponse)");
    assert!(
        !response.success,
        "must report failure via the typed SnapshotResponse so the leader retries \
         through the normal path, instead of inferring it from a dropped RPC channel"
    );
}

/// Regression test (PR #442 review, point B): learner_state.rs's InstallSnapshotChunk arm
/// unconditionally returns `Err(e)` on any Worker install failure, unlike follower which
/// checks `e.is_fatal()` first and only propagates genuinely fatal errors (absorbing
/// everything else with a `warn!` and continuing). This test uses a non-fatal error
/// (`SnapshotError::OperationFailed`, not one of the `is_fatal()`-listed variants) — the
/// existing `..._reports_failure_when_apply_fails_after_transfer` test above only exercises
/// `Error::Fatal`, which propagates under either implementation and so never caught this gap.
///
/// # Then
/// - handle_inbound_event must return Ok(()) — a non-fatal apply error must be absorbed
#[tokio::test]
async fn test_learner_install_snapshot_non_fatal_apply_error_does_not_propagate() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, ack_tx, _config| {
            let _ = ack_tx.try_send(SnapshotAck {
                seq: 0,
                status: ChunkStatus::Accepted as i32,
                next_requested: 1,
            });
            Ok(crate::PreparedSnapshot {
                metadata: Default::default(),
                temp_dir: tempfile::tempdir().unwrap(),
            })
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);
    context.handlers.state_machine_commands = fake_command_sink_install_snapshot(Err(
        crate::SnapshotError::OperationFailed("apply_snapshot_from_file failed".into()).into(),
    ));

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);

    let result = state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    assert!(
        result.is_ok(),
        "a non-fatal apply error must be absorbed (warn + continue), not propagated"
    );

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv()).await;
}

/// Regression test (PR #442 review, point C — found during review, not in the original
/// CodeRabbit list): learner_state.rs's `prepare_snapshot_stream` error branch already
/// sends the `SnapshotResponse{success: false}` reply correctly, but still unconditionally
/// `return Err(e)`. Follower's equivalent branch always returns `Ok(())` instead (every
/// `prepare_snapshot_stream` failure is treated as a recoverable transport-phase issue,
/// not gated on `is_fatal()` at all).
///
/// # Then
/// - handle_inbound_event must return Ok(()) — a non-fatal transfer-phase failure must not
///   propagate an error
#[tokio::test]
async fn test_learner_install_snapshot_stream_prepare_failure_does_not_propagate() {
    let (_graceful_tx, graceful_rx) = watch::channel(());
    let (mut context, _temp_dir) = mock_raft_context_with_temp(graceful_rx, None);

    let mut sm_handler = MockStateMachineHandler::new();
    sm_handler.expect_prepare_snapshot_stream().once().returning(
        |_first_chunk, _remaining, _ack_tx, _config| {
            Err(crate::SnapshotError::OperationFailed("decompress failed".into()).into())
        },
    );
    context.handlers.state_machine_handler = Arc::new(sm_handler);

    let mut state = LearnerState::<MockTypeConfig>::new(1, context.node_config.clone());
    state.update_current_term(2);

    let (resp_tx, mut resp_rx) = MaybeCloneOneshot::new();
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    let (tx, rx) = mpsc::channel(32);
    tx.send(SnapshotChunk {
        leader_term: 2,
        leader_id: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);

    let result = state
        .handle_inbound_event(
            InboundEvent::InstallSnapshotChunk(rx, resp_tx),
            &context,
            internal_event_tx,
        )
        .await;

    assert!(
        result.is_ok(),
        "a non-fatal prepare_snapshot_stream failure must not propagate an error"
    );

    let response = tokio::time::timeout(std::time::Duration::from_secs(2), resp_rx.recv())
        .await
        .expect("response must arrive within 2s")
        .expect("recv must not fail")
        .expect("response must be Ok(SnapshotResponse)");
    assert!(
        !response.success,
        "must report failure via the typed SnapshotResponse"
    );
}

// ============================================================================
// Role Transition Tests — scheduled_purge_upto / last_purged_index
// ============================================================================

/// From<&FollowerState>: both fields preserved across follower-to-learner demotion.
///
/// A follower with a pending purge intent must hand it off so the learner can resume
/// without recomputing the purge boundary.
#[test]
fn test_learner_from_follower_preserves_both_purge_fields() {
    let cfg = Arc::new(node_config("/tmp/test_learner_from_follower_purge"));
    let mut follower = FollowerState::<MockTypeConfig>::new(1, cfg, None, None);
    follower.last_purged_index = Some(LogId { term: 1, index: 12 });
    follower.pending_purge_upto = Some(LogId { term: 1, index: 10 });

    let learner = LearnerState::from(&follower);

    assert_eq!(
        learner.last_purged_index,
        Some(LogId { term: 1, index: 12 })
    );
    assert_eq!(
        learner.pending_purge_upto,
        Some(LogId { term: 1, index: 10 })
    );
}

/// From<&CandidateState>: last_purged_index preserved, scheduled_purge_upto reset.
///
/// Candidate has no scheduled_purge_upto field; the resulting learner always starts
/// with None so it does not replay a stale purge boundary from a prior election.
#[test]
fn test_learner_from_candidate_preserves_last_purged_index_resets_scheduled() {
    let cfg = Arc::new(node_config("/tmp/test_learner_from_candidate_purge"));
    let mut candidate = CandidateState::<MockTypeConfig>::new(1, cfg);
    candidate.last_purged_index = Some(LogId { term: 3, index: 15 });

    let learner = LearnerState::from(&candidate);

    assert_eq!(
        learner.last_purged_index,
        Some(LogId { term: 3, index: 15 })
    );
    assert_eq!(learner.pending_purge_upto, None);
}
