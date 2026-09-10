//! Raft gRPC service implementation handling RPC communication between cluster nodes
//! and client requests. Implements core Raft protocol logic for leader election,
//! log replication, and cluster configuration management.

use crate::Node;
use crate::proto_convert;
use d_engine_core::InboundEvent;
use d_engine_core::MaybeCloneOneshot;
use d_engine_core::RaftOneshot;
use d_engine_core::TypeConfig;
#[cfg(feature = "watch")]
use d_engine_core::WatchError;
use d_engine_core::client::ClientApiError;
use d_engine_core::config::ReadConsistencyPolicy;
use d_engine_proto::client::ClientReadRequest;
use d_engine_proto::client::ClientResponse;
use d_engine_proto::client::ClientWriteRequest;
use d_engine_proto::client::MembershipSnapshot as ProtoMembershipSnapshot;
use d_engine_proto::client::ScanRequest;
use d_engine_proto::client::WatchMembershipRequest;
use d_engine_proto::client::WatchRequest;
use d_engine_proto::client::raft_client_service_server::RaftClientService;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::cluster::cluster_management_service_server::ClusterManagementService;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::raft_election_service_server::RaftElectionService;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::raft_replication_service_server::RaftReplicationService;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotResponse;
use d_engine_proto::server::storage::snapshot_service_server::SnapshotService;
use futures::Stream;
use futures::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

#[tonic::async_trait]
impl<T> RaftElectionService for Node<T>
where
    T: TypeConfig,
{
    /// Handles RequestVote RPC calls from candidate nodes during leader elections
    /// # Raft Protocol Logic
    /// - Part of leader election mechanism (Section 5.2)
    /// - Validates candidate's term and log completeness
    /// - Grants vote if candidate's log is at least as up-to-date as local log
    async fn request_vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> std::result::Result<Response<VoteResponse>, Status> {
        if !self.is_rpc_ready() {
            // Routine bootstrap-race noise, not a fault: a peer's RPC arriving
            // before this node's own raft component finishes init resolves
            // itself within the next election round (#428).
            debug!(
                "[rpc|request_vote] My raft setup(Node:{}) is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(InboundEvent::ReceiveVoteRequest(
                request.into_inner(),
                resp_tx,
            ))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;
        let timeout_duration =
            Duration::from_millis(self.node_config.raft.election.election_timeout_min);
        handle_rpc_timeout(resp_rx, timeout_duration, "request_vote").await
    }
}
#[tonic::async_trait]
impl<T> RaftReplicationService for Node<T>
where
    T: TypeConfig,
{
    /// Processes AppendEntries RPC calls from cluster leader
    /// # Raft Protocol Logic
    /// - Heartbeat mechanism (Section 5.2)
    /// - Log replication entry point (Section 5.3)
    /// - Term comparison logic:
    ///   - If incoming term > current term: revert to follower state
    ///   - Reset election timeout on valid leader communication
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> std::result::Result<Response<AppendEntriesResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("[rpc|append_entries] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(InboundEvent::AppendEntries(
                request.into_inner(),
                vec![resp_tx],
            ))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.retry.append_entries.timeout_ms);

        handle_rpc_timeout(resp_rx, timeout_duration, "append_entries").await
    }

    type StreamAppendEntriesStream =
        Pin<Box<dyn Stream<Item = Result<AppendEntriesResponse, Status>> + Send>>;

    /// Processes a persistent bidirectional AppendEntries stream from the cluster leader.
    /// #446: responses are forwarded as soon as each one is ready, not in strict arrival
    /// order — leader-side match_index/next_index updates are already designed to
    /// tolerate out-of-order pipeline responses (`leader_state.rs`, "only advance,
    /// never retreat"), so nothing downstream needs strict ordering. Strict FIFO would
    /// let one response still waiting on this node's own durable_index (RPO=0) block
    /// every later, already-ready response on the same connection — including
    /// unrelated ones like heartbeats.
    ///
    /// Single task, bounded concurrency: reads a new request only while fewer than
    ///  `max_pending_append_responses` requests are still in flight, so a stalled fsync
    /// bounds memory/task growth instead of growing without limit.
    ///
    /// Known tradeoff, not an oversight: this is a single task, so a slow/stuck
    /// network write (`out_tx.send().await` blocking because the peer isn't reading)
    /// also delays reading new requests AND processing the shutdown signal, until the
    /// write unblocks or the connection dies. Accepted deliberately — if the peer
    /// isn't reading responses, there's no useful work to do by reading more requests
    /// either; this is legitimate backpressure, not a bug.
    async fn stream_append_entries(
        &self,
        request: tonic::Request<tonic::Streaming<AppendEntriesRequest>>,
    ) -> std::result::Result<tonic::Response<Self::StreamAppendEntriesStream>, tonic::Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!(
                "[rpc|stream_append_entries] Node-{} is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let mut in_stream = request.into_inner();
        let event_tx = self.event_tx.clone();
        let max_pending = self.node_config.raft.max_pending_append_responses;
        let mut shutdown = self.shutdown_signal.clone();
        let node_id = self.node_id;

        // Output: ACKs sent back to the leader over the bidi stream, in completion order.
        // Capacity matches max_pending — completed responses can never outnumber
        // in-flight requests, so there's no separate number to reason about here.
        let (out_tx, out_rx) = mpsc::channel::<Result<AppendEntriesResponse, Status>>(max_pending);

        // Single task: read requests, dispatch to the Raft loop, and forward whichever
        // response becomes ready first — bounded by `max_pending` in-flight responses.
        tokio::spawn(async move {
            use futures::StreamExt;
            use futures::stream::FuturesUnordered;

            let mut pending = FuturesUnordered::new();
            let mut inbound_open = true;

            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        break;
                    }
                    result = in_stream.next(), if inbound_open && pending.len() < max_pending => {
                        match result {
                            Some(Ok(req)) => {
                                let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
                                if event_tx.send(InboundEvent::AppendEntries(req, vec![resp_tx])).await.is_err() {
                                    debug!("[stream_append_entries|recv] event_tx closed");
                                    break;
                                }
                                pending.push(async move {
                                    match resp_rx.await {
                                        Ok(Ok(resp)) => Ok(resp),
                                        Ok(Err(status)) => Err(status),
                                        Err(_) => Err(Status::internal("Response channel closed")),
                                    }
                                });
                            }
                            Some(Err(e)) => {
                                // Debug: expected when the peer goes away (crash/restart/shutdown), self-heals.
                                debug!("[stream_append_entries|recv] stream error: {:?}", e);
                                 inbound_open = false;
                            }
                            None => inbound_open = false,
                        }
                    }
                    Some(result) = pending.next(), if !pending.is_empty() => {
                        // Observability only — behavior doesn't change, the send still
                        // runs to completion normally. If the peer isn't reading (network
                        // stall, dead connection with the TCP timeout not yet fired), this
                        // surfaces it instead of silently blocking with zero signal.
                        let mut send_fut = std::pin::pin!(out_tx.send(result));
                        let mut stuck_logged = false;
                        let closed = loop {
                            tokio::select! {
                                res = &mut send_fut => break res.is_err(),
                                _ = tokio::time::sleep(Duration::from_secs(5)), if !stuck_logged => {
                                    stuck_logged = true;
                                    error!(
                                        node_id,
                                        "stream_append_entries forwarder stuck sending a \
                                         response for >5s — peer may not be reading \
                                         (network stall or dead connection)"
                                    );
                                    metrics::counter!(
                                        "server.grpc.stream_append_entries.forwarder_stuck"
                                    )
                                    .increment(1);
                                }
                            }
                        };
                        if closed {
                            break;
                        }
                    }
                }
            }
        });

        let out_stream = tokio_stream::wrappers::ReceiverStream::new(out_rx);
        Ok(tonic::Response::new(Box::pin(out_stream)))
    }
}

#[tonic::async_trait]
impl<T> SnapshotService for Node<T>
where
    T: TypeConfig,
{
    async fn install_snapshot(
        &self,
        request: tonic::Request<Streaming<SnapshotChunk>>,
    ) -> std::result::Result<tonic::Response<SnapshotResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("install_snapshot: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();

        let mut tonic_stream = request.into_inner();
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Some(chunk) = tonic_stream.next().await {
                match chunk {
                    Ok(c) => {
                        if tx.send(c).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        self.event_tx
            .send(InboundEvent::InstallSnapshotChunk(rx, resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.node_config.raft.snapshot_rpc_timeout_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "install_snapshot").await
    }
}

#[tonic::async_trait]
impl<T> ClusterManagementService for Node<T>
where
    T: TypeConfig,
{
    /// Handles cluster membership changes (joint consensus)
    /// # Raft Protocol Logic
    /// - Implements cluster configuration changes (Section 6)
    /// - Validates new configuration against current cluster state
    /// - Ensures safety during membership transitions
    async fn update_cluster_conf(
        &self,
        request: tonic::Request<ClusterConfChangeRequest>,
    ) -> std::result::Result<Response<ClusterConfUpdateResponse>, Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!(
                "[rpc|update_cluster_conf_from_leader] Node-{} is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(InboundEvent::ClusterConfUpdate(
                request.into_inner(),
                resp_tx,
            ))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration = Duration::from_millis(self.node_config.retry.membership.timeout_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "update_cluster_conf_from_leader").await
    }

    /// Returns current cluster membership and state metadata
    /// # Usage
    /// - Administrative API for cluster inspection
    /// - Provides snapshot of current configuration
    async fn get_cluster_metadata(
        &self,
        request: tonic::Request<MetadataRequest>,
    ) -> std::result::Result<tonic::Response<ClusterMembership>, tonic::Status> {
        debug!("receive get_cluster_metadata");
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!(
                "[rpc|get_cluster_metadata] Node-{} is not ready!",
                self.node_id
            );
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(InboundEvent::ClusterConf(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "get_cluster_metadata").await
    }

    // Request to join the cluster as a new learner node
    async fn join_cluster(
        &self,
        request: tonic::Request<JoinRequest>,
    ) -> std::result::Result<tonic::Response<JoinResponse>, tonic::Status> {
        debug!("receive join_cluster");
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("[rpc|join_cluster] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(InboundEvent::JoinCluster(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "join_cluster").await
    }

    async fn discover_leader(
        &self,
        request: tonic::Request<LeaderDiscoveryRequest>,
    ) -> std::result::Result<tonic::Response<LeaderDiscoveryResponse>, tonic::Status> {
        debug!("receive discover_leader");
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("[rpc|discover_leader] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        self.event_tx
            .send(InboundEvent::DiscoverLeader(request.into_inner(), resp_tx))
            .await
            .map_err(|_| Status::internal("Event channel closed"))?;

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        handle_rpc_timeout(resp_rx, timeout_duration, "discover_leader").await
    }
}
#[tonic::async_trait]
impl<T> RaftClientService for Node<T>
where
    T: TypeConfig,
{
    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<d_engine_proto::client::WatchResponse, Status>> + Send>>;

    type WatchMembershipStream =
        Pin<Box<dyn Stream<Item = Result<ProtoMembershipSnapshot, Status>> + Send>>;

    /// Processes client write requests requiring consensus
    /// # Raft Protocol Logic
    /// - Entry point for client proposals (Section 7)
    /// - Validates requests before appending to leader's log
    /// - Ensures linearizable writes through log replication
    async fn handle_client_write(
        &self,
        request: tonic::Request<ClientWriteRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("[handle_client_write] Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let remote_addr = request.remote_addr();
        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);

        // Clone cmd_tx before async move to avoid capturing &self
        let cmd_tx = self.cmd_tx.clone();

        let request_future = async move {
            let proto_req: ClientWriteRequest = request.into_inner();
            let operation_present =
                proto_req.command.as_ref().and_then(|c| c.operation.as_ref()).is_some();
            if !operation_present {
                return Err(Status::invalid_argument(
                    "WriteCommand must contain an operation",
                ));
            }
            let core_req = proto_convert::to_core_write_req(proto_req);

            let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
            let t0 = std::time::Instant::now();
            cmd_tx
                .send(d_engine_core::ClientCmd::Propose(core_req, resp_tx))
                .await
                .map_err(|_| Status::internal("Command channel closed"))?;

            metrics::histogram!("server.rpc.cmd_channel_send_wait_ms", "op" => "propose")
                .record(t0.elapsed().as_secs_f64() * 1000.0);

            handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_write")
                .await
                .map(|resp| resp.map(proto_convert::to_proto_response))
        };

        let cancellation_future = async move {
            warn!("Request from {:?} cancelled by client", remote_addr);
            // If this future is executed it means the request future was dropped,
            // so it doesn't actually matter what is returned here
            Err::<Response<ClientResponse>, Status>(Status::cancelled(
                "Request cancelled by client",
            ))
        };

        with_cancellation_handler(request_future, cancellation_future).await
    }

    /// Handles client read requests with linearizability guarantees
    /// # Raft Protocol Logic
    /// - Implements lease-based leader reads (Section 6.4)
    /// - Verifies leadership before serving reads
    /// - Ensures read-after-write consistency
    async fn handle_client_read(
        &self,
        request: tonic::Request<ClientReadRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("handle_client_read: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let proto_req = request.into_inner();
        if let Some(raw) = proto_req.consistency_policy
            && d_engine_proto::client::ReadConsistencyPolicy::try_from(raw).is_err()
        {
            warn!(
                raw_value = raw,
                "Unknown consistency_policy value received, degrading to cluster default"
            );
        }

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);
        let core_req = proto_convert::to_core_read_req(proto_req);

        // Fast path: Eventual/LeaseRead → ReadHandle (ReadActor + cmd_tx fallback).
        {
            if let Some(ref policy) = core_req.consistency_policy
                && matches!(
                    policy,
                    ReadConsistencyPolicy::EventualConsistency | ReadConsistencyPolicy::LeaseRead
                )
            {
                return match self
                    .read_handle
                    .get_batch(
                        &core_req.keys,
                        policy.clone(),
                        core_req.client_id,
                        timeout_duration,
                    )
                    .await
                {
                    Ok(values) => Ok(tonic::Response::new(
                        proto_convert::fast_path_batch_read_response(&core_req.keys, values),
                    )),
                    Err(ClientApiError::Business { message, .. }) => Err(Status::internal(message)),
                    Err(ClientApiError::Network { message, .. }) => {
                        Err(Status::unavailable(message))
                    }
                    Err(other) => Err(Status::internal(format!("{other:?}"))),
                };
            }
        }

        // cmd_tx path: Linearizable or unrecognized policy.
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        let t0 = std::time::Instant::now();
        self.read_handle
            .cmd_tx
            .send(d_engine_core::ClientCmd::Read(core_req, resp_tx))
            .await
            .map_err(|_| Status::internal("Command channel closed"))?;
        metrics::histogram!("server.rpc.cmd_channel_send_wait_ms", "op" => "read")
            .record(t0.elapsed().as_secs_f64() * 1000.0);
        handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_read")
            .await
            .map(|resp| resp.map(proto_convert::to_proto_response))
    }

    /// Scan all keys under a prefix.
    ///
    /// Routes through the Raft command channel so the leader serves the scan
    /// (linearizable by default). Returns all matching entries plus the applied
    /// index at scan time — clients use the revision to filter watch events
    /// during reconnection.
    async fn handle_client_scan(
        &self,
        request: tonic::Request<ScanRequest>,
    ) -> std::result::Result<tonic::Response<ClientResponse>, tonic::Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("handle_client_scan: Node-{} is not ready!", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let req = request.into_inner();
        let (resp_tx, resp_rx) = MaybeCloneOneshot::new();
        let t0 = std::time::Instant::now();
        self.cmd_tx
            .send(d_engine_core::ClientCmd::Scan(req.prefix, resp_tx))
            .await
            .map_err(|_| Status::internal("Command channel closed"))?;
        metrics::histogram!("server.rpc.cmd_channel_send_wait_ms", "op" => "scan")
            .record(t0.elapsed().as_secs_f64() * 1000.0);

        let timeout_duration =
            Duration::from_millis(self.node_config.raft.general_raft_timeout_duration_in_ms);

        handle_rpc_timeout(resp_rx, timeout_duration, "handle_client_scan")
            .await
            .map(|resp| resp.map(proto_convert::to_proto_response))
    }

    /// Watch for changes to a specific key
    ///
    /// Returns a stream of events (PUT/DELETE) for the specified key.
    /// The stream will continue until the client disconnects or the server shuts down.
    ///
    /// # Arguments
    /// * `request` - Contains the key to watch
    ///
    /// # Returns
    /// A stream of WatchResponse messages containing PUT/DELETE events
    ///
    /// # Errors
    /// Returns Status::UNAVAILABLE if Watch is disabled in configuration
    #[cfg(feature = "watch")]
    async fn watch(
        &self,
        request: tonic::Request<WatchRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        let watch_request = request.into_inner();
        let key = watch_request.key;

        // Check if watch registry is available
        let registry = self.watch_registry.as_ref().ok_or_else(|| {
            Status::unavailable("Watch feature is disabled in server configuration")
        })?;

        let is_prefix = watch_request.prefix;
        let prev_kv = watch_request.prev_kv;
        info!(
            node_id = self.node_id,
            key = ?key,
            is_prefix,
            prev_kv,
            "Registering watch for key"
        );

        // Register watcher (exact or prefix) and get receiver
        let handle = if is_prefix {
            registry.register_prefix(key, prev_kv).map_err(|e| match e {
                WatchError::LimitExceeded(_) => Status::resource_exhausted(e.to_string()),
                WatchError::InvalidPrefix => Status::invalid_argument(e.to_string()),
            })?
        } else {
            registry
                .register(key, prev_kv)
                .map_err(|e| Status::resource_exhausted(e.to_string()))?
        };
        let (_watcher_id, _key, receiver) = handle.into_receiver();

        // Convert mpsc::Receiver<WatchEvent> -> Boxed Stream<WatchResponse> for gRPC.
        // WatchEvent (opaque) is converted back to proto WatchResponse at this boundary.
        let stream = tokio_stream::wrappers::ReceiverStream::new(receiver)
            .map(|e| Ok(d_engine_proto::client::WatchResponse::from(&e)))
            .chain(futures::stream::once(async {
                Err(Status::unavailable(
                    "Watch stream closed: server may have shut down or restarted. Please reconnect and re-register the watcher."
                ))
            }));

        Ok(tonic::Response::new(Box::pin(stream)))
    }

    #[cfg(not(feature = "watch"))]
    async fn watch(
        &self,
        _request: tonic::Request<WatchRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchStream>, tonic::Status> {
        Err(Status::unimplemented(
            "Watch feature is not compiled in this build",
        ))
    }

    /// Stream committed membership snapshots to the client.
    ///
    /// Sends the current snapshot immediately on connect (via `mark_changed`), then
    /// one snapshot per committed ConfChange. Closes with UNAVAILABLE on server shutdown
    /// so clients know to reconnect.
    async fn watch_membership(
        &self,
        _request: tonic::Request<WatchMembershipRequest>,
    ) -> std::result::Result<tonic::Response<Self::WatchMembershipStream>, tonic::Status> {
        if !self.is_rpc_ready() {
            // Bootstrap-race noise, not a fault (#428).
            debug!("[watch_membership] Node-{} is not ready", self.node_id);
            return Err(Status::unavailable("Service is not ready"));
        }

        let mut rx = self.membership_rx.clone();
        // Deliver the current snapshot immediately; subsequent items arrive on each ConfChange.
        rx.mark_changed();

        info!(node_id = self.node_id, "Membership watch stream opened");

        let stream = tokio_stream::wrappers::WatchStream::new(rx)
            .map(|s| {
                Ok(ProtoMembershipSnapshot {
                    members: s.members.into_iter().collect(),
                    learners: s.learners.into_iter().collect(),
                    committed_index: s.committed_index,
                })
            })
            .chain(futures::stream::once(async {
                Err(Status::unavailable(
                    "Membership watch stream closed: server shut down. Reconnect to re-subscribe.",
                ))
            }));

        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// Gracefully handles client request cancellations
/// # Functionality
/// - Manages cleanup of abandoned requests
/// - Tracks request cancellation metrics
/// - Prevents resource leaks from dropped requests
pub(crate) async fn with_cancellation_handler<FRequest, FCancellation>(
    request_future: FRequest,
    cancellation_future: FCancellation,
) -> std::result::Result<Response<ClientResponse>, Status>
where
    FRequest:
        Future<Output = std::result::Result<Response<ClientResponse>, Status>> + Send + 'static,
    FCancellation:
        Future<Output = std::result::Result<Response<ClientResponse>, Status>> + Send + 'static,
{
    let token = CancellationToken::new();
    // Will call token.cancel() when the future is dropped, such as when the client
    // cancels the request
    let _drop_guard = token.clone().drop_guard();
    let select_task = tokio::spawn(async move {
        // Can select on token cancellation on any cancellable future while handling the
        // request, allowing for custom cleanup code or monitoring
        select! {
            res = request_future => res,
            _ = token.cancelled() => cancellation_future.await,
        }
    });

    select_task.await.unwrap()
}

/// Centralized timeout handler for all RPC operations
/// # Features
/// - Uniform timeout enforcement across RPC types
/// - Detailed error categorization:
///   - Channel errors
///   - Application-level errors
///   - Deadline exceeded
/// - Logging and metrics integration
async fn handle_rpc_timeout<T, E>(
    resp_rx: impl Future<Output = Result<Result<T, Status>, E>>,
    timeout_duration: Duration,
    rpc_name: &'static str,
) -> Result<Response<T>, Status>
where
    T: std::fmt::Debug,
    E: std::fmt::Debug,
{
    debug!("grpc_raft_serice::handle_rpc_timeout::{}", rpc_name);

    match timeout(timeout_duration, resp_rx).await {
        Ok(Ok(Ok(response))) => {
            debug!("[{}] Success response: {:?}", rpc_name, &response);
            Ok(Response::new(response))
        }
        Ok(Ok(Err(status))) => {
            error!("[{}] Error status: {:?}", rpc_name, &status);
            Err(status)
        }
        Ok(Err(e)) => {
            error!("[{}] Channel error: {:?}", rpc_name, e);
            Err(Status::deadline_exceeded("RPC channel closed"))
        }
        Err(_) => {
            warn!(
                "[{}] Response timeout after {:?}",
                rpc_name, timeout_duration
            );
            Err(Status::deadline_exceeded("RPC timeout exceeded"))
        }
    }
}
