//! Subprocess adapter harness for SDK conformance vectors.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_core::Client;
use krabka_gateway::{
    codec::{CodecError, Decoded, EncodeBody, RecordCodec},
    config::GatewayConfig,
    produce::ProduceCore,
    serve,
    state::AppState,
};
use krabka_protocol::owned::incremental_alter_configs_request::{
    AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
};
use krabka_units::prelude::*;
use tokio::{
    io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader, BufWriter},
    net::TcpListener,
    process::{Child, ChildStdin, ChildStdout, Command as ProcessCommand},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    protocol::{CONTRACT_MAJOR, Command, Response},
    vectors::{ContractVersion, Vector, VectorError, load_vectors},
};

const LIVE_COMPATIBLE_VECTOR_IDS: &[&str] = &[
    "messaging_roundtrip",
    "ce_binary_mapping",
    "filter_delivers_matches_only",
    "header_shape",
    "queue_v1_1_ack_error_shape",
    "queue_v1_1_ack_shape",
    "queue_v1_1_acquire_error_shape",
    "queue_v1_1_acquire_shape",
    "queue_v1_1_live_error_mapping",
    "queue_v1_1_lock_duration_error_shape",
    "queue_v1_1_renew_shape",
    "queue_v1_1_session_ownership",
];
const ADAPTER_CALL_TIMEOUT: Duration = Duration::from_secs(30);

/// Harness configuration.
#[derive(Debug, Clone)]
pub struct HarnessConfig {
    /// Adapter executable path.
    pub adapter: PathBuf,
    /// Adapter executable arguments.
    pub adapter_args: Vec<String>,
    /// Vector directory.
    pub vectors_dir: PathBuf,
    /// Optional vector id filter.
    pub filter: Option<String>,
    /// Gateway endpoint sent through `Configure`.
    pub endpoint: String,
    /// Substrate used by the harness.
    pub substrate: HarnessSubstrate,
    /// Run only explicit live-only vectors supported by the current live Rust app SDK.
    pub live_compatible_only: bool,
}

/// Substrate used for adapter calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HarnessSubstrate {
    /// Use the configured endpoint as-is.
    External,
    /// Boot an in-process broker and plaintext h2c gateway on `127.0.0.1:0`.
    Live,
}

/// SDK conformance harness.
#[derive(Debug, Clone)]
pub struct Harness {
    config: HarnessConfig,
}

impl Harness {
    /// Create a harness.
    #[must_use]
    pub fn new(config: HarnessConfig) -> Self {
        Self { config }
    }

    /// Load vectors and run them against the adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when vectors cannot be loaded, the adapter or live
    /// substrate cannot start, adapter I/O fails, or shutdown times out.
    pub async fn run(&self) -> Result<RunSummary, HarnessError> {
        let adapter_version = self.discover_adapter_version().await?;
        let mut vectors = load_vectors(&self.config.vectors_dir)?;
        let mut skipped = newer_contract_skips(adapter_version, &vectors);
        vectors.retain(|vector| adapter_version.satisfies(vector));
        if let Some(filter) = &self.config.filter {
            vectors.retain(|vector| vector.id == *filter);
            skipped.retain(|skipped| skipped.vector_id == *filter);
        }
        if self.config.substrate != HarnessSubstrate::Live {
            let live_only = vectors
                .iter()
                .filter(|vector| vector.live_only)
                .map(|vector| SkippedVector {
                    vector_id: vector.id.clone(),
                    reason: "requires live substrate".into(),
                })
                .collect::<Vec<_>>();
            vectors.retain(|vector| !vector.live_only);
            skipped.extend(live_only);
        }
        let live_plan = if self.config.live_compatible_only {
            LiveCompatiblePlan::from_full_vectors(vectors)
        } else {
            LiveCompatiblePlan::full_vectors(vectors)
        };
        skipped.extend(live_plan.skipped);
        match self.config.substrate {
            HarnessSubstrate::External => {
                self.run_vectors_with_endpoint(live_plan.vectors, skipped, &self.config.endpoint)
                    .await
            }
            HarnessSubstrate::Live => {
                let topic_names = topic_names_for_vectors(&live_plan.vectors);
                let queue_groups = queue_groups_for_vectors(&live_plan.vectors);
                let live = LiveSubstrate::boot(&topic_names, &queue_groups).await?;
                let endpoint = live.endpoint.clone();
                let result = self
                    .run_vectors_with_endpoint(live_plan.vectors, skipped, &endpoint)
                    .await;
                live.shutdown().await?;
                result
            }
        }
    }

    /// Run already-loaded vectors.
    ///
    /// # Errors
    ///
    /// Returns an error when the adapter cannot start, adapter I/O fails, or an
    /// adapter call times out.
    pub async fn run_vectors(&self, vectors: Vec<Vector>) -> Result<RunSummary, HarnessError> {
        self.run_vectors_with_endpoint(vectors, vec![], &self.config.endpoint)
            .await
    }

    async fn run_vectors_with_endpoint(
        &self,
        vectors: Vec<Vector>,
        skipped: Vec<SkippedVector>,
        endpoint: &str,
    ) -> Result<RunSummary, HarnessError> {
        let mut passed = 0;
        let mut failed = vec![];
        for vector in vectors {
            match self.run_vector(&vector, endpoint).await? {
                Some(failure) => failed.push(failure),
                None => passed += 1,
            }
        }
        Ok(RunSummary {
            passed,
            failed,
            skipped,
        })
    }

    async fn run_vector(
        &self,
        vector: &Vector,
        endpoint: &str,
    ) -> Result<Option<VectorFailure>, HarnessError> {
        let mut adapter = AdapterProcess::spawn(&self.config.adapter, &self.config.adapter_args)?;
        let hello = adapter.call(Command::Hello).await?;
        match hello {
            Response::Hello {
                contract_major,
                contract_minor,
                ..
            } if ContractVersion::new(contract_major, contract_minor).satisfies(vector) => {}
            actual => {
                let expected = Response::Hello {
                    contract_major: CONTRACT_MAJOR,
                    contract_minor: vector.since.minor,
                    language: "<adapter>".into(),
                };
                return Ok(Some(VectorFailure {
                    vector_id: vector.id.clone(),
                    step: "hello".into(),
                    expected,
                    actual,
                }));
            }
        }
        let configured = adapter
            .call(Command::Configure {
                endpoint: endpoint.to_string(),
                bearer: None,
            })
            .await?;
        let expected_configuration =
            Response::Ok(serde_json::json!({ "bearer_configured": false }));
        if configured != expected_configuration {
            return Ok(Some(VectorFailure {
                vector_id: vector.id.clone(),
                step: "configure".into(),
                expected: expected_configuration,
                actual: configured,
            }));
        }
        for step in &vector.steps {
            let actual = adapter.call(step.command.clone()).await?;
            if actual != step.expect {
                return Ok(Some(VectorFailure {
                    vector_id: vector.id.clone(),
                    step: step.name.clone(),
                    expected: step.expect.clone(),
                    actual,
                }));
            }
        }
        Ok(None)
    }

    async fn discover_adapter_version(&self) -> Result<ContractVersion, HarnessError> {
        let mut adapter = AdapterProcess::spawn(&self.config.adapter, &self.config.adapter_args)?;
        let hello = adapter.call(Command::Hello).await?;
        let Response::Hello {
            contract_major,
            contract_minor,
            language: _,
        } = hello
        else {
            return Err(HarnessError::AdapterProtocol(
                "adapter hello did not return a hello response",
            ));
        };
        if contract_major != CONTRACT_MAJOR {
            return Err(HarnessError::AdapterProtocol(
                "adapter contract major is not supported",
            ));
        }
        Ok(ContractVersion::new(contract_major, contract_minor))
    }
}

fn newer_contract_skips(
    adapter_version: ContractVersion,
    vectors: &[Vector],
) -> Vec<SkippedVector> {
    vectors
        .iter()
        .filter(|vector| adapter_version < vector.since)
        .map(|vector| SkippedVector {
            vector_id: vector.id.clone(),
            reason: format!(
                "requires contract {}.{}; adapter declares {}.{}",
                vector.since.major,
                vector.since.minor,
                adapter_version.major,
                adapter_version.minor
            ),
        })
        .collect()
}

struct LiveCompatiblePlan {
    vectors: Vec<Vector>,
    skipped: Vec<SkippedVector>,
}

impl LiveCompatiblePlan {
    fn full_vectors(vectors: Vec<Vector>) -> Self {
        Self {
            vectors,
            skipped: vec![],
        }
    }

    fn from_full_vectors(vectors: Vec<Vector>) -> Self {
        let mut live_vectors = Vec::new();
        let mut skipped = Vec::new();

        for vector in vectors {
            if !LIVE_COMPATIBLE_VECTOR_IDS.contains(&vector.id.as_str()) {
                skipped.push(SkippedVector {
                    vector_id: vector.id,
                    reason: "not supported by live substrate".into(),
                });
                continue;
            }
            live_vectors.push(vector);
        }

        Self {
            vectors: live_vectors,
            skipped,
        }
    }
}

fn topic_names_for_vectors(vectors: &[Vector]) -> Vec<String> {
    let mut names = BTreeSet::new();
    for step in vectors.iter().flat_map(|vector| &vector.steps) {
        let topic = match &step.command {
            Command::Publish { topic, .. } | Command::PublishEvent { topic, .. } => topic,
            Command::Subscribe { topics, .. } => {
                for topic in topics {
                    if is_creatable_topic(topic) {
                        names.insert(topic.clone());
                    }
                }
                continue;
            }
            _ => continue,
        };
        if is_creatable_topic(topic) {
            names.insert(topic.clone());
        }
    }
    names.into_iter().collect()
}

/// The share groups that the vectors acquire from, each named once.
///
/// An empty group name is left out. The error vectors send one on purpose, and
/// the broker refuses a config for it.
fn queue_groups_for_vectors(vectors: &[Vector]) -> Vec<String> {
    vectors
        .iter()
        .flat_map(|vector| &vector.steps)
        .filter_map(|step| match &step.command {
            Command::QueueAcquire { group, .. } if !group.is_empty() => Some(group.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn is_creatable_topic(topic: &str) -> bool {
    !topic.is_empty() && !topic.starts_with("__missing_")
}

struct LiveSubstrate {
    endpoint: String,
    broker: BrokerHandle,
    shutdown: CancellationToken,
    gateway_task: JoinHandle<std::io::Result<()>>,
    _data_dir: tempfile::TempDir,
}

impl LiveSubstrate {
    async fn boot(topic_names: &[String], queue_groups: &[String]) -> Result<Self, HarnessError> {
        let data_dir = tempfile::TempDir::new()?;
        let mut broker_config = BrokerConfig::for_tests(data_dir.path().to_path_buf());
        broker_config.classic_group_initial_rebalance_delay = millis(1);
        // `for_tests` names the controller as `127.0.0.1:0`. The broker binds
        // an ephemeral port for it, but its heartbeat client dials the address
        // in `controller_quorum_voters`, which still says port 0. No heartbeat
        // arrives, so two seconds after start the controller marks the broker
        // dead, fences it, and leaves its partitions without a leader. Binding
        // the controller port first gives the voter entry the real address.
        let controller_listener = TcpListener::bind("127.0.0.1:0").await?;
        let controller_addr = controller_listener.local_addr()?;
        broker_config.controller_listen_addr = controller_addr;
        broker_config.controller_quorum_voters =
            vec![(broker_config.node_id, controller_addr.to_string())];
        let broker =
            Broker::start_with_controller_listener(broker_config, Some(controller_listener))
                .await
                .map_err(HarnessError::BrokerStart)?;
        let bootstrap = broker.listen_addr().to_string();
        create_topics(&bootstrap, topic_names).await?;
        set_share_groups_earliest(&bootstrap, queue_groups).await?;

        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?;
        let state = gateway_state(&bootstrap, listen_addr).await?;
        let app = krabka_gateway::router(state);
        let shutdown = CancellationToken::new();
        let gateway_shutdown = shutdown.clone();
        let gateway_task =
            tokio::spawn(async move { serve::serve(listener, app, None, gateway_shutdown).await });

        Ok(Self {
            endpoint: format!("http://{listen_addr}"),
            broker,
            shutdown,
            gateway_task,
            _data_dir: data_dir,
        })
    }

    async fn shutdown(self) -> Result<(), HarnessError> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.gateway_task)
            .await
            .map_err(|_| HarnessError::SubstrateTimeout)??
            .map_err(HarnessError::GatewayServe)?;
        self.broker.shutdown().await;
        Ok(())
    }
}

async fn create_topics(bootstrap: &str, topic_names: &[String]) -> Result<(), HarnessError> {
    if topic_names.is_empty() {
        return Ok(());
    }
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .map_err(HarnessError::Admin)?;
    let specs = topic_names
        .iter()
        .map(|name| CreateTopicSpec {
            name: name.clone(),
            partitions: 1,
            replicas: 1,
            configs: BTreeMap::new(),
        })
        .collect::<Vec<_>>();
    admin
        .create_topics(&specs, millis(10_000))
        .await
        .map(|_| ())
        .map_err(HarnessError::Admin)
}

/// Kafka resource type id for `GROUP`.
const RESOURCE_TYPE_GROUP: i8 = 32;

/// `SET` in the `IncrementalAlterConfigs` wire protocol.
const CONFIG_OPERATION_SET: i8 = 0;

/// Puts each share group on `share.auto.offset.reset=earliest`.
///
/// The queue vectors publish a record and then acquire it. KIP-932 starts a
/// share partition that has no state at `latest` by default, so without this
/// setting the first acquire does not see a record published before it. An
/// operator runs the same `kafka-configs --entity-type groups --alter` to make
/// a share group read a topic from its beginning.
async fn set_share_groups_earliest(bootstrap: &str, groups: &[String]) -> Result<(), HarnessError> {
    if groups.is_empty() {
        return Ok(());
    }
    let client = Client::builder()
        .bootstrap(bootstrap.to_string())
        .build()
        .await
        .map_err(HarnessError::Client)?;
    let response = client
        .send(IncrementalAlterConfigsRequest {
            resources: groups
                .iter()
                .map(|group| AlterConfigsResource {
                    resource_type: RESOURCE_TYPE_GROUP,
                    resource_name: group.clone(),
                    configs: vec![AlterableConfig {
                        name: "share.auto.offset.reset".into(),
                        config_operation: CONFIG_OPERATION_SET,
                        value: Some("earliest".into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .await
        .map_err(HarnessError::Client)?;
    if let Some(rejected) = response
        .responses
        .into_iter()
        .find(|resource| resource.error_code != 0)
    {
        return Err(HarnessError::GroupConfig {
            group: rejected.resource_name,
            code: rejected.error_code,
            message: rejected.error_message.unwrap_or_default(),
        });
    }
    Ok(())
}

async fn gateway_state(
    bootstrap: &str,
    listen_addr: SocketAddr,
) -> Result<Arc<AppState>, HarnessError> {
    let codec = Arc::new(ConformanceCodec);
    let produce = ProduceCore::new(bootstrap, "sdk-conformance", codec.clone(), None)
        .await
        .map_err(HarnessError::GatewayInit)?;
    let config = Arc::new(gateway_config(bootstrap, listen_addr));
    Ok(Arc::new(AppState {
        produce: Arc::new(produce),
        config,
        authz: Arc::new(krabka_gateway::authz::GatewayAuthz::new(Arc::new(
            krabka_authz::AllowAllAuthorizer,
        ))),
        codec,
        queue: Arc::new(krabka_gateway::queue::QueueSessionTable::default()),
    }))
}

#[derive(Debug)]
struct ConformanceCodec;

#[async_trait::async_trait]
impl RecordCodec for ConformanceCodec {
    async fn encode(&self, _topic: &str, body: EncodeBody) -> Result<bytes::Bytes, CodecError> {
        Ok(match body {
            EncodeBody::Raw(value) => value,
            EncodeBody::Structured { json, .. } => json,
        })
    }

    async fn decode(&self, _topic: &str, value: bytes::Bytes) -> Result<Decoded, CodecError> {
        let json = serde_json::from_slice::<serde_json::Value>(&value)
            .ok()
            .map(|_| value.clone());
        Ok(Decoded {
            value,
            schema: None,
            json,
        })
    }
}

fn gateway_config(bootstrap: &str, listen_addr: SocketAddr) -> GatewayConfig {
    GatewayConfig {
        bootstrap: bootstrap.to_string(),
        listen_addr,
        client_id: "sdk-conformance".into(),
        dedup_topic: "__krabka_grpc_dedup".into(),
        dedup_partitions: 4,
        dedup_window: hours(1),
        dedup_ownership_group: "sdk-conformance-owners".into(),
        dedup_txn_id_prefix: "sdk-conformance-dedup".into(),
        advertised_addr: listen_addr.to_string(),
        membership_topic: "__krabka_gateway_membership".into(),
        tls: None,
        broker_security: None,
        authz: None,
        webhooks: BTreeMap::new().into_iter().collect(),
        outbound: Vec::new(),
        schema_registry_url: None,
        runtime: krabka_gateway::config::GatewayRuntimeConfig::default(),
    }
}

/// Summary returned by a conformance run.
#[derive(Debug, Clone, PartialEq)]
pub struct RunSummary {
    /// Number of vectors that passed.
    pub passed: usize,
    /// Failed vector diagnostics.
    pub failed: Vec<VectorFailure>,
    /// Vectors intentionally excluded from this run.
    pub skipped: Vec<SkippedVector>,
}

impl RunSummary {
    /// Whether every vector passed.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.failed.is_empty()
    }
}

/// One vector excluded from the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedVector {
    /// Vector id.
    pub vector_id: String,
    /// Human-readable reason.
    pub reason: String,
}

/// One vector mismatch.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorFailure {
    /// Vector id.
    pub vector_id: String,
    /// Step name.
    pub step: String,
    /// Expected response.
    pub expected: Response,
    /// Actual response.
    pub actual: Response,
}

struct AdapterProcess {
    child: Child,
    stdin: BufWriter<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl AdapterProcess {
    fn spawn(adapter: &PathBuf, args: &[String]) -> Result<Self, HarnessError> {
        let mut child = ProcessCommand::new(adapter)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(HarnessError::Spawn)?;
        let stdin = child
            .stdin
            .take()
            .ok_or(HarnessError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(HarnessError::MissingPipe("stdout"))?;
        Ok(Self {
            child,
            stdin: BufWriter::new(stdin),
            stdout: BufReader::new(stdout),
        })
    }

    async fn call(&mut self, command: Command) -> Result<Response, HarnessError> {
        let line = serde_json::to_string(&command)?;
        self.stdin.write_all(line.as_bytes()).await?;
        self.stdin.write_all(b"\n").await?;
        self.stdin.flush().await?;
        let mut response = String::new();
        tokio::time::timeout(ADAPTER_CALL_TIMEOUT, self.stdout.read_line(&mut response))
            .await
            .map_err(|_| HarnessError::AdapterTimeout)??;
        if response.is_empty() {
            return Err(HarnessError::AdapterEof);
        }
        Ok(serde_json::from_str(response.trim_end())?)
    }
}

impl Drop for AdapterProcess {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Harness errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum HarnessError {
    /// Vector load failed.
    #[error("vectors: {0}")]
    Vectors(#[from] VectorError),
    /// Adapter process failed to spawn.
    #[error("spawn adapter: {0}")]
    Spawn(std::io::Error),
    /// Adapter pipe was not available.
    #[error("adapter missing {0} pipe")]
    MissingPipe(&'static str),
    /// Adapter I/O failed.
    #[error("adapter io: {0}")]
    Io(#[from] std::io::Error),
    /// Protocol JSON failed.
    #[error("adapter json: {0}")]
    Json(#[from] serde_json::Error),
    /// Adapter did not answer in time.
    #[error("adapter timed out")]
    AdapterTimeout,
    /// Adapter exited without a response.
    #[error("adapter exited before responding")]
    AdapterEof,
    /// Adapter violated the conformance protocol.
    #[error("adapter protocol: {0}")]
    AdapterProtocol(&'static str),
    /// Broker failed to start.
    #[error("live substrate broker start: {0}")]
    BrokerStart(krabka_broker::BrokerError),
    /// Admin client setup failed.
    #[error("live substrate admin: {0}")]
    Admin(krabka_client_admin::AdminError),
    /// A raw protocol client request failed.
    #[error("live substrate client: {0}")]
    Client(krabka_client_core::ClientError),
    /// The broker rejected a share group config.
    #[error("live substrate group config for {group}: error {code}: {message}")]
    GroupConfig {
        /// Share group name.
        group: String,
        /// Kafka error code.
        code: i16,
        /// Broker error message.
        message: String,
    },
    /// Gateway state failed to initialize.
    #[error("live substrate gateway init: {0}")]
    GatewayInit(krabka_gateway::error::GatewayError),
    /// Gateway server returned an error.
    #[error("live substrate gateway serve: {0}")]
    GatewayServe(std::io::Error),
    /// Gateway task join failed.
    #[error("live substrate gateway task: {0}")]
    GatewayTask(#[from] tokio::task::JoinError),
    /// Live substrate did not shut down in time.
    #[error("live substrate timed out")]
    SubstrateTimeout,
}
