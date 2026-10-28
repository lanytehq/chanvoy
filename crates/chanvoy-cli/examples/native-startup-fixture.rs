//! Explicit component fixture for a disposable native account home.
//! Never run against an operator home. It is not a host qualification or reaper.
//! The launcher module is the production implementation, not a test substitute.
#[path = "../src/managed_environment.rs"]
mod managed_environment;
#[path = "../src/managed_launch.rs"]
mod managed_launch;
// Compile the actual controller and its native observer, without exposing a
// lifecycle API or substituting inventory/exit evidence in this fixture.
#[allow(dead_code)]
#[path = "../src/lifecycle.rs"]
mod lifecycle;
#[path = "../src/managed_retire.rs"]
mod managed_retire;

use chanvoy_core::runtime::{
    control::{self, ControlDocument, ControlRoot},
    InstanceId, LeaseObservation, Namespace, NativeAccount, PathBytes, RuntimeAuthority,
    RuntimeBuildPin, RuntimeError, RuntimePhase, RuntimeRecord, SocketRoot,
};
use std::future::Future;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn require(value: bool, message: &'static str) -> Result<()> {
    if value {
        Ok(())
    } else {
        Err(std::io::Error::other(message).into())
    }
}

fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    if args.iter().any(|a| a == "--runtime-namespace") {
        // Same single-thread process-entry boundary as the real binary.
        let result = unsafe { chanvoy_cli::prepare_process_entry() };
        let mut invocation = match result {
            Ok(invocation) => invocation,
            Err(error) => child_failure(&error.to_string()),
        };
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        if let Err(error) = runtime.block_on(invocation.run()) {
            child_failure(&error.to_string());
        }
        // Keep the real invocation's lifetime anchor through kernel teardown.
        std::process::exit(0);
    }
    let result = (|| -> Result<()> {
        require(
            args.len() == 2
                && (args[1] == "--run-in-disposable-native-home"
                    || args[1] == "--run-retirement-in-disposable-native-home"
                    || args[1] == "--run-busy-in-disposable-native-home"
                    || args[1] == "--run-cancel-in-disposable-native-home"),
            "requires explicit disposable-home fixture invocation",
        )?;
        require(
            std::env::var("CHANVOY_NATIVE_FIXTURE_ISOLATED").as_deref() == Ok("1"),
            "fixture isolation acknowledgement missing",
        )?;
        require(
            RuntimeAuthority::observe()?.is_none(),
            "fixture refuses existing native authority",
        )?;
        let account = NativeAccount::resolve()?;
        let config = account.home().join("fixture-config");
        let sockets = account.home().join("fixture-sockets");
        for path in [&config, &config.join("profiles"), &sockets] {
            std::fs::DirBuilder::new().mode(0o700).create(path)?;
        }
        let mut profile_file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(config.join("profiles/fixture-native.toml"))?;
        profile_file.write_all(
            br#"name = "fixture-native"
role = "fixture"
scope = "fixture"
provider = "mattermost"
bot_username = "fixture-bot"
team_name = "fixture-team"
server_url = "http://127.0.0.1:9"
env_name = "FIXTURE_MM_TOKEN"
credential_mode = "env_name"
capability_class = "standard"
"#,
        )?;
        profile_file.sync_all()?;
        // Single-thread parent setup, before constructing the Tokio runtime.
        unsafe {
            std::env::set_var("CHANVOY_CONFIG_DIR", &config);
            std::env::set_var("CHANVOY_RUNTIME_DIR", &sockets);
            std::env::set_var("FIXTURE_MM_TOKEN", "test-token-native-fixture");
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(run_parent(
            args[1] != "--run-in-disposable-native-home",
            args[1] == "--run-busy-in-disposable-native-home",
            args[1] == "--run-cancel-in-disposable-native-home",
        ))
    })();
    if let Err(error) = result {
        eprintln!("fixture failed: {error}");
        if let Ok(account) = NativeAccount::resolve() {
            if let Ok(note) =
                std::fs::read_to_string(account.home().join("fixture-child-error.txt"))
            {
                eprintln!(
                    "child fixture diagnostic: {}",
                    note.chars().take(4096).collect::<String>()
                );
            }
        }
        std::process::exit(1);
    }
}

fn child_failure(message: &str) -> ! {
    if let Ok(account) = NativeAccount::resolve() {
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(account.home().join("fixture-child-error.txt"))
        {
            let _ = file.write_all(message.as_bytes());
        }
    }
    std::process::exit(1)
}

async fn exchange(
    path: &PathBytes,
    request: &ControlDocument,
    deadline: Instant,
) -> Result<Option<ControlDocument>> {
    tokio::time::timeout_at(deadline.into(), async {
        let mut stream = tokio::net::UnixStream::connect(path.to_path()).await?;
        let mut bytes = request.encode()?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        let mut reader = tokio::io::BufReader::new(stream);
        match control::read_rpc(&mut reader, ControlRoot::RetireResponse).await {
            Ok(response) => Ok(Some(response)),
            // Control responses hold no operational permit. Shutdown can close
            // this stream; no acknowledgement is then claimed. Independent
            // owned-child exit and exact publication checks below still apply.
            Err(control::ControlError::TruncatedFrame) => Ok(None),
            Err(error) => Err(Box::<dyn std::error::Error>::from(error)),
        }
    })
    .await?
}

async fn run_parent(managed_retirement: bool, check_busy: bool, check_cancel: bool) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(30);
    let profile = chanvoy_core::load_profile("fixture-native")?;
    let authority = RuntimeAuthority::initialize()?;
    let config_root = PathBytes::from_path(&std::fs::canonicalize(
        chanvoy_core::default_chanvoy_config_dir(),
    )?)?;
    let namespace = Namespace::derive(authority.uid(), &config_root, &profile.name)?;
    let operation = authority.try_operation(&namespace)?;
    let lease = authority.try_lifetime(&operation)?;
    let starting = authority.create_starting(
        &operation,
        &lease,
        &RuntimeRecord {
            schema_version: 1,
            namespace: namespace.clone(),
            instance_uuid: InstanceId::new(),
            revision: 0,
            phase: RuntimePhase::Starting,
            uid: authority.uid(),
            config_root,
            profile: profile.name.clone(),
            process: None,
            socket: None,
            build: None,
            ready: false,
        },
    )?;
    let mut launch = managed_launch::ManagedLaunch::new(
        authority,
        operation,
        lease,
        starting,
        SocketRoot::initialize()?,
        deadline,
    )?;
    // Synthetic already-validated provider identity isolates the native handoff
    // from remote authentication. No real provider or credential participates.
    launch.spawn(
        &profile,
        &chanvoy_core::Identity {
            id: "fixture-user".into(),
            username: profile.bot_username.clone(),
            is_bot: true,
            nickname: None,
            email: None,
        },
    )?;
    let host = chanvoy_core::resolve_host_build_info();
    let generation = RuntimeBuildPin {
        version: host.version,
        commit: host.commit,
        dirty: host.dirty,
    };
    let ready = launch.accept(&generation).await?;
    let observer = RuntimeAuthority::observe()?.ok_or("fixture authority disappeared")?;
    require(
        matches!(
            observer.try_operation(&namespace),
            Err(chanvoy_core::runtime::RuntimeError::Locked)
        ),
        "launcher lost operation ownership",
    )?;
    require(
        observer.observe_lease(&namespace)? == LeaseObservation::Contended,
        "live lease not retained",
    )?;
    let running = observer
        .read_record(&namespace)?
        .ok_or("running publication missing")?;
    require(
        running.record.phase == RuntimePhase::Running && running.record.revision == 2,
        "unexpected startup publication",
    )?;
    if check_busy {
        check_busy_subscription(&observer, &running, &generation, deadline).await?;
    }
    if managed_retirement {
        // Acceptance is complete. Release every parent lease reference before
        // a new operation, rather than letting a parent-held fd mask a broken
        // child-lifetime anchor. Tokio Child drop does not signal the child.
        drop(launch);
        return run_managed_retirement(
            observer,
            namespace,
            running,
            deadline,
            check_busy,
            check_cancel,
        )
        .await;
    }
    let request = ControlDocument::decode(
        ControlRoot::RetireRequest,
        &serde_json::to_vec(&serde_json::json!({
            "jsonrpc":"2.0", "id":InstanceId::new().uuid().to_string(), "method":"runtime_retire_v1",
            "params":{"schema_version":1,"kind":"retirement_params","action":"begin",
                "target":{"namespace":namespace,"instance_uuid":running.record.instance_uuid,
                    "record_revision":running.record.revision,"process":running.record.process},
                "caller_generation":generation}
        }))?,
    )?;
    // Exercise real native transport refusals before allowing retirement.
    for (pointer, value, reason) in [
        (
            "/params/target/record_revision",
            serde_json::json!(1),
            "revision_changed",
        ),
        (
            "/params/caller_generation/version",
            serde_json::json!("0.0.0"),
            "generation_refused",
        ),
        (
            "/params/action",
            serde_json::json!("continue_same_epoch"),
            "revision_changed",
        ),
    ] {
        let mut changed = request.value().clone();
        changed["id"] = serde_json::json!(InstanceId::new().uuid().to_string());
        *changed
            .pointer_mut(pointer)
            .ok_or("fixture request pointer")? = value;
        let refused_request =
            ControlDocument::decode(ControlRoot::RetireRequest, &serde_json::to_vec(&changed)?)?;
        let reply = exchange(&ready.socket.path, &refused_request, deadline)
            .await?
            .ok_or("refusal response missing")?;
        reply.correlate(&refused_request)?;
        require(
            reply.value()["result"]["target"] == refused_request.value()["params"]["target"],
            "refusal target changed",
        )?;
        require(
            reply.value()["result"]["outcome"] == "refused"
                && reply.value()["result"]["reason"] == reason,
            "unexpected refusal",
        )?;
        require(
            observer.unchanged(&running)?,
            "refusal changed running publication",
        )?;
    }
    let response = exchange(&ready.socket.path, &request, deadline).await?;
    let acknowledged = response.is_some();
    if let Some(response) = response {
        response.correlate(&request)?;
        require(
            response.value()["result"]["target"] == request.value()["params"]["target"],
            "retirement target changed",
        )?;
        require(
            response.value()["result"]["outcome"] == "committed",
            "retirement was not committed",
        )?;
    }
    require(
        launch.wait_child_exit().await?.success(),
        "child did not exit successfully",
    )?;
    let retiring = observer
        .read_record(&namespace)?
        .ok_or("retirement publication missing")?;
    require(
        retiring.record.phase == RuntimePhase::Retiring
            && retiring.record.revision == 3
            && retiring.record.instance_uuid == running.record.instance_uuid
            && retiring.record.process == running.record.process
            && retiring.record.socket == running.record.socket,
        "unexpected retirement publication",
    )?;
    require(
        observer.observe_lease(&namespace)? == LeaseObservation::Contended,
        "parent released lease before exit collection",
    )?;
    require(
        ready.socket.path.to_path().exists(),
        "child removed managed socket artifact",
    )?;
    drop(launch);
    require(
        observer.observe_lease(&namespace)? == LeaseObservation::Free,
        "lease remained held after both owners exited",
    )?;
    println!(
        "{}",
        serde_json::json!({"fixture":"native-startup-retirement","ready_revision":2,
        "retiring_revision":3,"retirement_acknowledged":acknowledged,
        "native_refusals_checked":3,
        "child_exited":true,"parent_kept_lease":true,
        "lease_free_after_parent_drop":true,"artifacts_retained":true,"qualification":false})
    );
    Ok(())
}

async fn run_managed_retirement(
    authority: RuntimeAuthority,
    namespace: Namespace,
    running: chanvoy_core::runtime::RecordSnapshot,
    deadline: Instant,
    busy_checked: bool,
    cancel_at_first_pending: bool,
) -> Result<()> {
    require(
        authority.observe_lease(&namespace)? == LeaseObservation::Contended,
        "child did not retain lease after launcher drop",
    )?;
    require(
        authority.unchanged(&running)?,
        "launcher drop changed running publication",
    )?;
    let operation = authority.try_operation(&namespace)?;
    let observer = RuntimeAuthority::observe()?.ok_or("fixture authority disappeared")?;
    let mut retirement = managed_retire::ManagedRetirement::prepare(
        authority,
        operation,
        running.clone(),
        deadline,
    )?;
    require(
        !retirement.completion_only(),
        "running attempt unexpectedly completion-only",
    )?;
    if cancel_at_first_pending {
        {
            let mut request = Box::pin(retirement.request());
            std::future::poll_fn(|cx| match request.as_mut().poll(cx) {
                std::task::Poll::Pending => std::task::Poll::Ready(Ok(())),
                std::task::Poll::Ready(_) => std::task::Poll::Ready(Err(
                    "fixture request completed without cancellation point",
                )),
            })
            .await?;
            // Drop only the borrowed future at its first actual Pending. The
            // controller remains owned through all independent checks below.
        }
        require(
            matches!(
                observer.try_operation(&namespace),
                Err(RuntimeError::Locked)
            ),
            "cancelled request released operation ownership",
        )?;
        require(
            observer.unchanged(&running)?
                && observer.observe_lease(&namespace)? == LeaseObservation::Contended,
            "first-pending cancellation changed running snapshot or lost child lease",
        )?;
        running
            .record
            .process
            .as_ref()
            .ok_or("process missing")?
            .revalidate()?;
        require(
            matches!(
                retirement.request().await,
                Err(RuntimeError::Invalid("retirement attempt already sent"))
            ),
            "cancelled attempt permitted an implicit retry",
        )?;
        println!(
            "{}",
            serde_json::json!({"fixture":"native-retirement-cancel-first-pending",
            "operation_retained":true,"running_record_unchanged":true,"child_lease_retained":true,
            "child_still_matches":true,"implicit_retry_refused":true,"qualification":false,
            "teardown":"disposable container account; no retirement or exit claim"})
        );
        return Ok(());
    }
    let acknowledged = match retirement.request().await? {
        managed_retire::RetirementReply::Accepted => true,
        managed_retire::RetirementReply::NoAcknowledgement => false,
        managed_retire::RetirementReply::Refused => {
            return Err("managed retirement refused".into());
        }
    };
    let exited = retirement.observe_exit().await?;
    require(
        exited.record.record.phase == RuntimePhase::Retiring
            && exited.record.record.revision == 3
            && exited.record.record.instance_uuid == running.record.instance_uuid
            && exited.record.record.process == running.record.process
            && exited.record.record.socket == running.record.socket
            && Some(exited.exit.process()) == running.record.process.as_ref(),
        "managed retirement exit changed target",
    )?;
    require(
        matches!(
            observer.try_operation(&namespace),
            Err(RuntimeError::Locked)
        ),
        "managed retirement released operation across request/exit",
    )?;
    require(
        observer.observe_lease(&namespace)? == LeaseObservation::Free,
        "lease remained held after verified child exit",
    )?;
    require(
        observer.unchanged(&exited.record)?
            && running
                .record
                .socket
                .as_ref()
                .ok_or("running socket missing")?
                .path
                .to_path()
                .exists(),
        "managed retirement removed or changed evidence",
    )?;
    drop(retirement);
    let _operation = observer.try_operation(&namespace)?;
    println!(
        "{}",
        serde_json::json!({"fixture":"native-managed-retirement","ready_revision":2,
            "retiring_revision":3,"retirement_acknowledged":acknowledged,
            "native_inventory_admitted":true,"child_alone_kept_lease":true,
            "whole_process_exit_observed":true,"lease_free_after_child_exit":true,
            "operation_retained_through_exit":true,"artifacts_retained":true,
            "busy_subscription_checked":busy_checked,
            "qualification":false})
    );
    Ok(())
}

/// A real persistent subscription owns an arena registration without a provider
/// request or test-only permit. Keep its connection open through the refusal,
/// then explicitly unsubscribe and wait for that correlated completion.
async fn check_busy_subscription(
    authority: &RuntimeAuthority,
    running: &chanvoy_core::runtime::RecordSnapshot,
    generation: &RuntimeBuildPin,
    deadline: Instant,
) -> Result<()> {
    let path = &running.record.socket.as_ref().ok_or("socket missing")?.path;
    let stream = tokio::time::timeout_at(
        deadline.into(),
        tokio::net::UnixStream::connect(path.to_path()),
    )
    .await??;
    let mut reader = tokio::io::BufReader::new(stream);
    let subscribe = legacy_request("subscribe", serde_json::json!({"filter":"all_monitored"}));
    let response = legacy_exchange(&mut reader, &subscribe, deadline).await?;
    let id = response["result"]["subscription_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or("subscription acknowledgement missing")?;
    let request = ControlDocument::decode(
        ControlRoot::RetireRequest,
        &serde_json::to_vec(&serde_json::json!({
            "jsonrpc":"2.0", "id":InstanceId::new().uuid().to_string(), "method":"runtime_retire_v1",
            "params":{"schema_version":1,"kind":"retirement_params","action":"begin",
                "target":{"namespace":running.record.namespace,"instance_uuid":running.record.instance_uuid,
                    "record_revision":running.record.revision,"process":running.record.process},
                "caller_generation":generation}
        }))?,
    )?;
    let response = exchange(path, &request, deadline)
        .await?
        .ok_or("Busy acknowledgement missing")?;
    response.correlate(&request)?;
    let result = &response.value()["result"];
    require(
        result["target"] == request.value()["params"]["target"]
            && result["outcome"] == "busy"
            && result["reason"] == "active_obligations"
            && result["request_committed"] == false
            && result["counts"]["streams"].as_u64().is_some_and(|n| n > 0)
            && result["facts"]["memory_retiring"] == false
            && result["facts"]["durable_retiring"] == false
            && result["facts"]["admission_closed"] == false
            && result["facts"]["shutdown_started"] == false
            && result["facts"]["durable_revision"] == running.record.revision,
        "active subscription did not block retirement without latching",
    )?;
    require(authority.unchanged(running)?, "Busy changed running record")?;
    let unsubscribe = legacy_request("unsubscribe", serde_json::json!({"subscription_id":id}));
    let response = legacy_exchange(&mut reader, &unsubscribe, deadline).await?;
    require(response["result"] == true, "unsubscribe not confirmed")?;
    require(
        authority.unchanged(running)?,
        "unsubscribe changed running record",
    )?;
    Ok(())
}

fn legacy_request(method: &str, params: serde_json::Value) -> serde_json::Value {
    serde_json::json!({"jsonrpc":"2.0","id":InstanceId::new().uuid().to_string(),
        "method":method,"params":params})
}

async fn legacy_exchange(
    reader: &mut tokio::io::BufReader<tokio::net::UnixStream>,
    request: &serde_json::Value,
    deadline: Instant,
) -> Result<serde_json::Value> {
    tokio::time::timeout_at(deadline.into(), async {
        let mut bytes = serde_json::to_vec(request)?;
        bytes.push(b'\n');
        reader.get_mut().write_all(&bytes).await?;
        // Intervening push notifications are allowed, under the same finite
        // byte/message budget and original deadline as the whole fixture.
        for _ in 0..32 {
            let mut bytes = Vec::new();
            (&mut *reader)
                .take(65_537)
                .read_until(b'\n', &mut bytes)
                .await?;
            require(
                bytes.len() <= 65_536 && bytes.last() == Some(&b'\n'),
                "legacy response framing incomplete or oversized",
            )?;
            let response: serde_json::Value = serde_json::from_slice(&bytes)?;
            require(response["jsonrpc"] == "2.0", "legacy response version")?;
            if response.get("id").is_none() && response["method"].is_string() {
                continue;
            }
            require(
                response["id"] == request["id"] && response["error"].is_null(),
                "legacy response failed or was uncorrelated",
            )?;
            return Ok(response);
        }
        Err("legacy notification budget exhausted".into())
    })
    .await?
}
