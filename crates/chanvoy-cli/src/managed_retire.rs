//! Bound graceful retirement. This does not authorize cleanup or a successor:
//! callers still need independent exit, free-lease and listener/artifact proof.
#![allow(dead_code)]
use chanvoy_core::runtime::RuntimeRecord;
use chanvoy_core::runtime::{
    control::{self, ControlDocument, ControlRoot},
    native_peer_pid, DiscoveryInventory, LegacyInventory, MarkerInventory, NativeCandidateKind,
    NativeInventory, OperationLock, RecordSnapshot, RuntimeAuthority, RuntimeBinding,
    RuntimeBuildPin, RuntimeError, RuntimePhase, RuntimeProcess, RuntimeResolution,
    RuntimeResolver,
};
use std::os::fd::AsFd;
use std::time::Instant;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt};

pub(crate) struct ManagedRetirement {
    // Lifetime of this attempt spans discovery, admission and exit observation.
    authority: RuntimeAuthority,
    operation: OperationLock,
    record: RecordSnapshot,
    binding: RuntimeBinding,
    process: RuntimeProcess,
    generation: RuntimeBuildPin,
    exit: super::lifecycle::ManagedExitWatch,
    entered_retiring: bool,
    sent: bool,
    deadline: Instant,
}
pub(crate) enum RetirementReply {
    Accepted,
    Refused,
    // EOF before any response byte, not a malformed or partial document.
    // This is no acceptance claim; independent publication/exit proof is needed.
    NoAcknowledgement,
}
/// Exit and the final retirement publication are separate evidence. This still
/// grants no cleanup authority: lease freedom and remaining-listener proof are
/// required while the same operation guard remains held.
pub(crate) struct ObservedRetirementExit {
    pub(crate) exit: super::lifecycle::WholeProcessExit,
    pub(crate) record: RecordSnapshot,
}
impl ManagedRetirement {
    pub(crate) fn prepare(
        authority: RuntimeAuthority,
        operation: OperationLock,
        record: RecordSnapshot,
        deadline: Instant,
    ) -> Result<Self, RuntimeError> {
        require_time_remaining(deadline)?;
        operation.verify(&authority)?;
        if operation.namespace() != &record.record.namespace || !authority.unchanged(&record)? {
            return Err(RuntimeError::Changed("retirement operation target"));
        }
        let inventory = NativeInventory::scan(&authority, &operation, deadline)?;
        let process = matching_candidate(&inventory, &record.record)?;
        let host = chanvoy_core::resolve_host_build_info();
        let generation = RuntimeBuildPin {
            version: host.version,
            commit: host.commit,
            dirty: host.dirty,
        };
        super::managed_launch::require_known_build(&generation)?;
        if record.record.build.as_ref() != Some(&generation) {
            return Err(RuntimeError::Unavailable("retirement generation mismatch"));
        }
        let caller = RuntimeProcess::observe_current()?;
        if caller.uid != process.uid
            || caller.executable != process.executable
            || caller.pid_namespace != process.pid_namespace
            || caller.mount_namespace != process.mount_namespace
        {
            return Err(RuntimeError::Unavailable(
                "retirement native caller binding",
            ));
        }
        let resolution = RuntimeResolver::resolve(
            &record.record.config_root.to_path(),
            &record.record.profile,
            &DiscoveryInventory {
                v1_markers: MarkerInventory::Present,
                legacy: LegacyInventory::Absent,
            },
        )?;
        let RuntimeResolution::Located(binding) = resolution else {
            return Err(RuntimeError::Unavailable("retirement locator"));
        };
        if binding.record() != Some(&record) {
            return Err(RuntimeError::Changed("retirement resolved record"));
        }
        binding.verify()?;
        let exit = super::lifecycle::ManagedExitWatch::capture(&process)
            .map_err(|_| RuntimeError::Unavailable("retirement same-birth exit observer"))?;
        let entered_retiring = record.record.phase == RuntimePhase::Retiring;
        if !entered_retiring && record.record.phase != RuntimePhase::Running {
            return Err(RuntimeError::Unavailable("retirement phase"));
        }
        require_time_remaining(deadline)?;
        Ok(Self {
            authority,
            operation,
            record,
            binding,
            process,
            generation,
            exit,
            entered_retiring,
            sent: false,
            deadline,
        })
    }
    /// An attempt entered Retiring may only complete that epoch. In particular,
    /// a restart caller must not advertise cycled=true or launch a successor.
    pub(crate) fn completion_only(&self) -> bool {
        self.entered_retiring
    }
    pub(crate) async fn request(&mut self) -> Result<RetirementReply, RuntimeError> {
        if self.sent {
            return Err(RuntimeError::Invalid("retirement attempt already sent"));
        }
        require_time_remaining(self.deadline)?;
        self.sent = true;
        tokio::time::timeout_at(self.deadline.into(), self.exchange())
            .await
            .map_err(|_| RuntimeError::Unavailable("retirement acknowledgement unconfirmed"))?
    }
    async fn exchange(&self) -> Result<RetirementReply, RuntimeError> {
        self.operation.verify(&self.authority)?;
        if !self.authority.unchanged(&self.record)? {
            return Err(RuntimeError::Changed("retirement record before request"));
        }
        self.binding.verify()?;
        self.verify_launch()?;
        let mut stream = tokio::net::UnixStream::connect(self.binding.socket_path()).await?;
        if native_peer_pid(stream.as_fd())? != self.process.pid {
            return Err(RuntimeError::Changed("retirement listener peer"));
        }
        self.verify_launch()?;
        self.binding.verify()?;
        require_time_remaining(self.deadline)?;
        let request = build_request(&self.record.record, &self.generation, self.entered_retiring)?;
        let mut bytes = request
            .encode()
            .map_err(|_| RuntimeError::Invalid("retirement request contract"))?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        let mut reader = tokio::io::BufReader::new(stream);
        let reply = read_response(&mut reader, &request, self.entered_retiring).await?;
        // No post-response liveness requirement: the independently registered
        // exit observer may already have received the graceful exit event.
        self.operation.verify(&self.authority)?;
        require_time_remaining(self.deadline)?;
        Ok(reply)
    }
    fn verify_launch(&self) -> Result<(), RuntimeError> {
        let record = &self.record.record;
        let marker = chanvoy_core::runtime::RuntimeLaunchMarker {
            namespace: record.namespace.clone(),
            instance_uuid: record.instance_uuid,
            config_root: record.config_root.clone(),
            profile: record.profile.clone(),
        };
        marker.check_arguments(
            self.process.executable.to_path().as_os_str(),
            &self.process.observe_arguments()?,
        )
    }
    pub(crate) async fn observe_exit(&self) -> Result<ObservedRetirementExit, RuntimeError> {
        require_time_remaining(self.deadline)?;
        self.operation.verify(&self.authority)?;
        let record = self
            .authority
            .read_record(self.operation.namespace())?
            .ok_or(RuntimeError::Changed("retirement publication missing"))?;
        if record.record != expected_retiring_record(&self.record.record)? {
            return Err(RuntimeError::Changed("retirement final publication"));
        }
        let exit = self
            .exit
            .observe_exit(self.deadline)
            .await
            .map_err(|_| RuntimeError::Unavailable("managed whole-process exit unconfirmed"))?;
        self.operation.verify(&self.authority)?;
        if !self.authority.unchanged(&record)?
            || record.record.process.as_ref() != Some(exit.process())
        {
            return Err(RuntimeError::Changed("retirement publication across exit"));
        }
        require_time_remaining(self.deadline)?;
        Ok(ObservedRetirementExit { exit, record })
    }
}

async fn read_response<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    request: &ControlDocument,
    continuing: bool,
) -> Result<RetirementReply, RuntimeError> {
    // Peek without consuming: only an empty EOF can become the typed no-ACK
    // result. Once a byte arrives, strict framing/contract/correlation failures
    // must remain errors, including a subsequent EOF before the delimiter.
    if reader
        .fill_buf()
        .await
        .map_err(|_| RuntimeError::Unavailable("retirement response read"))?
        .is_empty()
    {
        return Ok(RetirementReply::NoAcknowledgement);
    }
    let response = control::read_rpc(reader, ControlRoot::RetireResponse)
        .await
        .map_err(|_| RuntimeError::Invalid("retirement response framing/contract"))?;
    interpret_response(request, &response, continuing)
}
fn expected_retiring_record(initial: &RuntimeRecord) -> Result<RuntimeRecord, RuntimeError> {
    initial.validate()?;
    let mut expected = initial.clone();
    match initial.phase {
        RuntimePhase::Running => {
            expected.revision = initial.next_revision()?;
            expected.phase = RuntimePhase::Retiring;
            expected.ready = false;
        }
        RuntimePhase::Retiring => {}
        _ => {
            return Err(RuntimeError::Invalid(
                "retirement initial publication phase",
            ))
        }
    }
    expected.validate()?;
    Ok(expected)
}
fn require_time_remaining(deadline: Instant) -> Result<(), RuntimeError> {
    if Instant::now() >= deadline {
        Err(RuntimeError::Unavailable("retirement attempt deadline"))
    } else {
        Ok(())
    }
}
fn matching_candidate(
    inventory: &NativeInventory,
    record: &RuntimeRecord,
) -> Result<RuntimeProcess, RuntimeError> {
    record.validate()?;
    let mut matched = None;
    for candidate in &inventory.candidates {
        match &candidate.kind {
            NativeCandidateKind::Legacy { .. } => {
                return Err(RuntimeError::Unavailable(
                    "legacy candidate requires manual reconciliation",
                ))
            }
            NativeCandidateKind::Managed(marker) if marker.namespace == record.namespace => {
                if matched.is_some()
                    || marker.instance_uuid != record.instance_uuid
                    || record.process.as_ref() != Some(&candidate.process)
                {
                    return Err(RuntimeError::Changed("managed inventory target ambiguous"));
                }
                matched = Some(candidate.process.clone());
            }
            NativeCandidateKind::Managed(_) => {}
        }
    }
    matched.ok_or(RuntimeError::Unavailable(
        "managed target absent from native inventory",
    ))
}
fn build_request(
    record: &RuntimeRecord,
    generation: &RuntimeBuildPin,
    continuing: bool,
) -> Result<ControlDocument, RuntimeError> {
    record.validate()?;
    super::managed_launch::require_known_build(generation)?;
    let expected_phase = if continuing {
        RuntimePhase::Retiring
    } else {
        RuntimePhase::Running
    };
    if record.phase != expected_phase || record.build.as_ref() != Some(generation) {
        return Err(RuntimeError::Changed("retirement request phase/generation"));
    }
    let bytes=serde_json::to_vec(&serde_json::json!({
        "jsonrpc":"2.0","id":chanvoy_core::runtime::InstanceId::new().uuid().to_string(),"method":"runtime_retire_v1",
        "params":{"schema_version":1,"kind":"retirement_params","action":if continuing {"continue_same_epoch"} else {"begin"},
            "target":{"namespace":record.namespace,"instance_uuid":record.instance_uuid,
                "record_revision":record.revision,"process":record.process},"caller_generation":generation}
    })).map_err(|_|RuntimeError::Invalid("retirement request encoding"))?;
    ControlDocument::decode(ControlRoot::RetireRequest, &bytes)
        .map_err(|_| RuntimeError::Invalid("retirement request contract"))
}

fn interpret_response(
    request: &ControlDocument,
    response: &ControlDocument,
    continuing: bool,
) -> Result<RetirementReply, RuntimeError> {
    response
        .correlate(request)
        .map_err(|_| RuntimeError::Invalid("retirement response correlation"))?;
    if response.value()["result"]["target"] != request.value()["params"]["target"] {
        return Err(RuntimeError::Changed("retirement response target"));
    }
    let result = &response.value()["result"];
    let accepted = if continuing {
        result["outcome"] == "continued"
    } else {
        result["outcome"] == "committed"
    };
    if !accepted {
        return Ok(RetirementReply::Refused);
    }
    if result["facts"]["durable_retiring"] != true || result["facts"]["admission_closed"] != true {
        return Err(RuntimeError::Invalid("retirement admission facts"));
    }
    Ok(RetirementReply::Accepted)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn request() -> ControlDocument {
        ControlDocument::decode(ControlRoot::RetireRequest,include_bytes!("../../../schemas/runtime/control/v1/fixtures/chanvoy-runtime-control-v1/retire_rpc_request-positive.json")).unwrap()
    }
    fn response() -> serde_json::Value {
        serde_json::from_slice(include_bytes!("../../../schemas/runtime/control/v1/fixtures/chanvoy-runtime-control-v1/retire_rpc_response-positive.json")).unwrap()
    }
    #[tokio::test]
    async fn only_empty_eof_is_no_ack_not_partial_or_invalid_response() {
        let request = request();
        assert!(matches!(
            read_response(&mut b"".as_slice(), &request, false)
                .await
                .unwrap(),
            RetirementReply::NoAcknowledgement
        ));
        let mut valid = serde_json::to_vec(&response()).unwrap();
        let unterminated = valid.clone();
        valid.push(b'\n');
        assert!(matches!(
            read_response(&mut valid.as_slice(), &request, false)
                .await
                .unwrap(),
            RetirementReply::Accepted
        ));
        for bytes in [
            b"{".to_vec(),
            unterminated,
            b"\n".to_vec(),
            b" \n".to_vec(),
            b"invalid\n".to_vec(),
            b"\xff\n".to_vec(),
            b"{}\n".to_vec(),
            vec![b'x'; control::RPC_MAX_BYTES + 1],
        ] {
            assert!(matches!(
                read_response(&mut bytes.as_slice(), &request, false).await,
                Err(RuntimeError::Invalid(
                    "retirement response framing/contract"
                ))
            ));
        }
        for (pointer, value) in [
            (
                "/id",
                serde_json::json!(chanvoy_core::runtime::InstanceId::new().uuid().to_string()),
            ),
            ("/result/target/process/pid", serde_json::json!(124)),
        ] {
            let mut wrong = response();
            *wrong.pointer_mut(pointer).unwrap() = value;
            let mut bytes = serde_json::to_vec(&wrong).unwrap();
            bytes.push(b'\n');
            assert!(read_response(&mut bytes.as_slice(), &request, false)
                .await
                .is_err());
        }
    }
    #[tokio::test(start_paused = true)]
    async fn response_reset_and_deadline_never_become_empty_eof() {
        struct Reset;
        impl tokio::io::AsyncRead for Reset {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                _: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Err(std::io::ErrorKind::ConnectionReset.into()))
            }
        }
        let request = request();
        let mut reader = tokio::io::BufReader::new(Reset);
        assert!(matches!(
            read_response(&mut reader, &request, false).await,
            Err(RuntimeError::Unavailable("retirement response read"))
        ));
        let (reader, writer) = tokio::io::duplex(32);
        let mut reader = tokio::io::BufReader::new(reader);
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            read_response(&mut reader, &request, false),
        )
        .await
        .is_err());
        drop(writer);
    }
    #[test]
    fn request_requires_exact_phase_known_generation_and_valid_record() {
        use chanvoy_core::runtime::{InstanceId, Namespace, PathBytes, RuntimeSocket};
        let process: RuntimeProcess =
            serde_json::from_value(request().value()["params"]["target"]["process"].clone())
                .unwrap();
        let config_root = PathBytes::from_path(std::path::Path::new("/fixture/config")).unwrap();
        let generation = RuntimeBuildPin {
            version: "0.3.4".into(),
            commit: "a".repeat(40),
            dirty: Some(false),
        };
        let mut record = RuntimeRecord {
            schema_version: 1,
            namespace: Namespace::derive(process.uid, &config_root, "fixture").unwrap(),
            instance_uuid: InstanceId::new(),
            revision: 2,
            phase: RuntimePhase::Running,
            uid: process.uid,
            config_root,
            profile: "fixture".into(),
            socket: Some(RuntimeSocket {
                path: chanvoy_core::runtime::child_socket_path(
                    std::path::Path::new("/fixture/run"),
                    process.pid,
                    process.birth.platform(),
                )
                .unwrap(),
                identity: chanvoy_core::runtime::InodeIdentity {
                    device: 1,
                    inode: 2,
                },
            }),
            process: Some(process),
            build: Some(generation.clone()),
            ready: true,
        };
        assert!(build_request(&record, &generation, false).is_ok());
        assert!(build_request(&record, &generation, true).is_err());
        let expected = expected_retiring_record(&record).unwrap();
        assert_eq!(expected.revision, record.revision + 1);
        assert_eq!(expected.process, record.process);
        assert_eq!(expected.socket, record.socket);
        assert_eq!(expected.instance_uuid, record.instance_uuid);
        let mut exhausted = record.clone();
        exhausted.revision = u64::MAX;
        assert!(expected_retiring_record(&exhausted).is_err());
        exhausted.phase = RuntimePhase::Retiring;
        exhausted.ready = false;
        assert_eq!(expected_retiring_record(&exhausted).unwrap(), exhausted);
        record.phase = RuntimePhase::Retiring;
        record.ready = false;
        record.revision += 1;
        assert_eq!(expected, record);
        assert_eq!(expected_retiring_record(&record).unwrap(), record);
        assert!(build_request(&record, &generation, true).is_ok());
        assert!(build_request(&record, &generation, false).is_err());
        record.uid += 1;
        assert!(build_request(&record, &generation, true).is_err());
        record.uid -= 1;
        let unknown = RuntimeBuildPin {
            commit: "unknown".into(),
            ..generation.clone()
        };
        record.build = Some(unknown.clone());
        assert!(build_request(&record, &unknown, true).is_err());
        record.build = Some(generation.clone());
        let other = RuntimeBuildPin {
            commit: "b".repeat(40),
            ..generation
        };
        assert!(build_request(&record, &other, true).is_err());
    }
    #[test]
    fn correlated_response_cannot_substitute_target_or_complete_the_wrong_action() {
        let request = request();
        let doc = |value: &serde_json::Value| {
            ControlDocument::decode(
                ControlRoot::RetireResponse,
                &serde_json::to_vec(value).unwrap(),
            )
            .unwrap()
        };
        let mut value = response();
        assert!(matches!(
            interpret_response(&request, &doc(&value), false).unwrap(),
            RetirementReply::Accepted
        ));
        assert!(matches!(
            interpret_response(&request, &doc(&value), true).unwrap(),
            RetirementReply::Refused
        ));
        value["result"]["target"]["process"]["pid"] = serde_json::json!(124);
        assert!(interpret_response(&request, &doc(&value), false).is_err());
        value = response();
        value["id"] =
            serde_json::json!(chanvoy_core::runtime::InstanceId::new().uuid().to_string());
        assert!(interpret_response(&request, &doc(&value), false).is_err());
    }
}
