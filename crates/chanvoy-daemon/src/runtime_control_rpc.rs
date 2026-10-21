//! Local runtime-control v1 RPC routing.
//!
//! `runtime_retire_v1` and `runtime_observation_v1` are decoded through their
//! strict selected roots, answered as provider-free control exceptions (no
//! operational request permit), and encoded through the same roots. Until the
//! native lifecycle binding exists there is no retirement coordinator for this
//! daemon, so every retirement is refused with unknown target facts and no
//! shutdown, and observation reports application state only.

use crate::retirement_control::{Outcome, ResultFacts};
use crate::wait_owner::{AdmissionPhase, AdmissionSnapshot, WaitOwnerRegistry};
use chanvoy_core::runtime::control::{ControlDocument, ControlError, ControlRoot};
use chanvoy_core::runtime::RuntimePhase;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) const RETIRE_METHOD: &str = "runtime_retire_v1";
pub(crate) const OBSERVATION_METHOD: &str = "runtime_observation_v1";
/// Bound on writing one control reply before the client is dropped.
pub(crate) const REPLY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
const HUMAN_PROCEDURE: &str =
    "https://github.com/lanytehq/chanvoy/blob/main/docs/guides/runtime-recovery.md";

/// Route selection uses only the method name. The body is never trusted until
/// the selected root decodes it.
pub(crate) fn root_for(method: &str) -> Option<ControlRoot> {
    match method {
        RETIRE_METHOD => Some(ControlRoot::RetireRequest),
        OBSERVATION_METHOD => Some(ControlRoot::ObservationRequest),
        _ => None,
    }
}

/// The control root a frame selects, if any. Only method position counts, so
/// ordinary payload text that mentions a control method (a chat message, a
/// search query) keeps its legacy handler:
///
/// - a JSON object selects a root when any top-level `method` member (a
///   duplicate key included) names a control method;
/// - a JSON array selects one when any top-level element does, because a
///   lenient decoder can read an array positionally as a request;
/// - a frame that is not one JSON value (trailing data, BOM, malformed
///   content) selects one when its bytes name a control method in any ASCII
///   escape spelling. Legacy parsing would refuse such a frame anyway.
///
/// A selected frame is decoded only by the strict root and never reaches
/// legacy parsing.
pub(crate) fn frame_root(frame: &[u8]) -> Option<ControlRoot> {
    let names = match serde_json::from_slice::<Value>(frame) {
        Ok(Value::Object(_)) => top_level_methods(frame),
        Ok(Value::Array(items)) => items
            .into_iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        Ok(_) => Vec::new(),
        Err(_) => {
            let unescaped = unescape_ascii(frame);
            [RETIRE_METHOD, OBSERVATION_METHOD]
                .into_iter()
                .filter(|method| contains(&unescaped, method.as_bytes()))
                .map(str::to_owned)
                .collect()
        }
    };
    names.iter().find_map(|name| root_for(name))
}

/// Every top-level `method` value of an object, including duplicate keys
/// that `serde_json::Value` would collapse.
fn top_level_methods(frame: &[u8]) -> Vec<String> {
    struct Methods(Vec<String>);
    impl<'de> serde::Deserialize<'de> for Methods {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            struct Visit;
            impl<'de> serde::de::Visitor<'de> for Visit {
                type Value = Methods;
                fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str("a JSON object")
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> Result<Methods, A::Error> {
                    let mut methods = Vec::new();
                    while let Some(key) = map.next_key::<String>()? {
                        if key == "method" {
                            if let Value::String(name) = map.next_value::<Value>()? {
                                methods.push(name);
                            }
                        } else {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                    Ok(Methods(methods))
                }
            }
            d.deserialize_map(Visit)
        }
    }
    serde_json::from_slice::<Methods>(frame)
        .map(|Methods(methods)| methods)
        .unwrap_or_default()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Decode `\\u00XX` escapes so an escaped method name is still recognized.
/// Every other byte is copied unchanged.
fn unescape_ascii(frame: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(frame.len());
    let mut i = 0;
    while i < frame.len() {
        if frame[i..].starts_with(b"\\u00") && i + 6 <= frame.len() {
            if let Some(byte) = std::str::from_utf8(&frame[i + 4..i + 6])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
            {
                out.push(byte);
                i += 6;
                continue;
            }
        }
        out.push(frame[i]);
        i += 1;
    }
    out
}

/// The encoded reply and whether the connection must close after it.
pub(crate) struct Reply {
    pub bytes: Vec<u8>,
    pub close: bool,
}

/// Answer one control line as read from the transport, including its LF
/// delimiter; a line that ends without one (EOF first) is a truncated frame.
pub(crate) async fn answer(
    line: &[u8],
    root: ControlRoot,
    arena: &WaitOwnerRegistry,
    shutdown: &ShutdownStarter<'_>,
) -> Reply {
    let Some(frame) = line.strip_suffix(b"\n") else {
        return protocol_error(None, ControlError::TruncatedFrame);
    };
    let request = match ControlDocument::decode(root, frame) {
        Ok(request) => request,
        Err(error) => return protocol_error(None, error),
    };
    let id = request.value()["id"].clone();
    let projected = match root {
        // No coordinator is bound to this daemon yet: nothing can prove the
        // target, so this is a refusal with unknown target facts.
        ControlRoot::RetireRequest => retire(&request, &unbound(), shutdown).await,
        ControlRoot::ObservationRequest => observation_response(&request, arena),
        _ => Err(ControlError::WrongTransport),
    };
    match projected.and_then(|document| document.encode()) {
        Ok(bytes) => Reply {
            bytes,
            close: false,
        },
        Err(_) => protocol_error(Some(id), ControlError::InvalidContract),
    }
}

/// The daemon's graceful-shutdown signal and its latched "started" state.
/// `shutdown_started` describes the target, so once the signal is sent by any
/// path, every later response reports it.
pub(crate) struct ShutdownStarter<'a> {
    pub sender: &'a tokio::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    pub started: &'a AtomicBool,
}

impl ShutdownStarter<'_> {
    pub(crate) fn started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }
    /// Send the signal unless it was already taken; report the latched state.
    async fn start(&self) -> bool {
        if let Some(sender) = self.sender.lock().await.take() {
            if sender.send(()).is_ok() {
                self.started.store(true, Ordering::Release);
            }
        }
        self.started()
    }
}

/// Start shutdown only for an outcome that leaves the target durably
/// Retiring with an empty arena, then project the facts with the target's
/// shutdown state.
pub(crate) async fn retire(
    request: &ControlDocument,
    facts: &ResultFacts,
    shutdown: &ShutdownStarter<'_>,
) -> Result<ControlDocument, ControlError> {
    let shutdown_started = match facts.outcome {
        Outcome::Committed | Outcome::Continued => {
            // Check the projection first: shutdown never starts on facts the
            // contract cannot report.
            retire_response(request, facts, shutdown.started())?;
            shutdown.start().await
        }
        _ => shutdown.started(),
    };
    retire_response(request, facts, shutdown_started)
}

fn unbound() -> ResultFacts {
    ResultFacts {
        outcome: Outcome::NativeUnavailable,
        arena: None,
        publication: None,
    }
}

/// Closed protocol failure. Payloads are never echoed; framing or decode
/// failures close the exchange because the remaining bytes are not a frame.
fn protocol_error(id: Option<Value>, error: ControlError) -> Reply {
    let (code, message, close) = match error {
        ControlError::TooLarge
        | ControlError::InvalidUtf8
        | ControlError::InvalidJson
        | ControlError::TruncatedFrame => (-32600, "Invalid local control frame", true),
        // A well-formed frame that fails its root leaves the line framing intact.
        ControlError::InvalidContract => match id {
            Some(_) => (-32000, "Local control response unavailable", false),
            None => (-32602, "Invalid local control payload", false),
        },
        _ => (-32000, "Local control unavailable", true),
    };
    let body = json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": {"code": code, "message": message, "data": null},
    });
    let bytes = ControlDocument::decode(
        ControlRoot::RpcErrorResponse,
        &serde_json::to_vec(&body).expect("static error body"),
    )
    .and_then(|document| document.encode())
    .expect("protocol error body matches its root");
    Reply { bytes, close }
}

/// Project internal retirement facts onto `retire_rpc_response`. A
/// combination the contract cannot represent is an error, never an invented
/// fact; memory and admission facts come from the arena snapshot.
fn retire_response(
    request: &ControlDocument,
    facts: &ResultFacts,
    shutdown_started: bool,
) -> Result<ControlDocument, ControlError> {
    let target = request.value()["params"]["target"].clone();
    let begin = request.value()["params"]["action"] == "begin";
    let counts = |arena: &AdmissionSnapshot| {
        json!({
            "requests": arena.requests,
            "streams": arena.streams,
            "slots": arena.slots,
            "replacements": arena.replacements,
            "cleanup_obligations": arena.cleanup,
        })
    };
    let unknown_counts = json!({
        "requests": null, "streams": null, "slots": null,
        "replacements": null, "cleanup_obligations": null,
    });
    let durable = facts.publication.as_ref().filter(|view| view.confirmed);
    let durable_retiring = durable.map(|view| view.observed.phase == RuntimePhase::Retiring);
    let durable_revision = durable.map(|view| view.observed.revision);
    let known = |shutdown: bool, arena: &AdmissionSnapshot| {
        (
            json!({
                "memory_retiring": arena.phase == AdmissionPhase::Retiring,
                "durable_retiring": durable_retiring,
                "shutdown_started": shutdown,
                "responding_at_snapshot": true,
                "admission_closed": arena.phase == AdmissionPhase::Retiring,
                "durable_revision": durable_revision,
            }),
            counts(arena),
        )
    };
    let (outcome, committed, reason, body) = match (facts.outcome, &facts.arena) {
        (Outcome::Committed, Some(arena)) => (
            "committed",
            Some(true),
            "durable_retiring",
            known(shutdown_started, arena),
        ),
        (Outcome::Continued, Some(arena)) => (
            "continued",
            Some(false),
            "already_retiring",
            known(shutdown_started, arena),
        ),
        (Outcome::Busy, Some(arena)) => (
            "busy",
            Some(false),
            "active_obligations",
            known(false, arena),
        ),
        (Outcome::AlreadyRetiring, Some(arena)) => (
            "refused",
            Some(false),
            "already_retiring",
            known(shutdown_started, arena),
        ),
        (Outcome::RevisionChanged | Outcome::NotRetiring, Some(arena)) => (
            "refused",
            Some(false),
            "revision_changed",
            known(shutdown_started, arena),
        ),
        (Outcome::GenerationRefused, Some(arena)) => (
            "refused",
            Some(false),
            "generation_refused",
            known(shutdown_started, arena),
        ),
        (Outcome::PublicationUnconfirmed, Some(arena))
            if arena.phase == AdmissionPhase::Retiring =>
        {
            // Durable failure is known only from a confirmed non-Retiring read.
            let failed = facts.publication.as_ref().is_some_and(|view| {
                view.confirmed && view.observed.phase != RuntimePhase::Retiring
            });
            (
                "unconfirmed",
                // Publication is reached on begin only after this request
                // latched the arena; continuation never latches.
                Some(begin),
                "publication_unconfirmed",
                (
                    json!({
                        "memory_retiring": true,
                        "durable_retiring": if failed { json!(false) } else { Value::Null },
                        "shutdown_started": false,
                        "responding_at_snapshot": true,
                        "admission_closed": true,
                        "durable_revision": durable_revision,
                    }),
                    counts(arena),
                ),
            )
        }
        (Outcome::TargetMismatch | Outcome::NativeUnavailable, _) => (
            "refused",
            Some(false),
            if facts.outcome == Outcome::TargetMismatch {
                "target_mismatch"
            } else {
                "native_evidence_unavailable"
            },
            (
                json!({
                    "memory_retiring": null, "durable_retiring": null,
                    "shutdown_started": null, "responding_at_snapshot": true,
                    "admission_closed": null, "durable_revision": null,
                }),
                unknown_counts,
            ),
        ),
        _ => return Err(ControlError::InvalidContract),
    };
    let (facts_value, counts_value) = body;
    let document = json!({
        "jsonrpc": "2.0",
        "id": request.value()["id"],
        "result": {
            "schema_version": 1,
            "kind": "retirement_result",
            "origin": "daemon_response",
            "target": target,
            "outcome": outcome,
            "request_committed": committed,
            "facts": facts_value,
            "counts": counts_value,
            "reason": reason,
        },
    });
    let encoded = serde_json::to_vec(&document).map_err(|_| ControlError::InvalidJson)?;
    ControlDocument::decode(ControlRoot::RetireResponse, &encoded)
}

/// Application-only observation. The daemon cannot prove its own native
/// identity, death, lease or listener; those stay null, and without a bound
/// runtime record the epoch and generation are unknown.
fn observation_response(
    request: &ControlDocument,
    arena: &WaitOwnerRegistry,
) -> Result<ControlDocument, ControlError> {
    let snapshot = arena.admission_snapshot();
    let document = json!({
        "jsonrpc": "2.0",
        "id": request.value()["id"],
        "result": {
            "schema_version": 1,
            "kind": "lifecycle_observation",
            "source": "daemon_application",
            "epoch": null,
            "phase": match snapshot.phase {
                AdmissionPhase::Running => "running",
                AdmissionPhase::Retiring => "retiring",
            },
            "admission_closed": snapshot.phase == AdmissionPhase::Retiring,
            "counts": {
                "requests": snapshot.requests,
                "streams": snapshot.streams,
                "slots": snapshot.slots,
                "replacements": snapshot.replacements,
                "cleanup_obligations": snapshot.cleanup,
            },
            "native_process": null,
            "native_identity_proved": null,
            "whole_process_dead": null,
            "lease": null,
            "listener_present": null,
            "generation": null,
            "health": null,
            "unavailable_reason": "native_identity_unknown",
            "human_procedure": HUMAN_PROCEDURE,
            "observation_id": uuid::Uuid::new_v4().to_string(),
            "observed_at_unix_millis": chanvoy_core::now_unix_millis(),
        },
    });
    let encoded = serde_json::to_vec(&document).map_err(|_| ControlError::InvalidJson)?;
    ControlDocument::decode(ControlRoot::ObservationResponse, &encoded)
}

#[cfg(test)]
mod tests;
