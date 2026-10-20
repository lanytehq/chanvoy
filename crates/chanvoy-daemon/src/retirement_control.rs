//! Application retirement composition, deliberately not routed or activated.
//! Native C must supply fresh binding on every attempt before this may be used.
//! The coordinator owns its child publisher; its mutex serializes control work,
//! not admission. The shared arena lock is never held across publication I/O.
#![allow(dead_code)]

use crate::wait_owner::{AdmissionPhase, AdmissionSnapshot, RetirementDecision, WaitOwnerRegistry};
use chanvoy_core::runtime::{
    control::{ControlDocument, ControlRoot},
    InstanceId, Namespace, PublicationView, RecordPublisher, RuntimeBuildPin, RuntimeError,
    RuntimePhase, RuntimeProcess, RuntimeRecord,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

// Internal adapter for deterministic failure schedules. Production construction
// takes the actual child publisher by value; no competing publisher escapes.
pub(crate) trait Publication: Send {
    fn inspect(&self) -> Result<PublicationView, RuntimeError>;
    fn begin(&self) -> Result<(), RuntimeError>;
    fn retry(&self) -> Result<(), RuntimeError>;
}
impl Publication for RecordPublisher<'_> {
    fn inspect(&self) -> Result<PublicationView, RuntimeError> {
        self.inspect_publication()
    }
    fn begin(&self) -> Result<(), RuntimeError> {
        self.publish_retiring()
    }
    fn retry(&self) -> Result<(), RuntimeError> {
        self.retry_publication()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Begin,
    ContinueSameEpoch,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    namespace: Namespace,
    instance_uuid: InstanceId,
    record_revision: u64,
    process: RuntimeProcess,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    schema_version: u16,
    kind: String,
    action: Action,
    target: Target,
    caller_generation: RuntimeBuildPin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Committed,
    Continued,
    Busy,
    TargetMismatch,
    RevisionChanged,
    GenerationRefused,
    NativeUnavailable,
    AlreadyRetiring,
    NotRetiring,
    PublicationUnconfirmed,
}
/// Internal facts only; wire projection and shutdown frontiers remain separate.
#[derive(Debug)]
pub(crate) struct ResultFacts {
    pub outcome: Outcome,
    pub arena: Option<AdmissionSnapshot>,
    pub publication: Option<PublicationView>,
}

pub(crate) struct RetirementControl<P> {
    registry: Arc<WaitOwnerRegistry>,
    anchor: RuntimeRecord,
    publisher: Mutex<P>,
}
impl<'a> RetirementControl<RecordPublisher<'a>> {
    pub(crate) fn for_child(
        registry: Arc<WaitOwnerRegistry>,
        publisher: RecordPublisher<'a>,
    ) -> Result<Self, RuntimeError> {
        Self::bind(registry, publisher)
    }
}
impl<P: Publication> RetirementControl<P> {
    fn bind(registry: Arc<WaitOwnerRegistry>, publisher: P) -> Result<Self, RuntimeError> {
        let view = publisher.inspect()?;
        let arena = registry.admission_snapshot();
        if !view.confirmed
            || view.observed != view.logical
            || view.logical.phase != RuntimePhase::Running
            || arena.phase != AdmissionPhase::Running
            || arena.instance != view.logical.instance_uuid
            || view.logical.process.is_none()
            || view
                .logical
                .build
                .as_ref()
                .is_none_or(|g| g.dirty.is_none() || g.commit == "unknown")
        {
            return Err(RuntimeError::Invalid("retirement coordinator binding"));
        }
        Ok(Self {
            registry,
            anchor: view.logical,
            publisher: Mutex::new(publisher),
        })
    }

    /// Native recheck must independently revalidate the current child/config,
    /// process/peer/socket and lease binding. A decoded request is not that proof.
    /// There is no production caller until native C implements this boundary.
    pub(crate) fn attempt(
        &self,
        request: &ControlDocument,
        native_recheck: impl FnOnce(&RuntimeRecord) -> Result<(), RuntimeError>,
    ) -> Result<ResultFacts, RuntimeError> {
        if request.root() != ControlRoot::RetireRequest {
            return Err(RuntimeError::Invalid("retirement request root"));
        }
        let params: Params = serde_json::from_value(request.value()["params"].clone())
            .map_err(|_| RuntimeError::Invalid("retirement request projection"))?;
        let publisher = self
            .publisher
            .lock()
            .map_err(|_| RuntimeError::Unavailable("retirement control mutex"))?;
        let facts = |outcome, publication| ResultFacts {
            outcome,
            arena: Some(self.registry.admission_snapshot()),
            publication,
        };
        let view = match publisher.inspect() {
            Ok(view) => view,
            Err(_) => {
                return Ok(ResultFacts {
                    outcome: Outcome::PublicationUnconfirmed,
                    arena: None,
                    publication: None,
                })
            }
        };
        let record = &view.observed;
        // The anchored child never adopts a replacement epoch, process, socket,
        // profile, or generation. Revision is separately captured per request.
        if !same_identity(&self.anchor, record)
            || !same_identity(&self.anchor, &view.logical)
            || params.target.namespace != record.namespace
            || params.target.instance_uuid != record.instance_uuid
            || Some(&params.target.process) != record.process.as_ref()
        {
            return Ok(ResultFacts {
                outcome: Outcome::TargetMismatch,
                arena: None,
                publication: None,
            });
        }
        if params.target.record_revision != record.revision {
            return Ok(facts(Outcome::RevisionChanged, Some(view)));
        }
        if Some(&params.caller_generation) != record.build.as_ref() {
            return Ok(facts(Outcome::GenerationRefused, Some(view)));
        }
        if native_recheck(record).is_err() {
            return Ok(facts(Outcome::NativeUnavailable, Some(view)));
        }
        let arena = self.registry.admission_snapshot();
        let outcome = match params.action {
            Action::Begin => {
                if arena.phase == AdmissionPhase::Retiring {
                    return Ok(facts(Outcome::AlreadyRetiring, Some(view)));
                }
                if view.logical.phase != RuntimePhase::Running || !view.confirmed {
                    return Ok(facts(Outcome::PublicationUnconfirmed, Some(view)));
                }
                match self
                    .registry
                    .retire_if_idle(self.anchor.instance_uuid)
                    .map_err(|_| RuntimeError::Changed("retirement arena instance"))?
                {
                    RetirementDecision::Busy(snapshot) => {
                        return Ok(ResultFacts {
                            outcome: Outcome::Busy,
                            arena: Some(snapshot),
                            publication: Some(view),
                        })
                    }
                    RetirementDecision::AlreadyRetiring(_) => {
                        return Ok(facts(Outcome::AlreadyRetiring, Some(view)))
                    }
                    RetirementDecision::Closed(_) => Outcome::Committed,
                }
            }
            Action::ContinueSameEpoch => {
                if arena.phase != AdmissionPhase::Retiring {
                    return Ok(facts(Outcome::NotRetiring, Some(view)));
                }
                Outcome::Continued
            }
        };
        // No second idle decision on continuation, and no arena mutex across
        // storage. Stage failures leave memory closed even without pending data.
        let result = match view.logical.phase {
            RuntimePhase::Running => publisher.begin(),
            RuntimePhase::Retiring => publisher.retry(),
            _ => return Ok(facts(Outcome::PublicationUnconfirmed, Some(view))),
        };
        let after = publisher.inspect().ok();
        if result.is_err()
            || !after
                .as_ref()
                .is_some_and(|v| v.confirmed && v.observed.phase == RuntimePhase::Retiring)
        {
            return Ok(facts(Outcome::PublicationUnconfirmed, after));
        }
        Ok(facts(outcome, after))
    }
}

fn same_identity(a: &RuntimeRecord, b: &RuntimeRecord) -> bool {
    a.namespace == b.namespace
        && a.instance_uuid == b.instance_uuid
        && a.uid == b.uid
        && a.config_root == b.config_root
        && a.profile == b.profile
        && a.process == b.process
        && a.socket == b.socket
        && a.build == b.build
}

#[cfg(test)]
mod tests;
