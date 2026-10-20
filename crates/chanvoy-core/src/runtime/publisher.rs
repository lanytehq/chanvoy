//! Sole child publication and forward-only reconciliation. Stored process
//! fields are claims; qualified bootstrap must independently validate handoff.
use super::{
    LifetimeLease, RecordSnapshot, RuntimeAuthority, RuntimeBuildPin, RuntimeError, RuntimePhase,
    RuntimeProcess, RuntimeRecord, RuntimeSocket,
};
use std::sync::Mutex;

/// Read-only publication claims captured from the child publisher. Observing a
/// pending record on disk does not confirm its durability or prove native facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationView {
    pub observed: RuntimeRecord,
    pub logical: RuntimeRecord,
    pub confirmed: bool,
}

pub struct RecordPublisher<'a> {
    authority: &'a RuntimeAuthority,
    lease: LifetimeLease,
    inner: Mutex<PublicationState>,
}
struct PublicationState {
    durable: RecordSnapshot,
    pending: Option<RuntimeRecord>,
}
impl PublicationState {
    fn inspect(&self, observed: &RecordSnapshot) -> Result<PublicationView, RuntimeError> {
        if observed != &self.durable && self.pending.as_ref() != Some(&observed.record) {
            return Err(RuntimeError::Changed("publisher record epoch/revision"));
        }
        Ok(PublicationView {
            observed: observed.record.clone(),
            logical: self.logical().clone(),
            confirmed: self.pending.is_none() && observed == &self.durable,
        })
    }
    fn logical(&self) -> &RuntimeRecord {
        self.pending.as_ref().unwrap_or(&self.durable.record)
    }
    fn stage(&mut self, next: RuntimeRecord) -> Result<(), RuntimeError> {
        if self.pending.is_some() {
            return Err(RuntimeError::Unavailable(
                "publication requires forward reconciliation",
            ));
        }
        let current = &self.durable.record;
        if next.namespace != current.namespace
            || next.instance_uuid != current.instance_uuid
            || next.uid != current.uid
            || next.config_root != current.config_root
            || next.profile != current.profile
            || next.revision != current.next_revision()?
        {
            return Err(RuntimeError::Invalid("publisher epoch/revision"));
        }
        let allowed = match (current.phase, next.phase) {
            (RuntimePhase::Starting, RuntimePhase::Starting) => {
                current.revision == 0 && current.process.is_none()
            }
            (RuntimePhase::Starting, RuntimePhase::Running)
            | (RuntimePhase::Running, RuntimePhase::Retiring) => current.process == next.process,
            _ => false,
        };
        if !allowed {
            return Err(RuntimeError::Invalid("publisher phase transition"));
        }
        if current.phase == RuntimePhase::Running && current.socket != next.socket {
            return Err(RuntimeError::Changed("retirement socket claim"));
        }
        next.validate()?;
        self.pending = Some(next);
        Ok(())
    }
    fn finish(&mut self, published: RecordSnapshot) -> Result<(), RuntimeError> {
        if self.pending.as_ref() != Some(&published.record) {
            return Err(RuntimeError::Changed("publication outcome"));
        }
        self.durable = published;
        self.pending = None;
        Ok(())
    }
}
impl<'a> RecordPublisher<'a> {
    /// Called by the intended child only after the external native bootstrap
    /// has proved the inherited lease/handoff and this process's full identity.
    /// This method enforces local ownership and revision claims, not that proof.
    /// It owns the lifetime reference throughout every synchronous publication.
    pub fn for_current_child(
        authority: &'a RuntimeAuthority,
        lease: LifetimeLease,
        starting: RecordSnapshot,
        process: RuntimeProcess,
    ) -> Result<Self, RuntimeError> {
        lease.verify(authority)?;
        if process.pid.get() != std::process::id()
            || process.uid != authority.uid()
            || starting.record.namespace != *lease.namespace()
            || starting.record.phase != RuntimePhase::Starting
            || starting.record.revision != 0
            || !authority.unchanged(&starting)?
        {
            return Err(RuntimeError::Invalid("child publisher ownership"));
        }
        let mut next = starting.record.clone();
        next.revision = next.next_revision()?;
        next.process = Some(process);
        let mut inner = PublicationState {
            durable: starting,
            pending: None,
        };
        inner.stage(next)?;
        Ok(Self {
            authority,
            lease,
            inner: Mutex::new(inner),
        })
    }

    /// Recheck lease and exact disk state without writing or completing pending
    /// publication. A stale request must not cause a retry as a side effect of
    /// observation. External epoch/revision replacement refuses.
    pub fn inspect_publication(&self) -> Result<PublicationView, RuntimeError> {
        let inner = self
            .inner
            .lock()
            .map_err(|_| RuntimeError::Unavailable("publisher mutex"))?;
        self.lease.verify(self.authority)?;
        let observed = self
            .authority
            .read_record(&inner.durable.record.namespace)?
            .ok_or(RuntimeError::Changed("publisher record missing"))?;
        inner.inspect(&observed)
    }

    pub fn phase(&self) -> Result<RuntimePhase, RuntimeError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| RuntimeError::Unavailable("publisher mutex"))?
            .logical()
            .phase)
    }
    pub fn publish_running(
        &self,
        socket: RuntimeSocket,
        build: RuntimeBuildPin,
    ) -> Result<(), RuntimeError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| RuntimeError::Unavailable("publisher mutex"))?;
        let mut next = inner.logical().clone();
        next.revision = next.next_revision()?;
        next.phase = RuntimePhase::Running;
        next.ready = true;
        next.socket = Some(socket);
        next.build = Some(build);
        inner.stage(next)?;
        self.flush(&mut inner)
    }

    /// Arena retirement must already be committed. A write error leaves the
    /// logical phase Retiring, retains the lease and admits only forward retry.
    pub fn publish_retiring(&self) -> Result<(), RuntimeError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| RuntimeError::Unavailable("publisher mutex"))?;
        let mut next = inner.logical().clone();
        next.revision = next.next_revision()?;
        next.phase = RuntimePhase::Retiring;
        next.ready = false;
        inner.stage(next)?;
        self.flush(&mut inner)
    }

    /// Completes the pending child-start/Running/Retiring publication, never a
    /// rollback or another epoch. No operation lock is acquired by a publisher.
    pub fn retry_publication(&self) -> Result<(), RuntimeError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| RuntimeError::Unavailable("publisher mutex"))?;
        self.flush(&mut inner)
    }
    fn flush(&self, inner: &mut PublicationState) -> Result<(), RuntimeError> {
        self.lease.verify(self.authority)?;
        let Some(next) = inner.pending.clone() else {
            if !self.authority.unchanged(&inner.durable)? {
                return Err(RuntimeError::Changed(
                    "publisher durable record epoch/revision",
                ));
            }
            return Ok(());
        };
        let observed = self
            .authority
            .read_record(&next.namespace)?
            .ok_or(RuntimeError::Changed("publisher record missing"))?;
        if observed == inner.durable {
            let published = self.authority.publish_record(Some(&inner.durable), &next)?;
            inner.finish(published)
        } else if observed.record == next {
            // A previous sync may have failed after replacement. Confirm the
            // exact forward record and sync it, without issuing another rev.
            self.authority.confirm_publication(&observed)?;
            inner.finish(observed)
        } else {
            Err(RuntimeError::Changed("publisher record epoch/revision"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        filesystem::FileFingerprint, InodeIdentity, InstanceId, Namespace, NativeBirth, PathBytes,
        ProcessId,
    };
    use super::*;
    use std::path::Path;
    fn state() -> PublicationState {
        let config_root = PathBytes::from_path(Path::new("/owned/config")).unwrap();
        let record = RuntimeRecord {
            schema_version: 1,
            namespace: Namespace::derive(501, &config_root, "profile").unwrap(),
            instance_uuid: InstanceId::new(),
            revision: 1,
            phase: RuntimePhase::Running,
            uid: 501,
            config_root,
            profile: "profile".into(),
            ready: true,
            process: Some(RuntimeProcess {
                pid: ProcessId::new(123).unwrap(),
                uid: 501,
                birth: NativeBirth::MacOs {
                    seconds: 1,
                    microseconds: 2,
                },
                executable: PathBytes::from_path(Path::new("/owned/chanvoy")).unwrap(),
                pid_namespace: None,
                mount_namespace: None,
            }),
            socket: Some(RuntimeSocket {
                path: PathBytes::from_path(Path::new("/owned/r/123.sock")).unwrap(),
                identity: InodeIdentity {
                    device: 1,
                    inode: 3,
                },
            }),
            build: None,
        };
        let bytes = record.encode().unwrap();
        PublicationState {
            durable: RecordSnapshot {
                record,
                bytes: bytes.clone(),
                fingerprint: FileFingerprint {
                    identity: InodeIdentity {
                        device: 1,
                        inode: 2,
                    },
                    uid: 501,
                    mode: 0o100600,
                    size: bytes.len() as u64,
                    modified_secs: 1,
                    modified_nanos: 0,
                },
            },
            pending: None,
        }
    }
    #[test]
    fn inspection_does_not_complete_pending_publication_or_adopt_external_state() {
        let mut state = state();
        assert!(state.inspect(&state.durable).unwrap().confirmed);
        let mut next = state.durable.record.clone();
        next.revision += 1;
        next.phase = RuntimePhase::Retiring;
        next.ready = false;
        state.stage(next.clone()).unwrap();
        let before = state.inspect(&state.durable).unwrap();
        assert_eq!(before.observed.phase, RuntimePhase::Running);
        assert_eq!(before.logical.phase, RuntimePhase::Retiring);
        assert!(!before.confirmed);
        let mut replaced = state.durable.clone();
        replaced.record = next.clone();
        let after = state.inspect(&replaced).unwrap();
        assert_eq!(after.observed.revision, next.revision);
        assert!(!after.confirmed);
        assert_eq!(state.pending, Some(next));
        assert_eq!(state.durable.record.phase, RuntimePhase::Running);
        replaced.record.instance_uuid = InstanceId::new();
        assert!(state.inspect(&replaced).is_err());
        let mut substituted = state.durable.clone();
        substituted.fingerprint.identity.inode += 1;
        assert!(state.inspect(&substituted).is_err());
    }

    #[test]
    fn retiring_failure_remains_closed_and_accepts_only_its_exact_forward_publication() {
        let mut state = state();
        let mut retiring = state.durable.record.clone();
        retiring.phase = RuntimePhase::Retiring;
        retiring.ready = false;
        retiring.revision += 1;
        state.stage(retiring.clone()).unwrap();
        // An error from synchronous storage leaves this pending value intact.
        assert_eq!(state.logical().phase, RuntimePhase::Retiring);
        assert!(state.stage(state.durable.record.clone()).is_err());
        let mut wrong = state.durable.clone();
        wrong.record = retiring.clone();
        wrong.record.instance_uuid = InstanceId::new();
        assert!(state.finish(wrong).is_err());
        assert_eq!(state.logical().phase, RuntimePhase::Retiring);
        let mut exact = state.durable.clone();
        exact.record = retiring;
        state.finish(exact).unwrap();
        let mut reopen = state.durable.record.clone();
        reopen.revision += 1;
        reopen.phase = RuntimePhase::Running;
        reopen.ready = true;
        assert!(state.stage(reopen).is_err());
    }
    #[test]
    fn publisher_refuses_stale_revision_new_epoch_and_process_socket_substitution() {
        for substitution in 0..4 {
            let mut state = state();
            let mut next = state.durable.record.clone();
            next.revision += 1;
            next.phase = RuntimePhase::Retiring;
            next.ready = false;
            match substitution {
                0 => next.revision -= 1,
                1 => next.instance_uuid = InstanceId::new(),
                2 => {
                    next.process.as_mut().unwrap().birth = NativeBirth::MacOs {
                        seconds: 8,
                        microseconds: 9,
                    }
                }
                _ => next.socket.as_mut().unwrap().identity.inode += 1,
            }
            assert!(state.stage(next).is_err());
            assert!(state.pending.is_none());
        }
        let mut state = state();
        state.durable.record.revision = u64::MAX;
        let mut next = state.durable.record.clone();
        next.revision = 0;
        next.phase = RuntimePhase::Retiring;
        next.ready = false;
        assert!(state.stage(next).is_err());
    }
}
