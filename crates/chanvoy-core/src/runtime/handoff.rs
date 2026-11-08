//! Typed projections of the strict managed handoff roots. Successful decoding
//! proves only the wire contract; descriptor inheritance and native identity
//! must be corroborated separately before constructing a child publisher.
use super::control::{ControlDocument, ControlError, ControlRoot};
use super::{
    InodeIdentity, InstanceId, Namespace, PathBytes, RuntimeBuildPin, RuntimeError, RuntimePhase,
    RuntimeProcess, RuntimeRecord, RuntimeSocket,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBootstrap {
    pub schema_version: u16,
    pub kind: String,
    pub namespace: Namespace,
    pub instance_uuid: InstanceId,
    pub launcher_revision: u64,
    pub uid: u32,
    pub config_root: PathBytes,
    pub profile: String,
    pub socket_root: PathBytes,
    pub lease_descriptor: i32,
    pub lease_identity: InodeIdentity,
    pub validated_username: String,
    pub validated_user_id: String,
    pub profile_fingerprint: String,
    pub issued_at: u64,
    pub launcher_process: RuntimeProcess,
    pub bootstrap_nonce: String,
}

// Never print nonce, private paths, profile fingerprint, or identity payload.
impl std::fmt::Debug for RuntimeBootstrap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeBootstrap").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeReadiness {
    pub schema_version: u16,
    pub kind: String,
    pub namespace: Namespace,
    pub instance_uuid: InstanceId,
    pub revision: u64,
    pub process: RuntimeProcess,
    pub socket: RuntimeSocket,
    pub build: RuntimeBuildPin,
    pub handoff_validated: bool,
    pub listener_bound: bool,
    pub publication_confirmed: bool,
    pub ready: bool,
}

macro_rules! strict_document {
    ($ty:ty, $root:expr) => {
        impl $ty {
            pub fn decode(bytes: &[u8]) -> Result<Self, ControlError> {
                let document = ControlDocument::decode($root, bytes)?;
                serde_json::from_value(document.value().clone())
                    .map_err(|_| ControlError::InvalidContract)
            }
            pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
                let bytes = serde_json::to_vec(self).map_err(|_| ControlError::InvalidContract)?;
                ControlDocument::decode($root, &bytes)?.encode()
            }
        }
    };
}
strict_document!(RuntimeBootstrap, ControlRoot::Bootstrap);
strict_document!(RuntimeReadiness, ControlRoot::Readiness);

impl RuntimeBootstrap {
    /// Native v1 requires 64 lowercase hex characters. Two independent v4 UUIDs
    /// provide 244 random bits; the legacy hyphenated UUID nonce is not this wire format.
    pub fn fresh_nonce() -> String {
        format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        )
    }

    /// Check the handoff against the exact launcher record and independently
    /// loaded child context. This does not prove that an inherited descriptor
    /// shares the launcher's open file description.
    pub fn check_context(
        &self,
        starting: &RuntimeRecord,
        child: &RuntimeProcess,
        config_root: &PathBytes,
        profile: &crate::Profile,
        nonce: &str,
        now: u64,
    ) -> Result<(), RuntimeError> {
        starting.validate()?;
        // Also validate values when this object was constructed in memory.
        self.encode()
            .map_err(|_| RuntimeError::Invalid("bootstrap contract"))?;
        let age = now
            .checked_sub(self.issued_at)
            .ok_or(RuntimeError::Invalid("bootstrap issued in future"))?;
        if age > crate::BOOTSTRAP_MAX_AGE_SECS || self.bootstrap_nonce != nonce {
            return Err(RuntimeError::Invalid("bootstrap age/nonce"));
        }
        if self.uid != child.uid
            || self.launcher_process.uid != child.uid
            || self.launcher_process.pid == child.pid
            || self.launcher_process.executable != child.executable
            || self.launcher_process.birth.platform() != child.birth.platform()
            || self.launcher_process.pid_namespace != child.pid_namespace
            || self.launcher_process.mount_namespace != child.mount_namespace
            || self.config_root != *config_root
            || self.profile != profile.name
            || self.validated_username != profile.bot_username
            || self.profile_fingerprint != crate::compute_profile_fingerprint(profile)
            || self.namespace != Namespace::derive(child.uid, config_root, &profile.name)?
        {
            return Err(RuntimeError::Changed("bootstrap child context"));
        }
        if starting.phase != RuntimePhase::Starting
            || starting.revision != 0
            || starting.process.is_some()
            || starting.ready
            || starting.namespace != self.namespace
            || starting.instance_uuid != self.instance_uuid
            || starting.uid != self.uid
            || starting.config_root != self.config_root
            || starting.profile != self.profile
        {
            return Err(RuntimeError::Changed("bootstrap launcher record"));
        }
        Ok(())
    }
}

impl RuntimeReadiness {
    /// Match a readiness claim to the parent's exact observed record. Native
    /// peer/process/listener/lease proof is still required before handoff ends.
    pub fn check_record(&self, record: &RuntimeRecord) -> Result<(), RuntimeError> {
        record.validate()?;
        self.encode()
            .map_err(|_| RuntimeError::Invalid("readiness contract"))?;
        if record.phase != RuntimePhase::Running
            || !record.ready
            || record.namespace != self.namespace
            || record.instance_uuid != self.instance_uuid
            || record.revision != self.revision
            || record.process.as_ref() != Some(&self.process)
            || record.socket.as_ref() != Some(&self.socket)
            || record.build.as_ref() != Some(&self.build)
        {
            return Err(RuntimeError::Changed("readiness record"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_native_nonce_satisfies_bootstrap_contract() {
        let mut handoff = RuntimeBootstrap::decode(&fixture("runtime_bootstrap")).unwrap();
        handoff.bootstrap_nonce = RuntimeBootstrap::fresh_nonce();
        assert!(handoff.encode().is_ok());
        handoff.bootstrap_nonce = crate::generate_nonce();
        assert!(handoff.encode().is_err());
    }
    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(
            format!("../../schemas/runtime/control/v1/fixtures/chanvoy-runtime-control-v1/{name}-positive.json")
        )).unwrap()
    }
    #[test]
    fn bootstrap_projection_stays_strict_and_debug_redacts_nonce() {
        let handoff = RuntimeBootstrap::decode(&fixture("runtime_bootstrap")).unwrap();
        assert_eq!(
            RuntimeBootstrap::decode(&handoff.encode().unwrap())
                .unwrap()
                .bootstrap_nonce,
            handoff.bootstrap_nonce
        );
        let debug = format!("{handoff:?}");
        assert!(!debug.contains(&handoff.bootstrap_nonce));
        assert!(!debug.contains(&handoff.validated_username));
        let mut invalid = handoff.clone();
        invalid.lease_descriptor = 0;
        assert!(invalid.encode().is_err());
        let mut duplicate = fixture("runtime_bootstrap");
        duplicate.splice(1..1, b"\"schema_version\":1,".iter().copied());
        assert!(RuntimeBootstrap::decode(&duplicate).is_err());
    }
    #[test]
    fn readiness_requires_all_success_claims_but_they_do_not_prove_native_facts() {
        let ready = RuntimeReadiness::decode(&fixture("runtime_readiness")).unwrap();
        assert!(ready.encode().is_ok());
        let mut invalid = ready;
        invalid.publication_confirmed = false;
        assert!(invalid.encode().is_err());
    }
    #[test]
    fn bootstrap_rejects_substituted_context_epoch_nonce_and_time() {
        let mut handoff = RuntimeBootstrap::decode(&fixture("runtime_bootstrap")).unwrap();
        let profile: crate::Profile = serde_json::from_value(serde_json::json!({
            "name": "fixture", "role": "test", "scope": "test",
            "provider": "mattermost", "bot_username": "synthetic-bot",
            "server_url": "https://invalid.example", "env_name": "TEST_TOKEN"
        }))
        .unwrap();
        handoff.profile_fingerprint = crate::compute_profile_fingerprint(&profile);
        let starting = RuntimeRecord {
            schema_version: 1,
            namespace: handoff.namespace.clone(),
            instance_uuid: handoff.instance_uuid,
            revision: 0,
            phase: RuntimePhase::Starting,
            uid: handoff.uid,
            config_root: handoff.config_root.clone(),
            profile: handoff.profile.clone(),
            process: None,
            socket: None,
            build: None,
            ready: false,
        };
        let mut child = handoff.launcher_process.clone();
        child.pid = super::super::ProcessId::new(124).unwrap();
        let check =
            |h: &RuntimeBootstrap,
             r: &RuntimeRecord,
             c: &RuntimeProcess,
             p: &crate::Profile,
             nonce: &str,
             now| { h.check_context(r, c, &handoff.config_root, p, nonce, now) };
        assert!(check(
            &handoff,
            &starting,
            &child,
            &profile,
            &handoff.bootstrap_nonce,
            101
        )
        .is_ok());
        assert!(check(&handoff, &starting, &child, &profile, "wrong", 101).is_err());
        assert!(check(
            &handoff,
            &starting,
            &child,
            &profile,
            &handoff.bootstrap_nonce,
            99
        )
        .is_err());
        assert!(check(
            &handoff,
            &starting,
            &child,
            &profile,
            &handoff.bootstrap_nonce,
            161
        )
        .is_err());
        let mut changed = starting.clone();
        changed.instance_uuid = InstanceId::new();
        assert!(check(
            &handoff,
            &changed,
            &child,
            &profile,
            &handoff.bootstrap_nonce,
            101
        )
        .is_err());
        let mut changed = profile.clone();
        changed.env_name = "OTHER_TOKEN".into();
        assert!(check(
            &handoff,
            &starting,
            &child,
            &changed,
            &handoff.bootstrap_nonce,
            101
        )
        .is_err());
        let mut changed = child.clone();
        changed.uid += 1;
        assert!(check(
            &handoff,
            &starting,
            &changed,
            &profile,
            &handoff.bootstrap_nonce,
            101
        )
        .is_err());
        changed = child.clone();
        changed.mount_namespace = Some(InodeIdentity {
            device: 1,
            inode: 9,
        });
        assert!(check(
            &handoff,
            &starting,
            &changed,
            &profile,
            &handoff.bootstrap_nonce,
            101
        )
        .is_err());
        assert!(check(
            &handoff,
            &starting,
            &handoff.launcher_process,
            &profile,
            &handoff.bootstrap_nonce,
            101
        )
        .is_err());
    }

    #[test]
    fn readiness_rejects_record_epoch_revision_process_and_phase_changes() {
        let ready = RuntimeReadiness::decode(&fixture("runtime_readiness")).unwrap();
        let record = RuntimeRecord {
            schema_version: 1,
            namespace: ready.namespace.clone(),
            instance_uuid: ready.instance_uuid,
            revision: ready.revision,
            phase: RuntimePhase::Running,
            uid: ready.process.uid,
            config_root: PathBytes::from_path(std::path::Path::new("/owned/config")).unwrap(),
            profile: "fixture".into(),
            process: Some(ready.process.clone()),
            socket: Some(ready.socket.clone()),
            build: Some(ready.build.clone()),
            ready: true,
        };
        assert!(ready.check_record(&record).is_ok());
        for mutation in 0..8 {
            let mut changed = record.clone();
            match mutation {
                0 => changed.revision += 1,
                1 => changed.instance_uuid = InstanceId::new(),
                2 => {
                    changed.process.as_mut().unwrap().pid =
                        super::super::ProcessId::new(321).unwrap()
                }
                3 => changed.phase = RuntimePhase::Retiring,
                4 => changed.socket.as_mut().unwrap().identity.inode += 1,
                5 => changed.uid += 1,
                6 => {
                    changed.config_root =
                        PathBytes::from_path(std::path::Path::new("/other/config")).unwrap()
                }
                _ => changed.profile = "other".into(),
            }
            assert!(ready.check_record(&changed).is_err());
        }
    }
}
