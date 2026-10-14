//! Bounded application claims, requiring independent native corroboration.
use super::{Namespace, PathBytes, RuntimeError, RECORD_MAX_BYTES, RECORD_VERSION};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use std::{os::unix::ffi::OsStrExt, path::Path};
use uuid::{Uuid, Variant, Version};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct InstanceId(Uuid);
impl InstanceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn uuid(self) -> Uuid {
        self.0
    }
}
impl Default for InstanceId {
    fn default() -> Self {
        Self::new()
    }
}
impl<'de> Deserialize<'de> for InstanceId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        let id = Uuid::parse_str(&text).map_err(|_| D::Error::custom("invalid instance UUID"))?;
        if id.get_version() != Some(Version::Random)
            || id.get_variant() != Variant::RFC4122
            || id.to_string() != text
        {
            return Err(D::Error::custom("canonical UUID v4 required"));
        }
        Ok(Self(id))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ProcessId(i32);
impl ProcessId {
    pub fn new(pid: u32) -> Result<Self, RuntimeError> {
        let pid = i32::try_from(pid).map_err(|_| RuntimeError::Invalid("PID range"))?;
        if pid <= 0 {
            return Err(RuntimeError::Invalid("positive PID required"));
        }
        Ok(Self(pid))
    }
    pub fn get(self) -> u32 {
        self.0 as u32
    }
}
impl<'de> Deserialize<'de> for ProcessId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Self::new(u32::deserialize(d)?).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePhase {
    Starting,
    Running,
    Retiring,
    Exited,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativePlatform {
    Linux,
    MacOs,
}
impl NativePlatform {
    pub fn current() -> Result<Self, RuntimeError> {
        #[cfg(target_os = "linux")]
        {
            Ok(Self::Linux)
        }
        #[cfg(target_os = "macos")]
        {
            Ok(Self::MacOs)
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            Err(RuntimeError::Unavailable("unsupported native platform"))
        }
    }
    pub fn socket_root_budget(self) -> usize {
        match self {
            Self::MacOs => 87,
            Self::Linux => 91,
        }
    }
}

pub fn child_socket_path(
    root: &Path,
    pid: ProcessId,
    platform: NativePlatform,
) -> Result<PathBytes, RuntimeError> {
    PathBytes::from_path(root)?;
    // sun_path minus slash, max positive i32 PID + .sock and terminating NUL.
    if root.as_os_str().as_bytes().len() > platform.socket_root_budget() {
        return Err(RuntimeError::Invalid(
            "socket root exceeds worst-case byte budget",
        ));
    }
    PathBytes::from_path(&root.join(format!("{}.sock", pid.get())))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NativeBirth {
    Linux { clock_ticks: u64 },
    MacOs { seconds: u64, microseconds: u32 },
}
impl NativeBirth {
    pub fn platform(&self) -> NativePlatform {
        match self {
            Self::Linux { .. } => NativePlatform::Linux,
            Self::MacOs { .. } => NativePlatform::MacOs,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InodeIdentity {
    pub device: u64,
    pub inode: u64,
}
impl InodeIdentity {
    fn validate(self) -> Result<(), RuntimeError> {
        if self.inode == 0 {
            return Err(RuntimeError::Invalid("zero inode identity"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProcess {
    pub pid: ProcessId,
    pub uid: u32,
    pub birth: NativeBirth,
    pub executable: PathBytes,
    #[serde(deserialize_with = "required_option")]
    pub pid_namespace: Option<InodeIdentity>,
    #[serde(deserialize_with = "required_option")]
    pub mount_namespace: Option<InodeIdentity>,
}
impl RuntimeProcess {
    fn validate(&self, uid: u32) -> Result<(), RuntimeError> {
        if self.uid != uid {
            return Err(RuntimeError::Invalid("process/account UID mismatch"));
        }
        match self.birth {
            NativeBirth::Linux { .. } => {
                self.pid_namespace
                    .ok_or(RuntimeError::Invalid("missing PID namespace"))?
                    .validate()?;
                self.mount_namespace
                    .ok_or(RuntimeError::Invalid("missing mount namespace"))?
                    .validate()?;
            }
            NativeBirth::MacOs { microseconds, .. } => {
                if microseconds >= 1_000_000
                    || self.pid_namespace.is_some()
                    || self.mount_namespace.is_some()
                {
                    return Err(RuntimeError::Invalid("macOS birth/namespace claim"));
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSocket {
    pub path: PathBytes,
    pub identity: InodeIdentity,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeBuildPin {
    pub version: String,
    pub commit: String,
    #[serde(deserialize_with = "required_option")]
    pub dirty: Option<bool>,
}
impl RuntimeBuildPin {
    fn validate(&self) -> Result<(), RuntimeError> {
        if self.version.is_empty()
            || !(self.commit == "unknown"
                || (self.commit.len() == 40 && self.commit.bytes().all(|b| b.is_ascii_hexdigit())))
        {
            return Err(RuntimeError::Invalid("build pin claim"));
        }
        Ok(())
    }
}

/// Decoding provides no kernel ownership, death or destructive authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRecord {
    pub schema_version: u16,
    pub namespace: Namespace,
    pub instance_uuid: InstanceId,
    pub revision: u64,
    pub phase: RuntimePhase,
    pub uid: u32,
    pub config_root: PathBytes,
    pub profile: String,
    #[serde(deserialize_with = "required_option")]
    pub process: Option<RuntimeProcess>,
    #[serde(deserialize_with = "required_option")]
    pub socket: Option<RuntimeSocket>,
    #[serde(deserialize_with = "required_option")]
    pub build: Option<RuntimeBuildPin>,
    pub ready: bool,
}
impl RuntimeRecord {
    pub fn validate(&self) -> Result<(), RuntimeError> {
        if self.schema_version != RECORD_VERSION {
            return Err(RuntimeError::Unavailable("record version"));
        }
        if self.namespace != Namespace::derive(self.uid, &self.config_root, &self.profile)? {
            return Err(RuntimeError::Invalid("record namespace/input mismatch"));
        }
        if self.ready != (self.phase == RuntimePhase::Running) {
            return Err(RuntimeError::Invalid("phase/readiness claim mismatch"));
        }
        if self.revision == 0
            && (self.phase != RuntimePhase::Starting
                || self.process.is_some()
                || self.socket.is_some())
        {
            return Err(RuntimeError::Invalid("launcher revision zero claim"));
        }
        if matches!(self.phase, RuntimePhase::Running | RuntimePhase::Retiring)
            && (self.process.is_none() || self.socket.is_none())
        {
            return Err(RuntimeError::Invalid(
                "live phase lacks process/socket claim",
            ));
        }
        if let Some(process) = &self.process {
            process.validate(self.uid)?;
        }
        if let Some(socket) = &self.socket {
            socket.identity.validate()?;
            let process = self
                .process
                .as_ref()
                .ok_or(RuntimeError::Invalid("socket without process"))?;
            let path = socket.path.to_path();
            let root = path.parent().ok_or(RuntimeError::Invalid("socket root"))?;
            if socket.path != child_socket_path(root, process.pid, process.birth.platform())? {
                return Err(RuntimeError::Invalid("socket is not the child PID locator"));
            }
        }
        if let Some(build) = &self.build {
            build.validate()?;
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, RuntimeError> {
        self.validate()?;
        let bytes =
            serde_json::to_vec(self).map_err(|_| RuntimeError::Invalid("record encoding"))?;
        if bytes.len() > RECORD_MAX_BYTES {
            return Err(RuntimeError::Invalid("record byte cap"));
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8], expected: &Namespace, uid: u32) -> Result<Self, RuntimeError> {
        if bytes.len() > RECORD_MAX_BYTES {
            return Err(RuntimeError::Invalid("record byte cap"));
        }
        // Parser errors can include untrusted keys/content: never relay them.
        let record: Self =
            serde_json::from_slice(bytes).map_err(|_| RuntimeError::Invalid("record schema"))?;
        record.validate()?;
        if &record.namespace != expected || record.uid != uid {
            return Err(RuntimeError::Invalid(
                "record does not match selected namespace/account",
            ));
        }
        Ok(record)
    }
    pub fn next_revision(&self) -> Result<u64, RuntimeError> {
        self.revision
            .checked_add(1)
            .ok_or(RuntimeError::Invalid("record revision overflow"))
    }
}
fn required_option<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    fn running() -> RuntimeRecord {
        let config_root = PathBytes::from_path(Path::new("/owned/config")).unwrap();
        RuntimeRecord {
            schema_version: 1,
            namespace: Namespace::derive(501, &config_root, "été").unwrap(),
            instance_uuid: InstanceId::new(),
            revision: 1,
            phase: RuntimePhase::Running,
            uid: 501,
            config_root,
            profile: "été".into(),
            ready: true,
            process: Some(RuntimeProcess {
                pid: ProcessId::new(123).unwrap(),
                uid: 501,
                birth: NativeBirth::Linux { clock_ticks: 9 },
                executable: PathBytes::from_path(Path::new("/native/chanvoy")).unwrap(),
                pid_namespace: Some(InodeIdentity {
                    device: 1,
                    inode: 2,
                }),
                mount_namespace: Some(InodeIdentity {
                    device: 1,
                    inode: 3,
                }),
            }),
            socket: Some(RuntimeSocket {
                path: PathBytes::from_path(Path::new("/owned/r/123.sock")).unwrap(),
                identity: InodeIdentity {
                    device: 4,
                    inode: 8,
                },
            }),
            build: Some(RuntimeBuildPin {
                version: "0.3.3".into(),
                commit: "a".repeat(40),
                dirty: None,
            }),
        }
    }
    fn rejected(v: &Value, r: &RuntimeRecord) {
        assert!(
            RuntimeRecord::decode(&serde_json::to_vec(v).unwrap(), &r.namespace, r.uid).is_err()
        );
    }
    #[test]
    fn unavailable_dirty_survives_without_becoming_a_match() {
        let r = running();
        assert_eq!(
            RuntimeRecord::decode(&r.encode().unwrap(), &r.namespace, r.uid).unwrap(),
            r
        );
        assert!(r.build.unwrap().dirty.is_none());
    }
    #[test]
    fn unknown_fields_and_missing_nullable_evidence_are_refused_at_every_level() {
        let r = running();
        let original = serde_json::to_value(&r).unwrap();
        for p in [
            "",
            "/process",
            "/process/birth",
            "/socket",
            "/socket/identity",
            "/build",
        ] {
            let mut v = original.clone();
            v.pointer_mut(p)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert("unknown".into(), json!(1));
            rejected(&v, &r);
        }
        for (p, k) in [
            ("", "process"),
            ("", "socket"),
            ("", "build"),
            ("/process", "pid_namespace"),
            ("/process", "mount_namespace"),
            ("/build", "dirty"),
        ] {
            let mut v = original.clone();
            v.pointer_mut(p).unwrap().as_object_mut().unwrap().remove(k);
            rejected(&v, &r);
        }
    }
    #[test]
    fn namespace_uid_epoch_pid_locator_and_birth_claims_are_not_interchangeable() {
        let r = running();
        let original = serde_json::to_value(&r).unwrap();
        for (p, bad) in [
            ("/schema_version", json!(2)),
            ("/uid", json!(502)),
            ("/profile", json!("different")),
            ("/instance_uuid", json!(Uuid::nil().to_string())),
            ("/process/pid", json!(0)),
            ("/process/pid", json!(2147483648u64)),
            ("/process/uid", json!(502)),
            ("/process/pid_namespace", Value::Null),
            ("/socket/identity/inode", json!(0)),
            (
                "/socket/path",
                json!(PathBytes::from_path(Path::new("/owned/r/124.sock"))
                    .unwrap()
                    .hex()),
            ),
            ("/ready", json!(false)),
            ("/build/commit", json!("bad")),
        ] {
            let mut v = original.clone();
            *v.pointer_mut(p).unwrap() = bad;
            rejected(&v, &r);
        }
        assert!(RuntimeRecord::decode(&r.encode().unwrap(), &r.namespace, 502).is_err());
    }
    #[test]
    fn launching_retiring_exited_are_never_ready_and_revision_never_wraps() {
        let mut r = running();
        r.phase = RuntimePhase::Retiring;
        r.ready = false;
        r.validate().unwrap();
        r.phase = RuntimePhase::Exited;
        r.validate().unwrap();
        r.revision = u64::MAX;
        assert!(r.next_revision().is_err());
        r.phase = RuntimePhase::Starting;
        r.revision = 0;
        assert!(r.validate().is_err());
        r.process = None;
        r.socket = None;
        r.validate().unwrap();
        r.ready = true;
        assert!(r.validate().is_err());
    }
    #[test]
    fn caps_before_parse_and_rejects_duplicate_keys_and_trailing_data() {
        let r = running();
        let encoded = r.encode().unwrap();
        assert!(
            RuntimeRecord::decode(&vec![b' '; RECORD_MAX_BYTES + 1], &r.namespace, 501).is_err()
        );
        let mut trailing = encoded.clone();
        trailing.extend_from_slice(b" null");
        assert!(RuntimeRecord::decode(&trailing, &r.namespace, 501).is_err());
        let duplicate = String::from_utf8(encoded)
            .unwrap()
            .replacen("{", "{\"revision\":1,", 1);
        assert!(RuntimeRecord::decode(duplicate.as_bytes(), &r.namespace, 501).is_err());
    }
    #[test]
    fn full_socket_bytes_include_max_pid_and_nul_on_both_platforms() {
        for (platform, budget, capacity) in [
            (NativePlatform::MacOs, 87, 104),
            (NativePlatform::Linux, 91, 108),
        ] {
            let root = format!("/{}", "x".repeat(budget - 1));
            let p = child_socket_path(
                Path::new(&root),
                ProcessId::new(i32::MAX as u32).unwrap(),
                platform,
            )
            .unwrap()
            .to_path();
            assert_eq!(p.as_os_str().as_bytes().len() + 1, capacity);
            assert!(child_socket_path(
                Path::new(&(root + "x")),
                ProcessId::new(1).unwrap(),
                platform
            )
            .is_err());
        }
    }
}
