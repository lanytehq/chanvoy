//! Canonical managed-child argv. These markers are discovery hints until
//! corroborated against the exact record, native process and inherited handoff.
use super::{InstanceId, Namespace, PathBytes, RuntimeError};
use std::ffi::{OsStr, OsString};

#[derive(Clone, PartialEq, Eq)]
pub struct RuntimeLaunchMarker {
    pub namespace: Namespace,
    pub instance_uuid: InstanceId,
    pub config_root: PathBytes,
    pub profile: String,
}
impl RuntimeLaunchMarker {
    pub fn arguments(&self, executable: &OsStr) -> Result<Vec<OsString>, RuntimeError> {
        if self.profile.is_empty() || self.profile.as_bytes().contains(&0) {
            return Err(RuntimeError::Invalid("managed launch profile"));
        }
        // PathBytes is a validated lower-case hex string, preserving non-UTF8
        // canonical roots without an argv/environment decoding convention.
        let config_hex = serde_json::to_value(&self.config_root)
            .map_err(|_| RuntimeError::Invalid("managed launch config root"))?;
        let config_hex = config_hex
            .as_str()
            .ok_or(RuntimeError::Invalid("managed launch config root"))?;
        Ok(vec![
            executable.to_os_string(),
            "--profile".into(),
            self.profile.clone().into(),
            "daemon".into(),
            "serve".into(),
            "--runtime-namespace".into(),
            self.namespace.hex().into(),
            "--runtime-instance".into(),
            self.instance_uuid.uuid().to_string().into(),
            "--runtime-config-root".into(),
            config_hex.into(),
        ])
    }
    pub fn check_arguments(
        &self,
        executable: &OsStr,
        arguments: &[OsString],
    ) -> Result<(), RuntimeError> {
        if self.arguments(executable)? != arguments {
            return Err(RuntimeError::Changed("managed native argv binding"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::ffi::OsStringExt;
    #[test]
    fn marker_binds_exact_epoch_profile_and_lossless_config_without_duplicate_flags() {
        let path = std::path::PathBuf::from(OsString::from_vec(b"/owned/\xff".to_vec()));
        let config_root = PathBytes::from_path(&path).unwrap();
        let marker = RuntimeLaunchMarker {
            namespace: Namespace::derive(501, &config_root, "fixture").unwrap(),
            instance_uuid: InstanceId::new(),
            config_root,
            profile: "fixture".into(),
        };
        let executable = OsStr::new("/native/chanvoy");
        let args = marker.arguments(executable).unwrap();
        marker.check_arguments(executable, &args).unwrap();
        for index in [2, 6, 8, 10] {
            let mut changed = args.clone();
            changed[index] = "other".into();
            assert!(marker.check_arguments(executable, &changed).is_err());
        }
        let mut changed = args;
        changed.extend([OsString::from("--profile"), OsString::from("fixture")]);
        assert!(marker.check_arguments(executable, &changed).is_err());
    }
}
