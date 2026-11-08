//! Correlate the retained listening descriptor with its current pathname using
//! a native-peer-checked loopback exchange. Socket-object and filesystem inode
//! identities are not compared: those are different objects on Linux.
use chanvoy_core::runtime::{
    native_peer_pid, InodeIdentity, PathBytes, RuntimeError, RuntimeProcess, RuntimeSocket,
};
use std::os::fd::AsFd;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

pub(crate) async fn prove(
    listener: &UnixListener,
    path: &std::path::Path,
    process: &RuntimeProcess,
    deadline: std::time::Instant,
) -> Result<RuntimeSocket, RuntimeError> {
    if std::time::Instant::now() >= deadline {
        return Err(RuntimeError::Unavailable("listener proof deadline"));
    }
    if listener.local_addr()?.as_pathname() != Some(path) {
        return Err(RuntimeError::Changed("managed listener descriptor locator"));
    }
    process.revalidate()?;
    let before = capture_path(path, process.uid)?;
    let challenge = *uuid::Uuid::new_v4().as_bytes();
    let connect = async {
        let mut client = UnixStream::connect(path).await?;
        if native_peer_pid(client.as_fd())? != process.pid {
            return Err(RuntimeError::Changed("listener probe native peer"));
        }
        client.write_all(&challenge).await?;
        let mut echo = [0u8; 16];
        client.read_exact(&mut echo).await?;
        if echo != challenge {
            return Err(RuntimeError::Changed("listener probe response"));
        }
        Ok::<_, RuntimeError>(())
    };
    let accept = async {
        loop {
            let (mut accepted, _) = listener.accept().await?;
            // Pre-readiness connections from other processes are not admitted
            // application requests, and cannot hold the proof waiting on input.
            if native_peer_pid(accepted.as_fd())? != process.pid {
                continue;
            }
            let mut observed = [0u8; 16];
            accepted.read_exact(&mut observed).await?;
            if observed != challenge {
                return Err(RuntimeError::Changed("listener probe challenge"));
            }
            accepted.write_all(&challenge).await?;
            return Ok::<_, RuntimeError>(());
        }
    };
    // No spawned task can finish a late probe after this future is canceled.
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        tokio::try_join!(connect, accept)
    })
    .await
    .map_err(|_| RuntimeError::Unavailable("listener proof deadline"))??;
    process.revalidate()?;
    let after = capture_path(path, process.uid)?;
    if std::time::Instant::now() >= deadline {
        return Err(RuntimeError::Unavailable("listener proof deadline"));
    }
    if after != before {
        return Err(RuntimeError::Changed("listener pathname during proof"));
    }
    Ok(after)
}

pub(crate) fn capture_path(
    path: &std::path::Path,
    uid: u32,
) -> Result<RuntimeSocket, RuntimeError> {
    let meta = std::fs::symlink_metadata(path)?;
    if !meta.file_type().is_socket()
        || meta.uid() != uid
        || meta.mode() & 0o7777 != 0o600
        || meta.nlink() != 1
    {
        return Err(RuntimeError::Invalid(
            "managed listener pathname owner/type/mode",
        ));
    }
    Ok(RuntimeSocket {
        path: PathBytes::from_path(path)?,
        identity: InodeIdentity {
            device: meta.dev(),
            inode: meta.ino(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[tokio::test]
    async fn retained_listener_proves_path_and_refuses_same_process_replacement() {
        let fixture = tempfile::Builder::new()
            .prefix("cv-listener-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = fixture.path().join("s");
        let original = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let process = RuntimeProcess::observe_current().unwrap();
        let captured = prove(
            &original,
            &path,
            &process,
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(
            captured.identity.inode,
            std::fs::symlink_metadata(&path).unwrap().ino()
        );
        std::fs::rename(&path, fixture.path().join("old")).unwrap();
        let replacement = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        // Even the same process/PID at the same path cannot substitute for the
        // retained original listener. Its accept cannot receive this connect.
        assert!(prove(
            &original,
            &path,
            &process,
            std::time::Instant::now() + std::time::Duration::from_millis(100)
        )
        .await
        .is_err());
        drop(replacement);
        drop(original);
    }
    #[tokio::test]
    async fn canceled_listener_proof_leaves_no_late_exchange_or_path_mutation() {
        let fixture = tempfile::Builder::new()
            .prefix("cv-cancel-")
            .tempdir_in("/tmp")
            .unwrap();
        let path = fixture.path().join("s");
        let original = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::rename(&path, fixture.path().join("old")).unwrap();
        let replacement = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let process = RuntimeProcess::observe_current().unwrap();
        let before = capture_path(&path, process.uid).unwrap();
        // Drive the real proof until its connection reaches the replacement.
        // The original listener cannot accept it, so the proof is still pending.
        let mut proof = Box::pin(prove(
            &original,
            &path,
            &process,
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        ));
        let (mut connection, _) = tokio::select! {
            result = &mut proof => panic!("replacement proof unexpectedly completed: {result:?}"),
            accepted = replacement.accept() => accepted.unwrap(),
        };
        drop(proof);
        // The unspawned connect/accept futures are gone. Drain any bytes already
        // written and require EOF; no late task can keep the probe connection.
        let mut bytes = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            connection.read_to_end(&mut bytes),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(bytes.len() <= 16);
        assert_eq!(capture_path(&path, process.uid).unwrap(), before);
        assert_eq!(
            original.local_addr().unwrap().as_pathname(),
            Some(path.as_path())
        );
    }
}
