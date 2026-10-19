//! Test-owned foreground child and socket readiness diagnostics.

use std::{
    io,
    path::Path,
    process::ExitStatus,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, Command},
    task::JoinHandle,
    time::{timeout_at, Instant as Deadline},
};

struct Capture {
    task: JoinHandle<io::Result<()>>,
    bytes: Arc<Mutex<Vec<u8>>>,
}
impl Capture {
    fn start(mut reader: impl AsyncRead + Unpin + Send + 'static) -> Self {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let output = Arc::clone(&bytes);
        let task = tokio::spawn(async move {
            let mut buffer = [0; 4096];
            loop {
                let read = reader.read(&mut buffer).await?;
                if read == 0 {
                    return Ok(());
                }
                output
                    .lock()
                    .expect("capture buffer")
                    .extend_from_slice(&buffer[..read]);
            }
        });
        Self { task, bytes }
    }
    async fn collect(
        mut self,
        deadline: &mut Deadline,
        cleanup_started: &mut bool,
        label: &str,
        errors: &mut Vec<String>,
        failure_observed_at: &mut Option<Instant>,
    ) -> Vec<u8> {
        let result = timeout_at(*deadline, &mut self.task).await;
        let observed_at = Instant::now();
        if !matches!(&result, Ok(Ok(Ok(())))) {
            failure_observed_at.get_or_insert(observed_at);
        }
        match result {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => errors.push(format!("{label} capture: {error}")),
            Ok(Err(error)) => errors.push(format!("{label} collector join: {error}")),
            Err(_) => {
                errors.push(format!("{label} capture deadline"));
                if !*cleanup_started {
                    *deadline = Deadline::now() + Duration::from_secs(5);
                    *cleanup_started = true;
                }
                self.task.abort();
                // Abort alone is not collection. Preserve partial diagnostics.
                match timeout_at(*deadline, &mut self.task).await {
                    Ok(Err(error)) if error.is_cancelled() => {}
                    Ok(Ok(Ok(()))) => {}
                    Ok(other) => errors.push(format!("{label} aborted collector: {other:?}")),
                    Err(_) => errors.push(format!("{label} collector termination unconfirmed")),
                }
            }
        }
        // A failed join may leave a reader holding the Arc. Partial bytes
        // remain evidence, never a reason to panic before reporting cleanup.
        let bytes = self.bytes.lock().expect("capture buffer").clone();
        bytes
    }
}

pub struct Foreground {
    child: Child,
    stdout: Capture,
    stderr: Capture,
    observations: Arc<Mutex<Vec<String>>>,
    pub spawned_at: Instant,
}
#[derive(Debug)]
pub struct Collected {
    pub status: Option<ExitStatus>,
    pub forced: bool,
    pub errors: Vec<String>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub observations: Vec<String>,
    pub cleanup_deadline: Deadline,
    pub failure_observed_at: Option<Instant>,
    pub cleanup_started: bool,
}
impl Foreground {
    pub fn spawn(command: &mut Command) -> io::Result<Self> {
        use std::process::Stdio;
        let mut child = command
            .kill_on_drop(true)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        let spawned_at = Instant::now();
        let stdout = Capture::start(child.stdout.take().expect("piped stdout"));
        let stderr = Capture::start(child.stderr.take().expect("piped stderr"));
        Ok(Self {
            child,
            stdout,
            stderr,
            observations: Arc::new(Mutex::new(Vec::new())),
            spawned_at,
        })
    }

    pub async fn connect_ready(
        &mut self,
        path: &Path,
        started: Instant,
    ) -> Result<(UnixStream, Duration), String> {
        let deadline = Deadline::from_std(started + Duration::from_secs(4));
        loop {
            if let Some(status) = self
                .child
                .try_wait()
                .map_err(|e| format!("readiness child-status: {e}"))?
            {
                return Err(format!("readiness early child exit: {status}"));
            }
            if Deadline::now() >= deadline {
                return Err("readiness-connect deadline (4s)".into());
            }
            let attempt = timeout_at(deadline, UnixStream::connect(path))
                .await
                .map_err(|_| "readiness-connect deadline (4s)".to_string())?;
            let elapsed = started.elapsed();
            if Deadline::now() >= deadline {
                return Err("readiness-connect deadline (4s)".into());
            }
            match attempt {
                Ok(stream) => {
                    self.observations
                        .lock()
                        .expect("observations")
                        .push(format!("connected at {elapsed:?}"));
                    return Ok((stream, elapsed));
                }
                Err(error) => {
                    self.observations
                        .lock()
                        .expect("observations")
                        .push(format!("connect at {elapsed:?}: {error}"));
                    if !matches!(
                        error.kind(),
                        io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                    ) {
                        return Err(format!("readiness-connect: {error}"));
                    }
                }
            }
            // This is the existing observation cadence, bounded by the same
            // absolute startup deadline. No RPC is retried.
            tokio::time::sleep_until((Deadline::now() + Duration::from_millis(20)).min(deadline))
                .await;
        }
    }

    pub async fn collect(mut self, deadline: Deadline, force: bool) -> Collected {
        let mut errors = Vec::new();
        let mut failure_observed_at = None;
        let mut forced = force;
        let mut capture_deadline = deadline;
        let mut cleanup_started = force;
        if force {
            if let Err(error) = self.child.start_kill() {
                errors.push(format!("failure cleanup kill: {error}"));
            }
        }
        let initial_wait = timeout_at(deadline, self.child.wait()).await;
        let wait_observed_at = Instant::now();
        if !matches!(&initial_wait, Ok(Ok(_))) {
            failure_observed_at = Some(wait_observed_at);
        }
        let mut status = match initial_wait {
            Ok(Ok(status)) => Some(status),
            Ok(Err(error)) => {
                errors.push(format!("child collection: {error}"));
                None
            }
            Err(_) => {
                errors.push("child collection deadline".into());
                None
            }
        };
        if status.is_none() {
            forced = true;
            // A failed normal shutdown gets one bounded failure teardown.
            // An already-failed test must not gain another five-second window.
            if !force {
                capture_deadline = Deadline::now() + Duration::from_secs(5);
                cleanup_started = true;
            }
            if let Err(error) = self.child.start_kill() {
                errors.push(format!("failure cleanup kill: {error}"));
            }
            status = match timeout_at(capture_deadline, self.child.wait()).await {
                Ok(Ok(status)) => Some(status),
                Ok(Err(error)) => {
                    errors.push(format!("failure child collection: {error}"));
                    None
                }
                Err(_) => {
                    errors.push("failure child collection deadline; exit unconfirmed".into());
                    None
                }
            };
        }
        let stdout = self
            .stdout
            .collect(
                &mut capture_deadline,
                &mut cleanup_started,
                "stdout",
                &mut errors,
                &mut failure_observed_at,
            )
            .await;
        let stderr = self
            .stderr
            .collect(
                &mut capture_deadline,
                &mut cleanup_started,
                "stderr",
                &mut errors,
                &mut failure_observed_at,
            )
            .await;
        let observations = Arc::try_unwrap(self.observations)
            .unwrap_or_else(|shared| Mutex::new(shared.lock().expect("observations").clone()))
            .into_inner()
            .expect("observations");
        Collected {
            status,
            forced,
            errors,
            stdout,
            stderr,
            observations,
            cleanup_deadline: capture_deadline,
            failure_observed_at,
            cleanup_started,
        }
    }
}

pub async fn rpc_on_stream(
    mut stream: UnixStream,
    method: &str,
) -> Result<chanvoy_core::JsonRpcResponse, String> {
    let request = chanvoy_core::rpc_request(method, serde_json::json!({}));
    let body = serde_json::to_string(&request).map_err(|e| format!("{method} encode: {e}"))?;
    stream
        .write_all(format!("{body}\n").as_bytes())
        .await
        .map_err(|e| format!("{method} write: {e}"))?;
    let mut line = String::new();
    let count = BufReader::new(stream)
        .read_line(&mut line)
        .await
        .map_err(|e| format!("{method} read: {e}"))?;
    if count == 0 {
        return Err(format!("{method} read: EOF before response"));
    }
    serde_json::from_str(&line).map_err(|e| format!("{method} decode: {e}"))
}

pub async fn rpc_once(path: &Path, method: &str) -> Result<chanvoy_core::JsonRpcResponse, String> {
    let stream = UnixStream::connect(path)
        .await
        .map_err(|e| format!("{method} connect: {e}"))?;
    rpc_on_stream(stream, method).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{net::UnixListener, sync::oneshot};

    fn sleeper() -> Foreground {
        let mut command = Command::new("/bin/sh");
        command.env_clear().args([
            "-c",
            "printf synthetic-stdout; printf synthetic-stderr >&2; exec /bin/sleep 30",
        ]);
        Foreground::spawn(&mut command).unwrap()
    }

    // Publish a stream socket pathname without ever making it a listener.
    // Creating and dropping UnixListener instead leaves a window in which a
    // concurrently spawned child can inherit a live listener until exec.
    fn bind_without_listening(path: &Path) -> std::os::fd::OwnedFd {
        use std::os::{
            fd::{AsRawFd, FromRawFd, OwnedFd},
            unix::ffi::OsStrExt,
        };
        let bytes = path.as_os_str().as_bytes();
        // SAFETY: sockaddr_un is a plain C address structure; zero initializes
        // its padding and the pathname terminator.
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        assert!(bytes.len() < address.sun_path.len());
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        for (target, byte) in address.sun_path.iter_mut().zip(bytes) {
            *target = *byte as libc::c_char;
        }
        let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1;
        #[cfg(target_os = "macos")]
        {
            address.sun_len = length as u8;
        }
        // SAFETY: socket has no pointer arguments; transfer its fresh descriptor
        // into OwnedFd immediately so every subsequent exit closes it.
        let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(raw >= 0, "socket: {}", io::Error::last_os_error());
        let socket = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: socket owns a valid descriptor. A concurrent fork before
        // CLOEXEC is set can only inherit a socket that has never listened.
        let result = unsafe { libc::fcntl(socket.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) };
        assert_eq!(result, 0, "cloexec: {}", io::Error::last_os_error());
        // SAFETY: address remains alive and length covers its populated path,
        // including the terminating NUL, within the allocated structure.
        let result = unsafe {
            libc::bind(
                socket.as_raw_fd(),
                (&address as *const libc::sockaddr_un).cast(),
                length as libc::socklen_t,
            )
        };
        assert_eq!(result, 0, "bind: {}", io::Error::last_os_error());
        socket
    }

    #[tokio::test]
    async fn published_path_without_stream_listener_does_not_become_ready() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("published.sock");
        let published = bind_without_listening(&path);
        use std::os::unix::fs::FileTypeExt;
        assert!(std::fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_socket());
        let mut child = sleeper();
        let observations = Arc::clone(&child.observations);
        let mut readiness = Box::pin(child.connect_ready(&path, Instant::now()));
        let early = tokio::select! {
            result = &mut readiness => Some(result),
            _ = async {
                loop {
                    if !observations.lock().unwrap().is_empty() { break; }
                    tokio::task::yield_now().await;
                }
            } => None,
        };
        // Only after the first actual refused attempt do we publish a stream
        // listener. Path existence by itself was never sufficient.
        drop(published);
        std::fs::remove_file(&path).unwrap();
        let listener = UnixListener::bind(&path).unwrap();
        let (accepted, receive) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut line = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut line)
                .await
                .unwrap();
            let request: serde_json::Value = serde_json::from_str(&line).unwrap();
            let reply = serde_json::json!({"jsonrpc":"2.0", "id":request["id"], "result":{"synthetic":true}});
            stream
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .unwrap();
            let _ = accepted.send(request["method"].clone());
        });
        let was_pending = early.is_none();
        let connected = match early {
            Some(result) => result,
            None => readiness.as_mut().await,
        };
        drop(readiness);
        let result = match connected {
            Ok((stream, _)) => rpc_on_stream(stream, "profile_status").await,
            Err(error) => Err(error),
        };
        if !was_pending || result.is_err() {
            server.abort();
        }
        let joined = server.await;
        let collected = child
            .collect(Deadline::now() + Duration::from_secs(5), true)
            .await;
        assert!(
            was_pending,
            "readiness completed before listener publication: {result:?}; {collected:?}"
        );
        assert!(result.is_ok(), "{result:?}");
        assert!(joined.is_ok(), "{joined:?}");
        assert_eq!(receive.await.unwrap(), "profile_status");
        assert!(
            collected.status.is_some() && collected.errors.is_empty(),
            "{collected:?}"
        );
    }

    #[tokio::test]
    async fn early_child_exit_is_labeled_and_output_is_collected() {
        let directory = tempfile::tempdir().unwrap();
        let mut command = Command::new("/bin/sh");
        command.env_clear().args([
            "-c",
            "printf synthetic-stdout; printf synthetic-stderr >&2; exit 7",
        ]);
        let mut child = Foreground::spawn(&mut command).unwrap();
        let result = child
            .connect_ready(&directory.path().join("absent.sock"), Instant::now())
            .await;
        let collected = child
            .collect(Deadline::now() + Duration::from_secs(5), false)
            .await;
        assert!(
            matches!(result, Err(ref e) if e.contains("readiness early child exit")),
            "{result:?}"
        );
        assert_eq!(collected.status.and_then(|s| s.code()), Some(7));
        assert!(collected.errors.is_empty(), "{collected:?}");
        assert_eq!(collected.stdout, b"synthetic-stdout");
        assert_eq!(collected.stderr, b"synthetic-stderr");
        assert!(!collected.forced);
    }

    #[tokio::test]
    async fn failure_cleanup_reaps_only_owned_child_and_preserves_rpc_label() {
        let directory = tempfile::tempdir().unwrap();
        let mut command = Command::new("/bin/sh");
        command.env_clear().args([
            "-c",
            "printf synthetic-stdout; printf synthetic-stderr >&2; exec /bin/sleep 30",
        ]);
        let child = Foreground::spawn(&mut command).unwrap();
        // Wait for real output, not a guessed startup delay, before causing
        // a labeled RPC failure and exercising forced test-child cleanup.
        let output = Arc::clone(&child.stderr.bytes);
        let output_wait = tokio::time::timeout(Duration::from_secs(2), async {
            while output.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        drop(output);
        let failure = rpc_once(&directory.path().join("absent.sock"), "shutdown").await;
        let collected = child
            .collect(Deadline::now() + Duration::from_secs(5), true)
            .await;
        assert!(
            output_wait.is_ok(),
            "output wait exceeded 2s: {output_wait:?}; {collected:?}"
        );
        assert!(
            matches!(failure, Err(ref e) if e.starts_with("shutdown connect:")),
            "{failure:?}"
        );
        assert!(collected.forced);
        assert!(
            collected.status.is_some_and(|s| !s.success()),
            "{collected:?}"
        );
        assert!(collected.errors.is_empty(), "{collected:?}");
        assert_eq!(collected.stdout, b"synthetic-stdout");
        assert_eq!(collected.stderr, b"synthetic-stderr");
    }

    #[tokio::test]
    async fn accepted_stream_failure_keeps_call_label_without_reconnecting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("malformed.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut line = String::new();
            BufReader::new(&mut stream)
                .read_line(&mut line)
                .await
                .unwrap();
            // Closing this sole listener makes a new dial fail differently.
            drop(listener);
            stream.write_all(b"synthetic-invalid-json\n").await.unwrap();
        });
        let stream = UnixStream::connect(&path).await.unwrap();
        let result = rpc_on_stream(stream, "profile_status").await;
        let joined = server.await;
        assert!(joined.is_ok());
        assert!(
            matches!(result, Err(ref e) if e.starts_with("profile_status decode:")),
            "{result:?}"
        );
    }
}
