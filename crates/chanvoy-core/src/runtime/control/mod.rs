//! Strict, transport-selected local control documents.
//!
//! Validation establishes only the selected wire contract. It does not establish
//! native identity, process death, lease ownership, or permission to retire work.
//! No daemon method or lifecycle behavior is activated by this module.

mod strict;
#[cfg(test)]
mod tests;

use jsonschema::{Retrieve, Uri, Validator};
use serde_json::Value;
use std::sync::LazyLock;
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncRead, AsyncReadExt};

pub const RPC_MAX_BYTES: usize = 16_384;
pub const BOOTSTRAP_MAX_BYTES: usize = 32_768;
pub const READINESS_MAX_BYTES: usize = 16_384;

/// Chosen by the transport/caller, never inferred from an untrusted body label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRoot {
    RpcErrorResponse,
    RetirementNoAckReport,
    RetireRequest,
    RetireResponse,
    ObservationRequest,
    ObservationResponse,
    Bootstrap,
    Readiness,
    CliLifecycleReport,
}

macro_rules! schema {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../schemas/runtime/control/v1/",
            $name,
            ".selected.schema.json"
        ))
    };
}

impl ControlRoot {
    pub const ALL: [Self; 9] = [
        Self::RpcErrorResponse,
        Self::RetirementNoAckReport,
        Self::RetireRequest,
        Self::RetireResponse,
        Self::ObservationRequest,
        Self::ObservationResponse,
        Self::Bootstrap,
        Self::Readiness,
        Self::CliLifecycleReport,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::RpcErrorResponse => "runtime_rpc_error_response",
            Self::RetirementNoAckReport => "retirement_no_ack_report",
            Self::RetireRequest => "retire_rpc_request",
            Self::RetireResponse => "retire_rpc_response",
            Self::ObservationRequest => "observation_rpc_request",
            Self::ObservationResponse => "observation_rpc_response",
            Self::Bootstrap => "runtime_bootstrap",
            Self::Readiness => "runtime_readiness",
            Self::CliLifecycleReport => "cli_lifecycle_report",
        }
    }

    pub const fn max_bytes(self) -> usize {
        match self {
            Self::Bootstrap => BOOTSTRAP_MAX_BYTES,
            Self::Readiness => READINESS_MAX_BYTES,
            _ => RPC_MAX_BYTES,
        }
    }

    pub const fn is_rpc(self) -> bool {
        matches!(
            self,
            Self::RpcErrorResponse
                | Self::RetireRequest
                | Self::RetireResponse
                | Self::ObservationRequest
                | Self::ObservationResponse
        )
    }

    fn schema(self) -> &'static str {
        match self {
            Self::RpcErrorResponse => schema!("runtime_rpc_error_response"),
            Self::RetirementNoAckReport => schema!("retirement_no_ack_report"),
            Self::RetireRequest => schema!("retire_rpc_request"),
            Self::RetireResponse => schema!("retire_rpc_response"),
            Self::ObservationRequest => schema!("observation_rpc_request"),
            Self::ObservationResponse => schema!("observation_rpc_response"),
            Self::Bootstrap => schema!("runtime_bootstrap"),
            Self::Readiness => schema!("runtime_readiness"),
            Self::CliLifecycleReport => schema!("cli_lifecycle_report"),
        }
    }

    fn validator(self) -> Result<&'static Validator, ControlError> {
        static VALIDATORS: LazyLock<[Result<Validator, ()>; 9]> = LazyLock::new(|| {
            ControlRoot::ALL.map(|root| {
                let schema: Value = serde_json::from_str(root.schema()).map_err(|_| ())?;
                jsonschema::draft202012::options()
                    .with_retriever(NoRetrieval)
                    .build(&schema)
                    .map_err(|_| ())
            })
        });
        let index = Self::ALL.iter().position(|root| *root == self).unwrap();
        VALIDATORS[index]
            .as_ref()
            .map_err(|_| ControlError::SchemaUnavailable)
    }
}

struct NoRetrieval;
impl Retrieve for NoRetrieval {
    fn retrieve(&self, _: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external runtime-control schema retrieval is disabled".into())
    }
}

/// Diagnostics do not echo payloads, bootstrap nonces, paths, or schema values.
#[derive(Debug, Error)]
pub enum ControlError {
    #[error("runtime-control document exceeds its byte budget")]
    TooLarge,
    #[error("runtime-control document is not UTF-8")]
    InvalidUtf8,
    #[error("runtime-control document is not one strict JSON object")]
    InvalidJson,
    #[error("runtime-control document does not match its selected root")]
    InvalidContract,
    #[error("embedded runtime-control validator is unavailable")]
    SchemaUnavailable,
    #[error("runtime-control root is not valid for this transport")]
    WrongTransport,
    #[error("runtime-control RPC frame has no newline delimiter")]
    TruncatedFrame,
    #[error("runtime-control response does not correlate with its request")]
    Correlation,
    #[error("runtime-control input failed")]
    Io(#[source] std::io::Error),
}

/// An immutable document with the selected-root contract checked. The body
/// remains an application claim; native reconciliation is a separate boundary.
#[derive(Clone)]
pub struct ControlDocument {
    root: ControlRoot,
    value: Value,
}

impl std::fmt::Debug for ControlDocument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlDocument")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl ControlDocument {
    /// Decode a bounded document. RPC input here excludes its newline delimiter.
    pub fn decode(root: ControlRoot, bytes: &[u8]) -> Result<Self, ControlError> {
        if bytes.len() > root.max_bytes() {
            return Err(ControlError::TooLarge);
        }
        let text = std::str::from_utf8(bytes).map_err(|_| ControlError::InvalidUtf8)?;
        let value = strict::parse(text).map_err(|_| ControlError::InvalidJson)?;
        if !root.validator()?.is_valid(&value) {
            return Err(ControlError::InvalidContract);
        }
        Ok(Self { root, value })
    }

    pub fn root(&self) -> ControlRoot {
        self.root
    }
    pub fn value(&self) -> &Value {
        &self.value
    }

    /// Encode a checked object without a transport delimiter. Rechecking the
    /// byte budget accounts for encoding expansion; no mutable value is exposed.
    pub fn encode(&self) -> Result<Vec<u8>, ControlError> {
        let bytes = serde_json::to_vec(&self.value).map_err(|_| ControlError::InvalidJson)?;
        if bytes.len() > self.root.max_bytes() {
            return Err(ControlError::TooLarge);
        }
        Ok(bytes)
    }

    /// Check a received RPC response against the original request. Protocol
    /// failures with null id are valid documents but cannot establish correlation.
    /// This does not compare native targets, generations, or durable revisions.
    pub fn correlate(&self, request: &Self) -> Result<(), ControlError> {
        let compatible = matches!(
            (request.root, self.root),
            (
                ControlRoot::RetireRequest,
                ControlRoot::RetireResponse | ControlRoot::RpcErrorResponse
            ) | (
                ControlRoot::ObservationRequest,
                ControlRoot::ObservationResponse | ControlRoot::RpcErrorResponse
            )
        );
        if !compatible || self.value["id"].is_null() || self.value["id"] != request.value["id"] {
            return Err(ControlError::Correlation);
        }
        Ok(())
    }
}

/// Read exactly one LF-delimited RPC frame. At most cap+1 bytes are consumed,
/// including the delimiter/probe. Later buffered frames remain untouched.
/// A framing/size error poisons this exchange: callers must close the transport,
/// not retry against the remaining suffix as a new frame. Callers own deadlines.
pub async fn read_rpc<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    root: ControlRoot,
) -> Result<ControlDocument, ControlError> {
    if !root.is_rpc() {
        return Err(ControlError::WrongTransport);
    }
    let cap = root.max_bytes();
    let mut bytes = Vec::with_capacity(cap + 1);
    loop {
        let available = reader.fill_buf().await.map_err(ControlError::Io)?;
        if available.is_empty() {
            return Err(ControlError::TruncatedFrame);
        }
        let inspect = &available[..available.len().min(cap + 1 - bytes.len())];
        let newline = inspect.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(inspect.len(), |index| index + 1);
        bytes.extend_from_slice(&inspect[..take]);
        reader.consume(take);
        if newline.is_some() {
            bytes.pop();
            return ControlDocument::decode(root, &bytes);
        }
        if bytes.len() > cap {
            return Err(ControlError::TooLarge);
        }
    }
}

/// Read a non-RPC document through EOF, bounded to cap+1 before UTF-8/JSON
/// parsing. This does not open a path or substitute for credential-tier reads.
/// The caller supplies an already-authorized input and its transport deadline.
pub async fn read_document<R: AsyncRead + Unpin>(
    reader: &mut R,
    root: ControlRoot,
) -> Result<ControlDocument, ControlError> {
    if root.is_rpc() {
        return Err(ControlError::WrongTransport);
    }
    let mut bytes = Vec::with_capacity(root.max_bytes() + 1);
    reader
        .take((root.max_bytes() + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(ControlError::Io)?;
    ControlDocument::decode(root, &bytes)
}
