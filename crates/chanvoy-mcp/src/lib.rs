//! MCP 2026-07-28 access face on the existing chanvoy daemon.
//!
//! This crate is a tool adapter over [`chanvoy_daemon::DaemonClient`]. It does
//! not open a second Mattermost client and does not add daemon RPCs.
//! Blocking `wait` does not wake Grok Bot.

use chanvoy_core::ProfileStatus;
use thiserror::Error;

mod backend;
mod dispatch;
mod error;
mod http;
mod protocol;
mod stdio;
mod tools;

pub use backend::{ScriptedReply, ToolBackend};
pub use error::{ErrorClass, ToolErrorEnvelope};
pub use http::{
    origin_allowed, parse_loopback_bind, serve_http, serve_http_listener, validate_mcp_headers,
};
pub use protocol::PROTOCOL_VERSION;
pub use stdio::{serve_stdio, serve_stdio_io};
pub use tools::{is_declared_tool, tools_list, TOOL_NAMES};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpTool {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct McpManifest {
    pub protocol: String,
    pub tools: Vec<McpTool>,
}

#[derive(Debug, Error)]
pub enum McpError {
    #[error("mcp io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Bind(String),
}

/// Declared v1 tool names and the advertised protocol pin.
pub fn manifest() -> McpManifest {
    McpManifest {
        protocol: PROTOCOL_VERSION.to_string(),
        tools: TOOL_NAMES
            .iter()
            .map(|name| McpTool {
                name: (*name).to_string(),
                description: format!("chanvoy {name}"),
            })
            .collect(),
    }
}

pub fn bridge_status(profile: &ProfileStatus) -> String {
    format!(
        "chanvoy-mcp targets profile {} at {}",
        profile.profile_name,
        profile.socket_path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_lists_the_six_v1_tools_in_order() {
        let names: Vec<_> = manifest().tools.into_iter().map(|t| t.name).collect();
        assert_eq!(
            names,
            ["whoami", "read_channel", "show", "thread", "wait", "post"]
        );
        assert_eq!(manifest().protocol, PROTOCOL_VERSION);
    }
}
