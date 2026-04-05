use chanvoy_core::ProfileStatus;
use thiserror::Error;

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
    #[error("mcp bridge not implemented yet")]
    NotImplemented,
}

pub fn manifest() -> McpManifest {
    McpManifest {
        protocol: "json-rpc-stdio".to_string(),
        tools: vec![
            McpTool {
                name: "whoami".to_string(),
                description: "Resolve active chanvoy profile identity".to_string(),
            },
            McpTool {
                name: "read_channel".to_string(),
                description: "Read recent channel history through the local daemon".to_string(),
            },
            McpTool {
                name: "post_message".to_string(),
                description: "Post to a channel through the local daemon".to_string(),
            },
        ],
    }
}

pub fn bridge_status(profile: &ProfileStatus) -> String {
    format!(
        "chanvoy-mcp targets profile {} at {}",
        profile.profile_name,
        profile.socket_path.display()
    )
}
