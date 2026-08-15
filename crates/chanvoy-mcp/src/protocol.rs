//! MCP 2026-07-28 JSON-RPC framing (stdio and HTTP share this).

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ToolErrorEnvelope;

pub const PROTOCOL_VERSION: &str = "2026-07-28";
pub const SERVER_NAME: &str = "chanvoy";

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: Option<String>,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    /// Optional 2026-07-28 protocol pin on the request body.
    #[serde(default)]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: &'static str,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcResponse {
    pub fn result(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: Some(result),
            error: None,
        }
    }

    pub fn error(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data: None,
            }),
        }
    }

    pub fn parse_error() -> Self {
        Self::error(Value::Null, -32700, "parse error")
    }

    pub fn invalid_request(id: Value, message: impl Into<String>) -> Self {
        Self::error(id, -32600, message)
    }

    pub fn method_not_found(id: Value, method: &str) -> Self {
        Self::error(id, -32601, format!("method not found: {method}"))
    }

    pub fn invalid_params(id: Value, message: impl Into<String>) -> Self {
        Self::error(id, -32602, message)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolSuccess {
    pub content: Vec<TextContent>,
    #[serde(rename = "structuredContent")]
    pub structured_content: StructuredContent,
    #[serde(rename = "isError")]
    pub is_error: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TextContent {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct StructuredContent {
    pub result: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolFailure {
    pub content: Vec<TextContent>,
    #[serde(rename = "structuredContent")]
    pub structured_content: ToolErrorEnvelope,
    #[serde(rename = "isError")]
    pub is_error: bool,
}

impl ToolSuccess {
    pub fn from_result(result: Value) -> Result<Self, ToolErrorEnvelope> {
        let text = serde_json::to_string(&result)
            .map_err(|_| ToolErrorEnvelope::provider("failed to encode tool result"))?;
        let parsed: Value = serde_json::from_str(&text)
            .map_err(|_| ToolErrorEnvelope::provider("failed to reparse tool result"))?;
        if parsed != result {
            return Err(ToolErrorEnvelope::provider(
                "tool text and structured result diverged",
            ));
        }
        Ok(Self {
            content: vec![TextContent { kind: "text", text }],
            structured_content: StructuredContent { result },
            is_error: false,
        })
    }
}

impl ToolFailure {
    pub fn from_envelope(envelope: ToolErrorEnvelope) -> Self {
        let text = serde_json::to_string(&envelope)
            .unwrap_or_else(|_| r#"{"error":{"class":"provider","timeout":false,"retryable":false,"message":"encode failed"}}"#.into());
        Self {
            content: vec![TextContent { kind: "text", text }],
            structured_content: envelope,
            is_error: true,
        }
    }
}

pub fn success_value(result: Value) -> Result<Value, ToolErrorEnvelope> {
    Ok(serde_json::to_value(ToolSuccess::from_result(result)?).expect("tool success serializes"))
}

pub fn failure_value(envelope: ToolErrorEnvelope) -> Value {
    serde_json::to_value(ToolFailure::from_envelope(envelope)).expect("tool failure serializes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn text_and_structured_result_match() {
        let result = json!([{"id":"p1"}]);
        let ok = ToolSuccess::from_result(result.clone()).unwrap();
        let parsed: Value = serde_json::from_str(&ok.content[0].text).unwrap();
        assert_eq!(parsed, result);
        assert_eq!(ok.structured_content.result, result);
        assert!(!ok.is_error);
    }
}
