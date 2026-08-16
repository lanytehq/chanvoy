//! MCP 2026-07-28 JSON-RPC framing (stdio and HTTP share this).

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::error::ToolErrorEnvelope;

pub const PROTOCOL_VERSION: &str = "2026-07-28";
pub const SERVER_NAME: &str = "chanvoy";
pub const META_PROTOCOL_VERSION: &str = "io.modelcontextprotocol/protocolVersion";
pub const META_CLIENT_CAPABILITIES: &str = "io.modelcontextprotocol/clientCapabilities";
pub const META_CLIENT_INFO: &str = "io.modelcontextprotocol/clientInfo";
pub const META_SERVER_INFO: &str = "io.modelcontextprotocol/serverInfo";
pub const RPC_HEADER_MISMATCH: i64 = -32020;
pub const RPC_UNSUPPORTED_PROTOCOL: i64 = -32022;

#[derive(Debug, Clone, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: Option<String>,
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
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
            result: Some(stamp_complete(result)),
            error: None,
        }
    }

    pub fn error(id: Value, code: i64, message: impl Into<String>) -> Self {
        Self::error_with_data(id, code, message, None)
    }

    pub fn error_with_data(
        id: Value,
        code: i64,
        message: impl Into<String>,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
                data,
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

    pub fn header_mismatch(id: Value, message: impl Into<String>) -> Self {
        let message = message.into();
        Self::error_with_data(
            id,
            RPC_HEADER_MISMATCH,
            message.clone(),
            Some(json!({ "name": "HeaderMismatch", "message": message })),
        )
    }

    pub fn unsupported_protocol(id: Value, requested: &str) -> Self {
        Self::error_with_data(
            id,
            RPC_UNSUPPORTED_PROTOCOL,
            "unsupported protocol version",
            Some(json!({
                "requested": requested,
                "supported": [PROTOCOL_VERSION],
            })),
        )
    }
}

pub fn server_info() -> Value {
    json!({
        "name": SERVER_NAME,
        "version": env!("CARGO_PKG_VERSION"),
    })
}

pub fn result_meta() -> Value {
    json!({ META_SERVER_INFO: server_info() })
}

/// Stamp `resultType: complete` and server `_meta` on a successful result.
pub fn stamp_complete(mut result: Value) -> Value {
    if let Some(obj) = result.as_object_mut() {
        obj.entry("resultType").or_insert_with(|| json!("complete"));
        let meta = obj.entry("_meta").or_insert_with(|| json!({}));
        if let Some(meta_obj) = meta.as_object_mut() {
            meta_obj.entry(META_SERVER_INFO).or_insert_with(server_info);
        }
    }
    result
}

pub fn request_meta(params: &Value) -> Option<&serde_json::Map<String, Value>> {
    params.get("_meta").and_then(Value::as_object)
}

/// Require protocol version + client capabilities on a modern request.
pub fn validate_request_meta(params: &Value) -> Result<String, Box<JsonRpcResponse>> {
    let meta = request_meta(params).ok_or_else(|| {
        Box::new(JsonRpcResponse::invalid_params(
            Value::Null,
            "params._meta is required (protocolVersion and clientCapabilities)",
        ))
    })?;
    let version = meta
        .get(META_PROTOCOL_VERSION)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            Box::new(JsonRpcResponse::invalid_params(
                Value::Null,
                format!("params._meta[\"{META_PROTOCOL_VERSION}\"] is required"),
            ))
        })?;
    if version != PROTOCOL_VERSION {
        return Err(Box::new(JsonRpcResponse::unsupported_protocol(
            Value::Null,
            version,
        )));
    }
    match meta.get(META_CLIENT_CAPABILITIES) {
        Some(Value::Object(_)) => {}
        Some(_) => {
            return Err(Box::new(JsonRpcResponse::invalid_params(
                Value::Null,
                format!("params._meta[\"{META_CLIENT_CAPABILITIES}\"] must be an object"),
            )))
        }
        None => {
            return Err(Box::new(JsonRpcResponse::invalid_params(
                Value::Null,
                format!("params._meta[\"{META_CLIENT_CAPABILITIES}\"] is required"),
            )))
        }
    }
    Ok(version.to_string())
}

/// Test helper: attach required 2026-07-28 request `_meta`.
pub fn with_request_meta(mut params: Value) -> Value {
    if params.is_null() {
        params = json!({});
    }
    let obj = params.as_object_mut().expect("params object");
    obj.entry("_meta").or_insert_with(|| {
        json!({
            META_PROTOCOL_VERSION: PROTOCOL_VERSION,
            META_CLIENT_CAPABILITIES: {},
        })
    });
    params
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolSuccess {
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub content: Vec<TextContent>,
    #[serde(rename = "structuredContent")]
    pub structured_content: StructuredContent,
    #[serde(rename = "isError")]
    pub is_error: bool,
    #[serde(rename = "_meta")]
    pub meta: Value,
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
    #[serde(rename = "resultType")]
    pub result_type: &'static str,
    pub content: Vec<TextContent>,
    #[serde(rename = "structuredContent")]
    pub structured_content: ToolErrorEnvelope,
    #[serde(rename = "isError")]
    pub is_error: bool,
    #[serde(rename = "_meta")]
    pub meta: Value,
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
            result_type: "complete",
            content: vec![TextContent { kind: "text", text }],
            structured_content: StructuredContent { result },
            is_error: false,
            meta: result_meta(),
        })
    }
}

impl ToolFailure {
    pub fn from_envelope(envelope: ToolErrorEnvelope) -> Self {
        let text = serde_json::to_string(&envelope)
            .unwrap_or_else(|_| r#"{"error":{"class":"provider","timeout":false,"retryable":false,"message":"encode failed"}}"#.into());
        Self {
            result_type: "complete",
            content: vec![TextContent { kind: "text", text }],
            structured_content: envelope,
            is_error: true,
            meta: result_meta(),
        }
    }
}

pub fn success_value(result: Value) -> Result<Value, ToolErrorEnvelope> {
    Ok(serde_json::to_value(ToolSuccess::from_result(result)?).expect("tool success serializes"))
}

pub fn failure_value(envelope: ToolErrorEnvelope) -> Value {
    serde_json::to_value(ToolFailure::from_envelope(envelope)).expect("tool failure serializes")
}

pub fn cancelled_request_id(params: &Value) -> Option<Value> {
    params
        .get("requestId")
        .cloned()
        .or_else(|| params.get("request_id").cloned())
}

pub fn ids_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x == y,
        (Value::String(x), Value::String(y)) => x == y,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_and_structured_result_match() {
        let result = json!([{"id":"p1"}]);
        let ok = ToolSuccess::from_result(result.clone()).unwrap();
        let parsed: Value = serde_json::from_str(&ok.content[0].text).unwrap();
        assert_eq!(parsed, result);
        assert_eq!(ok.structured_content.result, result);
        assert!(!ok.is_error);
        assert_eq!(ok.result_type, "complete");
    }

    #[test]
    fn missing_meta_is_invalid_params() {
        let err = validate_request_meta(&json!({})).unwrap_err();
        assert_eq!(err.error.as_ref().unwrap().code, -32602);
    }

    #[test]
    fn ids_are_compared_by_json_type() {
        assert!(ids_equal(&json!(9), &json!(9)));
        assert!(ids_equal(&json!("9"), &json!("9")));
        assert!(!ids_equal(&json!(9), &json!("9")));
    }

    #[test]
    fn unsupported_meta_version_is_32022() {
        let err = validate_request_meta(&json!({
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2025-03-26",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }))
        .unwrap_err();
        let error = err.error.as_ref().unwrap();
        assert_eq!(error.code, RPC_UNSUPPORTED_PROTOCOL);
        assert_eq!(error.data.as_ref().unwrap()["requested"], "2025-03-26");
    }
}
