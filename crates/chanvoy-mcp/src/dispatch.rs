//! One dispatcher for stdio and loopback HTTP.

use serde_json::Value;

use crate::backend::ToolBackend;
use crate::protocol::{JsonRpcRequest, JsonRpcResponse, PROTOCOL_VERSION};
use crate::tools::{
    call_tool, initialize_result, is_declared_tool, parse_tools_call, server_discover, tools_list,
    unknown_tool_protocol_error,
};

pub async fn handle_request(raw: &str, backend: &ToolBackend) -> Option<JsonRpcResponse> {
    let request: JsonRpcRequest = match serde_json::from_str(raw) {
        Ok(req) => req,
        Err(_) => return Some(JsonRpcResponse::parse_error()),
    };
    if let Some(protocol) = request.protocol.as_deref() {
        if protocol != PROTOCOL_VERSION {
            return Some(JsonRpcResponse::invalid_request(
                request.id.clone().unwrap_or(Value::Null),
                format!("unsupported protocol {protocol}"),
            ));
        }
    }
    if request.jsonrpc.as_deref().is_some_and(|v| v != "2.0") {
        return Some(JsonRpcResponse::invalid_request(
            request.id.clone().unwrap_or(Value::Null),
            "jsonrpc must be \"2.0\"",
        ));
    }
    let id = match request.id.clone() {
        Some(id) => id,
        None => return None,
    };
    Some(dispatch_method(id, &request.method, request.params, backend).await)
}

pub async fn dispatch_method(
    id: Value,
    method: &str,
    params: Value,
    backend: &ToolBackend,
) -> JsonRpcResponse {
    match method {
        "initialize" => JsonRpcResponse::result(id, initialize_result()),
        "notifications/initialized" => JsonRpcResponse::result(id, Value::Null),
        "server/discover" => JsonRpcResponse::result(id, server_discover()),
        "tools/list" => JsonRpcResponse::result(id, tools_list()),
        "ping" => JsonRpcResponse::result(id, serde_json::json!({})),
        "tools/call" => match parse_tools_call(&params) {
            Ok((name, arguments)) => {
                if !is_declared_tool(&name) {
                    return JsonRpcResponse::invalid_params(id, unknown_tool_protocol_error(&name));
                }
                let result = call_tool(&name, arguments, backend).await;
                JsonRpcResponse::result(id, result)
            }
            Err(message) => JsonRpcResponse::invalid_params(id, message),
        },
        other => JsonRpcResponse::method_not_found(id, other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ScriptedReply, ToolBackend};
    use serde_json::json;

    #[tokio::test]
    async fn unknown_method_is_protocol_error() {
        let backend = ToolBackend::scripted(vec![]);
        let resp = handle_request(r#"{"jsonrpc":"2.0","id":1,"method":"nope"}"#, &backend)
            .await
            .unwrap();
        assert_eq!(resp.error.unwrap().code, -32601);
    }

    #[tokio::test]
    async fn unknown_tool_is_protocol_invalid_params() {
        let backend = ToolBackend::scripted(vec![]);
        let resp = handle_request(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"notify","arguments":{}}}"#,
            &backend,
        )
        .await
        .unwrap();
        assert_eq!(resp.error.unwrap().code, -32602);
    }

    #[tokio::test]
    async fn declared_tool_dispatches() {
        let backend = ToolBackend::scripted(vec![(
            "whoami",
            ScriptedReply::Ok(json!({"id":"u","username":"bot"})),
        )]);
        let resp = handle_request(
            r#"{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"whoami","arguments":{}}}"#,
            &backend,
        )
        .await
        .unwrap();
        let result = resp.result.unwrap();
        assert_eq!(result["isError"], false);
        assert_eq!(result["structuredContent"]["result"]["username"], "bot");
    }

    #[tokio::test]
    async fn malformed_json_is_parse_error() {
        let backend = ToolBackend::scripted(vec![]);
        let resp = handle_request("{nope", &backend).await.unwrap();
        assert_eq!(resp.error.unwrap().code, -32700);
    }
}
