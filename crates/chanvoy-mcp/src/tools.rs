//! Tool schemas, validation, and dispatch.

use chanvoy_core::{
    validate_wait_channel_v3_strings, validate_wait_channels_params, GetPostParams,
    PostMessageParams, ReadChannelParams, ReadThreadParams, WaitChannelV3Params,
    WaitChannelsParams,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::backend::ToolBackend;
use crate::error::ToolErrorEnvelope;
use crate::protocol::{failure_value, success_value, PROTOCOL_VERSION, SERVER_NAME};

pub const TOOL_NAMES: [&str; 6] = ["whoami", "read_channel", "show", "thread", "wait", "post"];

#[derive(Debug, Clone, Deserialize)]
struct ToolsCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitMode {
    Single,
    FanIn,
}

pub fn tools_list() -> Value {
    json!({
        "tools": TOOL_NAMES.iter().map(|name| tool_descriptor(name)).collect::<Vec<_>>(),
    })
}

pub fn server_discover() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "server": { "name": SERVER_NAME, "title": "chanvoy" },
        "tools": tools_list()["tools"],
    })
}

pub fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": { "tools": { "listChanged": false } },
        "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") },
        "instructions": "Chanvoy MCP is an access face on the local profile daemon. Blocking wait does not wake Grok Bot.",
    })
}

fn tool_descriptor(name: &str) -> Value {
    let (description, input_schema) = match name {
        "whoami" => (
            "Resolve the active chanvoy profile identity through the local daemon.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
        ),
        "read_channel" => (
            "Read recent channel history through the local daemon.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "required": ["channel"],
                "properties": {
                    "channel": { "type": "string" },
                    "since_secs": { "type": "integer", "minimum": 0 },
                    "after_post_id": { "type": "string" },
                    "since_last_mine": { "type": "boolean" },
                    "since_bootstrap": { "type": "boolean" },
                    "limit": { "type": "integer", "minimum": 1 },
                    "advance": { "type": "boolean" },
                    "team": { "type": "string" }
                }
            }),
        ),
        "show" => (
            "Fetch one post by id from a named channel.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "required": ["channel", "post_id"],
                "properties": {
                    "channel": { "type": "string" },
                    "post_id": { "type": "string" },
                    "team": { "type": "string" }
                }
            }),
        ),
        "thread" => (
            "Read a thread (root plus replies) through the local daemon.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "required": ["channel", "post_id"],
                "properties": {
                    "channel": { "type": "string" },
                    "post_id": { "type": "string" },
                    "latest": { "type": "boolean" },
                    "team": { "type": "string" }
                }
            }),
        ),
        "wait" => (
            "Block until a matching channel post or a clean deadman. Does not wake Grok Bot. mode routes single→wait_channel_v3 and fan_in→wait_channels_v1.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "required": ["mode"],
                "properties": {
                    "mode": { "type": "string", "enum": ["single", "fan_in"] },
                    "channel": { "type": "string" },
                    "timeout_secs": { "type": "integer", "minimum": 1 },
                    "team": { "type": "string" },
                    "contains": { "type": "string" },
                    "pattern": { "type": "string" },
                    "after": { "type": "string" },
                    "replace_wait_id": { "type": "string" },
                    "arms": { "type": "array" }
                }
            }),
        ),
        "post" => (
            "Post a message through the local daemon.",
            json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "required": ["channel", "message"],
                "properties": {
                    "channel": { "type": "string" },
                    "message": { "type": "string" },
                    "team": { "type": "string" },
                    "thread_root_id": { "type": "string" }
                }
            }),
        ),
        _ => unreachable!("unknown listed tool"),
    };
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "outputSchema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["result"],
            "properties": { "result": {} }
        }
    })
}

pub async fn call_tool(name: &str, arguments: Value, backend: &ToolBackend) -> Value {
    match dispatch(name, arguments, backend).await {
        Ok(result) => match success_value(result) {
            Ok(value) => value,
            Err(err) => failure_value(err),
        },
        Err(err) => failure_value(err),
    }
}

pub fn parse_tools_call(params: &Value) -> Result<(String, Value), String> {
    let parsed: ToolsCallParams = serde_json::from_value(params.clone())
        .map_err(|err| format!("malformed tools/call params: {err}"))?;
    if parsed.name.is_empty() {
        return Err("tools/call requires a name".into());
    }
    Ok((parsed.name, parsed.arguments))
}

pub fn unknown_tool_protocol_error(name: &str) -> String {
    format!("unknown tool: {name}")
}

pub fn is_declared_tool(name: &str) -> bool {
    TOOL_NAMES.contains(&name)
}

async fn dispatch(
    name: &str,
    arguments: Value,
    backend: &ToolBackend,
) -> Result<Value, ToolErrorEnvelope> {
    match name {
        "whoami" => {
            require_empty_or_object(&arguments)?;
            if let Some(obj) = arguments.as_object() {
                if !obj.is_empty() {
                    return Err(ToolErrorEnvelope::input(
                        "whoami accepts an empty object only",
                    ));
                }
            }
            backend.whoami().await
        }
        "read_channel" => {
            let params = parse_read_channel(arguments)?;
            backend.read_channel(params).await
        }
        "show" => {
            let params: GetPostParams = decode_args(arguments)?;
            if params.channel.is_empty() || params.post_id.is_empty() {
                return Err(ToolErrorEnvelope::input(
                    "show requires channel and post_id",
                ));
            }
            backend.get_post(params).await
        }
        "thread" => {
            let params: ReadThreadParams = decode_args(arguments)?;
            if params.channel.is_empty() || params.post_id.is_empty() {
                return Err(ToolErrorEnvelope::input(
                    "thread requires channel and post_id",
                ));
            }
            backend.read_thread(params).await
        }
        "wait" => dispatch_wait(arguments, backend).await,
        "post" => {
            let params: PostMessageParams = decode_args(arguments)?;
            if params.channel.is_empty() {
                return Err(ToolErrorEnvelope::input("post requires channel"));
            }
            backend.post_message(params).await
        }
        other => Err(ToolErrorEnvelope::input(unknown_tool_protocol_error(other))),
    }
}

async fn dispatch_wait(
    arguments: Value,
    backend: &ToolBackend,
) -> Result<Value, ToolErrorEnvelope> {
    let (mode, body) = split_wait_mode(arguments)?;
    match mode {
        WaitMode::Single => {
            let params: WaitChannelV3Params = decode_args(body)?;
            if params.timeout_secs == 0 {
                return Err(ToolErrorEnvelope::input(
                    "wait timeout_secs must be greater than zero",
                ));
            }
            validate_wait_channel_v3_strings(
                &params.channel,
                params.team.as_deref(),
                params.contains.as_deref(),
                params.pattern.as_deref(),
                params.after.as_deref(),
            )
            .map_err(|err| ToolErrorEnvelope::input(err.to_string()))?;
            backend.wait_channel_v3(params).await
        }
        WaitMode::FanIn => {
            let params: WaitChannelsParams = decode_args(body)?;
            validate_wait_channels_params(&params)
                .map_err(|err| ToolErrorEnvelope::input(err.to_string()))?;
            backend.wait_channels_v1(params).await
        }
    }
}

fn split_wait_mode(arguments: Value) -> Result<(WaitMode, Value), ToolErrorEnvelope> {
    let mut obj = match arguments {
        Value::Object(map) => map,
        _ => return Err(ToolErrorEnvelope::input("wait arguments must be an object")),
    };
    let mode = obj
        .remove("mode")
        .ok_or_else(|| ToolErrorEnvelope::input("wait requires mode: \"single\" or \"fan_in\""))?;
    let mode = mode
        .as_str()
        .ok_or_else(|| ToolErrorEnvelope::input("wait.mode must be a string"))?;
    let mode = match mode {
        "single" => WaitMode::Single,
        "fan_in" => WaitMode::FanIn,
        other => {
            return Err(ToolErrorEnvelope::input(format!(
                "wait.mode must be \"single\" or \"fan_in\", got {other}"
            )))
        }
    };
    Ok((mode, Value::Object(obj)))
}

fn parse_read_channel(arguments: Value) -> Result<ReadChannelParams, ToolErrorEnvelope> {
    #[derive(Deserialize)]
    struct ReadArgs {
        channel: String,
        #[serde(default)]
        since_secs: Option<u64>,
        #[serde(default)]
        after_post_id: Option<String>,
        #[serde(default)]
        since_last_mine: bool,
        #[serde(default)]
        since_bootstrap: bool,
        #[serde(default)]
        limit: Option<u32>,
        #[serde(default)]
        advance: bool,
        #[serde(default)]
        team: Option<String>,
    }
    let args: ReadArgs = decode_args(arguments)?;
    if args.channel.is_empty() {
        return Err(ToolErrorEnvelope::input("read_channel requires channel"));
    }
    let modes = [
        args.since_secs.is_some(),
        args.after_post_id.is_some(),
        args.since_last_mine,
        args.since_bootstrap,
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if modes > 1 {
        return Err(ToolErrorEnvelope::input(
            "read_channel since_secs, after_post_id, since_last_mine, and since_bootstrap are mutually exclusive",
        ));
    }
    if args.limit.is_some() && modes == 0 {
        return Err(ToolErrorEnvelope::input(
            "limit requires an explicit read mode (since_secs, after_post_id, since_last_mine, or since_bootstrap)",
        ));
    }
    Ok(ReadChannelParams {
        channel: args.channel,
        since_minutes: None,
        since_secs: args.since_secs,
        after_post_id: args.after_post_id,
        since_last_mine: args.since_last_mine,
        since_bootstrap: args.since_bootstrap,
        limit: args.limit,
        advance: args.advance,
        team: args.team,
    })
}

fn decode_args<T: for<'de> Deserialize<'de>>(arguments: Value) -> Result<T, ToolErrorEnvelope> {
    serde_json::from_value(arguments)
        .map_err(|err| ToolErrorEnvelope::input(format!("invalid tool arguments: {err}")))
}

fn require_empty_or_object(value: &Value) -> Result<(), ToolErrorEnvelope> {
    match value {
        Value::Null | Value::Object(_) => Ok(()),
        _ => Err(ToolErrorEnvelope::input("arguments must be an object")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ScriptedReply, ToolBackend};
    use serde_json::json;

    #[test]
    fn tools_list_is_deterministic() {
        let list = tools_list();
        let names: Vec<&str> = list["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, TOOL_NAMES);
    }

    #[tokio::test]
    async fn unknown_tool_is_input_when_dispatched() {
        let backend = ToolBackend::scripted(vec![]);
        let out = call_tool("notify", json!({}), &backend).await;
        assert_eq!(out["isError"], true);
        assert_eq!(out["structuredContent"]["error"]["class"], "input");
    }

    #[tokio::test]
    async fn read_channel_rejects_bare_limit_and_mutual_exclusion() {
        let backend = ToolBackend::scripted(vec![]);
        let bare = call_tool("read_channel", json!({"channel":"ops","limit":5}), &backend).await;
        assert_eq!(bare["structuredContent"]["error"]["class"], "input");
        let both = call_tool(
            "read_channel",
            json!({"channel":"ops","since_secs":60,"since_bootstrap":true}),
            &backend,
        )
        .await;
        assert_eq!(both["structuredContent"]["error"]["class"], "input");
    }

    #[tokio::test]
    async fn wait_requires_mode_and_does_not_call_v2() {
        let backend = ToolBackend::scripted(vec![]);
        let missing = call_tool("wait", json!({"channel":"ops","timeout_secs":5}), &backend).await;
        assert_eq!(missing["structuredContent"]["error"]["class"], "input");
        assert!(missing["structuredContent"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("mode"));
    }

    #[tokio::test]
    async fn wait_single_dispatches_v3() {
        let backend = ToolBackend::scripted(vec![(
            "wait_channel_v3",
            ScriptedReply::Ok(
                json!({"channel":"ops","messages":[{"id":"p1","user_id":"u","username":"n","message":"hi","create_at":1,"root_id":"p1"}]}),
            ),
        )]);
        let out = call_tool(
            "wait",
            json!({"mode":"single","channel":"ops","timeout_secs":5}),
            &backend,
        )
        .await;
        assert_eq!(out["isError"], false);
        assert_eq!(out["structuredContent"]["result"]["channel"], "ops");
        let text: Value =
            serde_json::from_str(out["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(text, out["structuredContent"]["result"]);
    }

    #[tokio::test]
    async fn wait_fan_in_dispatches_v1_not_repeated_single() {
        let backend = ToolBackend::scripted(vec![(
            "wait_channels_v1",
            ScriptedReply::Ok(
                json!({"mode":"fan_in","channels":[],"matched_channel":{"team":"t","channel":"a"},"messages":[]}),
            ),
        )]);
        let out = call_tool(
            "wait",
            json!({
                "mode":"fan_in",
                "timeout_secs": 5,
                "arms": [
                    {"team":"org-lanytehq","channel":"a"},
                    {"team":"org-lanytehq","channel":"b"}
                ]
            }),
            &backend,
        )
        .await;
        assert_eq!(out["isError"], false);
        assert_eq!(out["structuredContent"]["result"]["mode"], "fan_in");
    }

    #[tokio::test]
    async fn whoami_and_array_results_share_wrapper() {
        let who = ToolBackend::scripted(vec![(
            "whoami",
            ScriptedReply::Ok(json!({"id":"u1","username":"bot","is_bot":true})),
        )]);
        let who_out = call_tool("whoami", json!({}), &who).await;
        assert!(who_out["structuredContent"]["result"].is_object());

        let read = ToolBackend::scripted(vec![("read_channel", ScriptedReply::Ok(json!([])))]);
        let read_out = call_tool(
            "read_channel",
            json!({"channel":"ops","since_secs":60}),
            &read,
        )
        .await;
        assert!(read_out["structuredContent"]["result"].is_array());
    }
}
