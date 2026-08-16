//! Newline-delimited JSON-RPC on stdio. Diagnostics go to stderr.

use std::io::{self, Write};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::backend::ToolBackend;
use crate::dispatch::handle_request;
use crate::error::ToolErrorEnvelope;
use crate::protocol::{
    cancelled_request_id, failure_value, ids_equal, JsonRpcRequest, JsonRpcResponse,
};

pub async fn serve_stdio(backend: ToolBackend) -> io::Result<()> {
    let stdin = tokio::io::stdin();
    let stdout = tokio::io::stdout();
    serve_stdio_io(BufReader::new(stdin), stdout, backend).await
}

pub async fn serve_stdio_io<R, W>(
    mut reader: BufReader<R>,
    mut writer: W,
    backend: ToolBackend,
) -> io::Result<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut pending: Option<String> = None;
    loop {
        let line = if let Some(line) = pending.take() {
            line
        } else {
            let mut buf = String::new();
            let n = reader.read_line(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            buf
        };
        if line.trim().is_empty() {
            continue;
        }
        if is_idle_notification(&line) {
            continue;
        }

        let current_id = serde_json::from_str::<JsonRpcRequest>(&line)
            .ok()
            .and_then(|r| r.id);
        let mut work = Box::pin(handle_request(&line, &backend));
        loop {
            let mut lookahead = String::new();
            tokio::select! {
                biased;
                resp = &mut work => {
                    if let Some(resp) = resp {
                        write_response(&mut writer, &resp).await?;
                    }
                    break;
                }
                read = reader.read_line(&mut lookahead) => {
                    match read {
                        Ok(0) => {
                            drop(work);
                            write_response(&mut writer, &disconnect_response(&line)).await?;
                            let _ = writeln!(
                                io::stderr(),
                                "chanvoy mcp: stdin closed; cancelled in-flight daemon call"
                            );
                            return Err(io::Error::new(
                                io::ErrorKind::ConnectionAborted,
                                "mcp stdin closed during an in-flight call",
                            ));
                        }
                        Ok(_) => {
                            if let Some(cancel_id) = parse_cancelled(&lookahead) {
                                if current_id
                                    .as_ref()
                                    .is_some_and(|id| ids_equal(id, &cancel_id))
                                {
                                    drop(work);
                                    let _ = writeln!(
                                        io::stderr(),
                                        "chanvoy mcp: request cancelled; dropped in-flight daemon call"
                                    );
                                    break;
                                }
                                // Id-less nonmatching cancel: keep racing the same wait.
                                continue;
                            }
                            if is_idle_notification(&lookahead) {
                                continue;
                            }
                            if let Some(resp) = work.await {
                                write_response(&mut writer, &resp).await?;
                            }
                            pending = Some(lookahead);
                            break;
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
        }
    }
}

fn is_idle_notification(line: &str) -> bool {
    let Ok(req) = serde_json::from_str::<JsonRpcRequest>(line) else {
        return false;
    };
    req.id.is_none()
}

fn parse_cancelled(line: &str) -> Option<serde_json::Value> {
    let req: JsonRpcRequest = serde_json::from_str(line).ok()?;
    if req.id.is_some() || req.method != "notifications/cancelled" {
        return None;
    }
    cancelled_request_id(&req.params)
}

async fn write_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    resp: &JsonRpcResponse,
) -> io::Result<()> {
    let line = serde_json::to_string(resp)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await
}

fn disconnect_response(line: &str) -> JsonRpcResponse {
    let id = serde_json::from_str::<serde_json::Value>(line)
        .ok()
        .and_then(|v| v.get("id").cloned())
        .unwrap_or(serde_json::Value::Null);
    JsonRpcResponse::result(
        id,
        failure_value(ToolErrorEnvelope::provider(
            "mcp client disconnected; in-flight daemon call cancelled",
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ScriptedReply, ToolBackend};
    use crate::protocol::{with_request_meta, PROTOCOL_VERSION};
    use serde_json::json;
    use tokio::io::BufReader;

    fn modern_line(id: u64, method: &str, params: serde_json::Value) -> String {
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": with_request_meta(params),
            }))
            .unwrap()
        )
    }

    #[tokio::test]
    async fn stdio_tools_list_is_stdout_pure() {
        let backend = ToolBackend::scripted(vec![]);
        let input = modern_line(1, "tools/list", json!({}));
        let reader = BufReader::new(input.as_bytes());
        let mut out = Vec::new();
        serve_stdio_io(reader, &mut out, backend).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.starts_with('{'));
        assert!(text.contains("whoami"));
        let lines: Vec<_> = text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(parsed.get("result").is_some());
        assert!(parsed.get("error").is_none());
        assert_eq!(parsed["result"]["resultType"], "complete");
        let _ = PROTOCOL_VERSION;
    }

    #[tokio::test]
    async fn stdio_eof_cancels_in_flight_wait() {
        let backend =
            ToolBackend::scripted(vec![("wait_channel_v3", ScriptedReply::HangUntilCancel)]);
        let input = modern_line(
            9,
            "tools/call",
            json!({"name":"wait","arguments":{"mode":"single","channel":"ops","timeout_secs":30}}),
        );
        let reader = BufReader::new(input.as_bytes());
        let mut out = Vec::new();
        let err = serve_stdio_io(reader, &mut out, backend)
            .await
            .expect_err("eof during wait");
        assert_eq!(err.kind(), io::ErrorKind::ConnectionAborted);
        let parsed: serde_json::Value =
            serde_json::from_str(String::from_utf8(out).unwrap().trim()).unwrap();
        assert_eq!(
            parsed["result"]["structuredContent"]["error"]["timeout"],
            false
        );
        assert_eq!(
            parsed["result"]["structuredContent"]["error"]["class"],
            "provider"
        );
    }

    #[tokio::test]
    async fn stdio_nonmatching_cancel_keeps_racing_until_exact_id() {
        let backend =
            ToolBackend::scripted(vec![("wait_channel_v3", ScriptedReply::HangUntilCancel)]);
        let wait = modern_line(
            9,
            "tools/call",
            json!({"name":"wait","arguments":{"mode":"single","channel":"ops","timeout_secs":30}}),
        );
        let wrong =
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"9"}}"#;
        let right =
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9}}"#;
        let list = modern_line(10, "tools/list", json!({}));
        let input = format!("{wait}{wrong}\n{right}\n{list}");
        let reader = BufReader::new(input.as_bytes());
        let mut out = Vec::new();
        serve_stdio_io(reader, &mut out, backend).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<_> = text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "{text}");
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(parsed["result"]["tools"].is_array());
        assert_eq!(parsed["id"], 10);
    }

    #[tokio::test]
    async fn stdio_cancelled_notification_drops_wait() {
        let backend =
            ToolBackend::scripted(vec![("wait_channel_v3", ScriptedReply::HangUntilCancel)]);
        let wait = modern_line(
            9,
            "tools/call",
            json!({"name":"wait","arguments":{"mode":"single","channel":"ops","timeout_secs":30}}),
        );
        let cancel =
            r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9}}"#;
        let list = modern_line(10, "tools/list", json!({}));
        let input = format!("{wait}{cancel}\n{list}");
        let reader = BufReader::new(input.as_bytes());
        let mut out = Vec::new();
        serve_stdio_io(reader, &mut out, backend).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<_> = text.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "{text}");
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(parsed["result"]["tools"].is_array());
        assert_eq!(parsed["id"], 10);
    }

    #[tokio::test]
    async fn wait_daemon_eof_is_not_deadman() {
        let backend = ToolBackend::scripted(vec![("wait_channel_v3", ScriptedReply::Eof)]);
        let input = modern_line(
            3,
            "tools/call",
            json!({"name":"wait","arguments":{"mode":"single","channel":"ops","timeout_secs":5}}),
        );
        let reader = BufReader::new(input.as_bytes());
        let mut out = Vec::new();
        serve_stdio_io(reader, &mut out, backend).await.unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(String::from_utf8(out).unwrap().trim()).unwrap();
        assert_eq!(parsed["result"]["isError"], true);
        assert_eq!(
            parsed["result"]["structuredContent"]["error"]["class"],
            "provider"
        );
        assert_eq!(
            parsed["result"]["structuredContent"]["error"]["timeout"],
            false
        );
    }
}
