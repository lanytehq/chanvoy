//! Newline-delimited JSON-RPC on stdio. Diagnostics go to stderr.

use std::io;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::backend::ToolBackend;
use crate::dispatch::handle_request;
use crate::protocol::JsonRpcResponse;

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
    loop {
        let mut buf = String::new();
        let n = reader.read_line(&mut buf).await?;
        if n == 0 {
            return Ok(());
        }
        if buf.trim().is_empty() {
            continue;
        }
        // A complete line is a complete request. Closing stdin after the
        // last line (the `echo | chanvoy mcp` pipe case) must finish that
        // call — not treat EOF as cancel. Process death still drops the
        // UDS future. HTTP disconnect is the request-scoped cancel path.
        if let Some(resp) = handle_request(&buf, &backend).await {
            write_response(&mut writer, &resp).await?;
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{ScriptedReply, ToolBackend};
    use tokio::io::BufReader;

    #[tokio::test]
    async fn stdio_tools_list_is_stdout_pure() {
        let backend = ToolBackend::scripted(vec![]);
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/list\"}\n";
        let reader = BufReader::new(&input[..]);
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
    }

    #[tokio::test]
    async fn wait_daemon_eof_is_not_deadman() {
        let backend = ToolBackend::scripted(vec![("wait_channel_v3", ScriptedReply::Eof)]);
        let input = b"{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"wait\",\"arguments\":{\"mode\":\"single\",\"channel\":\"ops\",\"timeout_secs\":5}}}\n";
        let reader = BufReader::new(&input[..]);
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
