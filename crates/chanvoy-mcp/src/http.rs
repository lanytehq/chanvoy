//! Loopback Streamable HTTP (`POST /mcp` only). No SSE in v1.

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::backend::ToolBackend;
use crate::dispatch::handle_request;
use crate::error::ToolErrorEnvelope;
use crate::protocol::{failure_value, JsonRpcResponse, PROTOCOL_VERSION, SERVER_NAME};
use crate::tools::parse_tools_call;

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
pub struct HttpError {
    pub status: u16,
    pub message: String,
}

impl HttpError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

pub fn parse_loopback_bind(raw: &str) -> Result<SocketAddr, String> {
    let addr: SocketAddr = raw
        .parse()
        .map_err(|_| format!("invalid listen address {raw}"))?;
    match addr.ip() {
        IpAddr::V4(ip) if ip == Ipv4Addr::LOCALHOST => Ok(addr),
        _ => Err(format!(
            "mcp HTTP must bind 127.0.0.1, not {addr} (0.0.0.0 / :: / ::1 refused)"
        )),
    }
}

/// Allow only the serialized loopback Origin `http://127.0.0.1` with an
/// optional numeric port. Userinfo, query, fragment, path, and spoofed
/// authorities (`http://127.0.0.1:9@evil.example`) are refused.
pub fn origin_allowed(origin: Option<&str>) -> Result<(), HttpError> {
    let Some(origin) = origin.filter(|s| !s.is_empty()) else {
        return Err(HttpError::new(403, "Origin header is required"));
    };
    if origin != origin.trim() {
        return Err(HttpError::new(403, "Origin refused"));
    }
    const PREFIX: &str = "http://127.0.0.1";
    if origin == PREFIX {
        return Ok(());
    }
    let Some(port) = origin.strip_prefix("http://127.0.0.1:") else {
        return Err(HttpError::new(
            403,
            "Origin must be http://127.0.0.1 (non-loopback Origin refused)",
        ));
    };
    if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
        return Err(HttpError::new(403, "Origin refused"));
    }
    if port.parse::<u16>().is_err() {
        return Err(HttpError::new(403, "Origin refused"));
    }
    Ok(())
}

pub fn validate_mcp_headers(
    headers: &HashMap<String, String>,
    body: &str,
) -> Result<(), HttpError> {
    let version = header(headers, "mcp-protocol-version")
        .ok_or_else(|| HttpError::new(400, "MCP-Protocol-Version is required"))?;
    if version != PROTOCOL_VERSION {
        return Err(HttpError::new(
            400,
            format!("unsupported MCP-Protocol-Version {version}"),
        ));
    }
    let method = header(headers, "mcp-method")
        .ok_or_else(|| HttpError::new(400, "Mcp-Method is required"))?;
    let parsed: serde_json::Value =
        serde_json::from_str(body).map_err(|_| HttpError::new(400, "request body is not JSON"))?;
    let body_method = parsed
        .get("method")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HttpError::new(400, "JSON-RPC method missing"))?;
    if method != body_method {
        return Err(HttpError::new(
            400,
            "Mcp-Method does not match JSON-RPC method",
        ));
    }
    let name =
        header(headers, "mcp-name").ok_or_else(|| HttpError::new(400, "Mcp-Name is required"))?;
    if body_method == "tools/call" {
        let params = parsed
            .get("params")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let (tool, _) = parse_tools_call(&params).map_err(|err| HttpError::new(400, err))?;
        if name != tool {
            return Err(HttpError::new(
                400,
                "Mcp-Name does not match tools/call name",
            ));
        }
    } else if name != SERVER_NAME && name != body_method {
        return Err(HttpError::new(
            400,
            "Mcp-Name must be chanvoy or the JSON-RPC method",
        ));
    }
    Ok(())
}

pub async fn serve_http(backend: ToolBackend, listen: &str) -> io::Result<()> {
    let addr = parse_loopback_bind(listen)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    eprintln!("chanvoy mcp http listen {bound}");
    serve_http_listener(backend, listener).await
}

pub async fn serve_http_listener(backend: ToolBackend, listener: TcpListener) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let backend = backend.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, backend).await {
                let _ = writeln!(std::io::stderr(), "chanvoy mcp http: {err}");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream, backend: ToolBackend) -> io::Result<()> {
    let request = match read_http_request(&mut stream).await {
        Ok(req) => req,
        Err(err) => {
            write_http(
                &mut stream,
                err.status,
                err.message.as_bytes(),
                "text/plain",
            )
            .await?;
            return Ok(());
        }
    };
    if request.method != "POST" || request.path != "/mcp" {
        write_http(&mut stream, 404, b"not found", "text/plain").await?;
        return Ok(());
    }
    if let Err(err) = origin_allowed(request.header("origin")) {
        write_http(
            &mut stream,
            err.status,
            err.message.as_bytes(),
            "text/plain",
        )
        .await?;
        return Ok(());
    }
    if let Err(err) = validate_mcp_headers(&request.headers, &request.body) {
        write_http(
            &mut stream,
            err.status,
            err.message.as_bytes(),
            "text/plain",
        )
        .await?;
        return Ok(());
    }

    let mut work = Box::pin(handle_request(&request.body, &backend));
    tokio::select! {
        resp = &mut work => {
            let Some(resp) = resp else {
                write_http(&mut stream, 204, b"", "text/plain").await?;
                return Ok(());
            };
            let body = serde_json::to_vec(&resp)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
            write_http(&mut stream, 200, &body, "application/json").await
        }
        disconnect = watch_disconnect(&mut stream) => {
            drop(work);
            if disconnect {
                let cancelled = JsonRpcResponse::result(
                    serde_json::Value::Null,
                    failure_value(ToolErrorEnvelope::provider(
                        "mcp client disconnected; in-flight daemon call cancelled",
                    )),
                );
                // Best-effort: client is gone; still try to close cleanly.
                let _ = cancelled;
            }
            Ok(())
        }
    }
}

async fn watch_disconnect(stream: &mut TcpStream) -> bool {
    let mut buf = [0u8; 1];
    loop {
        match tokio::time::timeout(Duration::from_secs(3600), stream.readable()).await {
            Ok(Ok(())) => match stream.try_read(&mut buf) {
                Ok(0) => return true,
                Ok(_) => continue,
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
                Err(_) => return true,
            },
            Ok(Err(_)) => return true,
            Err(_) => continue,
        }
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: String,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

async fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest, HttpError> {
    let mut buf = Vec::new();
    let header_end;
    loop {
        let mut chunk = [0u8; 1024];
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|err| HttpError::new(400, err.to_string()))?;
        if n == 0 {
            return Err(HttpError::new(400, "empty request"));
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_HEADER_BYTES {
            return Err(HttpError::new(413, "headers too large"));
        }
        if let Some(idx) = find_double_crlf(&buf) {
            header_end = idx;
            break;
        }
    }
    let header_text = std::str::from_utf8(&buf[..header_end])
        .map_err(|_| HttpError::new(400, "headers are not utf-8"))?;
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    let content_length = headers
        .get("content-length")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > MAX_BODY_BYTES {
        return Err(HttpError::new(413, "body too large"));
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let mut chunk = vec![0u8; content_length - body.len()];
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|err| HttpError::new(400, err.to_string()))?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    let body = String::from_utf8(body).map_err(|_| HttpError::new(400, "body is not utf-8"))?;
    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

async fn write_http(
    stream: &mut TcpStream,
    status: u16,
    body: &[u8],
    content_type: &str,
) -> io::Result<()> {
    let reason = match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        _ => "Error",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(header.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

fn header<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    headers.get(name).map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdrs(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn bind_refuses_non_loopback() {
        assert!(parse_loopback_bind("127.0.0.1:0").is_ok());
        assert!(parse_loopback_bind("0.0.0.0:8080").is_err());
        assert!(parse_loopback_bind("[::1]:8080").is_err());
        assert!(parse_loopback_bind("[::]:8080").is_err());
    }

    #[test]
    fn origin_enforced() {
        assert!(origin_allowed(None).is_err());
        assert!(origin_allowed(Some("")).is_err());
        assert!(origin_allowed(Some("null")).is_err());
        assert!(origin_allowed(Some("https://evil.example")).is_err());
        assert!(origin_allowed(Some("http://localhost")).is_err());
        assert!(origin_allowed(Some("http://[::1]")).is_err());
        assert!(origin_allowed(Some("http://127.0.0.1")).is_ok());
        assert!(origin_allowed(Some("http://127.0.0.1:9")).is_ok());
        assert!(origin_allowed(Some("http://127.0.0.1:18041")).is_ok());
    }

    #[test]
    fn origin_rejects_spoofed_authority() {
        for bad in [
            "http://127.0.0.1:9@evil.example",
            "http://127.0.0.1@evil.example",
            "http://user@127.0.0.1",
            "http://user:pass@127.0.0.1:9",
            "http://127.0.0.1.evil.example",
            "http://127.0.0.1/path",
            "http://127.0.0.1?q=1",
            "http://127.0.0.1#frag",
            "http://127.0.0.1:9/",
            "http://127.0.0.1:abc",
            "http://127.0.0.1:65536",
            "http://127.0.0.1:",
            " http://127.0.0.1",
            "http://127.0.0.1 ",
        ] {
            assert!(origin_allowed(Some(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn headers_must_match_body() {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let ok = hdrs(&[
            ("MCP-Protocol-Version", PROTOCOL_VERSION),
            ("Mcp-Method", "tools/list"),
            ("Mcp-Name", "chanvoy"),
        ]);
        assert!(validate_mcp_headers(&ok, body).is_ok());

        let bad_ver = hdrs(&[
            ("MCP-Protocol-Version", "2025-03-26"),
            ("Mcp-Method", "tools/list"),
            ("Mcp-Name", "chanvoy"),
        ]);
        assert!(validate_mcp_headers(&bad_ver, body).is_err());

        let mismatch = hdrs(&[
            ("MCP-Protocol-Version", PROTOCOL_VERSION),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "chanvoy"),
        ]);
        assert!(validate_mcp_headers(&mismatch, body).is_err());
    }

    #[test]
    fn tools_call_name_must_match_header() {
        let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami","arguments":{}}}"#;
        let ok = hdrs(&[
            ("MCP-Protocol-Version", PROTOCOL_VERSION),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "whoami"),
        ]);
        assert!(validate_mcp_headers(&ok, body).is_ok());
        let bad = hdrs(&[
            ("MCP-Protocol-Version", PROTOCOL_VERSION),
            ("Mcp-Method", "tools/call"),
            ("Mcp-Name", "post"),
        ]);
        assert!(validate_mcp_headers(&bad, body).is_err());
    }

    #[tokio::test]
    async fn loopback_post_roundtrip_is_json_not_sse() {
        use crate::backend::{ScriptedReply, ToolBackend};
        use tokio::io::AsyncReadExt;

        let backend = ToolBackend::scripted(vec![(
            "whoami",
            ScriptedReply::Ok(serde_json::json!({"id":"u","username":"bot"})),
        )]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        tokio::spawn(async move {
            let _ = serve_http_listener(backend, listener).await;
        });

        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"whoami","arguments":{}}}"#;
        let req = format!(
            "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: http://127.0.0.1\r\nMCP-Protocol-Version: {PROTOCOL_VERSION}\r\nMcp-Method: tools/call\r\nMcp-Name: whoami\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 200"));
        assert!(text.contains("application/json"));
        assert!(!text.to_ascii_lowercase().contains("text/event-stream"));
        assert!(text.contains("\"username\":\"bot\""));
    }

    #[tokio::test]
    async fn loopback_refuses_missing_and_non_loopback_origin() {
        use crate::backend::ToolBackend;
        use tokio::io::AsyncReadExt;

        let backend = ToolBackend::scripted(vec![]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve_http_listener(backend, listener).await;
        });

        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let req = format!(
            "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: https://evil.example\r\nMCP-Protocol-Version: {PROTOCOL_VERSION}\r\nMcp-Method: tools/list\r\nMcp-Name: chanvoy\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 403"), "{text}");
    }

    #[tokio::test]
    async fn loopback_refuses_spoofed_loopback_origin() {
        use crate::backend::ToolBackend;
        use tokio::io::AsyncReadExt;

        let backend = ToolBackend::scripted(vec![]);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve_http_listener(backend, listener).await;
        });

        let body = br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;
        let req = format!(
            "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1\r\nOrigin: http://127.0.0.1:9@evil.example\r\nMCP-Protocol-Version: {PROTOCOL_VERSION}\r\nMcp-Method: tools/list\r\nMcp-Name: chanvoy\r\nContent-Length: {}\r\n\r\n",
            body.len()
        );
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(req.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 403"), "{text}");
    }
}
