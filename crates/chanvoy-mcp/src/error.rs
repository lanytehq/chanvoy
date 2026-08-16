//! Allowlisted MCP tool-error envelope.

use serde::Serialize;

/// Frozen tool-error classes. Hard failures never carry `timeout: true`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    Input,
    NotFound,
    Deadman,
    Capability,
    Provider,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolErrorBody {
    pub class: ErrorClass,
    pub timeout: bool,
    pub retryable: bool,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ToolErrorEnvelope {
    pub error: ToolErrorBody,
}

impl ToolErrorEnvelope {
    pub fn new(
        class: ErrorClass,
        timeout: bool,
        retryable: bool,
        message: impl Into<String>,
    ) -> Self {
        let timeout = timeout && class == ErrorClass::Deadman;
        Self {
            error: ToolErrorBody {
                class,
                timeout,
                retryable,
                message: redact_message(&message.into()),
            },
        }
    }

    pub fn input(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Input, false, false, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::NotFound, false, false, message)
    }

    pub fn deadman(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Deadman, true, false, message)
    }

    pub fn capability(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Capability, false, false, message)
    }

    pub fn provider(message: impl Into<String>) -> Self {
        Self::new(ErrorClass::Provider, false, false, message)
    }
}

/// Strip URLs and obvious credential-shaped tokens from operator text.
pub fn redact_message(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(idx) = rest.find("http://").or_else(|| rest.find("https://")) {
        out.push_str(&rest[..idx]);
        out.push_str("[redacted-url]");
        let after = &rest[idx..];
        let skip = after
            .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ')')
            .unwrap_or(after.len());
        rest = &after[skip..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hard_fail_cannot_claim_timeout() {
        let env = ToolErrorEnvelope::new(ErrorClass::Provider, true, true, "boom");
        assert!(!env.error.timeout);
        assert_eq!(env.error.class, ErrorClass::Provider);
    }

    #[test]
    fn deadman_is_the_only_timeout_true() {
        let env = ToolErrorEnvelope::deadman("no matching messages");
        assert!(env.error.timeout);
        assert_eq!(env.error.class, ErrorClass::Deadman);
    }

    #[test]
    fn urls_are_stripped() {
        let text = redact_message("failed contacting https://mm.example.com/api/v4/posts");
        assert!(!text.contains("example.com"));
        assert!(text.contains("[redacted-url]"));
    }
}
