//! Claude Code's `stream-json` wire format, normalized into [`AgentEvent`]s.
//!
//! Everything here is deliberately tolerant. Frames are matched on the few
//! fields we need and anything unrecognized becomes [`AgentEvent::Unknown`]
//! rather than a parse error, so a change upstream costs us one adapter, not
//! the whole binary.

use serde_json::Value;

use crate::event::AgentEvent;

fn text_at(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(String::from)
}

/// Turn one wire frame into zero or more normalized events.
///
/// Zero is a real answer: a single assistant frame can carry several content
/// blocks, and most `stream_event` frames are partial-message bookkeeping that
/// carries no information the core wants.
pub fn normalize(v: &Value) -> Vec<AgentEvent> {
    match v.get("type").and_then(Value::as_str) {
        Some("system") if text_at(v, "subtype").as_deref() == Some("init") => {
            vec![AgentEvent::Ready {
                session_id: text_at(v, "session_id").unwrap_or_default(),
                model: text_at(v, "model"),
                cwd: text_at(v, "cwd"),
                tools: v
                    .get("tools")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(String::from)
                            .collect()
                    })
                    .unwrap_or_default(),
            }]
        }

        // Transient "requesting"/"responding" spinner state. No information a
        // chat channel can use, and it fires constantly.
        Some("system") if text_at(v, "subtype").as_deref() == Some("status") => Vec::new(),

        Some("rate_limit_event") => {
            let info = v.get("rate_limit_info");
            let window = |name: &str| {
                info.and_then(|i| i.get("unifiedWindows"))
                    .and_then(|w| w.get(name))
                    .and_then(|w| w.get("utilization"))
                    .and_then(Value::as_f64)
            };
            vec![AgentEvent::RateLimit {
                five_hour: window("five_hour"),
                seven_day: window("seven_day"),
            }]
        }

        // Replies to control requests we sent. The handshake response is
        // bookkeeping; an error-shaped one is not.
        Some("control_response") => {
            let response = v.get("response");
            match response
                .and_then(|r| r.get("subtype"))
                .and_then(Value::as_str)
            {
                Some("error") => vec![AgentEvent::Error {
                    message: format!(
                        "control request failed: {}",
                        response
                            .and_then(|r| r.get("error"))
                            .map(|e| e.to_string())
                            .unwrap_or_else(|| "unspecified".into())
                    ),
                }],
                _ => Vec::new(),
            }
        }

        Some("stream_event") => stream_event(v),
        Some("assistant") => assistant(v),

        Some("result") => {
            let subtype = text_at(v, "subtype");
            vec![AgentEvent::TurnEnd {
                ok: subtype.as_deref() == Some("success"),
                detail: subtype,
            }]
        }

        Some("control_request") => control_request(v),

        // `user` frames are the agent echoing tool results back into its own
        // transcript. Nothing for a chat channel to render.
        Some("user") => Vec::new(),

        _ => vec![AgentEvent::Unknown { raw: v.clone() }],
    }
}

/// Partial-message frames. Only text deltas carry anything a reader wants;
/// the message/content-block start and stop frames are framing we re-derive
/// from the completed `assistant` frame anyway.
fn stream_event(v: &Value) -> Vec<AgentEvent> {
    let event = match v.get("event") {
        Some(e) => e,
        None => return vec![AgentEvent::Unknown { raw: v.clone() }],
    };

    if event.get("type").and_then(Value::as_str) != Some("content_block_delta") {
        return Vec::new();
    }

    let delta = match event.get("delta") {
        Some(d) => d,
        None => return Vec::new(),
    };

    match delta.get("type").and_then(Value::as_str) {
        Some("text_delta") => delta
            .get("text")
            .and_then(Value::as_str)
            .map(|text| {
                vec![AgentEvent::TextDelta {
                    text: text.to_string(),
                }]
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn assistant(v: &Value) -> Vec<AgentEvent> {
    let blocks = v
        .get("message")
        .and_then(|m| m.get("content"))
        .and_then(Value::as_array);

    let blocks = match blocks {
        Some(b) => b,
        None => return vec![AgentEvent::Unknown { raw: v.clone() }],
    };

    blocks
        .iter()
        .filter_map(|block| match block.get("type").and_then(Value::as_str) {
            Some("text") => block
                .get("text")
                .and_then(Value::as_str)
                .map(|t| AgentEvent::Text {
                    text: t.to_string(),
                }),
            Some("tool_use") => Some(AgentEvent::ToolCall {
                id: text_at(block, "id").unwrap_or_default(),
                name: text_at(block, "name").unwrap_or_default(),
                input: block.get("input").cloned().unwrap_or(Value::Null),
            }),
            Some("thinking") => Some(AgentEvent::Thinking {
                text: text_at(block, "thinking").unwrap_or_default(),
            }),
            // Anything newer: don't pretend it didn't happen.
            _ => Some(AgentEvent::Unknown { raw: block.clone() }),
        })
        .collect()
}

fn control_request(v: &Value) -> Vec<AgentEvent> {
    let request_id = match text_at(v, "request_id") {
        Some(id) => id,
        None => return vec![AgentEvent::Unknown { raw: v.clone() }],
    };

    let request = match v.get("request") {
        Some(r) => r,
        None => return vec![AgentEvent::Unknown { raw: v.clone() }],
    };

    match request.get("subtype").and_then(Value::as_str) {
        // The in-process SDK's permission callback, when it is routed over the
        // wire. Not what we get in practice — see PreToolUse below — but it is
        // cheap to accept both.
        Some("can_use_tool") => vec![AgentEvent::PermissionRequest {
            request_id,
            tool: text_at(request, "tool_name").unwrap_or_default(),
            input: request.get("input").cloned().unwrap_or(Value::Null),
            suggestions: request
                .get("permission_suggestions")
                .cloned()
                .unwrap_or(Value::Null),
        }],

        // The hook we registered during the handshake, fired once per tool
        // call. This is the gate that actually works in `--print` mode.
        Some("hook_callback") => {
            let input = request.get("input").cloned().unwrap_or(Value::Null);
            vec![AgentEvent::PermissionRequest {
                request_id,
                tool: text_at(&input, "tool_name").unwrap_or_default(),
                input: input.get("tool_input").cloned().unwrap_or(Value::Null),
                suggestions: Value::Null,
            }]
        }

        _ => vec![AgentEvent::Unknown { raw: v.clone() }],
    }
}

/// Which control-request shape a pending permission question came from, so the
/// answer can be addressed in the same dialect.
///
/// Kept out of [`AgentEvent`] on purpose: seam B stays vendor-neutral, and the
/// adapter remembers this itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionKind {
    CanUseTool,
    PreToolUseHook,
}

/// Classify a control request so the session can remember how to answer it.
pub fn permission_kind(v: &Value) -> Option<PermissionKind> {
    match v
        .get("request")
        .and_then(|r| r.get("subtype"))
        .and_then(Value::as_str)
    {
        Some("can_use_tool") => Some(PermissionKind::CanUseTool),
        Some("hook_callback") => Some(PermissionKind::PreToolUseHook),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn init_frame_becomes_ready() {
        let frame = json!({
            "type": "system",
            "subtype": "init",
            "session_id": "abc",
            "model": "claude-opus-5",
            "cwd": "/home/wy/src",
            "tools": ["Bash", "Read"]
        });
        match normalize(&frame).as_slice() {
            [AgentEvent::Ready {
                session_id, tools, ..
            }] => {
                assert_eq!(session_id, "abc");
                assert_eq!(tools.len(), 2);
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    #[test]
    fn can_use_tool_becomes_permission_request() {
        let frame = json!({
            "type": "control_request",
            "request_id": "req-1",
            "request": {
                "subtype": "can_use_tool",
                "tool_name": "Bash",
                "input": { "command": "ls" }
            }
        });
        match normalize(&frame).as_slice() {
            [AgentEvent::PermissionRequest {
                request_id, tool, ..
            }] => {
                assert_eq!(request_id, "req-1");
                assert_eq!(tool, "Bash");
            }
            other => panic!("expected PermissionRequest, got {other:?}"),
        }
    }

    #[test]
    fn assistant_frame_splits_into_one_event_per_block() {
        let frame = json!({
            "type": "assistant",
            "message": { "content": [
                { "type": "text", "text": "running it" },
                { "type": "tool_use", "id": "t1", "name": "Bash", "input": { "command": "ls" } }
            ]}
        });
        let events = normalize(&frame);
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], AgentEvent::Text { .. }));
        assert!(matches!(events[1], AgentEvent::ToolCall { .. }));
    }

    #[test]
    fn text_delta_is_the_only_stream_event_we_forward() {
        let delta = json!({
            "type": "stream_event",
            "event": { "type": "content_block_delta", "delta": { "type": "text_delta", "text": "hi" } }
        });
        assert_eq!(normalize(&delta).len(), 1);

        let start = json!({ "type": "stream_event", "event": { "type": "message_start" } });
        assert!(normalize(&start).is_empty());
    }

    #[test]
    fn hook_callback_becomes_a_permission_request() {
        let frame = json!({
            "type": "control_request",
            "request_id": "req-9",
            "request": {
                "subtype": "hook_callback",
                "callback_id": "omatether-pretooluse",
                "input": {
                    "hook_event_name": "PreToolUse",
                    "tool_name": "Bash",
                    "tool_input": { "command": "echo hi" }
                }
            }
        });
        match normalize(&frame).as_slice() {
            [AgentEvent::PermissionRequest {
                request_id,
                tool,
                input,
                ..
            }] => {
                assert_eq!(request_id, "req-9");
                assert_eq!(tool, "Bash");
                assert_eq!(input["command"], "echo hi");
            }
            other => panic!("expected PermissionRequest, got {other:?}"),
        }
        assert_eq!(
            permission_kind(&frame),
            Some(PermissionKind::PreToolUseHook)
        );
    }

    #[test]
    fn rate_limit_event_is_modelled() {
        let frame = json!({
            "type": "rate_limit_event",
            "rate_limit_info": { "unifiedWindows": {
                "five_hour": { "utilization": 0.25 },
                "seven_day": { "utilization": 0.04 }
            }}
        });
        match normalize(&frame).as_slice() {
            [AgentEvent::RateLimit { five_hour, .. }] => {
                assert_eq!(*five_hour, Some(0.25));
            }
            other => panic!("expected RateLimit, got {other:?}"),
        }
    }

    #[test]
    fn error_shaped_control_response_surfaces() {
        let frame = json!({
            "type": "control_response",
            "response": { "subtype": "error", "error": "initialize: hooks must map…" }
        });
        assert!(matches!(
            normalize(&frame).as_slice(),
            [AgentEvent::Error { .. }]
        ));
    }

    #[test]
    fn unmodelled_top_level_frames_survive_as_unknown() {
        let frame = json!({ "type": "something_new_in_2027", "payload": 1 });
        assert!(matches!(
            normalize(&frame).as_slice(),
            [AgentEvent::Unknown { .. }]
        ));
    }
}
