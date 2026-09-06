//! One-time Photon project setup.
//!
//! A fresh Photon project knows nothing about you. Credentials alone are not
//! enough: your phone has to be registered as a Spectrum user before inbound
//! messages route anywhere, and the number *you text to reach the agent* is
//! assigned per user, per project — so a new project means a new number, not
//! the one an older project was using.
//!
//! All of this runs on the project credentials over HTTP Basic, so it needs no
//! dashboard login.

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use serde_json::{json, Value};

const SPECTRUM_HOST: &str = "https://spectrum.photon.codes";

fn basic_auth(project_id: &str, project_secret: &str) -> String {
    let raw = format!("{project_id}:{project_secret}");
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(raw)
    )
}

/// Spectrum wraps collections inconsistently; accept either shape.
fn as_list(value: &Value) -> Vec<Value> {
    if let Some(items) = value.as_array() {
        return items.clone();
    }
    for key in ["data", "results", "users", "items"] {
        if let Some(items) = value.get(key).and_then(Value::as_array) {
            return items.clone();
        }
    }
    Vec::new()
}

fn digits(value: &str) -> String {
    value.chars().filter(|c| c.is_ascii_digit()).collect()
}

/// The number to text to reach the agent — the dashboard's "TEXTS ON" column.
///
/// This is the user's `assignedPhoneNumber`, not their own `phoneNumber`. On
/// shared-number plans there is no separate line entry, so this per-user field
/// is the only source of truth.
fn assigned_line(user: &Value) -> Option<String> {
    user.get("assignedPhoneNumber")
        .and_then(Value::as_str)
        .filter(|line| !line.is_empty())
        .map(String::from)
}

fn find_by_phone(users: &[Value], phone: &str) -> Option<Value> {
    let wanted = digits(phone);
    users
        .iter()
        .find(|user| {
            user.get("phoneNumber")
                .and_then(Value::as_str)
                .map(|p| digits(p) == wanted)
                .unwrap_or(false)
        })
        .cloned()
}

/// Register `phone` with the project if it is not already there, then report
/// the line to text. Safe to re-run.
pub async fn run(project_id: &str, project_secret: &str, phone: &str) -> Result<()> {
    if !phone.starts_with('+') || digits(phone).len() < 7 {
        bail!("phone must be E.164, e.g. +15551234567 — got {phone}");
    }

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let url = format!("{SPECTRUM_HOST}/projects/{project_id}/users/");
    let auth = basic_auth(project_id, project_secret);

    // Listing first doubles as a credential check, with a far clearer failure
    // than a gRPC stream that quietly never delivers.
    let response = http
        .get(&url)
        .header("authorization", &auth)
        .send()
        .await
        .context("reaching Photon — check the network")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("Photon rejected the project credentials ({status}): {body}");
    }

    let users = as_list(&response.json::<Value>().await.context("decoding users")?);
    println!("project  {project_id}");
    println!("users    {}", users.len());

    let user = match find_by_phone(&users, phone) {
        Some(user) => {
            println!("phone    {phone} (already registered)");
            user
        }
        None => {
            let response = http
                .post(&url)
                .header("authorization", &auth)
                .json(&json!({ "type": "shared", "phoneNumber": phone }))
                .send()
                .await
                .context("registering the phone number")?;

            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("Photon refused to register {phone} ({status}): {body}");
            }

            println!("phone    {phone} (registered)");
            response.json::<Value>().await.context("decoding new user")?
        }
    };

    match assigned_line(&user) {
        Some(line) => {
            println!("line     {line}");
            println!();
            println!("Text {line} from {phone} to reach the agent.");
            println!("Then add to ~/.config/switchboard/env:");
            println!();
            println!("  SWITCHBOARD_PHOTON_PROJECT_ID={project_id}");
            println!("  SWITCHBOARD_PHOTON_PROJECT_SECRET=...");
            println!("  SWITCHBOARD_PHOTON_ALLOWED_USERS={phone}");
        }
        None => {
            println!("line     (not assigned yet)");
            println!();
            println!(
                "Photon has not assigned this user an iMessage line yet. It usually \
                 arrives within a minute — re-run this command, or check the \
                 dashboard's TEXTS ON column."
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_is_project_id_and_secret() {
        // "id:secret" base64-encoded.
        assert_eq!(basic_auth("id", "secret"), "Basic aWQ6c2VjcmV0");
    }

    #[test]
    fn collections_unwrap_from_either_shape() {
        assert_eq!(as_list(&json!([{ "a": 1 }])).len(), 1);
        assert_eq!(as_list(&json!({ "data": [{ "a": 1 }, { "b": 2 }] })).len(), 2);
        assert_eq!(as_list(&json!({ "nothing": true })).len(), 0);
    }

    #[test]
    fn phones_match_on_digits_not_formatting() {
        let users = vec![json!({ "phoneNumber": "+1 555 123 4567", "id": "u1" })];
        assert!(find_by_phone(&users, "+15551234567").is_some());
        assert!(find_by_phone(&users, "+15551234560").is_none());
    }

    #[test]
    fn the_line_to_text_is_the_assigned_number_not_your_own() {
        let user = json!({
            "phoneNumber": "+15551234567",
            "assignedPhoneNumber": "+15551234567"
        });
        assert_eq!(assigned_line(&user).as_deref(), Some("+15551234567"));

        // A freshly created user has none yet.
        let fresh = json!({ "phoneNumber": "+15551234567" });
        assert_eq!(assigned_line(&fresh), None);
    }
}
