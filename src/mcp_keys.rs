//! API keys for the MCP channel: who may call, proven rather than assumed.
//!
//! Being on the tailnet says a request came from one of the owner's devices. It
//! does not say which program on that device sent it, and every device on the
//! tailnet — a VPS, a shared node — could otherwise run commands here as the
//! owner. A key names one agent, can be revoked on its own, and has to be
//! presented on every request.
//!
//! **Only hashes are stored.** A key is shown once, when it is made, and the
//! file keeps its SHA-256. The key is 32 random bytes, so a fast hash is enough:
//! there is nothing to brute-force that a slow one would protect. The file is
//! read on every request rather than at startup, so `add` and `revoke` take
//! effect at once, without the restart that would cut off whatever chat turn
//! is running.
//!
//! The file is `$XDG_CONFIG_HOME/omatether/mcp-keys`, mode 0600, one key per
//! line: `name<TAB>sha256-hex<TAB>created-unix-seconds`.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use base64::Engine as _;

/// What every key starts with, so one pasted somewhere it should not be is
/// recognizable for what it is.
const PREFIX: &str = "omt_";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub name: String,
    hash: [u8; 32],
    pub created: u64,
}

pub fn default_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default()).join(".config")
        })
        .join("omatether/mcp-keys")
}

/// Every key in the file. A missing file is no keys, not an error.
pub fn load(path: &Path) -> Result<Vec<Entry>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let mut entries = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('\t');
        let (Some(name), Some(hash)) = (fields.next(), fields.next()) else {
            continue;
        };
        let Some(hash) = from_hex(hash) else { continue };
        let created = fields.next().and_then(|c| c.parse().ok()).unwrap_or(0);
        entries.push(Entry {
            name: name.to_string(),
            hash,
            created,
        });
    }
    Ok(entries)
}

/// The name of the key `presented` matches, if any.
///
/// Every stored hash is compared, in constant time, whether or not an earlier
/// one matched, so the time taken says nothing about which key was close.
pub fn verify(entries: &[Entry], presented: &str) -> Option<String> {
    let presented = digest(presented.trim());
    let mut found = None;
    for entry in entries {
        let same = entry
            .hash
            .iter()
            .zip(presented.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
        if same && found.is_none() {
            found = Some(entry.name.clone());
        }
    }
    found
}

/// Make a key named `name`, store its hash, and return the key itself — the
/// only time it exists anywhere but with whoever it is given to.
pub fn add(path: &Path, name: &str) -> Result<String> {
    validate(name)?;
    let mut entries = load(path)?;
    if entries.iter().any(|e| e.name == name) {
        bail!("there is already a key named '{name}' — revoke it first to replace it");
    }

    let mut bytes = [0u8; 32];
    ring::rand::SecureRandom::fill(&ring::rand::SystemRandom::new(), &mut bytes)
        .map_err(|_| anyhow::anyhow!("the system random number generator failed"))?;
    let key = format!(
        "{PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
    );

    let created = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    entries.push(Entry {
        name: name.to_string(),
        hash: digest(&key),
        created,
    });
    save(path, &entries)?;
    Ok(key)
}

// ---- offers: a key handed to one device, never shown to anyone else ---------

/// A key waiting to be picked up by one tailnet device.
///
/// What it does not hold is the key. That is generated at the moment of the
/// claim and returned to the claimant alone, so there is never a copy for the
/// owner's terminal, a chat, or an agent driving either of them to see.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offer {
    pub name: String,
    pub node: String,
    pub expires: u64,
}

pub fn offers_path(keys: &Path) -> PathBuf {
    keys.with_file_name("mcp-key-offers")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Every offer still open. Expired ones are dropped as they are read.
pub fn load_offers(keys: &Path) -> Result<Vec<Offer>> {
    let path = offers_path(keys);
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    let now = now();
    Ok(text
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
        .filter_map(|line| {
            let mut f = line.split('\t');
            Some(Offer {
                name: f.next()?.to_string(),
                node: f.next()?.to_string(),
                expires: f.next()?.parse().ok()?,
            })
        })
        .filter(|offer| offer.expires > now)
        .collect())
}

/// Hold a key named `name` for the tailnet device `node`, for `minutes`.
/// Replaces any earlier offer for the same name.
pub fn offer(keys: &Path, name: &str, node: &str, minutes: u64) -> Result<Offer> {
    validate(name)?;
    if node.is_empty() || node.contains(['\t', '\n']) {
        bail!("give the device's tailnet name, as `tailscale status` shows it");
    }
    if load(keys)?.iter().any(|e| e.name == name) {
        bail!("there is already a key named '{name}' — revoke it first to replace it");
    }
    let mut offers = load_offers(keys)?;
    offers.retain(|o| o.name != name);
    let offer = Offer {
        name: name.to_string(),
        node: node.to_string(),
        expires: now() + minutes.clamp(1, 24 * 60) * 60,
    };
    offers.push(offer.clone());
    save_offers(keys, &offers)?;
    Ok(offer)
}

/// Withdraw the offer for `name`. Reports whether there was one.
pub fn withdraw(keys: &Path, name: &str) -> Result<bool> {
    let mut offers = load_offers(keys)?;
    let before = offers.len();
    offers.retain(|o| o.name != name);
    save_offers(keys, &offers)?;
    Ok(offers.len() != before)
}

/// Take the open offer for `node`: make its key, store the hash, close the
/// offer, and return the name and the key. `None` when nothing is waiting
/// for that device.
///
/// The offer is closed before the key is returned, so a second claim — even
/// one racing the first — finds nothing.
pub fn claim(keys: &Path, node: &str) -> Result<Option<(String, String)>> {
    let mut offers = load_offers(keys)?;
    let Some(index) = offers
        .iter()
        .position(|o| o.node.eq_ignore_ascii_case(node))
    else {
        return Ok(None);
    };
    let offer = offers.remove(index);
    save_offers(keys, &offers)?;
    let key = add(keys, &offer.name)?;
    Ok(Some((offer.name, key)))
}

fn save_offers(keys: &Path, offers: &[Offer]) -> Result<()> {
    let mut text = String::from(
        "# omatether MCP key offers: name, tailnet device, expires (unix time).\n\
         # Each is claimed once by that device; the key is made at the claim.\n",
    );
    for o in offers {
        text.push_str(&format!("{}\t{}\t{}\n", o.name, o.node, o.expires));
    }
    write_private(&offers_path(keys), &text)
}

fn validate(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 40
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !valid {
        bail!("a key name is up to 40 letters, digits, '-' and '_'");
    }
    Ok(())
}

/// Remove the key named `name`. Reports whether there was one.
pub fn revoke(path: &Path, name: &str) -> Result<bool> {
    let mut entries = load(path)?;
    let before = entries.len();
    entries.retain(|e| e.name != name);
    if entries.len() == before {
        return Ok(false);
    }
    save(path, &entries)?;
    Ok(true)
}

fn save(path: &Path, entries: &[Entry]) -> Result<()> {
    let mut text = String::from(
        "# omatether MCP API keys: name, SHA-256 of the key, created (unix time).\n\
         # Manage with `omatether mcp-key add|list|revoke`. Keys themselves are not stored.\n",
    );
    for entry in entries {
        text.push_str(&format!(
            "{}\t{}\t{}\n",
            entry.name,
            to_hex(&entry.hash),
            entry.created
        ));
    }
    write_private(path, &text)
}

/// Write a whole file, 0600 in a 0700 directory, through a temporary file and
/// a rename, so a request reading it mid-write sees the old contents or the
/// new, never half.
fn write_private(path: &Path, text: &str) -> Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};

    let dir = path.parent().context("key file has no directory")?;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)?;

    let name = path
        .file_name()
        .context("key file has no name")?
        .to_string_lossy();
    let tmp = dir.join(format!(".{name}.tmp"));
    let _ = std::fs::remove_file(&tmp);
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&tmp)
        .with_context(|| format!("writing {}", tmp.display()))?
        .write_all(text.as_bytes())?;
    std::fs::rename(&tmp, path).with_context(|| format!("replacing {}", path.display()))?;
    Ok(())
}

fn digest(key: &str) -> [u8; 32] {
    let digest = ring::digest::digest(&ring::digest::SHA256, key.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "omt-keys-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        dir.join("mcp-keys")
    }

    #[test]
    fn a_key_made_is_accepted_and_only_its_hash_is_kept() {
        let path = temp();
        let key = add(&path, "grokbot").unwrap();
        assert!(key.starts_with(PREFIX) && key.len() > 40);

        let entries = load(&path).unwrap();
        assert_eq!(verify(&entries, &key).as_deref(), Some("grokbot"));
        assert_eq!(
            verify(&entries, &format!(" {key}\n")).as_deref(),
            Some("grokbot")
        );
        assert_eq!(verify(&entries, "omt_wrong"), None);
        assert_eq!(verify(&entries, ""), None);

        let file = std::fs::read_to_string(&path).unwrap();
        assert!(!file.contains(&key), "the key itself must not be stored");
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn keys_are_told_apart_and_revoked_one_at_a_time() {
        let path = temp();
        let a = add(&path, "a").unwrap();
        let b = add(&path, "b").unwrap();
        assert_ne!(a, b);
        assert!(add(&path, "a").is_err(), "names are unique");
        assert!(add(&path, "bad name").is_err());

        assert!(revoke(&path, "a").unwrap());
        assert!(!revoke(&path, "a").unwrap());
        let entries = load(&path).unwrap();
        assert_eq!(verify(&entries, &a), None);
        assert_eq!(verify(&entries, &b).as_deref(), Some("b"));
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn an_offer_is_claimed_once_by_its_device_only() {
        let path = temp();
        offer(&path, "grokbot", "grokbot-user-518630511", 15).unwrap();
        assert!(
            load(&path).unwrap().is_empty(),
            "no key exists until the claim"
        );

        assert_eq!(
            claim(&path, "macbook-air").unwrap(),
            None,
            "another device gets nothing"
        );
        let (name, key) = claim(&path, "grokbot-user-518630511").unwrap().unwrap();
        assert_eq!(name, "grokbot");
        assert_eq!(
            verify(&load(&path).unwrap(), &key).as_deref(),
            Some("grokbot")
        );
        assert_eq!(
            claim(&path, "grokbot-user-518630511").unwrap(),
            None,
            "only once"
        );
        assert!(
            offer(&path, "grokbot", "x", 15).is_err(),
            "the key exists now"
        );
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn an_expired_offer_cannot_be_claimed() {
        let path = temp();
        offer(&path, "late", "node-a", 15).unwrap();
        // Rewrite it as already expired.
        save_offers(
            &path,
            &[Offer {
                name: "late".into(),
                node: "node-a".into(),
                expires: 1,
            }],
        )
        .unwrap();
        assert_eq!(claim(&path, "node-a").unwrap(), None);
        assert!(load(&path).unwrap().is_empty());
        std::fs::remove_dir_all(path.parent().unwrap()).ok();
    }

    #[test]
    fn no_file_is_no_keys() {
        assert!(load(Path::new("/nonexistent/omt/mcp-keys"))
            .unwrap()
            .is_empty());
    }
}
