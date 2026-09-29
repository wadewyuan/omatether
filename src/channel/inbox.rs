//! Where a file sent in a chat lands on disk.
//!
//! The whole of the attachment design is here in one sentence: **the bytes go
//! to a file and the agent is told the path.** Every agent this bridge drives
//! is a CLI with filesystem tools — claude reads an image through `Read`, codex
//! and pi read files too, and the detached tier gets the prompt as argv — so a
//! path in the prompt is the one representation all nine understand. Sending
//! base64 up a vision API would be nine separate pieces of work and would skip
//! the tmux tier entirely.
//!
//! Files land under the state directory rather than in the thread's working
//! directory. A screenshot dropped into a git checkout shows up in `git status`
//! and eventually in somebody's commit; the state directory belongs to this
//! service and can be pruned without asking.

use std::io;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::ThreadKey;

/// The most one attachment may occupy.
///
/// Telegram's own Bot API refuses to serve a file over 20 MB through
/// `getFile` — this is that ceiling, applied to every channel so the rule a
/// person meets is the same one wherever they are texting from.
pub const MAX_BYTES: u64 = 20 * 1024 * 1024;

/// How long a received file is kept.
///
/// Long enough to still be there when someone comes back to a conversation the
/// next day, short enough that a phone's photo library does not accumulate here
/// forever. Nothing depends on a file surviving: the agent reads it during the
/// turn it arrived in.
pub const KEEP: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// Reduce a name from the wire to something safe to join onto a path.
///
/// The sender is on the allowlist, so this is not the security boundary it
/// would be on an open service — but `file_name` is still a string a remote
/// party chose, and `../../.ssh/authorized_keys` is not a file name. Only the
/// last component survives, and only characters that cannot mean anything to a
/// shell or a path.
pub fn sanitize(name: &str) -> String {
    let base = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim_matches('.');

    let cleaned: String = base
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => c,
            _ => '_',
        })
        .take(80)
        .collect();

    if cleaned.is_empty() {
        "file".to_string()
    } else {
        cleaned
    }
}

/// Write `bytes` into this thread's inbox and report where they went.
///
/// Mode 0600 and a 0700 directory, for the same reason the spill directory is:
/// what arrives here is whatever someone photographed or exported — a
/// screenshot of a dashboard, a log with a token in it — and the process umask
/// would otherwise decide who else on the machine can read it.
///
/// The name carries seconds *and* nanoseconds because two photos sent together
/// arrive in the same second and would otherwise be one file, with the second
/// one's `create_new` failing after the first had already been announced.
pub fn save(dir: &Path, thread: &ThreadKey, name: &str, bytes: &[u8]) -> io::Result<PathBuf> {
    use io::Write as _;

    let thread_dir = dir.join(thread.to_string().replace([':', '/'], "-"));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&thread_dir)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let path = thread_dir.join(format!(
        "{}-{:09}-{}",
        now.as_secs(),
        now.subsec_nanos(),
        sanitize(name)
    ));

    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?
        .write_all(bytes)?;

    Ok(path)
}

/// Delete anything in the inbox older than [`KEEP`].
///
/// Best effort and deliberately quiet: this runs on the way past, whenever a
/// file is received, because an inbox that only grows is a disk that fills up
/// months after anyone was thinking about attachments. A failure here must
/// never cost the message that triggered it.
pub fn prune(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    let now = SystemTime::now();
    for thread_dir in entries.flatten() {
        let Ok(files) = std::fs::read_dir(thread_dir.path()) else {
            continue;
        };
        for file in files.flatten() {
            let old = file
                .metadata()
                .and_then(|m| m.modified())
                .map(|t| now.duration_since(t).unwrap_or_default() > KEEP)
                .unwrap_or(false);
            if old {
                let _ = std::fs::remove_file(file.path());
            }
        }
        // Empties itself once the last file in a thread has aged out; fails
        // harmlessly while anything is still in there.
        let _ = std::fs::remove_dir(thread_dir.path());
    }
}

/// A size a person can read at a glance, for the line the agent is shown.
pub fn human(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    match bytes {
        0 => "unknown size".to_string(),
        b if b < KB => format!("{b} B"),
        b if b < MB => format!("{:.0} kB", b as f64 / KB as f64),
        b => format!("{:.1} MB", b as f64 / MB as f64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ThreadKey {
        ThreadKey {
            channel: "telegram",
            chat_id: "5".into(),
            topic_id: None,
        }
    }

    #[test]
    fn a_name_from_the_wire_cannot_escape_the_inbox() {
        assert_eq!(sanitize("../../.ssh/authorized_keys"), "authorized_keys");
        assert_eq!(sanitize("/etc/passwd"), "passwd");
        assert_eq!(sanitize("..").as_str(), "file");
        assert_eq!(sanitize(""), "file");
        // Anything a shell could read as syntax becomes an underscore.
        assert_eq!(sanitize("a b;rm -rf ~.png"), "a_b_rm_-rf__.png");
        // Ordinary names survive intact.
        assert_eq!(
            sanitize("screenshot_2026-09-29.png"),
            "screenshot_2026-09-29.png"
        );
    }

    #[test]
    fn a_saved_file_is_readable_by_nobody_else() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = std::env::temp_dir().join(format!("omatether-inbox-{}", uuid::Uuid::new_v4()));
        let path = save(&dir, &key(), "shot.png", b"bytes").unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), b"bytes");
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o077, 0, "mode was {mode:o}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn two_files_sent_together_do_not_become_one() {
        let dir = std::env::temp_dir().join(format!("omatether-inbox-{}", uuid::Uuid::new_v4()));
        let first = save(&dir, &key(), "shot.png", b"one").unwrap();
        let second = save(&dir, &key(), "shot.png", b"two").unwrap();

        assert_ne!(first, second);
        assert_eq!(std::fs::read(&first).unwrap(), b"one");
        assert_eq!(std::fs::read(&second).unwrap(), b"two");

        std::fs::remove_dir_all(&dir).ok();
    }
}
