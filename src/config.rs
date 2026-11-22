//! `~/.config/omatether/env`: where the configuration lives, and how `serve`
//! reads it.
//!
//! systemd hands the file over as `EnvironmentFile=`, and for a long time that
//! was the only way it was read. launchd has nothing like it — a LaunchAgent's
//! environment is spelt out inside its plist, which would put a bot token in a
//! world-readable file under `~/Library` — so `serve` now loads the file
//! itself, and the service manager only has to start the binary.
//!
//! Anything already in the environment wins. Under systemd both sources hold
//! the same values; an operator testing a change with `OMATETHER_X=… omatether
//! serve` must not be overruled by the file they are trying not to edit.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// `$XDG_CONFIG_HOME/omatether/env`, else `~/.config/omatether/env` — on macOS
/// too, where `~/.config` is what command-line tools use in practice and what
/// the README and setup name.
pub fn env_path() -> PathBuf {
    config_home().join("omatether/env")
}

pub fn config_home() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// `KEY=VALUE` lines. Blank lines and `#` comments are skipped, and so is
/// anything without an `=`. A value wrapped in one pair of matching quotes is
/// unwrapped, as systemd does, so a file written for one reads the same here.
pub fn parse(text: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            if key.is_empty() {
                continue;
            }
            values.insert(key.to_string(), unquote(value.trim()).to_string());
        }
    }
    values
}

fn unquote(value: &str) -> &str {
    for quote in ['"', '\''] {
        if value.len() >= 2 && value.starts_with(quote) && value.ends_with(quote) {
            return &value[1..value.len() - 1];
        }
    }
    value
}

/// Read `path` into this process's environment, leaving alone any variable
/// already set. A missing file is not an error: the environment may carry
/// everything, as it does in tests and under a unit that sets it all.
///
/// Returns the names that were taken from the file, for the log.
///
/// Called first thing in `serve`, before any task that could read the
/// environment is spawned: `set_var` is only sound while nothing else is
/// reading it.
pub fn load_into_env(path: &Path) -> Result<Vec<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
    };
    warn_if_readable_by_others(path);
    let mut taken = Vec::new();
    for (key, value) in parse(&text) {
        if std::env::var_os(&key).is_none() {
            std::env::set_var(&key, value);
            taken.push(key);
        }
    }
    Ok(taken)
}

/// The file is a shell on this machine. Said rather than refused: setup
/// tightens it on every run, and a service that stops starting over a mode
/// bit is a worse failure than the warning.
fn warn_if_readable_by_others(path: &Path) {
    use std::os::unix::fs::PermissionsExt as _;
    if let Ok(meta) = std::fs::metadata(path) {
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            tracing::warn!(
                "{} is mode {mode:o} — it holds credentials; chmod 600 it \
                 (`omatether setup` does)",
                path.display()
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_env_file_reads_the_way_systemd_reads_it() {
        let values = parse(
            "# a comment\n\
             \n\
             OMATETHER_TELEGRAM_TOKEN=123:abc=def\n\
             OMATETHER_AGENT = codex \n\
             QUOTED=\"two words\"\n\
             SINGLE='x'\n\
             LOPSIDED=\"x\n\
             not a pair\n\
             =nokey\n",
        );
        // Only the first `=` splits: tokens and URLs carry their own.
        assert_eq!(values["OMATETHER_TELEGRAM_TOKEN"], "123:abc=def");
        assert_eq!(values["OMATETHER_AGENT"], "codex");
        assert_eq!(values["QUOTED"], "two words");
        assert_eq!(values["SINGLE"], "x");
        assert_eq!(values["LOPSIDED"], "\"x");
        assert_eq!(values.len(), 5);
    }

    #[test]
    fn the_environment_wins_over_the_file() {
        let dir = std::env::temp_dir().join(format!("omatether-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("env");
        std::fs::write(
            &path,
            "OMATETHER_TEST_CONFIG_SET=from-file\nOMATETHER_TEST_CONFIG_NEW=from-file\n",
        )
        .unwrap();
        std::env::set_var("OMATETHER_TEST_CONFIG_SET", "from-env");
        std::env::remove_var("OMATETHER_TEST_CONFIG_NEW");

        let taken = load_into_env(&path).unwrap();

        assert_eq!(
            std::env::var("OMATETHER_TEST_CONFIG_SET").unwrap(),
            "from-env"
        );
        assert_eq!(
            std::env::var("OMATETHER_TEST_CONFIG_NEW").unwrap(),
            "from-file"
        );
        assert_eq!(taken, vec!["OMATETHER_TEST_CONFIG_NEW".to_string()]);
        assert!(load_into_env(&dir.join("absent")).unwrap().is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
