//! Which file an agent may put into a chat, and what stops it putting any file there.
//!
//! Inbound is the easy direction: the person on the phone chose the file, and the
//! only question is where the bytes land (see [`crate::channel::inbox`]). This is the
//! other direction, and the thing asking is an agent that has spent the turn reading
//! whatever it liked. "Send `path`" arriving as text the model wrote is not a
//! permission, so the check happens here, in one place, before anything is queued
//! to a channel.
//!
//! The rule is *where the work is*: a file in this thread's working directory, in
//! what this service received or spilled, or in the system temp directory. That
//! covers everything a turn plausibly wants to hand over — a screenshot it took, a
//! chart it rendered, a log it cut.
//!
//! Containment alone does not keep secrets in, because the working directory can
//! be `$HOME`: the packaged unit serves `--dir %h`. So nothing hidden is sent either
//! — no dot-named file or directory *below the root it was found under*. That is
//! what stops `~/.ssh/id_rsa`, `~/.config/omatether/env`, a browser profile and a
//! project's `.env`, and it is relative to the root so that a thread working inside
//! `~/.config/thing` can still hand over what it made there. The service's own
//! `state.db` stays out by the roots being `in/` and `out/`, not the directory
//! holding it.
//!
//! A refusal is reported to the chat rather than swallowed. A file that went
//! nowhere with nothing said about it is the same failure shape as a dropped
//! message, and this module exists to avoid exactly that.

use std::path::{Path, PathBuf};

use crate::channel::inbox;

/// The most a channel is asked to carry.
///
/// Telegram's own ceiling for an uploaded document is 50 MB, and its `sendPhoto`
/// refuses a picture over 10 MB; the adapter picks the method and reports the size
/// when neither fits. This is the outer bound, so the sentence someone reads names
/// the real file rather than a platform's limit they cannot check.
pub const MAX_BYTES: u64 = 50 * 1024 * 1024;

/// Where `path` sits under `root`, after both are resolved; `None` if it does not.
///
/// `starts_with` on the raw strings is not enough: `~/Work/build/../../.ssh` is
/// inside `~/Work` as written and outside it as a path. Canonicalizing both sides
/// is what makes the comparison mean containment.
fn below(root: &Path, path: &Path) -> Option<PathBuf> {
    let root = root.canonicalize().ok()?;
    let path = path.canonicalize().ok()?;
    path.strip_prefix(root).ok().map(Path::to_path_buf)
}

/// Whether any step of a path below a root is a dotfile or dot-directory.
fn hidden(relative: &Path) -> bool {
    relative
        .components()
        .any(|part| part.as_os_str().to_string_lossy().starts_with('.'))
}

/// `~/x` as a shell would read it, since that is how a person types a path from a
/// phone. Only the current user's: `~other` is left alone and reported as not there.
fn expand_home(written: &str) -> Option<PathBuf> {
    let rest = written.strip_prefix("~/")?;
    Some(PathBuf::from(std::env::var_os("HOME")?).join(rest))
}

/// Resolve what the agent wrote into a path this service is willing to send.
///
/// `roots[0]` is the thread's working directory, which is what a relative path is
/// relative to; every root is a candidate for containment. The caller decides what
/// those are (see [`crate::core`]) so the policy is one function with no opinions
/// about where it is running, and a test can hand it two directories instead of
/// the machine's real ones.
///
/// `Err(String)` rather than `anyhow::Error` because the string is the reply: it
/// goes to the chat as "I won't send X — <this>". It has to read as a sentence to a
/// person on a phone, not as a Rust error chain.
pub fn resolve(written: &str, roots: &[PathBuf]) -> Result<PathBuf, String> {
    let written = written.trim();
    if written.is_empty() {
        return Err("no path was given".to_string());
    }

    let cwd = roots.first().cloned().unwrap_or_else(|| PathBuf::from("."));

    // Relative to the thread's directory, which is where the agent put the file it
    // just made, and which is also the root the check below is about.
    let candidate = match expand_home(written) {
        Some(path) => path,
        None => cwd.join(written),
    };

    let path = candidate
        .canonicalize()
        .map_err(|_| format!("{written} is not there"))?;

    // Policy before facts about the file: what is outside the rules gets the same
    // answer whether it is a file, a directory or 2 GB.
    let found: Vec<PathBuf> = roots.iter().filter_map(|root| below(root, &path)).collect();
    if found.is_empty() {
        return Err(format!(
            "it is outside the directories I work in ({})",
            roots
                .iter()
                .map(|root| root.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if found.iter().all(|relative| hidden(relative)) {
        return Err("it is a hidden path, which is where secrets live".to_string());
    }

    if !path.is_file() {
        return Err(format!("{} is not a file", path.display()));
    }

    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    if size > MAX_BYTES {
        return Err(format!(
            "it is {}, which is over the {} I can send",
            inbox::human(size),
            inbox::human(MAX_BYTES)
        ));
    }

    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scratch directories: one that is "the work", one that is "the state".
    struct Scratch {
        cwd: PathBuf,
        state: PathBuf,
    }

    fn scratch(tag: &str) -> Scratch {
        let base =
            std::env::temp_dir().join(format!("omatether-sendfile-{tag}-{}", std::process::id()));
        std::fs::remove_dir_all(&base).ok();
        let cwd = base.join("work");
        let state = base.join("state");
        std::fs::create_dir_all(&cwd).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        Scratch { cwd, state }
    }

    fn file(dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, "x").unwrap();
        path
    }

    fn clean(scratch: &Scratch) {
        std::fs::remove_dir_all(scratch.cwd.parent().unwrap()).ok();
    }

    #[test]
    fn a_file_in_the_working_directory_is_allowed() {
        let s = scratch("cwd");
        let path = file(&s.cwd, "chart.png");
        let roots = [s.cwd.clone(), s.state.clone()];

        assert_eq!(resolve("chart.png", &roots), Ok(path.clone()));
        assert_eq!(resolve(&path.to_string_lossy(), &roots), Ok(path));
        clean(&s);
    }

    #[test]
    fn a_file_in_the_service_state_directory_is_allowed() {
        let s = scratch("state");
        let path = file(&s.state, "spill.txt");

        // By its full path: a relative one means the working directory.
        assert_eq!(
            resolve(path.to_str().unwrap(), &[s.cwd.clone(), s.state.clone()]),
            Ok(path)
        );
        clean(&s);
    }

    #[test]
    fn nothing_hidden_is_sent_even_from_a_working_directory_of_home() {
        // The packaged unit serves `--dir %h`, so `~/.ssh` is inside the root.
        let s = scratch("hidden");
        std::fs::create_dir_all(s.cwd.join(".ssh")).unwrap();
        file(&s.cwd.join(".ssh"), "id_rsa");
        file(&s.cwd, ".env");
        let roots = [s.cwd.clone()];

        for written in [".ssh/id_rsa", ".env"] {
            let why = resolve(written, &roots).unwrap_err();
            assert!(why.contains("hidden"), "{written}: {why}");
        }
        clean(&s);
    }

    #[test]
    fn a_link_to_a_hidden_file_is_as_hidden_as_the_file() {
        let s = scratch("link");
        std::fs::create_dir_all(s.cwd.join(".ssh")).unwrap();
        let key = file(&s.cwd.join(".ssh"), "id_rsa");
        std::os::unix::fs::symlink(&key, s.cwd.join("notes.txt")).unwrap();

        let why = resolve("notes.txt", &[s.cwd.clone()]).unwrap_err();
        assert!(why.contains("hidden"), "{why}");
        clean(&s);
    }

    #[test]
    fn hidden_is_judged_below_the_root_not_above_it() {
        // A thread working inside a dot-directory can hand over what it made.
        let s = scratch("inside");
        let root = s.cwd.join(".config").join("thing");
        std::fs::create_dir_all(&root).unwrap();
        let path = file(&root, "chart.png");

        assert_eq!(resolve("chart.png", &[root]), Ok(path));
        clean(&s);
    }

    #[test]
    fn a_refused_path_says_nothing_about_what_is_there() {
        let s = scratch("policy");
        let elsewhere = s.cwd.parent().unwrap().to_path_buf();

        let why = resolve(elsewhere.to_str().unwrap(), &[s.cwd.clone()]).unwrap_err();
        assert!(why.contains("outside"), "{why}");
        clean(&s);
    }

    #[test]
    fn a_file_outside_the_working_directories_is_refused() {
        let s = scratch("outside");
        let elsewhere = s.cwd.parent().unwrap().join("private.txt");
        std::fs::write(&elsewhere, "secret").unwrap();

        let why = resolve(
            elsewhere.to_str().unwrap(),
            &[s.cwd.clone(), s.state.clone()],
        )
        .unwrap_err();
        assert!(why.contains("outside"), "{why}");
        clean(&s);
    }

    #[test]
    fn a_path_that_walks_out_of_the_working_directory_is_refused() {
        // The reason containment is checked on canonicalized paths: as written,
        // this one *looks* like it is under the working directory.
        let s = scratch("walk");
        std::fs::create_dir_all(s.cwd.join("build")).unwrap();
        let outside = s.cwd.parent().unwrap().join("secret.txt");
        std::fs::write(&outside, "secret").unwrap();

        let written = "build/../../secret.txt";
        let why = resolve(written, &[s.cwd.clone(), s.state.clone()]);
        assert!(matches!(why, Err(_)), "{why:?}");
        assert!(why.unwrap_err().contains("outside"));
        clean(&s);
    }

    #[test]
    fn a_missing_file_says_so() {
        let s = scratch("missing");
        let why = resolve("nope.png", &[s.cwd.clone()]).unwrap_err();
        assert!(why.contains("not there"), "{why}");
        clean(&s);
    }

    #[test]
    fn a_directory_is_not_a_file() {
        let s = scratch("dir");
        std::fs::create_dir_all(s.cwd.join("build")).unwrap();
        let why = resolve("build", &[s.cwd.clone()]).unwrap_err();
        assert!(why.contains("not a file"), "{why}");
        clean(&s);
    }
}
