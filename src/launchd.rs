//! The macOS half of `omatether setup`'s service step: a per-user
//! LaunchAgent, the launchd counterpart of the systemd user unit.
//!
//! Three things differ from systemd and shaped this file:
//!
//! - **No `EnvironmentFile`.** A LaunchAgent's environment lives in its
//!   plist, and `~/Library/LaunchAgents` is no place for a bot token. So the
//!   plist carries only `PATH`, and `serve` reads `~/.config/omatether/env`
//!   itself ([`crate::config::load_into_env`]).
//! - **No journal.** stdout and stderr go to `~/Library/Logs/omatether.log`,
//!   which Console.app also shows. launchd does not rotate it.
//! - **A loaded job does not re-read its plist.** `kickstart -k` restarts the
//!   process with the plist launchd already holds, so a changed plist means
//!   `bootout` and `bootstrap` again, not a restart.
//!
//! The rendering and parsing here are plain functions so they are tested on
//! Linux too; only [`install`] touches `launchctl`. **Unverified:** all of the
//! `launchctl` half, which has not yet been run on a Mac.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

use crate::setup::{same_file, Prompter, SETTLE};

/// The job's name in launchd, and the plist's file name.
pub const LABEL: &str = "com.github.wadewyuan.omatether";

pub fn plist_path(home: &Path) -> PathBuf {
    home.join("Library/LaunchAgents").join(format!("{LABEL}.plist"))
}

pub fn log_path(home: &Path) -> PathBuf {
    home.join("Library/Logs/omatether.log")
}

/// Same reason as the systemd unit's explicit `PATH`: a LaunchAgent starts
/// with `/usr/bin:/bin:/usr/sbin:/sbin` and sources no shell profile, so it
/// would find none of the agents. mise's shims first, for the reason the unit
/// gives (an agent's own installer leaves an older copy in `~/.local/bin`);
/// then Homebrew on Apple Silicon and on Intel, where `brew` puts `node`,
/// `tailscale` and npm-installed agents.
fn path_env(home: &Path) -> String {
    [
        home.join(".local/share/mise/shims").display().to_string(),
        home.join(".local/bin").display().to_string(),
        "/opt/homebrew/bin".to_string(),
        "/usr/local/bin".to_string(),
        "/usr/bin".to_string(),
        "/bin".to_string(),
        "/usr/sbin".to_string(),
        "/sbin".to_string(),
    ]
    .join(":")
}

/// The LaunchAgent for `exe`.
///
/// `KeepAlive` on an unsuccessful exit is `Restart=on-failure`: a `serve`
/// that exits cleanly stays down, one that crashes comes back (launchd waits
/// at least `ThrottleInterval` between starts, the counterpart of
/// `RestartSec`).
pub fn render_plist(exe: &Path, home: &Path) -> String {
    let string = |s: &str| format!("<string>{}</string>", xml_escape(s));
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	{label}
	<key>ProgramArguments</key>
	<array>
		{exe}
		<string>serve</string>
		<string>--dir</string>
		{home}
	</array>
	<key>WorkingDirectory</key>
	{home}
	<key>EnvironmentVariables</key>
	<dict>
		<key>PATH</key>
		{path}
	</dict>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>ThrottleInterval</key>
	<integer>5</integer>
	<key>StandardOutPath</key>
	{log}
	<key>StandardErrorPath</key>
	{log}
</dict>
</plist>
"#,
        label = string(LABEL),
        exe = string(&exe.display().to_string()),
        home = string(&home.display().to_string()),
        path = string(&path_env(home)),
        log = string(&log_path(home).display().to_string()),
    )
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// The binary a plist runs: the first `<string>` of `ProgramArguments`, or
/// `Program` when that is set. Loose on purpose — a hand-edited plist is
/// still XML that this reads, not a format we own.
pub fn plist_exe(text: &str) -> Option<PathBuf> {
    let first_string = |from: &str| -> Option<String> {
        let start = from.find("<string>")? + "<string>".len();
        let end = start + from[start..].find("</string>")?;
        Some(xml_unescape(from[start..end].trim()))
    };
    let after = |key: &str| text.find(key).map(|at| &text[at + key.len()..]);
    after("<key>Program</key>")
        .and_then(first_string)
        .or_else(|| after("<key>ProgramArguments</key>").and_then(first_string))
        .map(PathBuf::from)
}

/// What `launchctl print` says about the job's process: `Some(pid)` while it
/// runs.
pub fn running_pid(print: &str) -> Option<u32> {
    let running = print
        .lines()
        .any(|line| line.trim() == "state = running");
    if !running {
        return None;
    }
    print
        .lines()
        .find_map(|line| line.trim().strip_prefix("pid = "))
        .and_then(|pid| pid.trim().parse().ok())
}

/// Get the LaunchAgent in place for this binary, load it, and check that it
/// stayed up. Mirrors the systemd path in [`crate::setup`]: a plist that runs
/// some other binary is replaced only with consent and kept as `.bak`.
pub async fn install(prompter: &mut Prompter) -> Result<()> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")?;
    // Not canonicalized, unlike the systemd path: run through Homebrew's
    // `bin/` symlink, the plist should name the symlink, which survives a
    // `brew upgrade`. The Cellar path behind it does not.
    let exe = std::env::current_exe().context("locating this binary")?;
    let plist = plist_path(&home);
    let wanted = render_plist(&exe, &home);

    println!("Service (launchd):");
    let mut rewrote = false;
    match std::fs::read_to_string(&plist) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            write_plist(&plist, &wanted)?;
            println!("  ✓ wrote {}", plist.display());
            rewrote = true;
        }
        Err(e) => return Err(e).with_context(|| format!("reading {}", plist.display())),
        Ok(text) if text == wanted => {
            println!("  ✓ the LaunchAgent is installed and runs this binary");
        }
        Ok(text) => match plist_exe(&text) {
            Some(found) if same_file(&found, &exe) => {
                println!("  ✓ the LaunchAgent runs this binary (edited by hand; kept as it is)");
            }
            found => {
                let found = found.map_or("nothing".into(), |p| p.display().to_string());
                println!("  · {} runs {found},", plist.display());
                println!("    not this binary ({}).", exe.display());
                if prompter
                    .confirm("  Replace it? The old one is kept as .bak", true)
                    .await?
                {
                    let bak = set_aside(&plist)?;
                    write_plist(&plist, &wanted)?;
                    println!("  ✓ rewrote it; the old one is {}", bak.display());
                    rewrote = true;
                }
            }
        },
    }

    let uid = uid().await?;
    let domain = format!("gui/{uid}");
    let target = format!("{domain}/{LABEL}");
    let was_loaded = launchctl(&["print", &target]).await.is_ok();
    let plist_arg = plist.display().to_string();

    // A loaded job keeps the plist it was loaded with, so a rewritten one has
    // to go out and come back in; an unchanged one only needs its process
    // restarted, for the env file that just changed under it.
    let started = if was_loaded && !rewrote {
        launchctl(&["kickstart", "-k", &target]).await.map(|_| ())
    } else {
        if was_loaded {
            launchctl(&["bootout", &target]).await.ok();
        }
        // A job disabled by an earlier `launchctl disable` refuses to load
        // until enabled, with an error that does not say so.
        launchctl(&["enable", &target]).await.ok();
        bootstrap(&domain, &plist_arg).await
    };
    if let Err(e) = started {
        println!("  ✗ launchctl would not start it: {e:#}");
        println!("    By hand: launchctl bootstrap {domain} {plist_arg}");
        return Ok(());
    }

    tokio::time::sleep(SETTLE).await;
    let pid = launchctl(&["print", &target])
        .await
        .ok()
        .and_then(|print| running_pid(&print));
    if let Some(pid) = pid {
        if was_loaded {
            println!("  ✓ restarted with the new configuration, and it is running (pid {pid})");
        } else {
            println!("  ✓ loaded, and running (pid {pid}); it starts again at login");
        }
        return Ok(());
    }

    println!("  ✗ the service did not stay up. Its last words:");
    let log = std::fs::read_to_string(log_path(&home)).unwrap_or_default();
    let lines: Vec<&str> = log.lines().collect();
    for line in &lines[lines.len().saturating_sub(15)..] {
        println!("    {line}");
    }
    bail!("the service is not running — fix the above and re-run `omatether setup`")
}

/// `bootstrap` straight after `bootout` can fail with "Input/output error"
/// while launchd is still tearing the old job down; a short retry covers it.
async fn bootstrap(domain: &str, plist: &str) -> Result<()> {
    let mut last = None;
    for _ in 0..4 {
        match launchctl(&["bootstrap", domain, plist]).await {
            Ok(_) => return Ok(()),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Err(last.expect("the loop ran"))
}

fn write_plist(path: &Path, text: &str) -> Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    }
    if let Some(dir) = log_path(Path::new(&std::env::var_os("HOME").unwrap_or_default())).parent()
    {
        std::fs::create_dir_all(dir).ok();
    }
    std::fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

fn set_aside(path: &Path) -> Result<PathBuf> {
    let mut bak = path.with_extension("plist.bak");
    let mut n = 1;
    while bak.exists() {
        bak = path.with_extension(format!("plist.bak.{n}"));
        n += 1;
    }
    std::fs::rename(path, &bak)
        .with_context(|| format!("moving {} to {}", path.display(), bak.display()))?;
    Ok(bak)
}

async fn uid() -> Result<String> {
    let out = tokio::process::Command::new("id")
        .arg("-u")
        .output()
        .await
        .context("running `id -u`")?;
    let uid = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if !out.status.success() || uid.is_empty() {
        bail!("`id -u` did not say who this is");
    }
    Ok(uid)
}

/// Run `launchctl`, returning its stdout, or its own words on failure.
async fn launchctl(args: &[&str]) -> Result<String> {
    let out = tokio::process::Command::new("launchctl")
        .args(args)
        .output()
        .await
        .context("running launchctl")?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    } else {
        let said = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let said = if said.is_empty() {
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        } else {
            said
        };
        bail!("launchctl {}: {said}", args.join(" "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_plist_names_this_binary_and_reads_back() {
        let home = Path::new("/Users/someone");
        let exe = Path::new("/opt/homebrew/bin/omatether");
        let text = render_plist(exe, home);

        assert_eq!(plist_exe(&text), Some(exe.to_path_buf()));
        assert!(text.contains("<string>serve</string>"));
        assert!(text.contains("<string>/Users/someone/Library/Logs/omatether.log</string>"));
        // The PATH the agents are found on, in the order that matters.
        let path = path_env(home);
        let mise = path.find("mise/shims").unwrap();
        let local = path.find("/Users/someone/.local/bin").unwrap();
        let brew = path.find("/opt/homebrew/bin").unwrap();
        assert!(mise < local && local < brew, "{path}");
        // Credentials never go in here; serve reads the env file itself.
        assert!(!text.contains("OMATETHER_"), "{text}");
    }

    #[test]
    fn a_path_with_markup_in_it_stays_well_formed() {
        let exe = Path::new("/Users/a&b/<bin>/omatether");
        let text = render_plist(exe, Path::new("/Users/a&b"));
        assert!(text.contains("/Users/a&amp;b/&lt;bin&gt;/omatether"));
        assert_eq!(plist_exe(&text), Some(exe.to_path_buf()));
    }

    #[test]
    fn a_hand_written_plist_with_program_is_read_too() {
        let text = "<dict><key>Label</key><string>x</string>\
                    <key>Program</key><string>/usr/local/bin/omatether</string>\
                    <key>ProgramArguments</key><array><string>omatether</string></array></dict>";
        assert_eq!(plist_exe(text), Some(PathBuf::from("/usr/local/bin/omatether")));
        assert_eq!(plist_exe("<dict></dict>"), None);
    }

    #[test]
    fn running_is_read_off_launchctl_print() {
        // Trimmed from `launchctl print gui/501/<label>`.
        let running = "gui/501/com.github.wadewyuan.omatether = {\n\
                       \tactive count = 1\n\
                       \tpath = /Users/u/Library/LaunchAgents/com.github.wadewyuan.omatether.plist\n\
                       \tstate = running\n\
                       \n\
                       \tprogram = /opt/homebrew/bin/omatether\n\
                       \tpid = 4242\n\
                       }\n";
        assert_eq!(running_pid(running), Some(4242));

        let waiting = "\tstate = not running\n\tlast exit code = 1\n";
        assert_eq!(running_pid(waiting), None);
        // "spawn scheduled" is launchd waiting out the throttle after a crash:
        // not up, whatever pid an earlier line mentions.
        let throttled = "\tstate = spawn scheduled\n\tpid = 99\n";
        assert_eq!(running_pid(throttled), None);
    }
}
