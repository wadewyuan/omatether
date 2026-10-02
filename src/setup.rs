//! Interactive onboarding: from a fresh clone to a running service.
//!
//! `serve` reads its configuration from environment variables the systemd
//! unit loads out of `~/.config/omatether/env`; this command is how that
//! file comes to exist, and how the unit comes to run. Every step is
//! detection-first and safe to re-run — what is already configured is
//! reported and kept, never asked for again, so a half-finished setup can
//! simply be run a second time.
//!
//! Two properties are worth the code they cost. Every credential is checked
//! against the live service the moment it is typed (a wrong Telegram token
//! or Photon secret says so here, not in a journal nobody is watching), and
//! the Telegram user id is *detected* — the sender of the next message the
//! bot receives — rather than the operator being sent to find @userinfobot.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::channel::photon_setup;
use crate::channel::telegram_setup::{self, Wait};

/// The whole flow, top to bottom. Steps that find their work already done
/// say so and move on.
pub async fn run() -> Result<()> {
    let env_path = env_path();
    println!("omatether setup\n");

    check_dependencies();

    let mut env = EnvFile::load(&env_path)?;
    let mut prompter = Prompter::new();

    // Saved after each channel, not once at the end: a Photon step that fails
    // — a rejected secret, a closed stdin, a Ctrl-C — must not take a working
    // Telegram setup down with it.
    let telegram_bot = telegram(&mut env, &mut prompter).await?;
    env.save()?;
    photon(&mut env, &mut prompter).await?;
    env.save()?;

    if env.telegram().is_none() && env.photon().is_none() {
        bail!(
            "no channels configured — serve would refuse to start. Re-run \
             `omatether setup` and answer at least one of them."
        );
    }

    install_service(&mut prompter).await?;

    println!("\nDone. Send it a message to try it:");
    if let Some(bot) = telegram_bot {
        println!("  Telegram: @{bot}");
    }
    if env.photon().is_some() {
        println!("  iMessage: your Photon line (`omatether photon-setup --phone …` shows it)");
    }
    println!("`journalctl --user -fu omatether` watches it work.");
    Ok(())
}

// ---- dependencies ------------------------------------------------------

/// What the machine has versus what the bridge can use. Nothing here is
/// fatal: a missing agent only closes off that agent, a missing node only
/// closes off iMessage. Saying so beats discovering it from a silent chat.
fn check_dependencies() {
    println!("Checking what this machine has:");

    let agents = ["claude", "codex", "pi", "omarchy-agent"];
    let found: Vec<&str> = agents.iter().copied().filter(|a| on_path(a)).collect();
    if found.is_empty() {
        println!("  ✗ no agent CLI found — the bridge has nothing to drive.");
        println!("    Install one, e.g. omarchy-pkg-add claude-code");
    } else {
        println!("  ✓ agents: {}", found.join(", "));
    }

    if on_path("tmux") {
        println!("  ✓ tmux (detached agents: omp, opencode, crush, grok, gemini, copilot)");
    } else {
        println!("  · tmux missing — the detached tier needs it: omarchy-pkg-add tmux");
    }

    if on_path("node") {
        println!("  ✓ node (the iMessage channel's sidecar)");
    } else {
        println!("  · node missing — only iMessage via Photon needs it");
    }
    println!();
}

fn on_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths).any(|dir| {
                let candidate = dir.join(name);
                candidate.is_file()
            })
        })
        .unwrap_or(false)
}

// ---- telegram ----------------------------------------------------------

/// Returns the bot's name when Telegram ends up configured.
async fn telegram(env: &mut EnvFile, prompter: &mut Prompter) -> Result<Option<String>> {
    println!("Telegram:");

    if let Some((token, allowed)) = env.telegram() {
        match telegram_setup::get_me(&token).await {
            Ok(name) => {
                println!("  ✓ already configured: @{name}, allowed: {allowed}\n");
                return Ok(Some(name));
            }
            Err(e) => {
                // A configured-but-dead token is worth stopping for: the bot
                // was revoked or regenerated, and keeping it means a service
                // that starts and never speaks.
                println!("  ✗ the saved token no longer works ({e:#}).");
                if !prompter.confirm("  Replace it?", true).await? {
                    println!();
                    return Ok(None);
                }
            }
        }
    } else {
        println!("  Create a bot with @BotFather first — a new one, not a token");
        println!("  another program polls (two pollers steal each other's messages).");
    }

    let (token, bot_name) = loop {
        let token = prompter.ask("  Bot token").await?;
        if token.is_empty() {
            println!("  (skipping Telegram)\n");
            return Ok(None);
        }
        match telegram_setup::get_me(&token).await {
            Ok(name) => break (token, name),
            Err(e) => println!("  ✗ Telegram refused it: {e:#} — try again, or empty to skip"),
        }
    };
    println!("  ✓ the bot is @{bot_name}");

    // Asked again rather than bailed on: a typo here used to throw away the
    // token that had just been validated.
    let user_id = match detect_sender(&token, &bot_name, prompter).await? {
        Some(id) => id,
        None => loop {
            let id = prompter
                .ask("  Your numeric Telegram user id (@userinfobot will tell you), or empty to skip")
                .await?;
            if id.is_empty() {
                println!("  (skipping Telegram)\n");
                return Ok(None);
            }
            if id.chars().all(|c| c.is_ascii_digit()) {
                break id;
            }
            println!("  ✗ a Telegram user id is numeric — got '{id}'");
        },
    };

    env.set("OMATETHER_TELEGRAM_TOKEN", &token);
    env.set("OMATETHER_TELEGRAM_ALLOWED_USERS", &user_id);
    println!();
    Ok(Some(bot_name))
}

/// The allowlist entry, read off the next message the bot receives rather
/// than looked up by hand. Every way that can fail — a running service
/// holding the poll, nobody writing in time, "that isn't me" — ends at the
/// manual prompt rather than an error.
async fn detect_sender(
    token: &str,
    bot_name: &str,
    prompter: &mut Prompter,
) -> Result<Option<String>> {
    println!("  Now open a chat with @{bot_name} and send it any message —");
    println!("  your user id is read from that. Waiting two minutes…");

    let wait =
        telegram_setup::wait_for_private_sender(token, std::time::Duration::from_secs(120)).await?;
    match wait {
        Wait::Found(sender) => {
            println!("  Got a message from {} (id {}).", sender.name, sender.id);
            let mine = prompter.confirm("  Is that you?", true).await?;
            // Read or not, it was sent to setup, not to the agent.
            if let Err(e) = telegram_setup::acknowledge(token, sender.update_id).await {
                println!("  · could not mark it read ({e:#}); the service may");
                println!("    answer it as a prompt when it starts.");
            }
            Ok(mine.then_some(sender.id))
        }
        Wait::Conflict => {
            println!("  The running omatether service holds the poll, so the id");
            println!("  cannot be detected while it is up.");
            Ok(None)
        }
        Wait::TimedOut => {
            println!("  No message arrived in time.");
            Ok(None)
        }
    }
}

// ---- photon ------------------------------------------------------------

async fn photon(env: &mut EnvFile, prompter: &mut Prompter) -> Result<()> {
    println!("iMessage via Photon (optional):");

    if let Some((_id, _secret, allowed)) = env.photon() {
        println!("  ✓ already configured, allowed: {allowed}");
        println!("    (`omatether photon-setup --phone …` re-checks the line)\n");
        return Ok(());
    }

    if !prompter.confirm("  Set it up?", false).await? {
        println!();
        return Ok(());
    }

    // The channel is half a supervised Node process; without the sidecar's
    // dependencies installed the service will start and iMessage will not.
    let sidecar = crate::default_sidecar_dir();
    if sidecar.join("index.mjs").is_file() && !sidecar.join("node_modules").is_dir() {
        if on_path("npm")
            && prompter
                .confirm(
                    &format!(
                        "  Install the sidecar's dependencies in {}?",
                        sidecar.display()
                    ),
                    true,
                )
                .await?
        {
            let status = tokio::process::Command::new("npm")
                .arg("install")
                .current_dir(&sidecar)
                .status()
                .await
                .context("running npm install for the sidecar")?;
            if !status.success() {
                bail!("npm install in {} failed", sidecar.display());
            }
        } else {
            println!(
                "  · run `npm install` in {} before starting the service",
                sidecar.display()
            );
        }
    }

    let project_id = prompter.ask("  Project id (app.photon.codes)").await?;
    if project_id.is_empty() {
        println!("  (skipping Photon)\n");
        return Ok(());
    }
    let project_secret = prompter.ask("  Project secret").await?;
    let phone = prompter
        .ask("  Your phone, E.164 (e.g. +15551234567)")
        .await?;

    // Registers the phone if absent and prints the line to text; idempotent.
    photon_setup::run(&project_id, &project_secret, &phone).await?;

    env.set("OMATETHER_PHOTON_PROJECT_ID", &project_id);
    env.set("OMATETHER_PHOTON_PROJECT_SECRET", &project_secret);
    env.set("OMATETHER_PHOTON_ALLOWED_USERS", &phone);
    println!();
    Ok(())
}

// ---- the env file --------------------------------------------------------

/// `~/.config/omatether/env`, as written by this command: KEY=VALUE lines,
/// mode 600. Parsed loosely (blank lines and `#` comments survive a load →
/// save round trip only as far as the values do — comments are dropped, and
/// the file is small enough that this is honest rather than lossy in practice).
struct EnvFile {
    path: PathBuf,
    values: BTreeMap<String, String>,
    /// Set by `set`, cleared by `save`, so saving after every step writes
    /// only when a step actually changed something.
    changed: bool,
}

impl EnvFile {
    fn load(path: &Path) -> Result<Self> {
        let mut values = BTreeMap::new();
        match std::fs::read_to_string(path) {
            Ok(text) => {
                for line in text.lines() {
                    let line = line.trim();
                    if line.is_empty() || line.starts_with('#') {
                        continue;
                    }
                    if let Some((key, value)) = line.split_once('=') {
                        values.insert(key.trim().to_string(), value.trim().to_string());
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).context(format!("reading {}", path.display())),
        }
        Ok(Self {
            path: path.to_path_buf(),
            values,
            changed: false,
        })
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    fn set(&mut self, key: &str, value: &str) {
        if self.values.get(key).map(String::as_str) != Some(value) {
            self.values.insert(key.to_string(), value.to_string());
            self.changed = true;
        }
    }

    /// Telegram is configured only with both halves: a token and the
    /// allowlist that makes it safe to run.
    fn telegram(&self) -> Option<(String, String)> {
        Some((
            self.get("OMATETHER_TELEGRAM_TOKEN")?.to_string(),
            self.get("OMATETHER_TELEGRAM_ALLOWED_USERS")?.to_string(),
        ))
    }

    fn photon(&self) -> Option<(String, String, String)> {
        Some((
            self.get("OMATETHER_PHOTON_PROJECT_ID")?.to_string(),
            self.get("OMATETHER_PHOTON_PROJECT_SECRET")?.to_string(),
            self.get("OMATETHER_PHOTON_ALLOWED_USERS")?.to_string(),
        ))
    }

    /// Written with 600 from the first byte — the contents are a shell on
    /// this machine. An existing file with looser permissions is tightened
    /// rather than left as found, even when there is nothing new to write.
    fn save(&mut self) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt as _;
        use std::os::unix::fs::PermissionsExt as _;

        if !self.changed {
            if self.path.exists() {
                std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600))
                    .with_context(|| format!("tightening {}", self.path.display()))?;
            }
            return Ok(());
        }

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut text = String::new();
        for (key, value) in &self.values {
            text.push_str(&format!("{key}={value}\n"));
        }

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&self.path)
            .with_context(|| format!("writing {}", self.path.display()))?;
        use std::io::Write as _;
        file.write_all(text.as_bytes())?;
        drop(file);
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(0o600))?;
        self.changed = false;

        println!("  ✓ wrote {} (mode 600)", self.path.display());
        Ok(())
    }
}

fn env_path() -> PathBuf {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("omatether/env")
}

// ---- the service ---------------------------------------------------------

/// Where the package puts its unit, and the binary that unit runs.
const PACKAGED_UNIT: &str = "/usr/lib/systemd/user/omatether.service";
const PACKAGED_EXE: &str = "/usr/bin/omatether";

/// How long a freshly started service has to stay up before setup calls it
/// running. `systemctl start` returns as soon as a `Type=simple` unit has
/// forked, so a `serve` that dies on a bad credential a second later would
/// otherwise be reported as working.
const SETTLE: std::time::Duration = std::time::Duration::from_secs(4);

/// Get a unit in place that runs *this* binary, then start it and check it
/// stayed up.
///
/// A packaged install already has a unit, kept current by pacman, so nothing
/// is written; a copy in `~/.config/systemd/user` would shadow it and never
/// see another fix. A source checkout gets one generated, naming the binary
/// that ran setup. A user unit that runs some other binary — typically a
/// source build left behind after installing the package — is replaced only
/// with consent, and kept as `.bak`, because it may be hand-tuned.
async fn install_service(prompter: &mut Prompter) -> Result<()> {
    let exe = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .context("locating this binary")?;

    let unit_dir = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."))
        .join("systemd/user");
    let user_unit = unit_dir.join("omatether.service");
    let packaged = exe == Path::new(PACKAGED_EXE) && Path::new(PACKAGED_UNIT).is_file();

    println!("Service:");
    match (packaged, user_unit.exists()) {
        (true, false) => println!("  ✓ using the packaged user service"),
        (true, true) => {
            println!(
                "  · {} overrides the packaged unit, so package updates to it never apply.",
                user_unit.display()
            );
            if prompter
                .confirm("  Move it aside and use the packaged one?", true)
                .await?
            {
                let bak = set_aside(&user_unit)?;
                println!("  ✓ moved to {}", bak.display());
            }
        }
        (false, false) => {
            std::fs::create_dir_all(&unit_dir)
                .with_context(|| format!("creating {}", unit_dir.display()))?;
            std::fs::write(&user_unit, render_unit(&exe))
                .with_context(|| format!("writing {}", user_unit.display()))?;
            println!("  ✓ wrote {}", user_unit.display());
        }
        (false, true) => {
            let text = std::fs::read_to_string(&user_unit).unwrap_or_default();
            match unit_exe(&text) {
                Some(found) if same_file(&found, &exe) => {
                    println!("  ✓ the user service is installed and runs this binary");
                }
                found => {
                    let found = found.map_or("nothing".into(), |p| p.display().to_string());
                    println!("  · {} runs {found},", user_unit.display());
                    println!("    not this binary ({}).", exe.display());
                    if prompter
                        .confirm("  Replace it? The old unit is kept as .bak", true)
                        .await?
                    {
                        let bak = set_aside(&user_unit)?;
                        std::fs::write(&user_unit, render_unit(&exe))
                            .with_context(|| format!("writing {}", user_unit.display()))?;
                        println!("  ✓ rewrote it; the old one is {}", bak.display());
                    }
                }
            }
        }
    }

    // From here it is systemctl talking to the user manager; any of it can
    // fail in a headless or odd session, and none of it should lose the
    // configuration that is already written — warnings, not errors.
    let _ = systemctl(&["daemon-reload"]).await;

    let active = systemctl(&["is-active", "omatether"]).await;
    let was_active = matches!(active, Ok(out) if out.status.success());

    // Restart rather than start when it is up: the env file, and perhaps the
    // unit, just changed under it.
    let (verb, args): (&str, &[&str]) = if was_active {
        ("restart", &["restart", "omatether"])
    } else {
        ("start", &["enable", "--now", "omatether"])
    };
    match systemctl(args).await {
        Ok(out) if out.status.success() => {}
        Ok(out) => {
            println!(
                "  ✗ systemctl refused to {verb} it: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            println!("    Start it by hand: systemctl --user enable --now omatether");
            return Ok(());
        }
        Err(e) => {
            println!("  ✗ could not run systemctl: {e}");
            return Ok(());
        }
    }

    tokio::time::sleep(SETTLE).await;
    let up = matches!(
        systemctl(&["is-active", "omatether"]).await,
        Ok(out) if out.status.success()
    );
    if up {
        if was_active {
            println!("  ✓ restarted with the new configuration, and it is running");
        } else {
            println!("  ✓ enabled and running");
        }
        return Ok(());
    }

    // Said here, with the reason, rather than left for a silent chat.
    println!("  ✗ the service did not stay up. Its last words:");
    if let Ok(out) = tokio::process::Command::new("journalctl")
        .args(["--user", "-u", "omatether", "-n", "15", "--no-pager", "-o", "cat"])
        .output()
        .await
    {
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            println!("    {line}");
        }
    }
    bail!("the service is not running — fix the above and re-run `omatether setup`")
}

/// The binary a unit's `ExecStart` runs, with `%h` expanded — the specifier
/// systemd user units use for home, and the one a hand-written unit is most
/// likely to have.
fn unit_exe(unit: &str) -> Option<PathBuf> {
    let line = unit
        .lines()
        .rev()
        .find_map(|l| l.trim().strip_prefix("ExecStart="))?;
    // Prefixes like `-` or `@` change how systemd runs the command, not which.
    let cmd = line.trim_start_matches(['-', '@', ':', '+', '!']);
    let first = cmd.split_whitespace().next()?;
    let home = std::env::var("HOME").unwrap_or_default();
    Some(PathBuf::from(first.replace("%h", &home)))
}

fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => a == b,
    }
}

/// Rename `path` to `path.bak`, or `.bak.N` if that is taken, so nothing a
/// person wrote is ever lost to setup.
fn set_aside(path: &Path) -> Result<PathBuf> {
    let mut bak = path.with_extension("service.bak");
    let mut n = 1;
    while bak.exists() {
        bak = path.with_extension(format!("service.bak.{n}"));
        n += 1;
    }
    std::fs::rename(path, &bak)
        .with_context(|| format!("moving {} to {}", path.display(), bak.display()))?;
    Ok(bak)
}

async fn systemctl(args: &[&str]) -> std::io::Result<std::process::Output> {
    tokio::process::Command::new("systemctl")
        .arg("--user")
        .args(args)
        .output()
        .await
}

/// Keep in step with contrib/omatether.service — that one is the packaged
/// form (binary at /usr/bin), this one names the binary that is running the
/// setup.
fn render_unit(exe: &Path) -> String {
    format!(
        "[Unit]\n\
         Description=omatether - coding agent over chat\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         Type=simple\n\
         Environment=PATH=%h/.local/bin:%h/.local/share/mise/shims:/usr/share/omarchy/bin:/usr/local/bin:/usr/bin:/bin\n\
         EnvironmentFile=%h/.config/omatether/env\n\
         ExecStart={} serve --dir %h\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exe.display()
    )
}

// ---- prompting -----------------------------------------------------------

/// stdin, one question at a time. Answers are trimmed; an empty answer is
/// how every prompt says "skip" or takes its default.
struct Prompter {
    lines: tokio::io::Lines<BufReader<tokio::io::Stdin>>,
}

impl Prompter {
    fn new() -> Self {
        Self {
            lines: BufReader::new(tokio::io::stdin()).lines(),
        }
    }

    async fn ask(&mut self, prompt: &str) -> Result<String> {
        println!("{prompt}");
        print!("> ");
        use std::io::Write as _;
        std::io::stdout().flush()?;
        let line = self
            .lines
            .next_line()
            .await?
            .context("stdin closed mid-setup")?;
        Ok(line.trim().to_string())
    }

    /// A yes/no with an explicit default, so Enter is always safe.
    async fn confirm(&mut self, prompt: &str, default: bool) -> Result<bool> {
        let hint = if default { "[Y/n]" } else { "[y/N]" };
        let answer = self.ask(&format!("{prompt} {hint}")).await?;
        Ok(match answer.to_ascii_lowercase().as_str() {
            "" => default,
            "y" | "yes" => true,
            _ => false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_round_trip_and_comments_are_ignored() {
        let dir = std::env::temp_dir().join(format!("omatether-setup-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("env");
        std::fs::write(
            &path,
            "# a note\n\nOMATETHER_TELEGRAM_TOKEN = tok:en=with=equals \nOTHER=1\n",
        )
        .unwrap();

        let mut env = EnvFile::load(&path).unwrap();
        assert_eq!(
            env.get("OMATETHER_TELEGRAM_TOKEN"),
            Some("tok:en=with=equals")
        );
        env.set("OMATETHER_TELEGRAM_ALLOWED_USERS", "42");
        env.save().unwrap();

        let reloaded = EnvFile::load(&path).unwrap();
        assert_eq!(
            reloaded.telegram(),
            Some(("tok:en=with=equals".to_string(), "42".to_string()))
        );
        assert_eq!(reloaded.get("OTHER"), Some("1"));

        let mode = std::os::unix::fs::MetadataExt::mode(&std::fs::metadata(&path).unwrap()) & 0o777;
        assert_eq!(mode, 0o600, "the env file is a shell on this machine");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_missing_env_file_loads_empty() {
        let env = EnvFile::load(Path::new("/nonexistent/omatether/env")).unwrap();
        assert!(env.telegram().is_none());
        assert!(env.photon().is_none());
    }

    #[test]
    fn saving_with_nothing_changed_writes_nothing() {
        // Setup saves after every channel; a skipped one must not leave an
        // empty env file behind, nor rewrite an existing one.
        let dir = std::env::temp_dir().join(format!("omatether-setup-noop-{}", std::process::id()));
        let path = dir.join("env");
        let mut env = EnvFile::load(&path).unwrap();
        env.save().unwrap();
        assert!(!path.exists());

        env.set("OTHER", "1");
        env.save().unwrap();
        let written = std::fs::metadata(&path).unwrap().modified().unwrap();
        env.set("OTHER", "1");
        env.save().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            written
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_units_binary_is_read_with_home_expanded() {
        // The unit on the machine this was written on said `%h/…`, and a
        // plain string match called it stale.
        let home = std::env::var("HOME").unwrap();
        let unit = "[Service]\nExecStart=%h/src/omatether/target/release/omatether serve --dir %h/src\n";
        assert_eq!(
            unit_exe(unit),
            Some(PathBuf::from(format!("{home}/src/omatether/target/release/omatether")))
        );
        assert_eq!(
            unit_exe(&render_unit(Path::new("/usr/bin/omatether"))),
            Some(PathBuf::from("/usr/bin/omatether"))
        );
        assert_eq!(
            unit_exe("ExecStart=-/opt/omatether serve"),
            Some(PathBuf::from("/opt/omatether"))
        );
        assert_eq!(unit_exe("[Service]\nType=simple\n"), None);
    }

    #[test]
    fn setting_aside_never_overwrites_an_earlier_backup() {
        let dir = std::env::temp_dir().join(format!("omatether-setup-bak-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let unit = dir.join("omatether.service");

        std::fs::write(&unit, "first").unwrap();
        let a = set_aside(&unit).unwrap();
        std::fs::write(&unit, "second").unwrap();
        let b = set_aside(&unit).unwrap();

        assert_ne!(a, b);
        assert!(!unit.exists());
        assert_eq!(std::fs::read_to_string(&a).unwrap(), "first");
        assert_eq!(std::fs::read_to_string(&b).unwrap(), "second");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_generated_unit_names_the_binary_that_ran_setup() {
        let unit = render_unit(Path::new("/usr/bin/omatether"));
        assert!(unit.contains("ExecStart=/usr/bin/omatether serve --dir %h"));
        assert!(unit.contains("EnvironmentFile=%h/.config/omatether/env"));
        assert!(unit.contains("mise/shims"), "agents live behind mise shims");
    }
}
