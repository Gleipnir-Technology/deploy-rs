// SPDX-FileCopyrightText: 2020 Serokell <https://serokell.io/>
//
// SPDX-License-Identifier: MPL-2.0

use clap::Parser;
use deploy::cli::{self, Opts};
use deploy::report::DeployEvent;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixListener;
use tokio::sync::mpsc;

/// deploy-rs-daemon — accepts deployment commands over a Unix domain socket.
///
/// Access control relies on filesystem permissions of the socket file.
/// Only processes running as the same user (or root) can connect.
#[derive(Parser, Debug)]
struct DaemonArgs {
    /// Path to the Unix domain socket.
    #[arg(long, default_value = "/run/deploy-rs/daemon.sock")]
    socket: String,

    /// Enable debug logs.
    #[arg(short, long)]
    debug_logs: bool,
}

// ── Wire protocol types ─────────────────────────────────────────

/// A command received from a client over the Unix socket.
///
/// Every message must be a single JSON object on one line (NDJSON),
/// with a `command` field identifying the action.
#[derive(Deserialize, Debug)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Incoming {
    /// Trigger a full deployment (build + push + activate).
    Deploy(DeployPayload),
    /// Run all steps except the final activation.  Useful for pre‑flight checks.
    DryActivate(DeployPayload),
    /// Health check — the daemon responds immediately with `{"type":"result","success":true}`.
    Status,
}

/// Payload for `deploy` and `dry_activate` commands.
///
/// All fields are optional; sensible defaults are applied for omitted values.
#[derive(Deserialize, Debug)]
struct DeployPayload {
    target: Option<String>,
    targets: Option<Vec<String>>,

    #[serde(default)]
    file: Option<String>,

    #[serde(default)]
    checksigs: Option<bool>,

    #[serde(default)]
    extra_build_args: Option<Vec<String>>,

    #[serde(default)]
    debug_logs: Option<bool>,

    #[serde(default)]
    log_dir: Option<String>,

    #[serde(default)]
    keep_result: Option<bool>,

    #[serde(default)]
    result_path: Option<String>,

    #[serde(default)]
    skip_checks: Option<bool>,

    #[serde(default)]
    remote_build: Option<bool>,

    #[serde(default)]
    ssh_user: Option<String>,

    #[serde(default)]
    profile_user: Option<String>,

    #[serde(default)]
    ssh_opts: Option<String>,

    #[serde(default)]
    fast_connection: Option<bool>,

    #[serde(default)]
    auto_rollback: Option<bool>,

    #[serde(default)]
    hostname: Option<String>,

    #[serde(default)]
    magic_rollback: Option<bool>,

    #[serde(default)]
    confirm_timeout: Option<u16>,

    #[serde(default)]
    activation_timeout: Option<u16>,

    #[serde(default)]
    temp_path: Option<PathBuf>,

    #[serde(default)]
    sudo: Option<String>,

    #[serde(default)]
    boot: Option<bool>,
}

impl From<DeployPayload> for Opts {
    fn from(p: DeployPayload) -> Self {
        Opts {
            target: p.target,
            targets: p.targets,
            file: p.file,
            checksigs: p.checksigs.unwrap_or(false),
            interactive: false,
            extra_build_args: p.extra_build_args.unwrap_or_default(),
            debug_logs: p.debug_logs.unwrap_or(false),
            log_dir: p.log_dir,
            keep_result: p.keep_result.unwrap_or(false),
            result_path: p.result_path,
            skip_checks: p.skip_checks.unwrap_or(false),
            remote_build: p.remote_build.unwrap_or(false),
            ssh_user: p.ssh_user,
            profile_user: p.profile_user,
            ssh_opts: p.ssh_opts,
            fast_connection: p.fast_connection,
            auto_rollback: p.auto_rollback,
            hostname: p.hostname,
            magic_rollback: p.magic_rollback,
            confirm_timeout: p.confirm_timeout,
            activation_timeout: p.activation_timeout,
            temp_path: p.temp_path,
            dry_activate: false,
            boot: p.boot.unwrap_or(false),
            rollback_succeeded: None,
            sudo: p.sudo,
            interactive_sudo: Some(false),
        }
    }
}

/// Every line the daemon writes to the client is one of these,
/// serialised as a single‑line JSON object.
#[derive(Serialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Outgoing {
    /// A progress event emitted during deployment.
    Event {
        #[serde(flatten)]
        event: DeployEvent,
    },
    /// Terminal message — the deployment finished (successfully or not).
    Result {
        success: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
}

// ── Helpers ──────────────────────────────────────────────────────

/// Write a single NDJSON line to the writer.
async fn send_line(
    writer: &mut (impl AsyncWriteExt + Unpin),
    msg: &Outgoing,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut json = serde_json::to_string(msg)?;
    json.push('\n');
    writer.write_all(json.as_bytes()).await?;
    Ok(())
}

/// Execute a deploy (or dry‑activate) command, streaming events
/// back to the client over the Unix socket.
async fn run_deploy_cmd(
    payload: DeployPayload,
    dry_activate: bool,
    writer: &mut (impl AsyncWriteExt + Unpin),
) -> Result<(), Box<dyn std::error::Error>> {
    let mut opts: Opts = payload.into();
    opts.dry_activate = dry_activate;

    let (tx, mut rx) = mpsc::channel::<DeployEvent>(64);

    let handle = tokio::spawn(async move { cli::run_with_reporter(opts, tx).await });

    // Stream progress events to the client as they arrive.
    while let Some(event) = rx.recv().await {
        if send_line(writer, &Outgoing::Event { event }).await.is_err() {
            warn!("Client disconnected during deployment; discarding remaining events");
            break;
        }
    }

    // After the channel is closed (deploy finished), report the outcome.
    match handle.await? {
        Ok(()) => {
            send_line(writer, &Outgoing::Result {
                success: true,
                error: None,
            })
            .await?;
        }
        Err(e) => {
            send_line(writer, &Outgoing::Result {
                success: false,
                error: Some(e.to_string()),
            })
            .await?;
        }
    }

    Ok(())
}

// ── Connection handler ───────────────────────────────────────────

async fn handle_connection(
    stream: tokio::net::UnixStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    // Wait for the client to send a command, with a 10‑second timeout.
    match tokio::time::timeout(Duration::from_secs(10), buf_reader.read_line(&mut line)).await {
        Ok(Ok(0)) => return Ok(()), // EOF before data — clean disconnect
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            let _ = send_line(
                &mut writer,
                &Outgoing::Result {
                    success: false,
                    error: Some(format!("read error: {}", e)),
                },
            )
            .await;
            return Err(e.into());
        }
        Err(_) => {
            let _ = send_line(
                &mut writer,
                &Outgoing::Result {
                    success: false,
                    error: Some("read timeout".into()),
                },
            )
            .await;
            return Ok(());
        }
    }

    let cmd: Incoming = match serde_json::from_str(line.trim()) {
        Ok(c) => c,
        Err(e) => {
            let _ = send_line(
                &mut writer,
                &Outgoing::Result {
                    success: false,
                    error: Some(format!("invalid command: {}", e)),
                },
            )
            .await;
            return Ok(());
        }
    };

    info!("Received command: {:?}", cmd);

    match cmd {
        Incoming::Deploy(payload) => run_deploy_cmd(payload, false, &mut writer).await,
        Incoming::DryActivate(payload) => run_deploy_cmd(payload, true, &mut writer).await,
        Incoming::Status => {
            send_line(
                &mut writer,
                &Outgoing::Result {
                    success: true,
                    error: None,
                },
            )
            .await?;
            Ok(())
        }
    }
}

// ── Entry point ──────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = DaemonArgs::parse();

    // Remove a stale socket file left behind by a previous run.
    let _ = std::fs::remove_file(&args.socket);

    // Create the parent directory with owner‑only permissions.
    if let Some(parent) = Path::new(&args.socket).parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }

    let listener = UnixListener::bind(&args.socket)?;

    // Restrict the socket to owner‑only so that filesystem permissions
    // provide access control.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ =
            std::fs::set_permissions(&args.socket, std::fs::Permissions::from_mode(0o600));
    }

    deploy::init_logger(args.debug_logs, None, &deploy::LoggerType::Deploy)?;
    info!(
        "deploy-rs-daemon listening on {}",
        args.socket
    );

    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream).await {
                        error!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => error!("Accept error: {}", e),
        }
    }
}
