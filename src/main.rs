use tcpproxy::balancer::roundrobin::RoundRobinBalancer;
use tcpproxy::balancer::simplecooldownhandler::SimpleCooldownHandler;
use tcpproxy::errors::ProxyError;
use tcpproxy::logswriter::LogsWriter;
use tcpproxy::network::tokio::{HickoryTokioResolver, TokioConnector, TokioStreamListenerFactory};
use tcpproxy::state::TargetState;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use clap::Parser;
use std::net::{IpAddr, Ipv6Addr};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use tcpproxy::{config::RawConfig, proxy, state::ProxyConfig};

#[derive(Parser)]
#[command(
    version,
    about = "A simple TCP Proxy",
    long_about = "
A simple TCP proxy. Not intended for reuse.

Config.json must be updated with valid/test backends. The general structure is:

- A top-level list of Apps
- Each App defines the set of ports it will listen on.
- And a set of backend targets for the proxy to forward this traffic.
- Ports cannot be reused, but backends may apply to more than one App.
"
)]
struct Args {
    /// Configuration file for this proxy instance
    #[arg(long)]
    config: PathBuf,

    /// Maximum number of concurrent connections to allow.
    #[arg(long, default_value_t = 10000)]
    max_connections: usize,

    /// Maximum number of queued connections to allow before we start outright rejecting (not to be
    /// confused with the maximum dynamic pressure and aerodynamic stress on the proxy).
    #[arg(long, default_value_t = 100)]
    max_queue: usize,

    /// Writer to use for logs.
    #[arg(long, default_value_t = LogsWriter::Stderr)]
    logs_writer: LogsWriter,

    /// Local address to bind.
    #[arg(long, default_value_t = IpAddr::V6(Ipv6Addr::UNSPECIFIED))]
    bind: IpAddr,
}

async fn shutdown_signal(cancel_token: CancellationToken) {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

    select! {
        _ = tokio::signal::ctrl_c() => {
            info!("SIGINT received: shutting down");
        }
        _ = sigterm.recv() => {
            info!("SIGTERM received: shutting down");
        }
    }

    cancel_token.cancel();
}

async fn run() -> Result<(), ProxyError> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_writer(args.logs_writer.into_make_writer())
        .init();

    let raw_config = RawConfig::load_from_file(args.config)?;
    let config = Arc::new(ProxyConfig::try_from(&raw_config)?);
    let targets = TargetState::try_from(&raw_config)?;

    let cancel_token = CancellationToken::new();
    tokio::spawn(shutdown_signal(cancel_token.clone()));

    proxy::ProxyServer::new(
        TokioStreamListenerFactory,
        RoundRobinBalancer::new(
            config.clone(),
            HickoryTokioResolver(hickory_resolver::Resolver::builder_tokio()?.build()?),
            TokioConnector,
            SimpleCooldownHandler::new(targets),
        ),
        config.clone(),
        cancel_token,
        args.max_connections,
        args.max_queue,
        args.bind,
    )
    .run()
    .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(e) = run().await {
        error!(err = %e, "exiting: fatal error");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}
