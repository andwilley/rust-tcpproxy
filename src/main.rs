use tcpproxy::balancer::roundrobin::RoundRobinBalancer;
use tcpproxy::errors::ProxyError;
use tcpproxy::network::tokio::{TokioConnector, TokioResolver, TokioStreamListenerFactory};
use tcpproxy::state::TargetState;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tcpproxy::{config::RawConfig, proxy, state::ProxyConfig};

#[derive(Parser)]
#[command(version, about = "A simple TCP Proxy", long_about = None)]
struct Args {
    /// Configuration file for this proxy instance
    #[arg(long)]
    config: PathBuf,

    /// Maximum number of concurrent connections to allow.
    #[arg(long, default_value_t = 10000)]
    max_connections: usize,

    /// Maximum number of queued connections to allow (not to be confused with the maximum dynamic
    /// pressure and aerodynamic stress on the proxy).
    #[arg(long, default_value_t = 100)]
    max_queue: usize,
}

// Push a forced shutdown over the channel. Eventually use this to drain connections properly.
async fn shutdown_signal(cancel_token: CancellationToken) {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

    select! {
        _ = tokio::signal::ctrl_c() => {
            println!("SIGINT received: shutting down");
        }
        _ = sigterm.recv() => {
            println!("SIGTERM received: shutting down");
        }
    }

    cancel_token.cancel();
}

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    let args = Args::parse();
    let raw_config = RawConfig::load_from_file(args.config)?;
    let config = Arc::new(ProxyConfig::try_from(&raw_config)?);
    let targets = TargetState::try_from(&raw_config)?;

    let cancel_token = CancellationToken::new();
    tokio::spawn(shutdown_signal(cancel_token.clone()));

    proxy::ProxyServer::new(
        TokioStreamListenerFactory,
        RoundRobinBalancer::new(config.clone(), targets, TokioResolver, TokioConnector),
        config.clone(),
        cancel_token,
        args.max_connections,
        args.max_queue,
    )
    .run()
    .await?;

    Ok(())
}
