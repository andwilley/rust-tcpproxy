use tcpproxy::errors::ProxyError;
use tokio::select;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;

use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use tcpproxy::{config::RawConfig, proxy, state::ProxyState};

#[derive(Parser)]
#[command(version, about = "A simple TCP Proxy", long_about = None)]
struct Args {
    /// Configuration file for this proxy instance
    #[arg(long)]
    config: PathBuf,
}

// Push a forced shutdown over the channel. Eventually use this to drain connections properly.
async fn shutdown_signal(shutdown_tx: watch::Sender<()>) {
    let mut sigterm = signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");

    select! {
        _ = tokio::signal::ctrl_c() => {
            println!("SIGINT received: shutting down");
        }
        _ = sigterm.recv() => {
            println!("SIGTERM received: shutting down");
        }
    }

    let _ = shutdown_tx.send(());
}

#[tokio::main]
async fn main() -> Result<(), ProxyError> {
    let args = Args::parse();
    let raw_config = RawConfig::load_from_file(args.config)?;
    let proxy_config = ProxyState::try_from(raw_config)?;

    let (shutdown_tx, shutdown_rx) = watch::channel(());
    tokio::spawn(shutdown_signal(shutdown_tx));

    proxy::proxy_server(Arc::new(proxy_config), shutdown_rx).await?;

    Ok(())
}
