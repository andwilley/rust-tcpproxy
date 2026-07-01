use crate::state::BackendStatus;
use crate::state::ProxyState;
use anyhow::Context;
use anyhow::Result;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinSet;

pub async fn proxy_server(config: Arc<ProxyState>, shutdown_rx: watch::Receiver<()>) -> Result<()> {
    let mut handles = JoinSet::new();
    for port in config.ports() {
        handles.spawn(listen(port, config.clone(), shutdown_rx.clone()));
    }
    while let Some(res) = handles.join_next().await {
        match res {
            Ok(Ok(())) => println!("A listener exited successfully"),
            Ok(Err(listen_error)) => {
                return Err(listen_error).context("Listener exited with error");
            }
            Err(join_error) => {
                return Err(join_error.into());
            }
        }
    }
    Ok(())
}

async fn listen(
    port: u16,
    conf: Arc<ProxyState>,
    mut shutdown_rx: watch::Receiver<()>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("[::]:{port}")).await?;

    loop {
        select! {
            accept_result = listener.accept() => {
                let (in_sock, _source_addr) = accept_result?;
                let config = conf.clone();
                tokio::spawn(async move {
                    if let Err(e) = do_connection(port, &config, in_sock).await {
                        eprintln!("Connection to backends for port {port} failed: {e}")
                    }
                });
            }
            _ = shutdown_rx.changed() => {
                break Ok(());
            }
        }
    }
}

async fn do_connection(
    port: u16,
    config: &ProxyState,
    mut in_sock: TcpStream,
) -> std::io::Result<()> {
    let mut out_sock = connect_backend(port, &config).await?;
    tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}

// If we fail to connect, we should update a shared set of bad backends with a timestamp and
// schduled retry. It isn't critical that this is fully synchronized, its OK if a few listeners
// attempt to connect, just that we reasonably quickly mark it for the penalty box.
//
// If possible we could replace with just a generic strategy and implemnt it for each balancer.
async fn connect_backend(port: u16, config: &ProxyState) -> std::io::Result<TcpStream> {
    let Some(targets) = config.get_pool(port) else {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("No targets for port {port}, Something terrible has occurred"),
        ));
    };

    if targets.is_empty() {
        return Err(Error::new(
            ErrorKind::NotFound,
            format!("empty targets list for port {port}"),
        ));
    }

    let start_idx = match config.get_rr_counter(port) {
        Some(counter) => counter.fetch_add(1, Relaxed) % targets.len(),
        None => {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("failed to load round robin counter for port {port}"),
            ));
        }
    };

    for i in 0..targets.len() {
        let target = &targets[(start_idx + i) % targets.len()];
        {
            let target_status = config.get_target_status(target).unwrap().read().unwrap();
            match *target_status {
                BackendStatus::Dead => continue,
                BackendStatus::Alive(Some(time)) if time > Instant::now() => continue,
                _ => {}
            }
        }
        let sock_addr = match resolve(target).await {
            Ok(t) => t,
            // Deny list this target.
            Err(e) => {
                // If we can't update the target status, it means the lock was contended, which
                // means another task was trying to penalize or kill this target, which means our
                // problem is solved.
                let _ = config.update_target_status(target, BackendStatus::Dead);
                return Err(e);
            }
        };
        match TcpStream::connect(sock_addr).await {
            Ok(stream) => return Ok(stream),
            Err(e) => {
                let _ = config.update_target_status(
                    target,
                    BackendStatus::Alive(Some(Instant::now() + Duration::from_mins(30))),
                );
                eprintln!("failed to connect to backend target {target}: {e}");
            }
        };
    }

    Err(Error::new(
        ErrorKind::NotConnected,
        format!("Unable to connect to any backend for port {port}"),
    ))
}

async fn resolve(target: &str) -> std::io::Result<SocketAddr> {
    return match tokio::net::lookup_host(target).await?.next() {
        Some(a) => Ok(a),
        None => Err(Error::new(
            ErrorKind::AddrNotAvailable,
            format!("No IP address found for {target}"),
        )),
    };
}
