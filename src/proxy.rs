use crate::config::ProxyConfig;
use anyhow::Result;
use anyhow::anyhow;
use rand::seq::IndexedRandom;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinSet;

pub async fn proxy_server(
    config: Arc<ProxyConfig>,
    shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut handles = JoinSet::new();
    let ports = config.ports();
    for port in ports {
        handles.spawn(listen(port, config.clone(), shutdown_rx.clone()));
    }
    while let Some(res) = handles.join_next().await {
        match res {
            Ok(Ok(())) => {
                println!("A listener exited successfully");
            }
            Ok(Err(listen_error)) => {
                println!("listener exited with error: {listen_error}");
                return Err(anyhow!(listen_error));
            }
            Err(join_error) => {
                println!("join failed with error: {join_error}");
                return Err(anyhow!(join_error));
            }
        }
    }
    Ok(())
}

async fn listen(
    port: u16,
    config: Arc<ProxyConfig>,
    mut shutdown_rx: watch::Receiver<()>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(format!("[::]:{port}")).await?;

    loop {
        select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((mut in_sock, _source_addr)) => {
                        // These need timeouts (or keepalives)
                        // Use config to pick a backend
                        let Some(targets) = config.get_pool(port) else {
                            return Err(Error::new(ErrorKind::NotFound, format!("No targets for port {port} Something terrible has occurred")));
                        };
                        let Some(target) = targets.choose(&mut rand::rng()).cloned() else {
                            return Err(Error::new(ErrorKind::NotFound, format!("Empty targets list for port {port}")));
                        };

                        // TODO: we need to pass the whole targets list into here
                        // If we fail to connect we need to drop that from the list and try another
                        // one. Do this in a function like "connect_to_backend(backends)"
                        //
                        // If we fail to connect, we should update a shared set of bad backends
                        // with a timestamp and schduled retry. It isn't critical that this is fully
                        // synchronized, its OK if a few listeners attempt to connect, just that we
                        // eventually mark it for the penalty box.
                        //
                        // We'd like to replace rand with round robin using an atomic counter.
                        //
                        // If possible we could replace with just a generic strategy and implemnt
                        // it for each balancer.
                        tokio::spawn(async move {
                            let mut out_sock = match TcpStream::connect(target).await {
                                Ok(stream) => stream,
                                Err(e) => {
                                    eprintln!("failed to connect to backend target {target}: {e}");
                                }
                            };
                            match tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock)
                                .await {
                                    // TODO: Log the bytes.
                                    Ok((_out_bytes, _in_bytes)) => { return Ok(()) }
                                    Err(e) => { return Err(e) }
                                }
                        });
                    }
                    Err(e) => { return Err(e); }
                }
            }
            shutdown_result = shutdown_rx.changed() => {
                if shutdown_result.is_ok() {
                    let _ = shutdown_rx.borrow_and_update();
                }
                break Ok(());
            }


        }
    }
}
