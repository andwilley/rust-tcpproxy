use crate::errors::ProxyError;
use crate::state::BackendStatus;
use crate::state::ProxyState;
use crate::traits::AsyncStream;
use crate::traits::Resolver;
use crate::traits::StreamConnector;
use crate::traits::StreamListener;
use crate::traits::StreamListenerFactory;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

pub struct ProxyServer<L, C, R> {
    listener_factory: L,
    connector: C,
    resolver: R,
    state: Arc<ProxyState>,
    shutdown_rx: watch::Receiver<()>,
}

impl<L, C, R> ProxyServer<L, C, R>
where
    L: StreamListenerFactory + 'static,
    C: StreamConnector + 'static,
    R: Resolver + 'static,
{
    pub fn new(
        listener_factory: L,
        connector: C,
        resolver: R,
        state: ProxyState,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            listener_factory,
            connector,
            resolver,
            state: Arc::new(state),
            shutdown_rx,
        }
    }

    pub async fn run(self) -> Result<(), ProxyError> {
        let mut handles = JoinSet::new();
        for port in self.state.ports() {
            handles.spawn(listen(
                port,
                self.listener_factory.clone(),
                self.connector.clone(),
                self.resolver.clone(),
                self.state.clone(),
                self.shutdown_rx.clone(),
            ));
        }
        while let Some(res) = handles.join_next().await {
            match res {
                Ok(Ok(())) => println!("A listener exited successfully"),
                Ok(Err(listen_error)) => {
                    return Err(listen_error);
                }
                Err(join_error) => {
                    if join_error.is_cancelled() {
                        return Err(ProxyError::TaskCancellation {
                            message: join_error.to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}

async fn listen<L, C, R>(
    port: u16,
    listener_factory: L,
    connector: C,
    resolver: R,
    state: Arc<ProxyState>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<(), ProxyError>
where
    L: StreamListenerFactory + 'static,
    C: StreamConnector + 'static,
    R: Resolver + 'static,
{
    let listener = listener_factory.bind(format!("[::]:{port}")).await?;
    let mut connections = JoinSet::new();

    loop {
        select! {
            accept_result = listener.accept() => {
                // TODO: handle this result explicitly and backoff for transient errors
                let (in_sock, _source_addr) = accept_result?;
                let new_state = state.clone();
                let new_resolver = resolver.clone();
                let new_connector = connector.clone();
                connections.spawn(async move {
                    if let Err(e) = do_connection(port, &new_state, in_sock, new_resolver, new_connector).await {
                        eprintln!("Connection to backends for port {port} failed: {e}")
                    }
                });
            }
            _ = shutdown_rx.changed() => {
                break;
            }
        }
    }

    println!("Shutting down, attempting to drain open connections.");
    select! {
        _ = async {
            while connections.join_next().await.is_some() {}
        } => {}
        _ = sleep(Duration::from_secs(10)) => {
            println!("Shutdown deadline reached, aborting.")
        }
    }
    Ok(())
}

async fn do_connection<S, R, C>(
    port: u16,
    state: &ProxyState,
    mut in_sock: S,
    resolver: R,
    connector: C,
) -> Result<(), ProxyError>
where
    S: AsyncStream,
    R: Resolver,
    C: StreamConnector,
{
    let mut out_sock = connect_backend_rr(port, state, resolver, connector).await?;
    tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}

// TODO: This should be implemented by a LoadBalancer trait. It needs access to a round robin
// counter state map, this could live on the struct we impl it for as it does not apply to any other
// implemenation of LoadBalancer.
async fn connect_backend_rr<R, C>(
    port: u16,
    state: &ProxyState,
    resolver: R,
    connector: C,
) -> Result<C::Stream, ProxyError>
where
    R: Resolver,
    C: StreamConnector,
{
    let Some(targets) = state.get_pool(port) else {
        return Err(ProxyError::TargetResolutionError {
            port,
            message: "No targets configured".to_string(),
        });
    };

    if targets.is_empty() {
        return Err(ProxyError::TargetResolutionError {
            port,
            message: format!("empty targets list for port {port}"),
        });
    }

    let start_idx = match state.get_rr_counter(port) {
        Some(counter) => counter.fetch_add(1, Relaxed) % targets.len(),
        None => {
            return Err(ProxyError::ProxyStateError {
                message: format!("failed to load round robin counter for port {port}"),
            });
        }
    };

    for i in 0..targets.len() {
        let target = &targets[(start_idx + i) % targets.len()];
        {
            let target_status = state.get_target_status(target)?;
            match **target_status {
                BackendStatus::Drain => continue,
                BackendStatus::Alive(Some(time)) if time > Instant::now() => continue,
                _ => {}
            }
        }
        let sock_addr = match resolve(target, &resolver).await {
            Ok(t) => t,
            Err(e) => {
                let _ = state.update_target_status(target, BackendStatus::Drain);
                eprintln!("DNS resolution failed for {target}: {e}");
                continue;
            }
        };
        // TODO: This deadline and the delays below should be in config
        let connect_timeout = Duration::from_secs(5);
        match timeout(connect_timeout, connector.connect(sock_addr)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => {
                let _ = state.update_target_status(
                    target,
                    BackendStatus::Alive(Some(Instant::now() + Duration::from_mins(30))),
                );
                eprintln!("failed to connect to backend target {target}: {e}");
            }
            Err(_) => {
                let _ = state.update_target_status(
                    target,
                    BackendStatus::Alive(Some(Instant::now() + Duration::from_mins(30))),
                );
                eprintln!(
                    "connection to target {target} timed out after {:?}",
                    connect_timeout
                );
            }
        };
    }
    Err(ProxyError::ConnectionError {
        port,
        message: format!("Unable to connect to any backend for port {port}"),
    })
}

// TODO: consider caching resolved hosts
async fn resolve<R>(target: &str, resolver: &R) -> Result<SocketAddr, ProxyError>
where
    R: Resolver,
{
    return match resolver.lookup_host(target).await?.next() {
        Some(a) => Ok(a),
        None => Err(ProxyError::ProxyStateError {
            message: format!("No IP address found for {target}"),
        }),
    };
}
