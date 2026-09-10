use crate::balancer::traits::LoadBalancer;
use crate::errors::ProxyError;
use crate::network::traits::{AsyncStream, StreamListener, StreamListenerFactory};
use crate::state::ProxyConfig;
use std::io::ErrorKind::InvalidInput;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct ProxyServer<L, B> {
    listener_factory: L,
    balancer: Arc<B>,
    ports: Vec<u16>,
    cancel_token: CancellationToken,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    bind_addr: IpAddr,
}

impl<L, B> ProxyServer<L, B>
where
    L: StreamListenerFactory + 'static,
    B: LoadBalancer + 'static,
{
    pub fn new(
        listener_factory: L,
        balancer: B,
        config: Arc<ProxyConfig>,
        cancel_token: CancellationToken,
        max_connections: usize,
        max_queue: usize,
        bind_addr: IpAddr,
    ) -> Self {
        Self {
            listener_factory,
            balancer: Arc::new(balancer),
            ports: config.ports().collect(),
            cancel_token,
            open_connections: Arc::new(Semaphore::const_new(max_connections)),
            queued_connections: Arc::new(Semaphore::const_new(max_queue)),
            bind_addr,
        }
    }

    pub async fn run(self) -> Result<(), ProxyError> {
        let mut handles = JoinSet::new();
        for port in self.ports {
            handles.spawn(listen(
                port,
                self.listener_factory.clone(),
                self.balancer.clone(),
                self.open_connections.clone(),
                self.queued_connections.clone(),
                self.cancel_token.clone(),
                self.bind_addr,
            ));
        }
        let mut first_error: Option<ProxyError> = None;
        while let Some(res) = handles.join_next().await {
            match res {
                Ok(Ok(())) => debug!("A listener exited successfully"),
                Ok(Err(listen_error)) => {
                    error!(err = %listen_error, "listener failed, shutting down remaining listeners");
                    self.cancel_token.cancel();
                    first_error.get_or_insert(listen_error);
                }
                Err(join_error) => {
                    error!(err = %join_error, "listener task was cancelled or paniced");
                    self.cancel_token.cancel();
                    // Consider splitting cancellation and panic.
                    first_error.get_or_insert(ProxyError::TaskCancellation {
                        message: join_error.to_string(),
                    });
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

async fn listen<L, B>(
    port: u16,
    listener_factory: L,
    balancer: Arc<B>,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    cancel_token: CancellationToken,
    bind_addr: IpAddr,
) -> Result<(), ProxyError>
where
    L: StreamListenerFactory + 'static,
    B: LoadBalancer + 'static,
{
    let listener = listener_factory
        .bind(&SocketAddr::new(bind_addr, port))
        .await?;
    let mut connections = JoinSet::new();

    loop {
        while let Some(res) = connections.try_join_next() {
            if let Err(e) = res {
                error!(port, err = %e, "connection task panicked");
            }
        }
        select! {
            accept_result = listener.accept() => {
                let Err(e) = accept_connection(
                        accept_result,
                        port,
                        balancer.clone(),
                        open_connections.clone(),
                        queued_connections.clone(),
                        &mut connections
                    ).await else {
                        continue;
                    };
                return Err(e);
            }
            _ = cancel_token.cancelled() => {
                break;
            }
        }
    }

    // Stop accepting connections while we shut down.
    drop(listener);

    info!(port, "Shutting down, attempting to drain open connections.");
    select! {
        _ = async {
            while connections.join_next().await.is_some() {}
        } => {}
        _ = sleep(Duration::from_secs(10)) => {
            warn!(port, "Shutdown deadline reached, aborting.")
        }
    }
    Ok(())
}

/// If capacity is available in the active pool or the queue, spawn a task to connect this request.
async fn accept_connection<B, S>(
    accept_result: Result<(S, SocketAddr), ProxyError>,
    port: u16,
    balancer: Arc<B>,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    connections: &mut JoinSet<()>,
) -> Result<(), ProxyError>
where
    B: LoadBalancer + 'static,
    S: AsyncStream,
{
    let (in_sock, _source_addr) = match accept_result {
        Ok(res) => res,
        Err(e) => {
            if let ProxyError::IoError(ref io_err) = e {
                let kind = io_err.kind();
                if kind == InvalidInput {
                    error!(port, err = %e, "fatal error on accept");
                    return Err(e);
                }
                error!(port, err = %e, "transient error on accept");
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Ok(());
            }
            return Err(e);
        }
    };
    let new_balancer = balancer.clone();
    if let Ok(permit) = open_connections.clone().try_acquire_owned() {
        connections.spawn(async move {
            let _permit = permit;
            if let Err(e) = do_connection(port, in_sock, new_balancer).await {
                error!(port, err = %e, "Connection to backends failed")
            }
        });
        return Ok(());
    };

    if let Ok(queue_slot) = queued_connections.clone().try_acquire_owned() {
        let new_open_connections = open_connections.clone();
        connections.spawn(async move {
            let Ok(_permit) = new_open_connections.clone().acquire_owned().await else {
                return;
            };
            drop(queue_slot);
            if let Err(e) = do_connection(port, in_sock, new_balancer).await {
                error!(port, err = %e, "Connection to backends failed")
            }
        });
        return Ok(());
    }
    error!(port, "Connection dropped, too many connections");
    Ok(())
}

async fn do_connection<S, B>(port: u16, mut in_sock: S, balancer: Arc<B>) -> Result<(), ProxyError>
where
    S: AsyncStream,
    B: LoadBalancer,
{
    let (mut out_sock, _) = balancer.connect_backend(port).await?;
    tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}
