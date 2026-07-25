use crate::balancer::traits::LoadBalancer;
use crate::errors::ProxyError;
use crate::network::traits::{AsyncStream, StreamListener, StreamListenerFactory};
use crate::state::ProxyConfig;
use std::io::ErrorKind::{
    AddrInUse, AddrNotAvailable, InvalidData, InvalidInput, PermissionDenied,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub struct ProxyServer<L, B> {
    listener_factory: L,
    balancer: Arc<B>,
    ports: Vec<u16>,
    cancel_token: CancellationToken,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
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
    ) -> Self {
        Self {
            listener_factory,
            balancer: Arc::new(balancer),
            ports: config.ports().collect(),
            cancel_token,
            open_connections: Arc::new(Semaphore::const_new(max_connections)),
            queued_connections: Arc::new(Semaphore::const_new(max_queue)),
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

async fn listen<L, B>(
    port: u16,
    listener_factory: L,
    balancer: Arc<B>,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> Result<(), ProxyError>
where
    L: StreamListenerFactory + 'static,
    B: LoadBalancer + 'static,
{
    let listener = listener_factory.bind(&format!("[::]:{port}")).await?;
    let mut connections = JoinSet::new();

    loop {
        select! {
            accept_result = listener.accept() => {
                let (in_sock, _source_addr) = match accept_result {
                    Ok(res) => res,
                    Err(e) => {
                        if let ProxyError::IoError(ref io_err) = e {
                            let kind = io_err.kind();
                            if matches!(kind, InvalidInput | InvalidData | PermissionDenied | AddrInUse | AddrNotAvailable) {
                                eprintln!("fatal error on accept for port {port}");
                                return Err(e);
                            }
                            eprintln!("transient error on accept for port {port}");
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            continue;
                        }
                        return Err(e);
                    }
                };
                let new_balancer = balancer.clone();
                if let Ok(permit) = open_connections.clone().try_acquire_owned() {
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(e) = do_connection(port, in_sock, new_balancer).await {
                            eprintln!("Connection to backends for port {port} failed: {e}")
                        }
                    });
                    continue;
                };

                if let Ok(queue_slot) = queued_connections.clone().try_acquire_owned() {
                    let new_open_connections = open_connections.clone();
                    connections.spawn(async move {
                        let Ok(_permit) = new_open_connections.clone().acquire_owned().await else {
                            return;
                        };
                        drop(queue_slot);
                        if let Err(e) = do_connection(port, in_sock, new_balancer).await {
                            eprintln!("Connection to backends for port {port} failed: {e}")
                        }
                    });
                    continue;
                }
                eprintln!("Connection dropped for port {port}, too many connections");
                continue;
            }
            _ = cancel_token.cancelled() => {
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

async fn do_connection<S, B>(port: u16, mut in_sock: S, balancer: Arc<B>) -> Result<(), ProxyError>
where
    S: AsyncStream,
    B: LoadBalancer,
{
    let (mut out_sock, _) = balancer.connect_backend(port).await?;
    tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}
