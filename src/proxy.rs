use crate::errors::ProxyError;
use crate::state::ProxyConfig;
use crate::traits::AsyncStream;
use crate::traits::LoadBalancer;
use crate::traits::StreamListener;
use crate::traits::StreamListenerFactory;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::sleep;

pub struct ProxyServer<L, B> {
    listener_factory: L,
    balancer: Arc<B>,
    ports: Vec<u16>,
    shutdown_rx: watch::Receiver<()>,
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
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            listener_factory,
            balancer: Arc::new(balancer),
            ports: config.ports().collect(),
            shutdown_rx,
        }
    }

    pub async fn run(self) -> Result<(), ProxyError> {
        let mut handles = JoinSet::new();
        for port in self.ports {
            handles.spawn(listen(
                port,
                self.listener_factory.clone(),
                self.balancer.clone(),
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

async fn listen<L, B>(
    port: u16,
    listener_factory: L,
    balancer: Arc<B>,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<(), ProxyError>
where
    L: StreamListenerFactory + 'static,
    B: LoadBalancer + 'static,
{
    let listener = listener_factory.bind(format!("[::]:{port}")).await?;
    let mut connections = JoinSet::new();

    loop {
        select! {
            accept_result = listener.accept() => {
                // TODO: handle this result explicitly and backoff for transient errors
                let (in_sock, _source_addr) = accept_result?;
                let new_balancer = balancer.clone();
                connections.spawn(async move {
                    if let Err(e) = do_connection(port, in_sock, new_balancer).await {
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

async fn do_connection<S, B>(port: u16, mut in_sock: S, balancer: Arc<B>) -> Result<(), ProxyError>
where
    S: AsyncStream,
    B: LoadBalancer,
{
    let (mut out_sock, _) = balancer.connect_backend(port).await?;
    tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}
