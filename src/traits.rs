use crate::errors::ProxyError;
use crate::state::{BackendStatus, ProxyConfig, TargetState};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;
use std::{collections::HashMap, future::Future};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::timeout;

pub trait LoadBalancer: Send + Sync {
    type Resolver: Resolver;
    type Connector: StreamConnector;

    fn connect_backend(
        &self,
        for_port: u16,
    ) -> impl Future<
        Output = Result<(<Self::Connector as StreamConnector>::Stream, SocketAddr), ProxyError>,
    > + Send;
}

pub struct RoundRobinBalancer<R, C> {
    config: Arc<ProxyConfig>,
    targets: Arc<TargetState>,
    port_to_rr_counter: HashMap<u16, AtomicUsize>,
    resolver: R,
    connector: C,
}
impl<R, C> RoundRobinBalancer<R, C> {
    pub fn new(config: Arc<ProxyConfig>, targets: TargetState, resolver: R, connector: C) -> Self
    where
        R: Resolver,
        C: StreamConnector,
    {
        let mut counters = HashMap::<u16, AtomicUsize>::new();
        for port in config.ports() {
            counters.insert(port, AtomicUsize::new(0));
        }
        RoundRobinBalancer {
            config,
            targets: Arc::new(targets),
            port_to_rr_counter: counters,
            resolver,
            connector,
        }
    }
}
impl<R, C> LoadBalancer for RoundRobinBalancer<R, C>
where
    R: Resolver,
    C: StreamConnector,
{
    type Resolver = R;
    type Connector = C;

    async fn connect_backend(
        &self,
        for_port: u16,
    ) -> Result<
        (
            <<Self as LoadBalancer>::Connector as StreamConnector>::Stream,
            SocketAddr,
        ),
        ProxyError,
    > {
        let Some(backends) = &self.config.get_pool(for_port) else {
            return Err(ProxyError::TargetResolutionError {
                port: for_port,
                message: "No targets configured".to_string(),
            });
        };

        if backends.is_empty() {
            return Err(ProxyError::TargetResolutionError {
                port: for_port,
                message: format!("empty backends list for port {for_port}"),
            });
        }

        let start_idx = match &self.port_to_rr_counter.get(&for_port) {
            Some(counter) => counter.fetch_add(1, Relaxed) % backends.len(),
            None => {
                return Err(ProxyError::ProxyStateError {
                    message: format!("failed to load round robin counter for port {for_port}"),
                });
            }
        };

        // Consider a fall back if all backends are cooling down
        for i in 0..backends.len() {
            let target = &backends[(start_idx + i) % backends.len()];
            {
                let target_status = self.targets.get_target_status(target)?;
                match **target_status {
                    BackendStatus::Drain => continue,
                    BackendStatus::Alive(Some(time)) if time > Instant::now() => continue,
                    _ => {}
                }
            }

            // TODO: consider caching resolved hosts
            // Also consider moving resolution to a background task and updating no later than the
            // time to live for the resolved address.
            let sock_addr = match self.resolver.lookup_host(target).await {
                Ok(mut sock_addrs) => match sock_addrs.next() {
                    Some(a) => a,
                    None => {
                        let _ = self
                            .targets
                            .update_target_status(target, BackendStatus::Drain);
                        eprintln!(
                            "DNS resolution failed for {target}: No IP address found for port {for_port}"
                        );
                        continue;
                    }
                },
                Err(_) => {
                    let _ = self
                        .targets
                        .update_target_status(target, BackendStatus::Drain);
                    eprintln!(
                        "DNS resolution failed for {target}: No IP address found for port {for_port}"
                    );
                    continue;
                }
            };
            // TODO: This deadline and the delays below should be in config
            let connect_timeout = Duration::from_secs(5);
            match timeout(connect_timeout, self.connector.connect(sock_addr)).await {
                // Reset cooldown to empty here
                Ok(Ok(stream)) => return Ok((stream, sock_addr)),
                Ok(Err(e)) => {
                    let _ = self.targets.update_target_status(
                        target,
                        BackendStatus::Alive(Some(Instant::now() + Duration::from_mins(30))),
                    );
                    eprintln!("failed to connect to backend target {target}: {e}");
                }
                Err(_) => {
                    let _ = self.targets.update_target_status(
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
            port: for_port,
            message: format!("Unable to connect to any backend for port {for_port}"),
        })
    }
}

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

pub trait StreamListenerFactory: Send + Sync + Clone {
    type Listener: StreamListener;
    fn bind(&self, addr: String)
    -> impl Future<Output = Result<Self::Listener, ProxyError>> + Send;
}

pub trait StreamListener: Send + Sync {
    type Stream: AsyncStream;
    fn accept(&self)
    -> impl Future<Output = Result<(Self::Stream, SocketAddr), ProxyError>> + Send;
}

pub trait Resolver: Send + Sync + Clone {
    fn lookup_host(
        &self,
        host: &str,
    ) -> impl Future<Output = Result<impl Iterator<Item = SocketAddr>, ProxyError>> + Send;
}

pub trait StreamConnector: Send + Sync + Clone {
    type Stream: AsyncStream;
    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<Self::Stream, ProxyError>> + Send;
}

#[derive(Clone)]
pub struct TokioStreamListenerFactory;
impl StreamListenerFactory for TokioStreamListenerFactory {
    type Listener = TokioListener;
    async fn bind(&self, addr: String) -> Result<Self::Listener, ProxyError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(TokioListener(listener))
    }
}

pub struct TokioListener(pub tokio::net::TcpListener);
impl StreamListener for TokioListener {
    type Stream = tokio::net::TcpStream;
    async fn accept(&self) -> Result<(Self::Stream, SocketAddr), ProxyError> {
        let (stream, addr) = self.0.accept().await?;
        stream.set_nodelay(true)?;
        Ok((stream, addr))
    }
}

#[derive(Clone)]
pub struct TokioResolver;
impl Resolver for TokioResolver {
    async fn lookup_host(
        &self,
        host: &str,
    ) -> Result<impl Iterator<Item = SocketAddr>, ProxyError> {
        let addrs = tokio::net::lookup_host(host).await?;
        Ok(addrs)
    }
}

#[derive(Clone)]
pub struct TokioConnector;
impl StreamConnector for TokioConnector {
    type Stream = tokio::net::TcpStream;
    async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, ProxyError> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}
