use crate::balancer::traits::{
    ConnectResult::Failure, ConnectResult::Success, CooldownHandler, LoadBalancer,
};
use crate::errors::ProxyError;
use crate::network::traits::{Resolver, StreamConnector};
use crate::state::{BackendStatus, ProxyConfig};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;
use tokio::time::timeout;
use tracing::{error, warn};

pub struct RoundRobinBalancer<R, C, B> {
    config: Arc<ProxyConfig>,
    port_to_rr_counter: HashMap<u16, AtomicUsize>,
    resolver: R,
    connector: C,
    cooldown: B,
}

impl<R, C, B> RoundRobinBalancer<R, C, B> {
    pub fn new(config: Arc<ProxyConfig>, resolver: R, connector: C, cooldown: B) -> Self
    where
        R: Resolver,
        C: StreamConnector,
        B: CooldownHandler,
    {
        let mut counters = HashMap::<u16, AtomicUsize>::new();
        for port in config.ports() {
            counters.insert(port, AtomicUsize::new(0));
        }
        RoundRobinBalancer {
            config,
            port_to_rr_counter: counters,
            resolver,
            connector,
            cooldown,
        }
    }
}

impl<R, C, B> LoadBalancer for RoundRobinBalancer<R, C, B>
where
    R: Resolver,
    C: StreamConnector,
    B: CooldownHandler,
{
    type Resolver = R;
    type Connector = C;
    type Cooldown = B;

    async fn connect_backend(&self, for_port: u16) -> Result<(C::Stream, SocketAddr), ProxyError> {
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

        let mut cooling: Vec<(Instant, &str)> = Vec::new();
        for i in 0..backends.len() {
            let target = &backends[(start_idx + i) % backends.len()];
            let target_status = self.cooldown.get_target_status(target)?;
            match target_status {
                BackendStatus::Drain => continue,
                BackendStatus::Alive {
                    cool_until: Some(time),
                } if time > Instant::now() => {
                    cooling.push((time, target));
                    continue;
                }
                _ => {}
            }
            match self.connect(target, for_port).await {
                ConnectAttempt::Ready(s) => return Ok(s),
                ConnectAttempt::TryNext => continue,
                ConnectAttempt::Fatal(e) => return Err(e),
            };
        }
        // Since we failed to connect, try the cooling targets.
        cooling.sort(); // By cool_until ascending.
        for (_, target) in cooling {
            match self.connect(target, for_port).await {
                ConnectAttempt::Ready(s) => return Ok(s),
                ConnectAttempt::TryNext => continue,
                ConnectAttempt::Fatal(e) => return Err(e),
            };
        }
        Err(ProxyError::ConnectionError {
            port: for_port,
            message: format!("Unable to connect to any backend for port {for_port}"),
        })
    }
}

enum ConnectAttempt<S> {
    Ready(S),
    TryNext,
    Fatal(ProxyError),
}

impl<R, C, B> RoundRobinBalancer<R, C, B>
where
    R: Resolver,
    C: StreamConnector,
    B: CooldownHandler,
{
    const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

    async fn connect(
        &self,
        target: &str,
        for_port: u16,
    ) -> ConnectAttempt<(C::Stream, SocketAddr)> {
        let sock_addrs = match self.resolver.lookup_host(target).await {
            Ok(ips) => ips,
            Err(ProxyError::DnsNxError { message }) => {
                let _ = self.cooldown.report_connection_attempt(target, Failure);
                warn!(
                    port = for_port,
                    target, message, "DNS resolution failed: No IP addresses found",
                );
                return ConnectAttempt::TryNext;
            }
            Err(ProxyError::DnsTransientError { message }) => {
                let _ = self.cooldown.report_connection_attempt(target, Failure);
                error!(port = for_port, target, message, "DNS resolution failed");
                return ConnectAttempt::TryNext;
            }
            Err(e) => {
                return ConnectAttempt::Fatal(e);
            }
        };

        for sock_addr in sock_addrs {
            match timeout(Self::CONNECT_TIMEOUT, self.connector.connect(sock_addr)).await {
                Ok(Ok(stream)) => {
                    match self.cooldown.report_connection_attempt(target, Success) {
                        Ok(_) => {}
                        Err(e) => return ConnectAttempt::Fatal(e),
                    };
                    return ConnectAttempt::Ready((stream, sock_addr));
                }
                Ok(Err(e)) => {
                    warn!(
                        port = for_port,
                        target,
                        socket_address = %sock_addr,
                        err = %e,
                        "failed to connect to backend/port",
                    );
                    continue;
                }
                Err(_) => {
                    error!(
                        port = for_port,
                        target,
                        socket_address = %sock_addr,
                        "connection to target timed out after {:?}",
                        Self::CONNECT_TIMEOUT
                    );
                    continue;
                }
            };
        }
        let _ = self.cooldown.report_connection_attempt(target, Failure);
        ConnectAttempt::TryNext
    }
}
