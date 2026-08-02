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
use tracing::error;

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

    async fn connect_backend(
        &self,
        for_port: u16,
    ) -> Result<(<Self::Connector as StreamConnector>::Stream, SocketAddr), ProxyError> {
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

        // TODO: Consider a fall back if all backends are cooling down, cycle though by coolest and
        // if one succeeds reset the cooldown to now or null.
        for i in 0..backends.len() {
            let target = &backends[(start_idx + i) % backends.len()];
            {
                let target_status = self.cooldown.get_target_status(target)?;
                match target_status {
                    BackendStatus::Drain => continue,
                    BackendStatus::Alive {
                        cool_until: Some(time),
                    } if time > Instant::now() => continue,
                    _ => {}
                }
            }

            let sock_addrs = match self.resolver.lookup_host(target).await {
                Ok(ips) => ips,
                Err(ProxyError::DnsNxError { message }) => {
                    let _ = self.cooldown.drain(target);
                    error!(
                        port = for_port,
                        target, message, "DNS resolution failed: No IP addresses found",
                    );
                    continue;
                }
                Err(ProxyError::DnsTransientError { message }) => {
                    let _ = self.cooldown.report_connection_attempt(target, Failure);
                    error!(port = for_port, target, message, "DNS resolution failed");
                    continue;
                }
                Err(e) => {
                    return Err(e);
                }
            };
            for sock_addr in sock_addrs {
                let connect_timeout = Duration::from_secs(5);
                match timeout(connect_timeout, self.connector.connect(sock_addr)).await {
                    Ok(Ok(stream)) => {
                        self.cooldown.report_connection_attempt(target, Success)?;
                        return Ok((stream, sock_addr));
                    }
                    Ok(Err(e)) => {
                        error!(
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
                            connect_timeout
                        );
                        continue;
                    }
                };
            }
            let _ = self.cooldown.report_connection_attempt(target, Failure);
        }
        Err(ProxyError::ConnectionError {
            port: for_port,
            message: format!("Unable to connect to any backend for port {for_port}"),
        })
    }
}
