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

#[derive(Debug)]
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
    type Stream = C::Stream;

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
                message: format!("Empty backends list for port {for_port}"),
            });
        }

        let start_idx = match &self.port_to_rr_counter.get(&for_port) {
            Some(counter) => counter.fetch_add(1, Relaxed) % backends.len(),
            None => {
                return Err(ProxyError::ProxyStateError {
                    message: format!("Failed to load round robin counter for port {for_port}"),
                });
            }
        };

        let mut cooling: Vec<&str> = Vec::new();
        for i in 0..backends.len() {
            let target = &backends[(start_idx + i) % backends.len()];
            let target_status = self.cooldown.get_target_status(target)?;
            match target_status {
                BackendStatus::Drain => continue,
                BackendStatus::Alive {
                    cool_until: Some(time),
                } if time > Instant::now() => {
                    cooling.push(target);
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

        // Since we failed to connect, try the cooling targets in RR order.
        for target in cooling {
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
                    let _ = self.cooldown.report_connection_attempt(target, Success);
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

#[cfg(test)]
mod tests {
    use crate::{
        balancer::roundrobinharness::{
            ConnectedBackendData, RoundRobinHarness,
            fakes::{
                self, BackendScript, ConnectSpec, CooldownSpec,
                TestEvent::{ConnectAttempted, GotCooldownStatus, ResolveAttempted},
            },
        },
        config::{App, RawConfig},
        errors::ProxyError,
        state::ProxyConfig,
    };
    use std::{
        collections::HashMap,
        time::{Duration, Instant},
    };

    #[tokio::test(start_paused = true)]
    async fn backends_connect_in_order() {
        let mut behaviors = HashMap::new();
        behaviors.insert(BACKEND_0.to_string(), BackendScript::default());
        behaviors.insert(BACKEND_1.to_string(), BackendScript::default());
        behaviors.insert(BACKEND_2.to_string(), BackendScript::default());
        behaviors.insert(BACKEND_3.to_string(), BackendScript::default());
        behaviors.insert(BACKEND_4.to_string(), BackendScript::default());
        let harness = RoundRobinHarness::new(
            default_config(vec![
                BACKEND_0.to_string(),
                BACKEND_1.to_string(),
                BACKEND_2.to_string(),
                BACKEND_3.to_string(),
                BACKEND_4.to_string(),
            ]),
            behaviors,
        );

        let connection_0 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");
        let connection_1 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");
        let connection_2 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");
        let connection_3 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");
        let connection_4 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");

        let expected_0 = ConnectedBackendData::new(0, 0);
        let expected_1 = ConnectedBackendData::new(1, 0);
        let expected_2 = ConnectedBackendData::new(2, 0);
        let expected_3 = ConnectedBackendData::new(3, 0);
        let expected_4 = ConnectedBackendData::new(4, 0);
        assert_eq!(connection_0, expected_0);
        assert_eq!(connection_1, expected_1);
        assert_eq!(connection_2, expected_2);
        assert_eq!(connection_3, expected_3);
        assert_eq!(connection_4, expected_4);
    }

    #[tokio::test(start_paused = true)]
    async fn cooling_backends_connects_first_alive_in_rr_order() {
        let mut behaviors = HashMap::new();
        let later = Instant::now() + Duration::from_mins(5);
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Cooling { until: later },
                connect: ConnectSpec::NoConnection,
            },
        );
        behaviors.insert(
            BACKEND_1.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Cooling { until: later },
                connect: ConnectSpec::NoConnection,
            },
        );
        behaviors.insert(BACKEND_2.to_string(), BackendScript::default());
        // Should not get this far.
        behaviors.insert(BACKEND_3.to_string(), BackendScript::default());
        let harness = RoundRobinHarness::new(
            default_config(vec![
                BACKEND_0.to_string(),
                BACKEND_1.to_string(),
                BACKEND_2.to_string(),
                BACKEND_3.to_string(),
            ]),
            behaviors,
        );

        // skips all cooling backends and connects to first alive (backend 2)
        let connection = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");

        let expected = ConnectedBackendData::new(2, 0);
        assert_eq!(connection, expected);
        // Attempt results in skipping first 2 backends
        let expected_status_checks = vec![
            GotCooldownStatus {
                backend: BACKEND_0.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_1.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_2.to_string(),
            },
        ];
        let actual_cooldowns = harness.get_status_events();
        assert_eq!(actual_cooldowns, expected_status_checks);
        // And only attempts resolution for backend 2.
        let actual_resolves = harness.resolve_events();
        let expected_resolves = vec![ResolveAttempted {
            host: BACKEND_2.to_string(),
        }];
        assert_eq!(actual_resolves, expected_resolves);
    }

    #[tokio::test(start_paused = true)]
    async fn cooling_backends_skip_then_connect_in_order() {
        let mut behaviors = HashMap::new();
        let later = Instant::now() + Duration::from_mins(5);
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Cooling { until: later },
                connect: ConnectSpec::Success { bad_socket_cnt: 0 },
            },
        );
        behaviors.insert(
            BACKEND_1.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Cooling { until: later },
                connect: ConnectSpec::Success { bad_socket_cnt: 0 },
            },
        );
        behaviors.insert(
            BACKEND_2.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Cooling { until: later },
                connect: ConnectSpec::Success { bad_socket_cnt: 0 },
            },
        );
        let harness = RoundRobinHarness::new(
            default_config(vec![
                BACKEND_0.to_string(),
                BACKEND_1.to_string(),
                BACKEND_2.to_string(),
            ]),
            behaviors,
        );

        // each skips all backends and tries itself again in RR order.
        let connection_0 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");
        let connection_1 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");
        let connection_2 = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");

        // First connection
        let expected_0 = ConnectedBackendData::new(0, 0);
        // Each attempt results in skipping each backend then retying in RR order.
        let expected_first_connection_cooldowns = vec![
            GotCooldownStatus {
                backend: BACKEND_0.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_1.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_2.to_string(),
            },
        ];
        assert_eq!(connection_0, expected_0);
        let actual_cooldowns = harness.get_status_events();
        assert_eq!(actual_cooldowns[..3], expected_first_connection_cooldowns);

        // Second connection - check starts at index 1.
        let expected_1 = ConnectedBackendData::new(1, 0);
        assert_eq!(connection_1, expected_1);
        let expected_second_connection_cooldowns = vec![
            GotCooldownStatus {
                backend: BACKEND_1.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_2.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_0.to_string(),
            },
        ];
        assert_eq!(actual_cooldowns[3..6], expected_second_connection_cooldowns);

        // Third connection
        let expected_2 = ConnectedBackendData::new(2, 0);
        assert_eq!(connection_2, expected_2);
        let expected_third_connection_cooldowns = vec![
            GotCooldownStatus {
                backend: BACKEND_2.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_0.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_1.to_string(),
            },
        ];
        assert_eq!(actual_cooldowns[6..], expected_third_connection_cooldowns);
    }

    #[tokio::test(start_paused = true)]
    async fn no_targets_fails() {
        let harness =
            RoundRobinHarness::new(default_config(vec![BACKEND_0.to_string()]), HashMap::new());
        const BAD_PORT: u16 = 9876;
        match harness.connect_backend(BAD_PORT).await {
            Err(ProxyError::TargetResolutionError { port, message }) => {
                assert_eq!(port, BAD_PORT);
                assert_eq!(message, "No targets configured");
            }
            other => panic!("expected target resolution error but got: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn empty_targets_fails() {
        let harness = RoundRobinHarness::new(default_config(Vec::new()), HashMap::new());
        match harness.connect_backend(PORT).await {
            Err(ProxyError::TargetResolutionError { port, message }) => {
                assert_eq!(port, PORT);
                assert_eq!(message, format!("Empty backends list for port {PORT}"));
            }
            other => panic!("expected target resolution error but got: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn all_backends_drained_fail() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Drain,
                connect: ConnectSpec::NoConnection,
            },
        );
        behaviors.insert(
            BACKEND_1.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Drain,
                connect: ConnectSpec::NoConnection,
            },
        );
        behaviors.insert(
            BACKEND_2.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Drain,
                connect: ConnectSpec::NoConnection,
            },
        );
        behaviors.insert(
            BACKEND_3.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Drain,
                connect: ConnectSpec::NoConnection,
            },
        );
        behaviors.insert(
            BACKEND_4.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Drain,
                connect: ConnectSpec::NoConnection,
            },
        );
        let harness = RoundRobinHarness::new(
            default_config(vec![
                BACKEND_0.to_string(),
                BACKEND_1.to_string(),
                BACKEND_2.to_string(),
                BACKEND_3.to_string(),
                BACKEND_4.to_string(),
            ]),
            behaviors,
        );

        match harness.connect_backend(PORT).await {
            Err(ProxyError::ConnectionError { port, message }) => {
                assert_eq!(port, PORT);
                assert_eq!(
                    message,
                    format!("Unable to connect to any backend for port {PORT}")
                );
            }
            other => panic!("expected unable to connect but got: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cooldown_check_fail() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Fail,
                connect: ConnectSpec::NoConnection,
            },
        );
        let harness =
            RoundRobinHarness::new(default_config(vec![BACKEND_0.to_string()]), behaviors);

        match harness.connect_backend(PORT).await {
            Err(ProxyError::BackendNotFound { name }) => {
                assert_eq!(name, BACKEND_0);
            }
            other => panic!("expected cooldown check to fail but got: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn skips_transient_dns_errors_connects() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::ResolveError(fakes::ResolveErrorSpec::Transient),
            },
        );
        behaviors.insert(
            BACKEND_1.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::ResolveError(fakes::ResolveErrorSpec::Nx),
            },
        );
        behaviors.insert(BACKEND_2.to_string(), BackendScript::default());
        let harness = RoundRobinHarness::new(
            default_config(vec![
                BACKEND_0.to_string(),
                BACKEND_1.to_string(),
                BACKEND_2.to_string(),
            ]),
            behaviors,
        );

        // skips resolve fail backends and connects to backend 2
        let connection = harness
            .connect_backend(PORT)
            .await
            .expect("Should connect OK");

        let expected = ConnectedBackendData::new(2, 0);
        assert_eq!(connection, expected);
        // Attempt results in skipping first 2 backends
        let expected_status_checks = vec![
            GotCooldownStatus {
                backend: BACKEND_0.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_1.to_string(),
            },
            GotCooldownStatus {
                backend: BACKEND_2.to_string(),
            },
        ];
        let actual_cooldowns = harness.get_status_events();
        assert_eq!(actual_cooldowns, expected_status_checks);
        // Should attempt resolution on all backends.
        let actual_resolves = harness.resolve_events();
        let expected_resolves = vec![
            ResolveAttempted {
                host: BACKEND_0.to_string(),
            },
            ResolveAttempted {
                host: BACKEND_1.to_string(),
            },
            ResolveAttempted {
                host: BACKEND_2.to_string(),
            },
        ];
        assert_eq!(actual_resolves, expected_resolves);
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_resolution_error_fails() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::ResolveError(fakes::ResolveErrorSpec::Fatal),
            },
        );
        let harness =
            RoundRobinHarness::new(default_config(vec![BACKEND_0.to_string()]), behaviors);

        match harness.connect_backend(PORT).await {
            Err(ProxyError::ProxyStateError { message }) => {
                assert_eq!(message, format!("fatal resolve: {BACKEND_0}"));
            }
            other => panic!("expected resolve to fail fatally but got: {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn connect_fails_second_has_fail_sockets_connects() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::Fail,
            },
        );
        behaviors.insert(
            BACKEND_1.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::Success { bad_socket_cnt: 2 },
            },
        );
        let harness = RoundRobinHarness::new(
            default_config(vec![BACKEND_0.to_string(), BACKEND_1.to_string()]),
            behaviors,
        );

        let connection = harness.connect_backend(PORT).await.expect("connection OK");

        let expected = ConnectedBackendData::new(1, 2);
        assert_eq!(connection, expected);
        // Tries connections for backend 0 and for all 3 sockets on backend 1
        let actual_connect_events = harness.connect_events();
        let expected_connect_events = vec![
            ConnectAttempted(ConnectedBackendData::new(0, 0)),
            ConnectAttempted(ConnectedBackendData::new(1, 0)),
            ConnectAttempted(ConnectedBackendData::new(1, 1)),
            ConnectAttempted(ConnectedBackendData::new(1, 2)),
        ];
        assert_eq!(actual_connect_events, expected_connect_events);
    }

    #[tokio::test(start_paused = true)]
    async fn first_all_socks_fail_second_connects() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::SockFail { bad_socket_cnt: 2 },
            },
        );
        behaviors.insert(
            BACKEND_1.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::Success { bad_socket_cnt: 0 },
            },
        );
        let harness = RoundRobinHarness::new(
            default_config(vec![BACKEND_0.to_string(), BACKEND_1.to_string()]),
            behaviors,
        );

        let connection = harness.connect_backend(PORT).await.expect("connection OK");

        let expected = ConnectedBackendData::new(1, 0);
        assert_eq!(connection, expected);
        // Tries connections on 2 socks for backend 0 and succeeds on backend 1
        let actual_connect_events = harness.connect_events();
        let expected_connect_events = vec![
            ConnectAttempted(ConnectedBackendData::new(0, 0)),
            ConnectAttempted(ConnectedBackendData::new(0, 1)),
            ConnectAttempted(ConnectedBackendData::new(1, 0)),
        ];
        assert_eq!(actual_connect_events, expected_connect_events);
    }

    #[tokio::test(start_paused = true)]
    async fn connect_timeout_tries_next_backend() {
        let mut behaviors = HashMap::new();
        behaviors.insert(
            BACKEND_0.to_string(),
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::Timeout,
            },
        );
        behaviors.insert(BACKEND_1.to_string(), BackendScript::default());
        let harness = RoundRobinHarness::new(
            default_config(vec![BACKEND_0.to_string(), BACKEND_1.to_string()]),
            behaviors,
        );

        let start = tokio::time::Instant::now();
        let connection = harness.connect_backend(PORT).await.expect("connection OK");
        let elapsed = start.elapsed();

        assert_eq!(connection, ConnectedBackendData::new(1, 0));
        // Connection timeout elapsed
        assert_eq!(elapsed, Duration::from_secs(5));
        let expected_connect_events = vec![
            ConnectAttempted(ConnectedBackendData::new(0, 0)),
            ConnectAttempted(ConnectedBackendData::new(1, 0)),
        ];
        assert_eq!(harness.connect_events(), expected_connect_events);
    }

    const PORT: u16 = 8080;
    const BACKEND_0: &str = "backend.example.com:0";
    const BACKEND_1: &str = "backend.example.com:1";
    const BACKEND_2: &str = "backend.example.com:2";
    const BACKEND_3: &str = "backend.example.com:3";
    const BACKEND_4: &str = "backend.example.com:4";

    fn default_config(backends: Vec<String>) -> ProxyConfig {
        ProxyConfig::try_from(&RawConfig {
            apps: vec![App {
                name: "test".to_string(),
                ports: vec![PORT],
                targets: backends,
            }],
        })
        .expect("config")
    }
}
