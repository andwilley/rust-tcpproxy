use crate::{
    balancer::{
        roundrobin::RoundRobinBalancer,
        roundrobinharness::fakes::{ConnectSpec, FakeConnector, FakeCooldown, FakeResolver},
        traits::LoadBalancer,
    },
    errors::ProxyError,
    state::ProxyConfig,
};
use std::{
    collections::{HashMap, HashSet},
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

/// A test harness for unit testing the RoundRobinBalancer. Currently supports a single cripted
/// behvaior (cooldown and connect) per harness instance. Describe your scripted behavior for each
/// backend and construct the harness, then invoke connect_backend and assert on the data returned
/// or the events log, which records work done by the fakes in the test.
/// TODO:
/// - Support updating the script between invocations of connect_backend. This most likely looks
///   like a shared reference to the script maps owned by the harness and a method on the harness to
///   update or add a key.
/// - Support expecting logs and throwing when we see unexpected warnings or higher.
/// - Consider just IDing targets by their order in the config. On the fence as this may make that
///   behavior more surprising than forcing naming by the test writer, but it would simplify the
///   harness API.
/// - Hand out the test-side of the connection when we attempt connection. These tests don't require
///   it, but future tests would likely benefit. This almost certainly looks like creating the
///   connection on the invocation of the harness's connect_backend, passing the proxy side to the
///   connector and returning the test side to the caller with the connection result.
#[derive(Debug)]
pub struct RoundRobinHarness {
    events: Arc<Mutex<Vec<fakes::TestEvent>>>,
    balancer: RoundRobinBalancer<FakeResolver, FakeConnector, FakeCooldown>,
}

impl RoundRobinHarness {
    pub fn new(config: ProxyConfig, script: HashMap<String, fakes::BackendScript>) -> Self {
        let mut cooldown_script = HashMap::new();
        let mut resolve_script: HashMap<String, fakes::ResolveSpec> = HashMap::new();
        let mut seen = HashSet::new();
        for (target, behavior) in script {
            let (_, target_port) = target
                .split_once(":")
                .unwrap_or_else(|| panic!("parsable configured host:port but got {target}"));
            let target_id: u8 = target_port
                .parse()
                .unwrap_or_else(|_| panic!("port parsable as u8 but got {target_port}"));
            if seen.contains(&target_id) {
                panic!("target ID {target_id} already used");
            }
            seen.insert(target_id);

            cooldown_script.insert(target.clone(), behavior.cooldown);
            match behavior.connect {
                ConnectSpec::Success { bad_socket_cnt } => {
                    let mut resolved = Vec::new();
                    for i in 0..bad_socket_cnt {
                        let bad_sock = test_sock(target_id, i, fakes::SOCK_FAIL);
                        resolved.push(bad_sock);
                    }
                    let good_sock: SocketAddr =
                        test_sock(target_id, bad_socket_cnt, fakes::SOCK_GOOD);
                    resolved.push(good_sock);
                    resolve_script.insert(target, fakes::ResolveSpec::Success(resolved));
                }
                ConnectSpec::SockFail { bad_socket_cnt } => {
                    assert!(bad_socket_cnt > 0, "bad socket cnt must be greater than 0");
                    let mut resolved = Vec::new();
                    for i in 0..bad_socket_cnt {
                        let bad_sock = test_sock(target_id, i, fakes::SOCK_FAIL);
                        resolved.push(bad_sock);
                    }
                    resolve_script.insert(target, fakes::ResolveSpec::Success(resolved));
                }
                ConnectSpec::Fail => {
                    let conn_err_sock: SocketAddr =
                        test_sock(target_id, 0, fakes::SOCK_CONNECT_ERR);
                    resolve_script.insert(target, fakes::ResolveSpec::Success(vec![conn_err_sock]));
                }
                ConnectSpec::Timeout => {
                    let timeout_sock: SocketAddr = test_sock(target_id, 0, fakes::SOCK_TIMEOUT);
                    resolve_script.insert(target, fakes::ResolveSpec::Success(vec![timeout_sock]));
                }
                ConnectSpec::ResolveError(resolve_error) => {
                    resolve_script.insert(target, fakes::ResolveSpec::Error(resolve_error));
                }
                ConnectSpec::NoConnection => {}
            }
        }
        let events = Arc::new(Mutex::new(Vec::new()));
        let cooldown = FakeCooldown::new(cooldown_script, events.clone());
        let resolver = FakeResolver::new(Arc::new(resolve_script), events.clone());
        let connector = FakeConnector::new(events.clone());
        let balancer = RoundRobinBalancer::new(Arc::new(config), resolver, connector, cooldown);
        RoundRobinHarness { balancer, events }
    }

    /// Run the round robin balancer for the present script.
    pub async fn connect_backend(&self, for_port: u16) -> Result<ConnectedBackendData, ProxyError> {
        let res = self.balancer.connect_backend(for_port).await;
        match res {
            Ok((_, sock)) => Ok(sock.into()),
            Err(e) => Err(e),
        }
    }

    pub fn get_status_events(&self) -> Vec<fakes::TestEvent> {
        self.events_where(|e| matches!(e, fakes::TestEvent::GotCooldownStatus { .. }))
    }

    pub fn resolve_events(&self) -> Vec<fakes::TestEvent> {
        self.events_where(|e| matches!(e, fakes::TestEvent::ResolveAttempted { .. }))
    }

    pub fn events_where(
        &self,
        mut filter: impl FnMut(&fakes::TestEvent) -> bool,
    ) -> Vec<fakes::TestEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|e| filter(e))
            .cloned()
            .collect()
    }
}

/// Test-relevant data about a particular connected backend.
#[derive(Debug, PartialEq, Eq)]
pub struct ConnectedBackendData {
    pub target_id: usize,
    pub instance: usize,
}

impl ConnectedBackendData {
    pub fn new(target_id: usize, instance: usize) -> Self {
        ConnectedBackendData {
            target_id,
            instance,
        }
    }
}

impl From<SocketAddr> for ConnectedBackendData {
    fn from(source: SocketAddr) -> ConnectedBackendData {
        let SocketAddr::V4(addr) = source else {
            panic!("expected ipv4 address for test backend");
        };
        let [id, _, instance, _] = addr.ip().octets();
        ConnectedBackendData {
            target_id: id.into(),
            instance: instance.into(),
        }
    }
}

/// Create an identifiable socket which encodes the backend `id` (the configured backend's port), an
/// `instance` number to differentiate several sockets for that enumeration, and the `port` which is
/// mostly used to drive behavior.
///
/// socket address: id.instance.x.y:port
///                 ^  ^        ^ ^ ^
///                 |  |        | | |
///                 |  |        | | used to drive test behavior
///                 |  |        | unused
///                 |  |        unused
///                 |  an extra differentiator for uniqueness
///                 the ID provided via port number for this target
fn test_sock(id: u8, instance: u8, port: u16) -> SocketAddr {
    SocketAddr::from((Ipv4Addr::new(id, instance, 0, 0), port))
}

pub mod fakes {
    use crate::{
        balancer::traits::{ConnectResult, CooldownHandler},
        errors::ProxyError,
        network::traits::{Resolver, StreamConnector},
        state::BackendStatus,
    };
    use std::{
        collections::HashMap,
        future::pending,
        io::{Error, ErrorKind},
        net::SocketAddr,
        sync::{Arc, Mutex},
        thread,
        time::Instant,
    };
    use tokio::io::{DuplexStream, duplex};

    pub const SOCK_FAIL: u16 = 9999;
    pub const SOCK_TIMEOUT: u16 = 8888;
    pub const SOCK_GOOD: u16 = 1111;
    pub const SOCK_CONNECT_ERR: u16 = 7777;

    /// A script for how a particular backend should behave during a single test.
    #[derive(Debug)]
    pub struct BackendScript {
        pub cooldown: CooldownSpec,
        pub connect: ConnectSpec,
    }

    impl Default for BackendScript {
        fn default() -> Self {
            BackendScript {
                cooldown: CooldownSpec::Alive,
                connect: ConnectSpec::Success { bad_socket_cnt: 0 },
            }
        }
    }

    #[derive(Debug)]
    pub enum ConnectSpec {
        ResolveError(ResolveErrorSpec),
        Success {
            // This many resolved addrs will fail, then one, at instance `bad_socket_cnt` will
            // succeed.
            bad_socket_cnt: u8,
        },
        SockFail {
            // This many resolved addrs will fail, none will succeed.
            bad_socket_cnt: u8,
        },
        Fail,
        Timeout,
        NoConnection,
    }

    #[derive(Debug, PartialEq, Eq, Clone)]
    pub enum TestEvent {
        GotCooldownStatus {
            backend: String,
        },
        ResolveAttempted {
            host: String,
        },
        ConnectAttempted {
            sock: SocketAddr,
        },
        CooldownReported {
            backend: String,
            result: ConnectResult,
        },
    }

    /// Fake CooldownHandler built with a script for each backend it handles and which records
    /// invocations of this methods via test events. Cooldown status is set per-test attempt. We
    /// don't support a drain script since the proxy doesn't use it yet.
    #[derive(Debug)]
    pub struct FakeCooldown {
        script: HashMap<String, CooldownSpec>,
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    #[derive(Debug)]
    pub enum CooldownSpec {
        Alive,
        Cooling { until: Instant },
        Fail,
        Drain,
    }

    impl FakeCooldown {
        pub fn new(
            script: HashMap<String, CooldownSpec>,
            events: Arc<Mutex<Vec<TestEvent>>>,
        ) -> Self {
            FakeCooldown { script, events }
        }
    }

    impl CooldownHandler for FakeCooldown {
        fn get_target_status(&self, target: &str) -> Result<BackendStatus, ProxyError> {
            self.events
                .lock()
                .unwrap_or_else(|_| panic!("clean events lock for {target}"))
                .push(TestEvent::GotCooldownStatus {
                    backend: target.to_string(),
                });
            let behavior = self
                .script
                .get(target)
                .unwrap_or_else(|| panic!("backend exists in script for {target}"));

            match behavior {
                CooldownSpec::Alive => Ok(BackendStatus::Alive { cool_until: None }),
                CooldownSpec::Cooling { until } => Ok(BackendStatus::Alive {
                    cool_until: Some(*until),
                }),
                CooldownSpec::Fail => Err(ProxyError::BackendNotFound {
                    name: target.to_string(),
                }),
                CooldownSpec::Drain => Ok(BackendStatus::Drain),
            }
        }

        fn report_connection_attempt(
            &self,
            target: &str,
            result: ConnectResult,
        ) -> Result<(), ProxyError> {
            self.events
                .lock()
                .unwrap_or_else(|_| panic!("clean events lock for {target}"))
                .push(TestEvent::CooldownReported {
                    backend: target.to_string(),
                    result,
                });
            Ok(())
        }

        fn drain(&self, _target: &str) -> Result<(), ProxyError> {
            unimplemented!()
        }
    }

    impl Drop for FakeCooldown {
        fn drop(&mut self) {
            if thread::panicking() {
                eprintln!("--- FakeCooldown script dump ---");
                for (target, script) in self.script.iter() {
                    eprintln!("* {target}: {script:?}");
                }
            }
        }
    }

    /// Fake resolver which resolves backends based on a script and records attempts in test events.
    #[derive(Clone, Debug)]
    pub struct FakeResolver {
        script: ResolverScript,
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    #[derive(Clone, Debug)]
    pub enum ResolveErrorSpec {
        Nx,
        Transient,
        Fatal,
    }

    #[derive(Clone, Debug)]
    pub enum ResolveSpec {
        Success(Vec<SocketAddr>),
        Error(ResolveErrorSpec),
    }

    type ResolverScript = Arc<HashMap<String, ResolveSpec>>;

    impl FakeResolver {
        pub fn new(script: ResolverScript, events: Arc<Mutex<Vec<TestEvent>>>) -> Self {
            FakeResolver { script, events }
        }
    }

    impl Resolver for FakeResolver {
        async fn lookup_host(&self, host: &str) -> Result<Vec<SocketAddr>, ProxyError> {
            self.events
                .lock()
                .unwrap_or_else(|_| panic!("clean events lock for {host}"))
                .push(TestEvent::ResolveAttempted {
                    host: host.to_string(),
                });
            let behavior = self
                .script
                .get(host)
                .unwrap_or_else(|| panic!("backend exists in script for {host}"));

            match behavior {
                ResolveSpec::Success(socks) => Ok(socks.clone()),
                ResolveSpec::Error(ResolveErrorSpec::Nx) => Err(ProxyError::DnsNxError {
                    message: format!("nx: {host}"),
                }),
                ResolveSpec::Error(ResolveErrorSpec::Transient) => {
                    Err(ProxyError::DnsTransientError {
                        message: format!("transient: {host}"),
                    })
                }
                ResolveSpec::Error(ResolveErrorSpec::Fatal) => Err(ProxyError::ProxyStateError {
                    message: format!("fatal resolve: {host}"),
                }),
            }
        }
    }

    /// Fake connector which connects backends based on specifically assigned port numbers and
    /// records attempts in test events. This throws away the test-side of the connection.
    #[derive(Clone, Debug)]
    pub struct FakeConnector {
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    impl FakeConnector {
        const DUPLEX_BUF_SIZE: usize = 1024 * 8;

        pub fn new(events: Arc<Mutex<Vec<TestEvent>>>) -> Self {
            FakeConnector { events }
        }
    }

    impl StreamConnector for FakeConnector {
        type Stream = DuplexStream;

        async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, ProxyError> {
            self.events
                .lock()
                .unwrap_or_else(|_| panic!("clean events lock for {addr}"))
                .push(TestEvent::ConnectAttempted { sock: addr });
            match addr.port() {
                SOCK_GOOD => {
                    let (_test_side, proxy_side) = duplex(Self::DUPLEX_BUF_SIZE);
                    Ok(proxy_side)
                }
                SOCK_FAIL => Err(ProxyError::IoError(Error::new(
                    ErrorKind::AddrNotAvailable,
                    format!("sock connect fail {SOCK_FAIL}: {addr}"),
                ))),
                SOCK_CONNECT_ERR => Err(ProxyError::ConnectionError {
                    port: addr.port(),
                    message: "scripted connect error".to_string(),
                }),
                SOCK_TIMEOUT => pending().await,
                other => {
                    panic!("unknown scripted socket {other}")
                }
            }
        }
    }
}
