use tokio::io::{DuplexStream, duplex};

use crate::{
    balancer::{
        roundrobin::RoundRobinBalancer,
        roundrobinharness::fakes::{BackendBehavior, FakeConnector, FakeCooldown, FakeResolver},
        traits::LoadBalancer,
    },
    errors::ProxyError,
    state::{BackendStatus, ProxyConfig},
};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
};

/// TODO docs for harness
/// This currently only supports testing the RR balancer in isolation, so testing actual connections
/// isn't exposed yet.
/// - need to restructure this to make it easier to express the script without confusing alignment
///   between behaviors.
/// - consider making the last of any scripted behavior sticky so we don't have to fight alignment,
///   some java rpc mock libraries do this. We can use a cursor instead of consuming scripts.
/// - we should have an assertion that we used up the entire script
/// - we should allow assertions on filtered versions of scripts to avoid depending on indexes.
/// - we'll basically always want backends indexed in config order, just use index instead of port
/// - add a bunch of validation to catch misconfiguration at test setup
#[derive(Debug)]
pub struct RoundRobinHarness {
    events: Arc<Mutex<Vec<fakes::TestEvent>>>,
    balancer: RoundRobinBalancer<FakeResolver, FakeConnector, FakeCooldown>,
}

impl RoundRobinHarness {
    pub fn new(config: ProxyConfig, script: HashMap<String, Vec<fakes::BackendBehavior>>) -> Self {
        let mut cooldown_scripts = HashMap::new();
        let mut resolve_scripts = HashMap::new();
        let mut connect_scripts: HashMap<SocketAddr, VecDeque<Result<DuplexStream, ProxyError>>> =
            HashMap::new();
        for (target, behaviors) in script {
            let mut cooldown_script: VecDeque<Result<BackendStatus, ProxyError>> = VecDeque::new();
            let mut resolve_script: VecDeque<Result<Vec<SocketAddr>, ProxyError>> = VecDeque::new();
            let (_, target_port) = target
                .split_once(":")
                .unwrap_or_else(|| panic!("parsable configured host:port but got {target}"));
            let target_id: usize = target_port
                .parse()
                .unwrap_or_else(|_| panic!("port parsable as usize but got {target_port}"));
            assert!(
                target_id <= 255,
                "The backend port is used as an octet in the socket IP v4 address as an \
                identifier, so must be less than or equal to 255"
            );

            for (b, behavior) in behaviors.into_iter().enumerate() {
                match behavior {
                    BackendBehavior::ConnectSuccess {
                        cooldown,
                        bad_socket_cnt,
                    } => {
                        cooldown_script.push_back(Ok(BackendStatus::Alive {
                            cool_until: cooldown,
                        }));

                        let mut resolved = Vec::new();
                        for i in 0..bad_socket_cnt {
                            let bad_sock = test_sock(target_id, b, i, fakes::SOCK_FAIL);
                            resolved.push(bad_sock);
                        }
                        let good_sock: SocketAddr = test_sock(target_id, b, 0, fakes::SOCK_GOOD);
                        resolved.push(good_sock);
                        resolve_script.push_back(Ok(resolved));
                        let (_, proxy_end) = duplex(1024 * 8);
                        connect_scripts
                            .entry(good_sock)
                            .or_default()
                            .push_back(Ok(proxy_end));
                    }
                    BackendBehavior::ConnectSockFail { bad_socket_cnt } => {
                        cooldown_script.push_back(Ok(BackendStatus::Alive { cool_until: None }));

                        let mut resolved = Vec::new();
                        for i in 0..bad_socket_cnt {
                            let bad_sock = test_sock(target_id, b, i, fakes::SOCK_FAIL);
                            resolved.push(bad_sock);
                        }
                        resolve_script.push_back(Ok(resolved));
                    }
                    BackendBehavior::ConnectFail {
                        bad_socket_cnt,
                        connect_error,
                    } => {
                        cooldown_script.push_back(Ok(BackendStatus::Alive { cool_until: None }));

                        let mut resolved = Vec::new();
                        for i in 0..bad_socket_cnt {
                            let bad_sock = test_sock(target_id, b, i, fakes::SOCK_FAIL);
                            resolved.push(bad_sock);
                        }
                        let good_sock: SocketAddr = test_sock(target_id, b, 0, fakes::SOCK_GOOD);
                        resolved.push(good_sock);
                        resolve_script.push_back(Ok(resolved));
                        connect_scripts
                            .entry(good_sock)
                            .or_default()
                            .push_back(Err(connect_error));
                    }
                    BackendBehavior::ConnectTimeout { bad_socket_cnt } => {
                        cooldown_script.push_back(Ok(BackendStatus::Alive { cool_until: None }));

                        let mut resolved = Vec::new();
                        for i in 0..bad_socket_cnt {
                            let bad_sock = test_sock(target_id, b, i, fakes::SOCK_FAIL);
                            resolved.push(bad_sock);
                        }
                        let timeout_sock: SocketAddr =
                            test_sock(target_id, b, 0, fakes::SOCK_TIMEOUT);
                        resolved.push(timeout_sock);
                        resolve_script.push_back(Ok(resolved));
                        let (_, proxy_end) = duplex(1024 * 8);
                        connect_scripts
                            .entry(timeout_sock)
                            .or_default()
                            .push_back(Ok(proxy_end));
                    }
                    BackendBehavior::ResolveFail { resolve_error } => {
                        cooldown_script.push_back(Ok(BackendStatus::Alive { cool_until: None }));
                        resolve_script.push_back(Err(resolve_error));
                    }
                    BackendBehavior::CooldownCheckFail { cooldown_error } => {
                        cooldown_script.push_back(Err(cooldown_error));
                    }
                    BackendBehavior::CooldownSkip { cooldown } => {
                        cooldown_script.push_back(Ok(BackendStatus::Alive {
                            cool_until: Some(cooldown),
                        }));
                    }
                }
            }

            cooldown_scripts.insert(target.clone(), Mutex::new(cooldown_script));
            resolve_scripts.insert(target, Mutex::new(resolve_script));
        }
        let locked_connect_scripts = connect_scripts
            .into_iter()
            .map(|(sock, behaviors)| (sock, Mutex::new(behaviors)))
            .collect();
        let events = Arc::new(Mutex::new(Vec::new()));
        let cooldown = FakeCooldown::new(cooldown_scripts, events.clone());
        let resolver = FakeResolver::new(Arc::new(resolve_scripts), events.clone());
        let connector = FakeConnector::new(Arc::new(locked_connect_scripts), events.clone());
        let balancer = RoundRobinBalancer::new(Arc::new(config), resolver, connector, cooldown);
        RoundRobinHarness { balancer, events }
    }

    // TODO:
    // define methods on the harness to interact with the balancer and assert on events.
    pub async fn connect_backend(&self, for_port: u16) -> Result<ConnectedBackendData, ProxyError> {
        let res = self.balancer.connect_backend(for_port).await;
        match res {
            Ok((_, sock)) => Ok(sock.into()),
            Err(e) => Err(e),
        }
    }

    pub fn events(&self) -> Vec<fakes::TestEvent> {
        self.events.lock().expect("clean lock on events").clone()
    }

    pub fn print_events(&self) {
        eprintln!("--- SUT event log dump ---",);
        let Ok(events) = self.events.try_lock() else {
            eprintln!("    <locked>");
            return;
        };
        for (i, e) in events.iter().enumerate() {
            eprintln!("{i}: {e:?}");
        }
    }
}

impl Drop for RoundRobinHarness {
    fn drop(&mut self) {
        if thread::panicking() {
            self.print_events();
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConnectedBackendData {
    pub target_id: usize,
    pub behavior_num: usize,
    pub instance: usize,
}

impl ConnectedBackendData {
    pub fn with_defaults(target_id: usize) -> Self {
        ConnectedBackendData {
            target_id,
            behavior_num: 0,
            instance: 0,
        }
    }
}

impl From<SocketAddr> for ConnectedBackendData {
    fn from(source: SocketAddr) -> ConnectedBackendData {
        let SocketAddr::V4(addr) = source else {
            panic!("expected ipv4 address for test backend");
        };
        let [id, b, instance, _] = addr.ip().octets();
        ConnectedBackendData {
            target_id: id.into(),
            behavior_num: b.into(),
            instance: instance.into(),
        }
    }
}

/// Create an identifiable socket which encodes the backend `id` (the configured backend's port),
/// its order in the behavior list for that target `b`, an `instance` number to differentiate
/// several sockets for that enumeration, and the `port` which is mostly used to drive behavior.
///
/// socket address: id.b.instance.x:port
///                 ^  ^   ^      ^  ^
///                 |  |   |      |  |
///                 |  |   |      |  used to drive test behavior
///                 |  |   |      |
///                 |  |   |      unused
///                 |  |   |
///                 |  |   an extra differentiator for uniqueness
///                 |  |
///                 |  the configured backend's port number for easy identification
///                 |
///                 the index of this specified behavior for this target in the script
fn test_sock(id: usize, b: usize, instance: usize, port: u16) -> SocketAddr {
    format!("{}.{}.{}.0:{}", id, b, instance, port)
        .parse()
        .expect("parsed")
}

pub mod fakes {
    use crate::{
        balancer::traits::{ConnectResult, CooldownHandler},
        errors::ProxyError,
        network::traits::{Resolver, StreamConnector},
        state::BackendStatus,
    };
    use std::{
        collections::{HashMap, VecDeque},
        future::pending,
        io::{Error, ErrorKind},
        net::SocketAddr,
        sync::{Arc, Mutex},
        thread,
        time::Instant,
    };
    use tokio::io::DuplexStream;

    pub const SOCK_FAIL: u16 = 9999;
    pub const SOCK_TIMEOUT: u16 = 8888;
    pub const SOCK_GOOD: u16 = 1111;

    pub enum BackendBehavior {
        CooldownCheckFail {
            cooldown_error: ProxyError,
        },
        ResolveFail {
            resolve_error: ProxyError,
        },
        CooldownSkip {
            cooldown: Instant,
        },
        ConnectSuccess {
            cooldown: Option<Instant>,
            // This many will fail, then one will succeed.
            bad_socket_cnt: usize,
        },
        ConnectSockFail {
            bad_socket_cnt: usize,
        },
        ConnectFail {
            // This many will fail, then one will throw a different error.
            bad_socket_cnt: usize,
            connect_error: ProxyError,
        },
        ConnectTimeout {
            // This many will fail, then one will timeout.
            bad_socket_cnt: usize,
        },
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
    /// invocations of this methods via test events. We don't support a drain script since the proxy
    /// doesn't use it yet.
    #[derive(Debug)]
    pub struct FakeCooldown {
        script: HashMap<String, Mutex<VecDeque<Result<BackendStatus, ProxyError>>>>,
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    impl FakeCooldown {
        pub fn new(
            script: HashMap<String, Mutex<VecDeque<Result<BackendStatus, ProxyError>>>>,
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
            let mut behaviors = self
                .script
                .get(target)
                .unwrap_or_else(|| {
                    panic!("backend exists in script in specified order for {target}")
                })
                .lock()
                .unwrap_or_else(|_| panic!("clean script lock for {target}"));

            behaviors
                .pop_front()
                .unwrap_or_else(|| panic!("cooldown behavior exists for {target}"))
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
            panic!("unimplemented")
        }
    }

    impl Drop for FakeCooldown {
        fn drop(&mut self) {
            if thread::panicking() {
                eprintln!("--- FakeCooldown script remaining dump ---");
                for (k, vs) in self.script.iter() {
                    eprintln!("* {k}");
                    let vals = vs.try_lock();
                    let l = if vals.is_err() { "<locked>" } else { "" };
                    for (i, v) in vals.iter().enumerate() {
                        eprintln!("   {i}: {v:?}{l}");
                    }
                }
            }
        }
    }

    type ResolverScript =
        Arc<HashMap<String, Mutex<VecDeque<Result<Vec<SocketAddr>, ProxyError>>>>>;

    /// Fake resolver which resolves backends based on a script and records attempts in test events.
    #[derive(Clone, Debug)]
    pub struct FakeResolver {
        script: ResolverScript,
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

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
            let mut behaviors = self
                .script
                .get(host)
                .unwrap_or_else(|| panic!("backend exists in script in specified order for {host}"))
                .lock()
                .unwrap_or_else(|_| panic!("clean script lock for {host}"));

            behaviors
                .pop_front()
                .unwrap_or_else(|| panic!("resolve behavior exists for {host}"))
        }
    }

    impl Drop for FakeResolver {
        fn drop(&mut self) {
            if thread::panicking() {
                eprintln!("--- FakeResolver script remaining dump ---");
                for (k, vs) in self.script.iter() {
                    eprintln!("* {k}");
                    let vals = vs.try_lock();
                    let l = if vals.is_err() { "<locked>" } else { "" };
                    for (i, v) in vals.iter().enumerate() {
                        eprintln!("   {i}: {v:?}{l}");
                    }
                }
            }
        }
    }
    type ConnectorScript =
        Arc<HashMap<SocketAddr, Mutex<VecDeque<Result<DuplexStream, ProxyError>>>>>;

    /// Fake connector which connects backends based on a script and records attempts in test
    /// events.
    #[derive(Clone, Debug)]
    pub struct FakeConnector {
        script: ConnectorScript,
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    impl FakeConnector {
        pub fn new(script: ConnectorScript, events: Arc<Mutex<Vec<TestEvent>>>) -> Self {
            FakeConnector { script, events }
        }
    }

    impl StreamConnector for FakeConnector {
        type Stream = DuplexStream;

        async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, ProxyError> {
            if addr.port() == SOCK_FAIL {
                return Err(ProxyError::IoError(Error::new(
                    ErrorKind::AddrNotAvailable,
                    format!("sock connect fail {SOCK_FAIL}: {addr}"),
                )));
            }
            if addr.port() == SOCK_TIMEOUT {
                return pending().await;
            }
            let mut behaviors = self
                .script
                .get(&addr)
                .unwrap_or_else(|| panic!("backend exists for {addr} in script in specified order"))
                .lock()
                .unwrap_or_else(|_| panic!("clean script lock for {addr}"));
            self.events
                .lock()
                .unwrap_or_else(|_| panic!("clean events lock for {addr}"))
                .push(TestEvent::ConnectAttempted { sock: addr });

            behaviors
                .pop_front()
                .unwrap_or_else(|| panic!("connect behavior exists for {addr}"))
        }
    }

    impl Drop for FakeConnector {
        fn drop(&mut self) {
            if thread::panicking() {
                eprintln!("--- FakeConnector script remaining dump ---");
                for (k, vs) in self.script.iter() {
                    eprintln!("* {k}");
                    let vals = vs.try_lock();
                    let l = if vals.is_err() { "<locked>" } else { "" };
                    for (i, v) in vals.iter().enumerate() {
                        let f = format!("{:?}", v);
                        eprintln!("   {i}: {f:.25}{l}");
                    }
                }
            }
        }
    }
}
