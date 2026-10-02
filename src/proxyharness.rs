use crate::errors::ProxyError;
use crate::proxy::ProxyServer;
use crate::state::ProxyConfig;
use std::collections::HashMap;
use std::net::Ipv6Addr;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::DuplexStream;
use tokio::io::duplex;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Manages the running proxy and exposes connections and control. This only works for a single
/// threaded test, which is fine for some of the mechanics and good for determinism, but these are
/// insufficient to cover the concurrent surface area. Integration tests are needed.
///
/// TODO:
/// - add "unexpected warn+ logs throw" and "unreceived expected logs throw"
/// - impl Drop to ensure that the proxy task is cancelled properly
/// - We may need better detection of stream mis-alignment (see no_backend).
pub struct ProxyHarness {
    cancel_token: CancellationToken,
    task: JoinHandle<Result<(), ProxyError>>,
    accept_commands: HashMap<u16, UnboundedSender<fakes::AcceptCommand>>,
    connect_commands: HashMap<u16, UnboundedSender<fakes::ConnectCommand>>,
    duplex_buf_size: usize,
    client_ctr: usize,
    backend_ctr: usize,
}

impl ProxyHarness {
    pub fn builder(config: ProxyConfig) -> ProxyHarnessBuilder {
        ProxyHarnessBuilder {
            config,
            max_connections: 10,
            max_queue: 10,
            duplex_buf_size: 1024 * 64,
            bind_failures: HashMap::new(),
        }
    }

    /// Returns connected (client, backend) streams.
    pub async fn proxied(&mut self, port: u16) -> (DuplexStream, DuplexStream) {
        let (test_side_client, proxy_side_client) = duplex(self.duplex_buf_size);
        let (test_side_backend, proxy_side_backend) = duplex(self.duplex_buf_size);
        let (ack_tx, ack_rx) = oneshot::channel();
        let backend_sock = self.next_backend_sock();
        Self::send_command(
            &self.connect_commands,
            port,
            Ok((proxy_side_backend, backend_sock)),
        );
        let client_sock = self.next_client_sock();
        Self::send_command(
            &self.accept_commands,
            port,
            (Ok((proxy_side_client, client_sock)), ack_tx),
        );
        Self::await_ack(ack_rx).await;
        (test_side_client, test_side_backend)
    }

    /// Client connection fails at accept with the provided error. No connections.
    pub async fn accept_fail(&mut self, port: u16, e: ProxyError) {
        let (ack_tx, ack_rx) = oneshot::channel();
        Self::send_command(&self.accept_commands, port, (Err(e), ack_tx));
        Self::await_ack(ack_rx).await;
    }

    /// Returns the client side of a connection, there will be no backend side.
    pub async fn backend_connect_fail(&mut self, port: u16, e: ProxyError) -> DuplexStream {
        let (test_side_client, proxy_side_client) = duplex(self.duplex_buf_size);
        let client_sock = self.next_client_sock();
        let (ack_tx, ack_rx) = oneshot::channel();
        Self::send_command(&self.connect_commands, port, Err(e));
        Self::send_command(
            &self.accept_commands,
            port,
            (Ok((proxy_side_client, client_sock)), ack_tx),
        );
        Self::await_ack(ack_rx).await;
        test_side_client
    }

    /// No error in accept or connect, accept runs, but no connection is expected, e.g. too many
    /// connections.
    pub async fn no_backend(&mut self, port: u16) -> DuplexStream {
        let (test_side_client, proxy_side_client) = duplex(self.duplex_buf_size);
        let client_sock = self.next_client_sock();
        let (ack_tx, ack_rx) = oneshot::channel();
        Self::send_command(
            &self.accept_commands,
            port,
            (Ok((proxy_side_client, client_sock)), ack_tx),
        );
        Self::await_ack(ack_rx).await;
        test_side_client
    }

    /// Waits for the proxy to finish, propogating panics.
    pub async fn join(self) -> Result<(), ProxyError> {
        match self.task.await {
            Ok(res) => res,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => panic!("proxy task cancelled: {e}"),
        }
    }

    /// Tear down the proxy. Does not close connections. You must await the task via
    /// ProxyHarness::join().
    pub fn cancel(&self) {
        self.cancel_token.cancel();
    }

    /// Tears down the proxy. Does not close open connections. If connections remain open,
    /// the shutdown drain will run and drop them at the timeout.
    pub async fn shutdown(self) -> Result<(), ProxyError> {
        self.cancel();
        self.join().await
    }

    /// Tears down the proxy, expects a clean shutdown.
    pub async fn shutdown_assert_ok(self) {
        match self.shutdown().await {
            Ok(()) => {}
            Err(e) => panic!("expected OK on shutdown but got: {e}"),
        }
    }

    fn send_command<V>(channel_map: &HashMap<u16, UnboundedSender<V>>, port: u16, result: V) {
        channel_map
            .get(&port)
            .unwrap_or_else(|| {
                panic!(
                    "expected command channel to exist for port {port}: no command \
                            channel — not in config, or bind_fail was set"
                )
            })
            .send(result)
            .expect("successful send");
    }

    fn next_client_sock(&mut self) -> SocketAddr {
        let sock = format!("127.0.0.1:{}", 40000 + self.client_ctr)
            .parse()
            .expect("valid upstream address");
        self.client_ctr += 1;
        sock
    }

    fn next_backend_sock(&mut self) -> SocketAddr {
        let sock = format!("127.0.0.1:{}", 9000 + self.backend_ctr)
            .parse()
            .expect("valid downstream address");
        self.backend_ctr += 1;
        sock
    }

    async fn await_ack(ack: oneshot::Receiver<()>) {
        // TODO: put this timeout in config
        timeout(Duration::from_secs(5), ack)
            .await
            .expect("ack should not time out")
            .expect("acked");
    }
}

pub struct ProxyHarnessBuilder {
    config: ProxyConfig,
    max_connections: usize,
    max_queue: usize,
    bind_failures: HashMap<u16, ProxyError>,
    duplex_buf_size: usize,
}

impl ProxyHarnessBuilder {
    pub fn bind_fail(mut self, port: u16, e: ProxyError) -> Self {
        self.bind_failures.insert(port, e);
        self
    }

    pub fn max_connections(mut self, conns: usize) -> Self {
        self.max_connections = conns;
        self
    }

    pub fn max_queue(mut self, conns: usize) -> Self {
        self.max_queue = conns;
        self
    }

    pub fn duplex_buf_size(mut self, size: usize) -> Self {
        self.duplex_buf_size = size;
        self
    }

    pub fn start(self) -> ProxyHarness {
        let (listener_factory, accept_commands) =
            fakes::FakeListenerFactory::new(&self.config, self.bind_failures);
        let (balancer, connect_commands) = fakes::FakeLoadBalancer::new(&self.config);
        let cancel_token = CancellationToken::new();
        let proxy = ProxyServer::new(
            listener_factory,
            balancer,
            Arc::new(self.config),
            cancel_token.clone(),
            self.max_connections,
            self.max_queue,
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            1024 * 8, // copy_buf_size
        );
        let task = tokio::spawn(proxy.run());
        ProxyHarness {
            cancel_token,
            task,
            accept_commands,
            connect_commands,
            duplex_buf_size: self.duplex_buf_size,
            client_ctr: 0,
            backend_ctr: 0,
        }
    }
}

mod fakes {
    use crate::balancer::traits::LoadBalancer;
    use crate::{
        errors::ProxyError,
        network::traits::{StreamListener, StreamListenerFactory},
        state::ProxyConfig,
    };
    use std::{collections::HashMap, net::SocketAddr, sync::Arc, sync::Mutex as StdMutex};
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::{
        io::DuplexStream,
        sync::{
            Mutex,
            mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
            oneshot,
        },
    };

    // ------- Network Fakes

    // TODO: Documentation on how to use this fake with examples.

    #[derive(Clone)]
    pub struct FakeListenerFactory {
        behaviors: Arc<StdMutex<HashMap<u16, BindBehavior>>>,
    }

    #[derive(Debug)]
    pub struct FakeListener {
        commands: Mutex<UnboundedReceiver<AcceptCommand>>,
    }

    /// Alias for the request that arrives on the accept channel: a proper accept result and a one-shot
    /// ack to unblock the tester once accept has been handled.
    pub type AcceptCommand = (
        Result<(DuplexStream, SocketAddr), ProxyError>,
        oneshot::Sender<()>,
    );

    enum BindBehavior {
        Bind(UnboundedReceiver<AcceptCommand>),
        Fail(ProxyError),
    }

    impl FakeListenerFactory {
        /// Create the fake listener factory. Returns the factory and the per-port channel to issue
        /// commands to the test's `StreamListener::accept`.
        pub fn new(
            config: &ProxyConfig,
            mut bind_failures: HashMap<u16, ProxyError>,
        ) -> (
            FakeListenerFactory,
            HashMap<u16, UnboundedSender<AcceptCommand>>,
        ) {
            let mut accept_tx: HashMap<u16, UnboundedSender<AcceptCommand>> = HashMap::new();
            let mut behaviors: HashMap<u16, BindBehavior> = HashMap::new();
            for port in config.ports() {
                if let Some(e) = bind_failures.remove(&port) {
                    behaviors.insert(port, BindBehavior::Fail(e));
                    continue;
                }
                let (tx, rx) = unbounded_channel::<AcceptCommand>();
                accept_tx.insert(port, tx);
                behaviors.insert(port, BindBehavior::Bind(rx));
            }

            if !bind_failures.is_empty() {
                panic!("bind failure scheduled on unconfigured port(s) {bind_failures:?}");
            }

            (
                FakeListenerFactory {
                    behaviors: Arc::new(StdMutex::new(behaviors)),
                },
                accept_tx,
            )
        }
    }

    impl StreamListenerFactory for FakeListenerFactory {
        type Listener = FakeListener;
        async fn bind(&self, addr: &SocketAddr) -> Result<Self::Listener, ProxyError> {
            match self.behaviors.lock().unwrap().remove(&addr.port()) {
                Some(BindBehavior::Bind(rx)) => Ok(FakeListener {
                    commands: Mutex::new(rx),
                }),
                Some(BindBehavior::Fail(e)) => Err(e),
                None => panic!(
                    "Attempted to bind port {} which wasn't configured",
                    addr.port()
                ),
            }
        }
    }

    impl StreamListener for FakeListener {
        type Stream = DuplexStream;

        // This must remain cancel safe.
        async fn accept(&self) -> Result<(Self::Stream, SocketAddr), ProxyError> {
            let Some((result, ack)) = self.commands.lock().await.recv().await else {
                return std::future::pending().await;
            };
            let _ = ack.send(());
            result
        }
    }

    // ------- Balancer Fakes

    // TODO: Documentation for how to use these fakes with examples

    #[derive(Debug)]
    pub struct FakeLoadBalancer {
        connection_commands: HashMap<u16, StdMutex<UnboundedReceiver<ConnectCommand>>>,
    }

    /// Alias for the request that arrives on the connect channel.
    pub type ConnectCommand = Result<(DuplexStream, SocketAddr), ProxyError>;

    impl FakeLoadBalancer {
        pub fn new(
            config: &ProxyConfig,
        ) -> (
            FakeLoadBalancer,
            HashMap<u16, UnboundedSender<ConnectCommand>>,
        ) {
            let mut connection_commands = HashMap::new();
            let mut connect_tx = HashMap::new();
            for port in config.ports() {
                let (tx, rx) = unbounded_channel::<ConnectCommand>();
                connection_commands.insert(port, StdMutex::new(rx));
                connect_tx.insert(port, tx);
            }
            (
                FakeLoadBalancer {
                    connection_commands,
                },
                connect_tx,
            )
        }
    }

    // Note that when this panics, it results in a log, not an actual panic
    impl LoadBalancer for FakeLoadBalancer {
        type Stream = DuplexStream;

        async fn connect_backend(
            &self,
            for_port: u16,
        ) -> Result<(Self::Stream, SocketAddr), ProxyError> {
            match self.connection_commands.get(&for_port) {
                Some(connections) => match connections.lock().unwrap().try_recv() {
                    Ok(Ok(stream)) => Ok(stream),
                    Ok(Err(e)) => Err(e),
                    Err(TryRecvError::Empty) => panic!(
                        "port {for_port}: connect_backend with no queued command — more connection \
                    tasks than backends"
                    ),
                    Err(TryRecvError::Disconnected) => {
                        panic!("port {for_port}: harness dropped while the proxy was running")
                    }
                },
                None => panic!("No test connections available for {for_port}"),
            }
        }
    }
}
