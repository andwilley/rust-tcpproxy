use tokio::io::{DuplexStream, duplex};

use crate::{
    errors::ProxyError,
    network::traits::{StreamListener, StreamListenerFactory},
};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
};

// TODO Documention on how to use this fake with examples.

#[derive(Clone)]
pub struct FakeListenerFactory {
    port_behaviors: Arc<Mutex<HashMap<u16, BindBehavior>>>,
}
impl FakeListenerFactory {
    pub fn builder() -> FakeListenerFactoryBuilder {
        FakeListenerFactoryBuilder::default()
    }
}
impl StreamListenerFactory for FakeListenerFactory {
    type Listener = FakeListener;
    async fn bind(&self, addr: &SocketAddr) -> Result<Self::Listener, ProxyError> {
        let listener = match self.port_behaviors.lock().unwrap().remove(&addr.port()) {
            Some(behavior) => match behavior {
                BindBehavior::Bind(listener) => listener,
                BindBehavior::Fail(e) => return Err(e),
            },
            None => panic!("Attempted to bind a port with no defined behavior"),
        };
        Ok(listener)
    }
}

pub enum BindBehavior {
    Bind(FakeListener),
    Fail(ProxyError),
}

pub enum ConnectionSpec {
    /// Repeats this connection a specified number of times. Zero is allowed and will result in a
    /// port that will bind but never yeild a connection.
    Connect {
        count: usize,
    },
    Fail(ProxyError),
}

pub enum ConnectionResult {
    Connect((DuplexStream, SocketAddr)),
    Fail(ProxyError),
}

pub struct FakeListenerFactoryBuilder {
    ports: Vec<(u16, Vec<ConnectionSpec>)>,
    failed_ports: Vec<(u16, ProxyError)>,
    /// Defaults to 64 KiB
    duplex_buf: usize,
}

impl Default for FakeListenerFactoryBuilder {
    fn default() -> Self {
        FakeListenerFactoryBuilder {
            ports: Vec::new(),
            failed_ports: Vec::new(),
            duplex_buf: 1024 * 64,
        }
    }
}

impl FakeListenerFactoryBuilder {
    pub fn add_port_connections(
        mut self,
        port: u16,
        client_connections: Vec<ConnectionSpec>,
    ) -> Self {
        self.ports.push((port, client_connections));
        self
    }

    /// Overrides any behavior already specified by add_port_connections.
    pub fn fail_port(mut self, port: u16, error: ProxyError) -> Self {
        self.failed_ports.push((port, error));
        self
    }

    pub fn set_duplex_buffer(mut self, size: usize) -> Self {
        self.duplex_buf = size;
        self
    }

    pub fn build(self) -> (FakeListenerFactory, HashMap<u16, Vec<DuplexStream>>) {
        let mut behaviors: HashMap<u16, BindBehavior> = HashMap::new();
        let mut client_streams: HashMap<u16, Vec<DuplexStream>> = HashMap::new();
        let mut source_ctr = 0usize;

        for (port, specs) in self.ports {
            let mut sequence = VecDeque::new();
            for spec in specs {
                match spec {
                    ConnectionSpec::Connect { count } => {
                        for _ in 0..count {
                            let (proxy_side, test_side) = duplex(self.duplex_buf);
                            let source: SocketAddr = format!("127.0.0.1:{}", 40000 + source_ctr)
                                .parse()
                                .expect("valid source address");
                            source_ctr += 1;
                            client_streams.entry(port).or_default().push(test_side);
                            sequence.push_back(ConnectionResult::Connect((proxy_side, source)));
                        }
                    }
                    ConnectionSpec::Fail(e) => {
                        sequence.push_back(ConnectionResult::Fail(e));
                    }
                }
            }
            // build the behavior for the port
            behaviors.insert(port, BindBehavior::Bind(FakeListener::new(sequence)));
        }
        for (port, proxy_error) in self.failed_ports {
            behaviors.insert(port, BindBehavior::Fail(proxy_error));
        }
        (
            FakeListenerFactory {
                port_behaviors: Arc::new(Mutex::new(behaviors)),
            },
            client_streams,
        )
    }
}

pub struct FakeListener {
    connections: Mutex<VecDeque<ConnectionResult>>,
}

impl FakeListener {
    fn new(connections: VecDeque<ConnectionResult>) -> Self {
        Self {
            connections: Mutex::new(connections),
        }
    }
}

impl StreamListener for FakeListener {
    type Stream = DuplexStream;
    async fn accept(&self) -> Result<(Self::Stream, SocketAddr), ProxyError> {
        let next = self.connections.lock().unwrap().pop_front();
        match next {
            Some(ConnectionResult::Connect((stream, source))) => Ok((stream, source)),
            Some(ConnectionResult::Fail(e)) => Err(e),
            None => std::future::pending().await,
        }
    }
}
