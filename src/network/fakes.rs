use crate::{
    errors::ProxyError,
    network::traits::{StreamListener, StreamListenerFactory},
};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::io::{DuplexStream, duplex};

// TODO Documention on how to use this fake with examples.

pub enum ClientConnectionSpec {
    /// Repeats this connection a specified number of times. Zero is allowed and will result in a
    /// port that will bind but never yeild a connection.
    Connect {
        count: usize,
    },
    Fail(ProxyError),
}

#[derive(Clone)]
pub struct FakeListenerFactory {
    port_behaviors: Arc<Mutex<HashMap<u16, BindBehavior>>>,
}

impl FakeListenerFactory {
    pub fn builder() -> FakeListenerFactoryBuilder {
        FakeListenerFactoryBuilder {
            ports: Vec::new(),
            failed_ports: Vec::new(),
            duplex_buf: 1024 * 64,
        }
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
            None => panic!(
                "Attempted to bind port {} with no defined behavior",
                addr.port()
            ),
        };
        Ok(listener)
    }
}

enum BindBehavior {
    Bind(FakeListener),
    Fail(ProxyError),
}

enum ClientConnectionResult {
    Connect((DuplexStream, SocketAddr)),
    Fail(ProxyError),
}

/// The test stub for each upstream connection the test attempted to create per the spec.
pub enum ClientConnectionStub {
    Connect(DuplexStream),
    Fail,
}

pub struct FakeListenerFactoryBuilder {
    ports: Vec<(u16, Vec<ClientConnectionSpec>)>,
    failed_ports: Vec<(u16, ProxyError)>,
    /// Defaults to 64 KiB
    duplex_buf: usize,
}

impl FakeListenerFactoryBuilder {
    pub fn add_port_connections(
        mut self,
        port: u16,
        client_connections: Vec<ClientConnectionSpec>,
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

    /// Build the configured listener factory. Also returns the client-sides for each attempted
    /// connection in order of connection.
    pub fn build(self) -> (FakeListenerFactory, HashMap<u16, Vec<ClientConnectionStub>>) {
        let mut behaviors: HashMap<u16, BindBehavior> = HashMap::new();
        let mut test_sides: HashMap<u16, Vec<ClientConnectionStub>> = HashMap::new();
        let mut source_ctr = 0usize;

        for (port, specs) in self.ports {
            let mut sequence = VecDeque::new();
            for spec in specs {
                match spec {
                    ClientConnectionSpec::Connect { count } => {
                        for _ in 0..count {
                            let (proxy_side, test_side) = duplex(self.duplex_buf);
                            let source: SocketAddr = format!("127.0.0.1:{}", 40000 + source_ctr)
                                .parse()
                                .expect("valid source address");
                            source_ctr += 1;
                            test_sides
                                .entry(port)
                                .or_default()
                                .push(ClientConnectionStub::Connect(test_side));
                            sequence
                                .push_back(ClientConnectionResult::Connect((proxy_side, source)));
                        }
                    }
                    ClientConnectionSpec::Fail(e) => {
                        test_sides
                            .entry(port)
                            .or_default()
                            .push(ClientConnectionStub::Fail);
                        sequence.push_back(ClientConnectionResult::Fail(e));
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
            test_sides,
        )
    }
}

pub struct FakeListener {
    connections: Mutex<VecDeque<ClientConnectionResult>>,
}

impl FakeListener {
    fn new(connections: VecDeque<ClientConnectionResult>) -> Self {
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
            Some(ClientConnectionResult::Connect((stream, source))) => Ok((stream, source)),
            Some(ClientConnectionResult::Fail(e)) => Err(e),
            None => std::future::pending().await,
        }
    }
}
