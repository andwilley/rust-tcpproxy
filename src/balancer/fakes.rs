use crate::balancer::traits::LoadBalancer;
use crate::errors::ProxyError;
use crate::state::ProxyConfig;
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{DuplexStream, duplex};

pub enum BackendConnectionStub {
    Connect(DuplexStream),
    Fail,
}

pub enum BackendConnectionSpec {
    Connect { count: usize },
    Fail(ProxyError),
}

pub struct FakeLoadBalancerBuilder {
    config: Arc<ProxyConfig>,
    ports: Vec<(u16, Vec<BackendConnectionSpec>)>,
    /// Defaults to 64 KiB
    duplex_buf: usize,
}

enum BackendConnectionResult {
    Connect((DuplexStream, SocketAddr)),
    Fail(ProxyError),
}

impl FakeLoadBalancerBuilder {
    pub fn add_port_connections(
        mut self,
        port: u16,
        backend_connections: Vec<BackendConnectionSpec>,
    ) -> Self {
        self.ports.push((port, backend_connections));
        self
    }

    pub fn set_duplex_buffer(mut self, size: usize) -> Self {
        self.duplex_buf = size;
        self
    }

    /// Build the configured balancer. Also returns the downstream-sides for each attempted
    /// connection in order of connection.
    pub fn build(self) -> (FakeLoadBalancer, HashMap<u16, Vec<BackendConnectionStub>>) {
        let ports: HashSet<u16> = self.config.ports().collect();
        let mut port_connections: HashMap<u16, Mutex<VecDeque<BackendConnectionResult>>> =
            HashMap::new();
        let mut test_sides: HashMap<u16, Vec<BackendConnectionStub>> = HashMap::new();
        let mut downstream_ctr = 0usize;

        for (port, specs) in self.ports {
            if !ports.contains(&port) {
                panic!("Attempt to configure test behavior for an unbound port");
            }
            let mut sequence = VecDeque::new();
            for spec in specs {
                match spec {
                    BackendConnectionSpec::Connect { count } => {
                        for _ in 0..count {
                            let (proxy_side, test_side) = duplex(self.duplex_buf);
                            let downstream: SocketAddr =
                                format!("127.0.0.1:{}", 9000 + downstream_ctr)
                                    .parse()
                                    .expect("valid downstream address");
                            downstream_ctr += 1;
                            test_sides
                                .entry(port)
                                .or_default()
                                .push(BackendConnectionStub::Connect(test_side));
                            sequence.push_back(BackendConnectionResult::Connect((
                                proxy_side, downstream,
                            )));
                        }
                    }
                    BackendConnectionSpec::Fail(e) => {
                        test_sides
                            .entry(port)
                            .or_default()
                            .push(BackendConnectionStub::Fail);
                        sequence.push_back(BackendConnectionResult::Fail(e));
                    }
                }
            }
            // build the behavior for the port
            port_connections.insert(port, Mutex::new(sequence));
        }

        (FakeLoadBalancer { port_connections }, test_sides)
    }
}

pub struct FakeLoadBalancer {
    port_connections: HashMap<u16, Mutex<VecDeque<BackendConnectionResult>>>,
}

impl FakeLoadBalancer {
    pub fn builder(config: Arc<ProxyConfig>) -> FakeLoadBalancerBuilder {
        FakeLoadBalancerBuilder {
            config,
            ports: Vec::new(),
            duplex_buf: 1024 * 64,
        }
    }
}

impl LoadBalancer for FakeLoadBalancer {
    type Stream = DuplexStream;

    async fn connect_backend(
        &self,
        for_port: u16,
    ) -> Result<(Self::Stream, SocketAddr), ProxyError> {
        match self.port_connections.get(&for_port) {
            Some(connections) => match connections.lock().unwrap().pop_front() {
                Some(BackendConnectionResult::Connect(stream)) => Ok(stream),
                Some(BackendConnectionResult::Fail(e)) => Err(e),
                None => panic!("Exhausted test connections for {for_port}"),
            },
            None => panic!("No test connections available for {for_port}"),
        }
    }
}
