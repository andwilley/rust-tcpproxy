use crate::balancer::traits::LoadBalancer;
use crate::errors::ProxyError;
use crate::state::ProxyConfig;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use tokio::io::DuplexStream;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

// TODO: Documentation for how to use these fakes with examples

#[derive(Debug)]
pub struct FakeLoadBalancer {
    connection_commands: HashMap<u16, Mutex<UnboundedReceiver<ConnectCommand>>>,
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
            connection_commands.insert(port, Mutex::new(rx));
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
