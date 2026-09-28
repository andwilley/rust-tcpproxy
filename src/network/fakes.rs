use crate::{
    errors::ProxyError,
    network::traits::{StreamListener, StreamListenerFactory},
    state::ProxyConfig,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, sync::Mutex as StdMutex};
use tokio::{
    io::DuplexStream,
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
        oneshot,
    },
};

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
