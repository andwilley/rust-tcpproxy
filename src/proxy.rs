use crate::balancer::traits::LoadBalancer;
use crate::errors::ProxyError;
use crate::network::traits::{AsyncStream, StreamListener, StreamListenerFactory};
use crate::state::ProxyConfig;
use std::io::ErrorKind::{ConnectionAborted, ConnectionRefused, ConnectionReset};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub struct ProxyServer<L, B> {
    listener_factory: L,
    balancer: Arc<B>,
    ports: Vec<u16>,
    cancel_token: CancellationToken,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    bind_addr: IpAddr,
}

impl<L, B> ProxyServer<L, B>
where
    L: StreamListenerFactory + 'static,
    B: LoadBalancer + 'static,
{
    pub fn new(
        listener_factory: L,
        balancer: B,
        config: Arc<ProxyConfig>,
        cancel_token: CancellationToken,
        max_connections: usize,
        max_queue: usize,
        bind_addr: IpAddr,
    ) -> Self {
        Self {
            listener_factory,
            balancer: Arc::new(balancer),
            ports: config.ports().collect(),
            cancel_token,
            open_connections: Arc::new(Semaphore::const_new(max_connections)),
            queued_connections: Arc::new(Semaphore::const_new(max_queue)),
            bind_addr,
        }
    }

    pub async fn run(self) -> Result<(), ProxyError> {
        let mut handles = JoinSet::new();
        let mut port_listeners = Vec::with_capacity(self.ports.len());
        for port in self.ports {
            let addr = SocketAddr::new(self.bind_addr, port);
            let listener = self
                .listener_factory
                .bind(&addr)
                .await
                .map_err(|e| e.maybe_into_bind_error(&addr))?;
            port_listeners.push((port, listener));
        }
        for (port, listener) in port_listeners {
            handles.spawn(listen(
                port,
                listener,
                self.balancer.clone(),
                self.open_connections.clone(),
                self.queued_connections.clone(),
                self.cancel_token.clone(),
            ));
        }
        let mut first_error: Option<ProxyError> = None;
        while let Some(res) = handles.join_next().await {
            match res {
                Ok(Ok(())) => debug!("A listener exited successfully"),
                Ok(Err(listen_error)) => {
                    error!(err = %listen_error, "listener failed, shutting down remaining listeners");
                    self.cancel_token.cancel();
                    first_error.get_or_insert(listen_error);
                }
                Err(join_error) => {
                    error!(err = %join_error, "listener task was cancelled or panicked");
                    self.cancel_token.cancel();
                    // Consider splitting cancellation and panic.
                    first_error.get_or_insert(ProxyError::TaskCancellation {
                        message: join_error.to_string(),
                    });
                }
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

async fn listen<L, B>(
    port: u16,
    listener: L,
    balancer: Arc<B>,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    cancel_token: CancellationToken,
) -> Result<(), ProxyError>
where
    L: StreamListener + 'static,
    B: LoadBalancer + 'static,
{
    let mut connections = JoinSet::new();

    let accept_res = select!(
        result = async {
            loop {
                // Clean up finished tasks. This needs to wait for any currently running accepts to
                // finish, which may have delay logging panics by the max backoff.
                while let Some(res) = connections.try_join_next() {
                    if let Err(e) = res {
                        error!(port, err = %e, "connection task panicked");
                    }
                }
                let accept_result = listener.accept().await;
                match accept_connection(
                    accept_result,
                    port,
                    balancer.clone(),
                    open_connections.clone(),
                    queued_connections.clone(),
                    &mut connections
                ) {
                    // Try the next connection.
                    AcceptResult::Connected |
                    AcceptResult::RetryImmediately => { continue; }
                    // Fixed wait for new connections in this state.
                    AcceptResult::RejectedTooManyConnections => {
                        sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    // Tear down.
                    AcceptResult::PermanentError(e) => { break Err(e); }
                }
            }
        } => {
            result
        }
        _ = cancel_token.cancelled() => { Ok(()) }
    );

    // Start tearing down the other listeners if we exited for an error.
    if accept_res.is_err() {
        cancel_token.cancel();
    }

    // Stop accepting connections while we shut down.
    drop(listener);

    info!(port, "Shutting down, attempting to drain open connections.");
    select! {
        _ = async { while connections.join_next().await.is_some() {} } => {}
        _ = sleep(Duration::from_secs(10)) => {
            warn!(port, "Shutdown deadline reached, aborting.")
        }
    }
    accept_res
}

enum AcceptResult {
    Connected,
    RetryImmediately,
    RejectedTooManyConnections,
    PermanentError(ProxyError),
}

/// If capacity is available in the active pool or the queue, spawn a task to connect this request.
fn accept_connection<B, S>(
    accept_result: Result<(S, SocketAddr), ProxyError>,
    port: u16,
    balancer: Arc<B>,
    open_connections: Arc<Semaphore>,
    queued_connections: Arc<Semaphore>,
    connections: &mut JoinSet<()>,
) -> AcceptResult
where
    B: LoadBalancer + 'static,
    S: AsyncStream,
{
    let (in_sock, _source_addr) = match accept_result {
        Ok(res) => res,
        Err(e) => {
            if let ProxyError::IoError(io_err) = &e {
                let kind = io_err.kind();
                if matches!(
                    kind,
                    ConnectionRefused | ConnectionReset | ConnectionAborted
                ) {
                    warn!(port, err = %e, "transient accept error");
                    return AcceptResult::RetryImmediately;
                }
            }
            error!(port, err = %e, "permanent accept error");
            return AcceptResult::PermanentError(e);
        }
    };
    let new_balancer = balancer.clone();
    if let Ok(permit) = open_connections.clone().try_acquire_owned() {
        connections.spawn(async move {
            let _permit = permit;
            if let Err(e) = do_connection(port, in_sock, new_balancer).await {
                error!(port, err = %e, "Connection to backends failed")
            }
        });
        return AcceptResult::Connected;
    };

    if let Ok(queue_slot) = queued_connections.clone().try_acquire_owned() {
        let new_open_connections = open_connections.clone();
        connections.spawn(async move {
            let Ok(_permit) = new_open_connections.clone().acquire_owned().await else {
                return;
            };
            drop(queue_slot);
            if let Err(e) = do_connection(port, in_sock, new_balancer).await {
                error!(port, err = %e, "Connection to backends failed")
            }
        });
        return AcceptResult::Connected;
    }
    error!(port, "Connection dropped, too many connections");
    AcceptResult::RejectedTooManyConnections
}

async fn do_connection<S, B>(port: u16, mut in_sock: S, balancer: Arc<B>) -> Result<(), ProxyError>
where
    S: AsyncStream,
    B: LoadBalancer,
{
    let (mut out_sock, _) = balancer.connect_backend(port).await?;
    tokio::io::copy_bidirectional(&mut in_sock, &mut out_sock).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::balancer::fakes::{ConnectCommand, FakeLoadBalancer};
    use crate::config::{App, RawConfig};
    use crate::network::fakes::{AcceptCommand, FakeListenerFactory};
    use std::collections::HashMap;
    use std::net::Ipv6Addr;
    use tokio::io::{AsyncReadExt, DuplexStream};
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;

    #[tokio::test(start_paused = true)]
    async fn proxy_bytes_round_trip() {
        let config = default_config();
        let mut harness = ProxyHarness::builder(config).start();
        let (mut client, mut backend) = harness.proxied(PORT).await;

        assert_proxied_bytes(&mut client, &mut backend, b"ping").await;
        assert_proxied_bytes(&mut backend, &mut client, b"pong").await;

        // Close connections before shutdown.
        drop(client);
        drop(backend);

        harness.shutdown_assert_ok().await
    }

    #[tokio::test(start_paused = true)]
    async fn bind_fails_proxy_tears_down() {
        let config = default_config();
        let bind_error = ProxyError::IoError(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "bind error",
        ));
        let harness = ProxyHarness::builder(config)
            .bind_fail(PORT, bind_error)
            .start();
        match harness.join().await {
            Err(ProxyError::BindError { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::PermissionDenied);
            }
            e => {
                panic!("Expected bind error but got {:?}", e);
            }
        };
    }

    // accept fails for permanent error, brings the proxy down
    //   test this drains the connections on that port as well
    // transient accept error, tries again immediately
    // pool full, queue has space, accepts new connection
    //   when a connection ends the queued connection is accepted
    // pool full, queue full, does not accept new connection
    //   when a connection finishes, a new one is accepted
    // connect_backend fails, logs and allows new connections
    // connection task panics, logs and continues
    // shutdown with open connections, clean drain
    // shutdown with open connections, forces close after limit
    // listener tasks panics, proxy tears down
    // test connections can half close properly

    const PORT: u16 = 8080;

    fn default_config() -> ProxyConfig {
        ProxyConfig::try_from(&RawConfig {
            apps: vec![App {
                name: "test".to_string(),
                ports: vec![PORT],
                targets: vec!["backend.example.com:9000".to_string()],
            }],
        })
        .expect("config")
    }

    // TODO add a timeout here
    async fn assert_proxied_bytes<const N: usize>(
        source: &mut DuplexStream,
        dest: &mut DuplexStream,
        bytes: &[u8; N],
    ) {
        source.write_all(bytes).await.expect("successful write");
        let mut from_source = [0u8; N];
        dest.read_exact(&mut from_source)
            .await
            .expect("successful read");
        assert_eq!(&from_source, bytes);
    }

    /// Manages the running proxy and exposes connections and control.
    /// TODO:
    /// - timeout the ack receiver
    /// - add "unexpected warn+ logs throw" and "unreceived expected logs throw"
    /// - impl Drop to ensure that the proxy task is cancelled properly
    struct ProxyHarness {
        cancel_token: CancellationToken,
        task: JoinHandle<Result<(), ProxyError>>,
        accept_commands: HashMap<u16, UnboundedSender<AcceptCommand>>,
        connect_commands: HashMap<u16, UnboundedSender<ConnectCommand>>,
        duplex_buf_size: usize,
        client_ctr: usize,
        backend_ctr: usize,
    }

    impl ProxyHarness {
        fn builder(config: ProxyConfig) -> ProxyHarnessBuilder {
            ProxyHarnessBuilder {
                config,
                max_connections: 10,
                max_queue: 10,
                duplex_buf_size: 1024 * 64,
                bind_failures: HashMap::new(),
            }
        }

        /// Returns a connected (client, backend) streams.
        async fn proxied(&mut self, port: u16) -> (DuplexStream, DuplexStream) {
            let (test_side_client, proxy_side_client) = duplex(self.duplex_buf_size);
            let (test_side_backend, proxy_side_backend) = duplex(self.duplex_buf_size);
            let client_addr = format!("127.0.0.1:{}", 40000 + self.client_ctr)
                .parse()
                .expect("valid downstream address");
            let backend_addr = format!("127.0.0.1:{}", 9000 + self.backend_ctr)
                .parse()
                .expect("valid downstream address");
            let (ack_tx, ack_rx) = oneshot::channel();
            self.connect_commands
                .get(&port)
                .unwrap()
                .send(Ok((proxy_side_backend, backend_addr)))
                .expect("successful send");
            self.accept_commands
                .get(&port)
                .expect("port {port}: no accept channel, not in config or bind fail set")
                .send((Ok((proxy_side_client, client_addr)), ack_tx))
                .expect("successful send");
            ack_rx.await.expect("acked");
            (test_side_client, test_side_backend)
        }

        /// Client connection fails at accept with the provided error. No connections.
        async fn accept_fail(&mut self, port: u16, e: ProxyError) {
            let (ack_tx, ack_rx) = oneshot::channel();
            self.accept_commands
                .get(&port)
                .expect("port {port}: no accept channel, not in config or bind fail set")
                .send((Err(e), ack_tx))
                .expect("successful send");
            ack_rx.await.expect("acked");
        }

        /// Returns the client side of a connection, there will be no backend side.
        async fn connect_fail(&mut self, port: u16, e: ProxyError) -> DuplexStream {
            let (test_side_client, proxy_side_client) = duplex(self.duplex_buf_size);
            let client_addr = format!("127.0.0.1:{}", 40000 + self.client_ctr)
                .parse()
                .expect("valid downstream address");
            let (ack_tx, ack_rx) = oneshot::channel();
            self.connect_commands
                .get(&port)
                .unwrap()
                .send(Err(e))
                .expect("successful send");
            self.accept_commands
                .get(&port)
                .expect("port {port}: no accept channel, not in config or bind fail set")
                .send((Ok((proxy_side_client, client_addr)), ack_tx))
                .expect("successful send");
            ack_rx.await.expect("acked");
            test_side_client
        }

        /// No error in accept or connect, accept runs, but no connection is expected, e.g. too many
        /// connections.
        async fn no_backend(&mut self, port: u16) -> DuplexStream {
            let (test_side_client, proxy_side_client) = duplex(self.duplex_buf_size);
            let client_addr = format!("127.0.0.1:{}", 40000 + self.client_ctr)
                .parse()
                .expect("valid downstream address");
            let (ack_tx, ack_rx) = oneshot::channel();
            self.accept_commands
                .get(&port)
                .expect("port {port}: no accept channel, not in config or bind fail set")
                .send((Ok((proxy_side_client, client_addr)), ack_tx))
                .expect("successful send");
            ack_rx.await.expect("acked");
            test_side_client
        }

        /// Waits for the proxy to finish, propogating panics.
        async fn join(self) -> Result<(), ProxyError> {
            match self.task.await {
                Ok(res) => res,
                Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                Err(e) => panic!("proxy task cancelled: {e}"),
            }
        }

        /// Tear down the proxy. Does not close connections. You must await the task via
        /// ProxyHarness::join().
        fn cancel(&self) {
            self.cancel_token.cancel();
        }

        /// Tears down the proxy.
        async fn shutdown(self) -> Result<(), ProxyError> {
            self.cancel();
            self.join().await
        }

        /// Tears down the proxy, expects a clean shutdown.
        async fn shutdown_assert_ok(self) {
            match self.shutdown().await {
                Ok(()) => {}
                Err(e) => panic!("expected OK on shutdown but got: {e}"),
            }
        }
    }

    struct ProxyHarnessBuilder {
        config: ProxyConfig,
        max_connections: usize,
        max_queue: usize,
        bind_failures: HashMap<u16, ProxyError>,
        duplex_buf_size: usize,
    }

    impl ProxyHarnessBuilder {
        fn bind_fail(mut self, port: u16, e: ProxyError) -> Self {
            self.bind_failures.insert(port, e);
            self
        }

        fn max_connections(mut self, conns: usize) -> Self {
            self.max_connections = conns;
            self
        }

        fn max_queue(mut self, conns: usize) -> Self {
            self.max_queue = conns;
            self
        }

        fn duplex_buf_size(mut self, size: usize) -> Self {
            self.duplex_buf_size = size;
            self
        }

        fn start(self) -> ProxyHarness {
            let (listener_factory, accept_commands) =
                FakeListenerFactory::new(&self.config, self.bind_failures);
            let (balancer, connect_commands) = FakeLoadBalancer::new(&self.config);
            let cancel_token = CancellationToken::new();
            let proxy = ProxyServer::new(
                listener_factory,
                balancer,
                Arc::new(self.config),
                cancel_token.clone(),
                self.max_connections,
                self.max_queue,
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
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
}
