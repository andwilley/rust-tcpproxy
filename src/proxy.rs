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
                // finish, which may delay logging panics by the max backoff. Consider moving this
                // into the select.
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
                    // Fail fast for queue full.
                    AcceptResult::RejectedTooManyConnections => {
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
    use harness::ProxyHarness;
    use std::collections::HashMap;
    use std::net::Ipv6Addr;
    use tokio::io::{AsyncReadExt, DuplexStream};
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::sync::oneshot;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    #[tokio::test(start_paused = true)]
    async fn proxy_bytes_round_trip() {
        let config = default_config();
        let mut harness = ProxyHarness::builder(config).start();
        let (mut client, mut backend) = harness.proxied(PORT).await;

        assert_proxied_bytes(
            &mut client,
            &mut backend,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut backend,
            &mut client,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

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

    #[tokio::test(start_paused = true)]
    async fn accept_fails_permanent_proxy_tears_down() {
        let config = default_config();
        let accept_perm_error = ProxyError::IoError(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "accept error",
        ));
        let mut harness = ProxyHarness::builder(config).start();
        let (mut client1, mut backend1) = harness.proxied(PORT).await;

        assert_proxied_bytes(
            &mut client1,
            &mut backend1,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut backend1,
            &mut client1,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        harness.accept_fail(PORT, accept_perm_error).await;

        match harness.join().await {
            Err(ProxyError::IoError(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::AddrNotAvailable);
            }
            e => {
                panic!("Expected teardown for accept error but got {:?}", e);
            }
        };

        assert_connection_closed(&mut client1, START_PAUSED_ASSERT_WAIT).await;
        assert_connection_closed(&mut backend1, START_PAUSED_ASSERT_WAIT).await;
    }

    #[tokio::test(start_paused = true)]
    async fn accept_fails_transient_proxy_continues() {
        let config = default_config();
        let accept_trans_error = ProxyError::IoError(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            "accept error",
        ));
        let mut harness = ProxyHarness::builder(config).start();

        harness.accept_fail(PORT, accept_trans_error).await;
        let (mut client, mut backend) = harness.proxied(PORT).await;

        assert_proxied_bytes(
            &mut client,
            &mut backend,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut backend,
            &mut client,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        // Close connections before shutdown.
        drop(client);
        drop(backend);

        harness.shutdown_assert_ok().await
    }

    /// With 2 active slots and 2 queue slots, the first two connections can transfer bytes
    /// immediately, but the queued connection must wait for an active pair to close. The second in
    /// the queue remains in the queue unconnected.
    #[tokio::test(start_paused = true)]
    async fn at_max_connections_queue_available_accepts_proxies_on_active_close() {
        let config = default_config();
        let mut harness = ProxyHarness::builder(config)
            .max_connections(2)
            .max_queue(2)
            .start();

        let (mut proxied_client_1, mut proxied_backend_1) = harness.proxied(PORT).await;
        let (mut proxied_client_2, mut proxied_backend_2) = harness.proxied(PORT).await;
        let (mut queued_client_1, mut queued_backend_1) = harness.proxied(PORT).await;
        let (mut queued_client_2, mut queued_backend_2) = harness.proxied(PORT).await;

        // Accepted connections proxy.
        assert_proxied_bytes(
            &mut proxied_client_1,
            &mut proxied_backend_1,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut proxied_backend_1,
            &mut proxied_client_1,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut proxied_client_2,
            &mut proxied_backend_2,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut proxied_backend_2,
            &mut proxied_client_2,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        // Queued connections are open but do not proxy.
        let queue_bytes = b"queue1";
        assert_connection_open_idle(&mut queued_client_1, START_PAUSED_ASSERT_WAIT).await;
        assert_no_proxied_bytes(
            &mut queued_client_1,
            &mut queued_backend_1,
            queue_bytes,
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_connection_open_idle(&mut queued_client_2, START_PAUSED_ASSERT_WAIT).await;
        assert_no_proxied_bytes(
            &mut queued_client_2,
            &mut queued_backend_2,
            b"queue2",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        // Close an active connection
        drop(proxied_client_1);
        drop(proxied_backend_1);

        // Next in the queue was accepted and tx from above is read and proxies.
        assert_read_bytes(&mut queued_backend_1, queue_bytes, START_PAUSED_ASSERT_WAIT).await;

        // Second in the queue still queued
        assert_connection_open_idle(&mut queued_client_2, START_PAUSED_ASSERT_WAIT).await;
        assert_no_proxied_bytes(
            &mut queued_client_2,
            &mut queued_backend_2,
            b"queue2",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        drop(proxied_client_2);
        drop(proxied_backend_2);
        drop(queued_client_1);
        drop(queued_backend_1);
        drop(queued_client_2);
        drop(queued_backend_2);

        harness.shutdown_assert_ok().await
    }

    /// With 2 active slots and 1 queue slot, the first two connections can transfer bytes
    /// immediately, the third is queued, and the fourth is dropped
    #[tokio::test(start_paused = true)]
    async fn at_max_connections_max_queue_drops_for_too_many_connections() {
        let config = default_config();
        let mut harness = ProxyHarness::builder(config)
            .max_connections(2)
            .max_queue(1)
            .start();

        let (mut proxied_client_1, mut proxied_backend_1) = harness.proxied(PORT).await;
        let (mut proxied_client_2, mut proxied_backend_2) = harness.proxied(PORT).await;
        let (mut queued_client_1, mut queued_backend_1) = harness.proxied(PORT).await;
        let mut dropped_client = harness.no_backend(PORT).await;

        // Accepted connections proxy.
        assert_proxied_bytes(
            &mut proxied_client_1,
            &mut proxied_backend_1,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut proxied_backend_1,
            &mut proxied_client_1,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut proxied_client_2,
            &mut proxied_backend_2,
            b"request",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;
        assert_proxied_bytes(
            &mut proxied_backend_2,
            &mut proxied_client_2,
            b"response",
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        // Queued connection is open but does not proxy.
        let queue_bytes = b"queue1";
        assert_connection_open_idle(&mut queued_client_1, START_PAUSED_ASSERT_WAIT).await;
        assert_no_proxied_bytes(
            &mut queued_client_1,
            &mut queued_backend_1,
            queue_bytes,
            START_PAUSED_ASSERT_WAIT,
        )
        .await;

        // Latest connection is dropped
        assert_connection_closed(&mut dropped_client, START_PAUSED_ASSERT_WAIT).await;

        drop(proxied_client_1);
        drop(proxied_backend_1);
        drop(proxied_client_2);
        drop(proxied_backend_2);
        drop(queued_client_1);
        drop(queued_backend_1);
        drop(dropped_client);

        harness.shutdown_assert_ok().await
    }

    // connect_backend fails, logs and allows new connections
    // connection task panics, logs and continues
    // shutdown with open connections, clean drain
    // shutdown with open connections, forces close after limit
    // listener tasks panics, proxy tears down
    // test connections can half close properly

    const START_PAUSED_ASSERT_WAIT: Duration = Duration::from_secs(1);
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

    async fn assert_proxied_bytes<const N: usize>(
        source: &mut DuplexStream,
        dest: &mut DuplexStream,
        bytes: &[u8; N],
        wait: Duration,
    ) {
        source.write_all(bytes).await.expect("successful write");
        assert_read_bytes(dest, bytes, wait).await;
    }

    async fn assert_read_bytes<const N: usize>(
        conn: &mut DuplexStream,
        bytes: &[u8; N],
        wait: Duration,
    ) {
        let mut from_source = [0u8; N];
        timeout(wait, conn.read_exact(&mut from_source))
            .await
            .expect("read shouldn't time out. check for misaligned streams in the harness")
            .expect("successful read");
        assert_eq!(&from_source, bytes);
    }

    /// This depends on a timeout and will be flakey if the test writer isn't careful. Most useful
    /// in `start_paused`.
    async fn assert_no_proxied_bytes<const N: usize>(
        source: &mut DuplexStream,
        dest: &mut DuplexStream,
        bytes: &[u8; N],
        wait: Duration,
    ) {
        source.write_all(bytes).await.expect("successful write");
        let mut from_source = [0u8; N];
        match timeout(wait, dest.read_exact(&mut from_source)).await {
            Err(_timeout) => {}
            _ => panic!("Expected attempt to proxy bytes to timeout, but it didn't"),
        }
    }

    async fn assert_connection_closed(conn: &mut DuplexStream, wait: Duration) {
        let buf: &mut [u8] = &mut [0u8; 4];
        match timeout(wait, conn.read(buf)).await {
            Ok(Ok(0)) => {}
            Ok(any) => panic!("expected disconnectioned stream but read returned: {any:?}"),
            Err(e) => panic!("expected disconnected stream but still awaiting writes: {e}"),
        }
    }

    /// Note that this isn't great, it requires that there are no pending bytes to be read on the
    /// existing connection and it chews up 50ms (if not running `start_paused`) of latency to check
    /// something a peekable stream would be able to see immediately. Most useful in `start_paused`
    async fn assert_connection_open_idle(conn: &mut DuplexStream, wait: Duration) {
        let buf: &mut [u8] = &mut [0u8; 4];
        match timeout(wait, conn.read(buf)).await {
            Err(_timeout) => {}
            Ok(Ok(_)) => panic!("expected idle open stream but found bytes"),
            Ok(Err(e)) => panic!("expected idle open stream but got: {e}"),
        }
    }

    mod harness {
        use super::*;

        /// Manages the running proxy and exposes connections and control. This is only works for a
        /// single threaded test, which is fine for some of the mechanics and good for determinism,
        /// but these are insufficient to cover the concurrent surface area. Integration tests are
        /// needed.
        ///
        /// TODO:
        /// - add "unexpected warn+ logs throw" and "unreceived expected logs throw"
        /// - impl Drop to ensure that the proxy task is cancelled properly
        /// - We may need better detection of stream mis-alignment (see no_backend).
        pub struct ProxyHarness {
            cancel_token: CancellationToken,
            task: JoinHandle<Result<(), ProxyError>>,
            accept_commands: HashMap<u16, UnboundedSender<AcceptCommand>>,
            connect_commands: HashMap<u16, UnboundedSender<ConnectCommand>>,
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

            /// Returns a connected (client, backend) streams.
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

            fn send_command<V>(
                channel_map: &HashMap<u16, UnboundedSender<V>>,
                port: u16,
                result: V,
            ) {
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

            #[expect(dead_code)]
            pub fn duplex_buf_size(mut self, size: usize) -> Self {
                self.duplex_buf_size = size;
                self
            }

            pub fn start(self) -> ProxyHarness {
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
}
