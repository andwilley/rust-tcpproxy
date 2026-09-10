use hickory_resolver::net::runtime::TokioRuntimeProvider;
use tracing::debug;

use crate::{
    errors::ProxyError,
    network::traits::{Resolver, StreamConnector, StreamListener, StreamListenerFactory},
};
use std::net::SocketAddr;

#[derive(Clone)]
pub struct TokioStreamListenerFactory;
impl StreamListenerFactory for TokioStreamListenerFactory {
    type Listener = TokioListener;
    async fn bind(&self, addr: &SocketAddr) -> Result<Self::Listener, ProxyError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(TokioListener(listener))
    }
}

pub struct TokioListener(pub tokio::net::TcpListener);
impl StreamListener for TokioListener {
    type Stream = tokio::net::TcpStream;
    async fn accept(&self) -> Result<(Self::Stream, SocketAddr), ProxyError> {
        let (stream, addr) = self.0.accept().await?;
        if let Err(e) = stream.set_nodelay(true) {
            debug!(peer = %addr, err = %e, "set_nodelay failed");
        }
        Ok((stream, addr))
    }
}

#[derive(Clone)]
pub struct HickoryTokioResolver(pub hickory_resolver::Resolver<TokioRuntimeProvider>);
impl Resolver for HickoryTokioResolver {
    async fn lookup_host(&self, host: &str) -> Result<Vec<SocketAddr>, ProxyError> {
        // Make sure its not already an IP.
        if let Ok(addr) = host.parse::<SocketAddr>() {
            return Ok(vec![addr]);
        }
        let Some((hostname, port_string)) = host.split_once(":") else {
            return Err(ProxyError::BadAddressError {
                addr: host.to_string(),
                message: "could not split into host and port".to_string(),
            });
        };
        let Ok(port) = port_string.parse::<u16>() else {
            return Err(ProxyError::BadAddressError {
                addr: host.to_string(),
                message: "could not parse port as u16".to_string(),
            });
        };
        let addrs = self.0.lookup_ip(hostname).await?;
        Ok(addrs
            .iter()
            .map(|addr| SocketAddr::new(addr, port))
            .collect())
    }
}

#[derive(Clone)]
pub struct TokioConnector;
impl StreamConnector for TokioConnector {
    type Stream = tokio::net::TcpStream;
    async fn connect(&self, addr: SocketAddr) -> Result<Self::Stream, ProxyError> {
        let stream = tokio::net::TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(stream)
    }
}
