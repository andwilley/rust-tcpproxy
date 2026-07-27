use crate::{
    errors::ProxyError,
    network::traits::{Resolver, StreamConnector, StreamListener, StreamListenerFactory},
};
use std::net::SocketAddr;

#[derive(Clone)]
pub struct TokioStreamListenerFactory;
impl StreamListenerFactory for TokioStreamListenerFactory {
    type Listener = TokioListener;
    async fn bind(&self, addr: &str) -> Result<Self::Listener, ProxyError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(TokioListener(listener))
    }
}

pub struct TokioListener(pub tokio::net::TcpListener);
impl StreamListener for TokioListener {
    type Stream = tokio::net::TcpStream;
    async fn accept(&self) -> Result<(Self::Stream, SocketAddr), ProxyError> {
        let (stream, addr) = self.0.accept().await?;
        stream.set_nodelay(true)?;
        Ok((stream, addr))
    }
}

#[derive(Clone)]
pub struct TokioResolver;
impl Resolver for TokioResolver {
    async fn lookup_host(
        &self,
        host: &str,
    ) -> Result<impl Iterator<Item = SocketAddr>, ProxyError> {
        let addrs = tokio::net::lookup_host(host).await?;
        Ok(addrs)
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
