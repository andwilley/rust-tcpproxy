use crate::errors::ProxyError;
use std::future::Future;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncStream for T {}

pub trait StreamListenerFactory: Send + Sync + Clone {
    type Listener: StreamListener;
    fn bind(&self, addr: String)
    -> impl Future<Output = Result<Self::Listener, ProxyError>> + Send;
}

pub trait StreamListener: Send + Sync {
    type Stream: AsyncStream;
    fn accept(&self)
    -> impl Future<Output = Result<(Self::Stream, SocketAddr), ProxyError>> + Send;
}

pub trait Resolver: Send + Sync + Clone {
    fn lookup_host(
        &self,
        host: &str,
    ) -> impl Future<Output = Result<impl Iterator<Item = SocketAddr>, ProxyError>> + Send;
}

pub trait StreamConnector: Send + Sync + Clone {
    type Stream: AsyncStream;
    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<Self::Stream, ProxyError>> + Send;
}

#[derive(Clone)]
pub struct TokioStreamListenerFactory;
impl StreamListenerFactory for TokioStreamListenerFactory {
    type Listener = TokioListener;
    async fn bind(&self, addr: String) -> Result<Self::Listener, ProxyError> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(TokioListener(listener))
    }
}

pub struct TokioListener(pub tokio::net::TcpListener);
impl StreamListener for TokioListener {
    type Stream = tokio::net::TcpStream;
    async fn accept(&self) -> Result<(Self::Stream, SocketAddr), ProxyError> {
        self.0.accept().await.map_err(Into::into)
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
        Ok(stream)
    }
}
