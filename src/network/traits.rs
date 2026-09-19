use crate::errors::ProxyError;
use std::future::Future;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

pub trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send + 'static {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + 'static> AsyncStream for T {}

pub trait StreamListenerFactory: Send + Sync + Clone {
    type Listener: StreamListener;
    fn bind(
        &self,
        addr: &SocketAddr,
    ) -> impl Future<Output = Result<Self::Listener, ProxyError>> + Send;
}

pub trait StreamListener: Send + Sync {
    type Stream: AsyncStream;

    /// This needs to be cancel safe. Accept may be cancelled in-flight by the select it runs in
    /// to clean up other tasks in the join set. If another task is selected before this one it must
    /// be guaranteed that no connection was accepted.
    fn accept(&self)
    -> impl Future<Output = Result<(Self::Stream, SocketAddr), ProxyError>> + Send;
}

pub trait Resolver: Send + Sync + Clone + 'static {
    fn lookup_host(
        &self,
        host: &str,
    ) -> impl Future<Output = Result<Vec<SocketAddr>, ProxyError>> + Send;
}

pub trait StreamConnector: Send + Sync + Clone + 'static {
    type Stream: AsyncStream;
    fn connect(
        &self,
        addr: SocketAddr,
    ) -> impl Future<Output = Result<Self::Stream, ProxyError>> + Send;
}
