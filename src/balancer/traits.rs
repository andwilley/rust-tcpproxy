use crate::errors::ProxyError;
use crate::network::traits::{Resolver, StreamConnector};
use std::future::Future;
use std::net::SocketAddr;

pub trait LoadBalancer: Send + Sync {
    type Resolver: Resolver;
    type Connector: StreamConnector;

    fn connect_backend(
        &self,
        for_port: u16,
    ) -> impl Future<
        Output = Result<(<Self::Connector as StreamConnector>::Stream, SocketAddr), ProxyError>,
    > + Send;
}
