use crate::errors::ProxyError;
use crate::network::traits::{Resolver, StreamConnector};
use crate::state::BackendStatus;
use std::future::Future;
use std::net::SocketAddr;

pub trait LoadBalancer: Send + Sync + 'static {
    type Resolver: Resolver;
    type Connector: StreamConnector;
    type Cooldown: CooldownHandler;

    fn connect_backend(
        &self,
        for_port: u16,
    ) -> impl Future<
        Output = Result<(<Self::Connector as StreamConnector>::Stream, SocketAddr), ProxyError>,
    > + Send;
}

#[derive(PartialEq)]
pub enum ConnectResult {
    Success,
    Failure,
}

pub trait CooldownHandler: Send + Sync + 'static {
    /// Get the BackendStatus for the provided target.
    fn get_target_status(&self, target: &str) -> Result<BackendStatus, ProxyError>;

    /// Report the success or failure to connect to this backend. Only updates the cooldown state,
    /// use `drain` to durably shutoff a backend.
    fn report_connection_attempt(
        &self,
        target: &str,
        result: ConnectResult,
    ) -> Result<(), ProxyError>;

    /// Durably drain traffic from this backend.
    fn drain(&self, target: &str) -> Result<(), ProxyError>;
}
