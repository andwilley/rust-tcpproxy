use thiserror::Error;

#[derive(Error, Debug)]
pub enum ProxyError {
    #[error("No target found for backend named {name}")]
    BackendNotFound { name: String },

    #[error("Error ingesting proxy config: {message}")]
    ConfigIngestError { message: String },

    #[error("Address {addr} is invalid: {message}")]
    BadAddressError { addr: String, message: String },

    #[error("Error resolving targets for {port}: {message}")]
    TargetResolutionError { port: u16, message: String },

    #[error("Connection error for port {port}: {message}")]
    ConnectionError { port: u16, message: String },

    #[error("Error in Proxy state: {message}")]
    ProxyStateError { message: String },

    #[error("Network I/O error {0}")]
    IoError(#[from] std::io::Error),

    #[error("Json parse error {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("A task was cancelled: {message}")]
    TaskCancellation { message: String },

    #[error("Error acquiring semaphore permit {0}")]
    QueueAcquireError(#[from] tokio::sync::AcquireError),

    #[error("Transient DNS error: {message}")]
    DnsTransientError { message: String },

    #[error("Permanent DNS error: {message}")]
    DnsNxError { message: String },
}

impl From<hickory_resolver::net::NetError> for ProxyError {
    fn from(e: hickory_resolver::net::NetError) -> Self {
        if e.is_nx_domain() || e.is_no_records_found() {
            ProxyError::DnsNxError {
                message: e.to_string(),
            }
        } else {
            ProxyError::DnsTransientError {
                message: e.to_string(),
            }
        }
    }
}
