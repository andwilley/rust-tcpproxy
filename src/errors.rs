use std::io::ErrorKind;
use thiserror::Error;

use serde_json::error::Category;

#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProxyError {
    #[error("No target found for backend named {name}")]
    BackendNotFound { name: String },

    #[error("Error ingesting proxy config: {message}")]
    ConfigIngestError { message: String },

    #[error("Error resolving targets for {port}: {message}")]
    TargetResolutionError { port: u16, message: String },

    #[error("Connection error for port {port}: {message}")]
    ConnectionError { port: u16, message: String },

    #[error("Error in Proxy state: {message}")]
    ProxyStateError { message: String },

    #[error("Network I/O error {kind}: {message}")]
    IoError { kind: ErrorKind, message: String },

    #[error("Json parse error {category:?}: {message}")]
    JsonError { category: Category, message: String },

    #[error("A task was cancelled: {message}")]
    TaskCancellation { message: String },
}

impl From<std::io::Error> for ProxyError {
    fn from(e: std::io::Error) -> Self {
        ProxyError::IoError {
            kind: e.kind(),
            message: e.to_string(),
        }
    }
}

impl From<serde_json::Error> for ProxyError {
    fn from(e: serde_json::Error) -> Self {
        ProxyError::JsonError {
            category: e.classify(),
            message: e.to_string(),
        }
    }
}
