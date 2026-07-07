use std::{fmt, io::ErrorKind};

use serde_json::error::Category;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyError {
    BackendNotFound { name: String },
    ConfigIngestError { message: String },
    TargetResolutionError { port: u16, message: String },
    ConnectionError { port: u16, message: String },
    ProxyStateError { message: String },
    IoError { kind: ErrorKind, message: String },
    JsonError { category: Category, message: String },
    TaskCancellation { message: String },
}

impl fmt::Display for ProxyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BackendNotFound { name } => {
                write!(f, "No target found for backend named {}", name)
            }
            Self::ConfigIngestError { message } => {
                write!(f, "Error ingesting proxy config: {}", message)
            }
            Self::TargetResolutionError { port, message } => {
                write!(f, "Error resolving targets for {}: {}", port, message)
            }
            Self::ConnectionError { port, message } => {
                write!(f, "Connection error for port {}: {}", port, message)
            }
            Self::ProxyStateError { message } => {
                write!(f, "Error in Proxy state: {}", message)
            }
            Self::IoError { kind, message } => {
                write!(f, "Network I/O error {}: {}", kind, message)
            }
            Self::JsonError { category, message } => {
                write!(f, "Json parse error {:?}: {}", category, message)
            }
            Self::TaskCancellation { message } => {
                write!(f, "A task was cancelled: {}", message)
            }
        }
    }
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
