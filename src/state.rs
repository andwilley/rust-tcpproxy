use crate::config::RawConfig;
use crate::errors::ProxyError;
use arc_swap::{ArcSwap, Guard};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

pub struct ProxyState {
    target_pools: Vec<Vec<String>>,
    port_to_pool: HashMap<u16, usize>,
    port_to_rr_counter: HashMap<u16, AtomicUsize>,
    target_status: HashMap<String, ArcSwap<BackendStatus>>,
}

pub enum BackendStatus {
    /// Don't call this backend until the specified Some(time).
    Alive(Option<Instant>),
    Drain,
}

impl ProxyState {
    pub fn get_pool(&self, port: u16) -> Option<&[String]> {
        let idx = *self.port_to_pool.get(&port)?;
        self.target_pools.get(idx).map(|target| target.as_slice())
    }

    pub fn get_rr_counter(&self, port: u16) -> Option<&AtomicUsize> {
        self.port_to_rr_counter.get(&port)
    }

    pub fn ports(&self) -> impl Iterator<Item = u16> {
        self.port_to_pool.keys().copied()
    }

    pub fn get_target_status(&self, target: &str) -> Result<Guard<Arc<BackendStatus>>, ProxyError> {
        self.target_status
            .get(target)
            .map(|status_val| status_val.load())
            .ok_or(ProxyError::BackendNotFound {
                name: target.into(),
            })
    }

    pub fn update_target_status(
        &self,
        target: &str,
        status: BackendStatus,
    ) -> Result<(), ProxyError> {
        let backend_swap: &ArcSwap<BackendStatus> =
            self.target_status
                .get(target)
                .ok_or(ProxyError::BackendNotFound {
                    name: target.into(),
                })?;
        backend_swap.store(Arc::new(status));
        Ok(())
    }
}

impl TryFrom<RawConfig> for ProxyState {
    type Error = ProxyError;

    fn try_from(raw: RawConfig) -> Result<ProxyState, Self::Error> {
        let mut target_pools = Vec::new();
        let mut port_to_pool = HashMap::<u16, usize>::new();
        let mut port_to_rr_counter = HashMap::<u16, AtomicUsize>::new();
        let mut target_status = HashMap::<String, ArcSwap<BackendStatus>>::new();

        for app in raw.apps {
            for target in &app.targets {
                target_status.insert(
                    target.clone(),
                    ArcSwap::from_pointee(BackendStatus::Alive(None)),
                );
            }
            target_pools.push(app.targets);

            for port in app.ports {
                match port_to_pool.entry(port) {
                    Entry::Occupied(_) => {
                        return Err(ProxyError::ConfigIngestError {
                            message: format!("Duplicate port definition {}", port),
                        });
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(target_pools.len());
                        port_to_rr_counter.insert(port, AtomicUsize::new(0));
                    }
                }
            }
        }
        Ok(ProxyState {
            target_pools,
            port_to_pool,
            port_to_rr_counter,
            target_status,
        })
    }
}
