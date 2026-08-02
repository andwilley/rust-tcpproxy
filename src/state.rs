use crate::config::RawConfig;
use crate::errors::ProxyError;
use arc_swap::{ArcSwap, Guard};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Instant;

pub struct ProxyConfig {
    target_pools: Vec<Vec<String>>,
    port_to_pool: HashMap<u16, usize>,
}

pub struct TargetState {
    target_status: HashMap<String, ArcSwap<BackendStatus>>,
}

#[derive(Clone, Copy)]
pub enum BackendStatus {
    Alive {
        cool_until: Option<Instant>,
    },
    Drain,
}

impl TargetState {
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

impl ProxyConfig {
    pub fn get_pool(&self, port: u16) -> Option<&[String]> {
        let idx = *self.port_to_pool.get(&port)?;
        self.target_pools.get(idx).map(|target| target.as_slice())
    }

    pub fn ports(&self) -> impl Iterator<Item = u16> {
        self.port_to_pool.keys().copied()
    }
}

impl TryFrom<&RawConfig> for TargetState {
    type Error = ProxyError;

    fn try_from(raw: &RawConfig) -> Result<TargetState, Self::Error> {
        let mut target_status = HashMap::<String, ArcSwap<BackendStatus>>::new();

        for app in &raw.apps {
            for target in &app.targets {
                target_status.insert(
                    target.clone(),
                    ArcSwap::from_pointee(BackendStatus::Alive { cool_until: None }),
                );
            }
        }
        Ok(TargetState { target_status })
    }
}

impl TryFrom<&RawConfig> for ProxyConfig {
    type Error = ProxyError;

    fn try_from(raw: &RawConfig) -> Result<ProxyConfig, Self::Error> {
        let mut target_pools = Vec::new();
        let mut port_to_pool = HashMap::<u16, usize>::new();

        for app in &raw.apps {
            target_pools.push(app.targets.clone());

            for port in &app.ports {
                match port_to_pool.entry(*port) {
                    Entry::Occupied(_) => {
                        return Err(ProxyError::ConfigIngestError {
                            message: format!("Duplicate port definition {}", port),
                        });
                    }
                    Entry::Vacant(entry) => {
                        entry.insert(target_pools.len() - 1);
                    }
                }
            }
        }
        Ok(ProxyConfig {
            target_pools,
            port_to_pool,
        })
    }
}
