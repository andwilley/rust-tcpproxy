use crate::config::RawConfig;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::RwLock;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

pub struct ProxyState {
    target_pools: Vec<Vec<String>>,
    port_to_pool: HashMap<u16, usize>,
    port_to_rr_counter: HashMap<u16, AtomicUsize>,
    target_status: HashMap<String, RwLock<BackendStatus>>,
}

pub enum BackendStatus {
    /// Don't call this backend until the specified time.
    Alive(Option<Instant>),
    Dead,
}

pub enum TargetStatusUpdateError {
    TargetNotFound,
    Contended,
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

    pub fn get_target_status(&self, target: &str) -> Option<&RwLock<BackendStatus>> {
        self.target_status.get(target)
    }

    pub fn update_target_status(
        &self,
        target: &str,
        status: BackendStatus,
    ) -> Result<(), TargetStatusUpdateError> {
        let lock = self
            .target_status
            .get(target)
            .ok_or(TargetStatusUpdateError::TargetNotFound)?;
        match lock.try_write() {
            Ok(mut target_status) => {
                *target_status = status;
                Ok(())
            }
            Err(std::sync::TryLockError::WouldBlock) => Err(TargetStatusUpdateError::Contended),
            Err(std::sync::TryLockError::Poisoned(_)) => panic!("Lock poisoned for host {target}"),
        }
    }
}

impl TryFrom<RawConfig> for ProxyState {
    type Error = anyhow::Error;

    fn try_from(raw: RawConfig) -> Result<ProxyState, Self::Error> {
        let mut target_pools = Vec::new();
        let mut port_to_pool = HashMap::<u16, usize>::new();
        let mut port_to_rr_counter = HashMap::<u16, AtomicUsize>::new();
        let mut target_status = HashMap::<String, RwLock<BackendStatus>>::new();

        for app in raw.apps {
            for target in &app.targets {
                target_status.insert(target.clone(), RwLock::new(BackendStatus::Alive(None)));
            }
            target_pools.push(app.targets);

            for port in app.ports {
                match port_to_pool.entry(port) {
                    Entry::Occupied(_) => {
                        return Err(anyhow::anyhow!("Duplicate port definition {}", port));
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
