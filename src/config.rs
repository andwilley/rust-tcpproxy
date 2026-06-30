use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicUsize;
use std::{fs, path::PathBuf};

#[derive(Deserialize, Serialize)]
pub struct RawConfig {
    #[serde(rename = "Apps")]
    apps: Vec<App>,
}

#[derive(Deserialize, Serialize)]
pub struct App {
    #[serde(rename = "Name")]
    name: String,

    #[serde(rename = "Ports")]
    ports: Vec<u16>,

    #[serde(rename = "Targets")]
    targets: Vec<String>,
}

impl RawConfig {
    pub fn load_from_file(path: PathBuf) -> anyhow::Result<RawConfig> {
        let content = fs::read_to_string(path)?;
        let config: RawConfig = serde_json::from_str(&content)?;

        Ok(config)
    }
}

pub struct ProxyConfig {
    target_pools: Vec<Vec<String>>,
    port_to_pool: HashMap<u16, usize>,
    port_to_rr_counter: HashMap<u16, AtomicUsize>,
}

impl ProxyConfig {
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
}

impl TryFrom<RawConfig> for ProxyConfig {
    type Error = anyhow::Error;

    fn try_from(raw: RawConfig) -> Result<ProxyConfig, Self::Error> {
        let mut target_pools = Vec::new();
        let mut port_to_pool = HashMap::<u16, usize>::new();
        let mut port_to_rr_counter = HashMap::<u16, AtomicUsize>::new();

        for app in raw.apps {
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
        Ok(ProxyConfig {
            target_pools,
            port_to_pool,
            port_to_rr_counter,
        })
    }
}
