use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

#[derive(Deserialize, Serialize)]
pub struct RawConfig {
    #[serde(rename = "Apps")]
    pub apps: Vec<App>,
}

#[derive(Deserialize, Serialize)]
pub struct App {
    #[serde(rename = "Name")]
    pub name: String,

    #[serde(rename = "Ports")]
    pub ports: Vec<u16>,

    #[serde(rename = "Targets")]
    pub targets: Vec<String>,
}

impl RawConfig {
    pub fn load_from_file(path: PathBuf) -> anyhow::Result<RawConfig> {
        let content = fs::read_to_string(path)?;
        let config: RawConfig = serde_json::from_str(&content)?;

        Ok(config)
    }
}
