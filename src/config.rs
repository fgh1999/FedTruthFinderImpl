use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SecurityPath {
    pub sk_path: String,
    pub pk_path: String,
    pub ca_path: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MasterConfig {
    pub addr: String,
    pub keys: SecurityPath,
    pub group_num: i32,
    pub connection_max_num_per_slave: u64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ClientAddrs {
    // pub domain_name: String,
    pub remote_addr: String,
    pub mailbox_addr: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SlaveConfig {
    pub addrs: ClientAddrs,
    pub error_rate: f64,
    pub keys: SecurityPath,
    pub connection_max_num: u64,
}

enum ConfigPath {
    Server(String),
    Client(String),
}

enum Config {
    Server(MasterConfig),
    Client(SlaveConfig),
}

#[allow(dead_code)]
pub fn load_client_config(path: &str) -> SlaveConfig {
    match load_and_parse_config(ConfigPath::Client(String::from(path))) {
        Config::Client(config) => config,
        _ => panic!(format!("{} does not point to a client config", &path)),
    }
}

#[allow(dead_code)]
pub fn load_server_config(path: &str) -> MasterConfig {
    match load_and_parse_config(ConfigPath::Server(String::from(path))) {
        Config::Server(config) => config,
        _ => panic!(format!("{} does not point to a server config", path)),
    }
}

fn load_and_parse_config(path: ConfigPath) -> Config {
    match path {
        ConfigPath::Client(path) => {
            let file = File::open(path).unwrap();
            let decoded: SlaveConfig = serde_json::from_reader(&file).unwrap();
            Config::Client(decoded)
        }
        ConfigPath::Server(path) => {
            let file = File::open(path).unwrap();
            let decoded: MasterConfig = serde_json::from_reader(&file).unwrap();
            Config::Server(decoded)
        }
    }
}
