use serde::{Deserialize, Serialize};
use std::fs::File;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SecurityPath {
    pub sk_path: String,
    pub pk_path: String,
    pub ca_path: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub addr: String,
    pub keys: SecurityPath,
    pub group_num: i32,
    pub client_num: i32,
    pub error_rate: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ClientAddrs {
    // pub domain_name: String,
    pub remote_addr: String,
    pub mailbox_addr: String,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub addrs: ClientAddrs,
    pub keys: SecurityPath,
    pub event_file_path: String,
}

enum ConfigPath {
    Server(String),
    Client(String),
}

enum Config {
    Server(ServerConfig),
    Client(ClientConfig),
}

#[allow(dead_code)]
pub fn load_client_config(path: &str) -> ClientConfig {
    match load_and_parse_config(ConfigPath::Client(String::from(path))) {
        Config::Client(config) => config,
        _ => panic!("{} does not point to a client config", &path),
    }
}

#[allow(dead_code)]
pub fn load_server_config(path: &str) -> ServerConfig {
    match load_and_parse_config(ConfigPath::Server(String::from(path))) {
        Config::Server(config) => config,
        _ => panic!("{} does not point to a server config", path),
    }
}

fn load_and_parse_config(path: ConfigPath) -> Config {
    match path {
        ConfigPath::Client(path) => {
            let file = File::open(path).unwrap();
            let decoded: ClientConfig = serde_json::from_reader(&file).unwrap();
            Config::Client(decoded)
        }
        ConfigPath::Server(path) => {
            let file = File::open(path).unwrap();
            let decoded: ServerConfig = serde_json::from_reader(&file).unwrap();
            Config::Server(decoded)
        }
    }
}
