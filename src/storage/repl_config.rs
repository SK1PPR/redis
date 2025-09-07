use rand::distr::{Alphanumeric, SampleString};

#[derive(Debug, Clone)]
pub struct MasterConfig {
    pub host: String,
    pub port: u16,
    pub connected_slaves: usize,
    pub replication_id: String,
    pub replication_offset: u64,
}

impl MasterConfig {
    pub fn new(host: String, port: u16) -> Self {
        MasterConfig {
            host,
            port,
            connected_slaves: 0,
            replication_id: MasterConfig::generate_replication_id(),
            replication_offset: 0,
        }
    }

    fn generate_replication_id() -> String {
        Alphanumeric.sample_string(&mut rand::rng(), 40)
    }

    pub fn to_string(&self) -> String {
        format!(
            "role:master\nconnected_slaves:{}\nmaster_replid:{}\nmaster_repl_offset:{}",
            self.connected_slaves, self.replication_id, self.replication_offset
        )
    }
}

#[derive(Debug, Clone)]
pub struct SlaveConfig {
    pub host: String,
    pub port: u16,
    pub master_host: String,
    pub master_port: u16,
    pub replication_id: String,
    pub replication_offset: u64,
    pub connected: bool,
}

impl SlaveConfig {
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    pub fn new(host: String, port: u16, master_host: String, master_port: u16) -> Self {
        SlaveConfig {
            host,
            port,
            master_host,
            master_port,
            replication_id: MasterConfig::generate_replication_id(),
            replication_offset: 0,
            connected: false,
        }
    }

    pub fn to_string(&self) -> String {
        format!(
            "role:slave\nmaster_host:{}\nmaster_port:{}\nmaster_replid:{}\nmaster_repl_offset:{}\nconnected:{}",
            self.master_host, self.master_port, self.replication_id, self.replication_offset, self.connected
        )
    }
}

#[derive(Debug, Clone)]
pub enum ReplConfig {
    Master(MasterConfig),
    Slave(SlaveConfig),
}

impl ReplConfig {
    pub fn new_master(host: String, port: u16) -> Self {
        ReplConfig::Master(MasterConfig::new(host, port))
    }

    pub fn new_slave(host: String, port: u16, master_host: String, master_port: u16) -> Self {
        ReplConfig::Slave(SlaveConfig::new(host, port, master_host, master_port))
    }

    pub fn get_addr(&self) -> String {
        match self {
            ReplConfig::Master(cfg) => format!("{}:{}", cfg.host, cfg.port),
            ReplConfig::Slave(cfg) => format!("{}:{}", cfg.host, cfg.port),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            ReplConfig::Master(cfg) => cfg.to_string(),
            ReplConfig::Slave(cfg) => cfg.to_string(),
        }
    }

    pub fn is_master(&self) -> bool {
        matches!(self, ReplConfig::Master(_))
    }

    pub fn is_slave(&self) -> bool {
        matches!(self, ReplConfig::Slave(_))
    }

    pub fn get_master_addr(&self) -> Option<String> {
        match self {
            ReplConfig::Slave(cfg) => Some(format!("{}:{}", cfg.master_host, cfg.master_port)),
            _ => None,
        }
    }

    pub fn get_replication_id(&self) -> String {
        match self {
            ReplConfig::Master(cfg) => cfg.replication_id.clone(),
            ReplConfig::Slave(cfg) => cfg.replication_id.clone(),
        }
    }
}
