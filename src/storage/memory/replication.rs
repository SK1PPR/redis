use super::{MemoryStorage, Replication};

impl Replication for MemoryStorage {
    fn add_replication_client(&mut self, token: mio::Token) {
        self.replication_clients.insert(token);
    }
}
